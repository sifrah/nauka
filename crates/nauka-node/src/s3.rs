//! The S3 endpoint.
//!
//! `s3s` owns the protocol — SigV4, XML, routing, the 99-operation service
//! trait — and we implement the operations against the Nauka engine:
//! objects become erasure-coded manifests, the bucket/key index and the
//! credentials live in the Raft registry, so every node is a complete
//! endpoint and any of them can serve any request.
//!
//! Operations we have not implemented yet inherit the trait's default,
//! which answers a correct `NotImplemented` S3 error. That is deliberate:
//! a conformant refusal is a valid answer, a silent wrong one is not.

use std::collections::BTreeMap;
use std::sync::Arc;

use s3s::auth::{S3Auth, SecretKey};
use s3s::dto::*;
use s3s::{s3_error, Body, S3Request, S3Response, S3Result, S3};

use crate::api::ApiState;

/// Bridges SigV4 to the replicated credential store. `s3s` verifies the
/// signature itself; we only supply the secret for an access key, so the
/// secret never travels and a wrong signature never reaches an operation.
pub struct NaukaAuth {
    state: Arc<ApiState>,
}

#[async_trait::async_trait]
impl S3Auth for NaukaAuth {
    async fn get_secret_key(&self, access_key: &str) -> S3Result<SecretKey> {
        match self.state.app.app_state().s3.credentials.get(access_key) {
            Some(c) => Ok(SecretKey::from(c.secret_access_key.clone())),
            None => Err(s3_error!(InvalidAccessKeyId)),
        }
    }
}

pub struct NaukaS3 {
    state: Arc<ApiState>,
}

impl NaukaS3 {
    pub fn new(state: Arc<ApiState>) -> Self {
        Self { state }
    }

    fn now() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }

    fn timestamp(secs: u64) -> Timestamp {
        Timestamp::from(std::time::UNIX_EPOCH + std::time::Duration::from_secs(secs))
    }

    /// Writes a command through Raft, mapping a refusal to an S3 error.
    async fn write(&self, cmd: nauka_raft::types::AppCommand) -> S3Result<Option<String>> {
        match self.state.app.write(cmd).await {
            Ok(r) if r.ok => Ok(r.info),
            Ok(r) => Err(s3_error!(
                InternalError,
                "{}",
                r.info.unwrap_or_else(|| "refused".into())
            )),
            Err(e) => Err(s3_error!(InternalError, "{e:#}")),
        }
    }

    fn require_bucket(&self, name: &str) -> S3Result<nauka_s3::Bucket> {
        self.state
            .app
            .app_state()
            .s3
            .buckets
            .get(name)
            .cloned()
            .ok_or_else(|| s3_error!(NoSuchBucket))
    }
}

#[async_trait::async_trait]
impl S3 for NaukaS3 {
    async fn create_bucket(
        &self,
        req: S3Request<CreateBucketInput>,
    ) -> S3Result<S3Response<CreateBucketOutput>> {
        let name = req.input.bucket;
        if !nauka_s3::naming::valid_bucket_name(&name) {
            return Err(s3_error!(InvalidBucketName));
        }
        if self.state.app.app_state().s3.buckets.contains_key(&name) {
            // AWS answers BucketAlreadyOwnedByYou to the owner, and
            // BucketAlreadyExists to anyone else. Single-tenant clusters
            // only ever hit the first.
            return Err(s3_error!(BucketAlreadyOwnedByYou));
        }
        let bucket = nauka_s3::Bucket {
            created_at: Self::now(),
            owner: req.credentials.map(|c| c.access_key).unwrap_or_default(),
            object_lock_enabled: req.input.object_lock_enabled_for_bucket.unwrap_or(false),
            ..Default::default()
        };
        self.write(nauka_raft::types::AppCommand::CreateBucket {
            name: name.clone(),
            bucket: Box::new(bucket),
        })
        .await?;
        Ok(S3Response::new(CreateBucketOutput {
            location: Some(format!("/{name}")),
        }))
    }

    async fn head_bucket(
        &self,
        req: S3Request<HeadBucketInput>,
    ) -> S3Result<S3Response<HeadBucketOutput>> {
        self.require_bucket(&req.input.bucket)?;
        Ok(S3Response::new(HeadBucketOutput::default()))
    }

    async fn delete_bucket(
        &self,
        req: S3Request<DeleteBucketInput>,
    ) -> S3Result<S3Response<DeleteBucketOutput>> {
        self.require_bucket(&req.input.bucket)?;
        // The emptiness check lives in the state machine, where the log
        // serializes it: checking here would race with a concurrent PUT.
        match self
            .state
            .app
            .write(nauka_raft::types::AppCommand::DeleteBucket {
                name: req.input.bucket,
            })
            .await
        {
            Ok(r) if r.ok => Ok(S3Response::new(DeleteBucketOutput::default())),
            Ok(_) => Err(s3_error!(BucketNotEmpty)),
            Err(e) => Err(s3_error!(InternalError, "{e:#}")),
        }
    }

    async fn list_buckets(
        &self,
        req: S3Request<ListBucketsInput>,
    ) -> S3Result<S3Response<ListBucketsOutput>> {
        let s3 = self.state.app.app_state().s3;
        let access_key = req.credentials.map(|c| c.access_key).unwrap_or_default();
        let visible = match s3.credentials.get(&access_key) {
            Some(c) => c.visible_buckets(s3.buckets.keys()),
            None => s3.buckets.keys().cloned().collect(),
        };
        let buckets: Vec<Bucket> = visible
            .into_iter()
            .filter_map(|name| {
                let b = s3.buckets.get(&name)?;
                Some(Bucket {
                    name: Some(name.clone()),
                    creation_date: Some(Self::timestamp(b.created_at)),
                    ..Default::default()
                })
            })
            .collect();
        Ok(S3Response::new(ListBucketsOutput {
            buckets: Some(buckets),
            owner: Some(Owner {
                display_name: Some("nauka".into()),
                id: Some(access_key),
            }),
            ..Default::default()
        }))
    }

    async fn put_object(
        &self,
        req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        let input = req.input;
        self.require_bucket(&input.bucket)?;
        if !nauka_s3::naming::valid_key(&input.key) {
            return Err(s3_error!(InvalidArgument, "invalid key"));
        }

        // Buffer to disk while hashing twice: BLAKE3 addresses the content
        // for the engine, MD5 becomes the ETag the client expects.
        let tmp = self.state.tmp_dir.join(format!("s3-{}", uuid_like()));
        let (size, blake, md5) = write_body(input.body, &tmp).await.map_err(|e| {
            let _ = std::fs::remove_file(&tmp);
            s3_error!(InternalError, "{e:#}")
        })?;

        // An empty object has no shards to place — it is pure metadata.
        let content = if size == 0 {
            let _ = tokio::fs::remove_file(&tmp).await;
            None
        } else {
            let result = crate::api::dispatch_file(
                &self.state,
                &tmp,
                size,
                blake,
                Some(input.key.clone()),
                None,
            )
            .await;
            let _ = tokio::fs::remove_file(&tmp).await;
            let (manifest, _degraded) = result.map_err(|e| s3_error!(InternalError, "{e:#}"))?;
            Some(manifest.file_hash)
        };

        let etag = nauka_s3::naming::etag_single(&md5);
        let version = nauka_s3::ObjectVersion {
            version_id: "null".into(),
            content,
            size,
            etag: etag.clone(),
            last_modified: Self::now(),
            content_type: input.content_type.map(|v| v.to_string()),
            user_metadata: input
                .metadata
                .map(|m| m.into_iter().collect())
                .unwrap_or_default(),
            system_metadata: BTreeMap::new(),
            storage_class: input.storage_class.map(|s| s.as_str().to_owned()),
            tags: BTreeMap::new(),
            checksums: BTreeMap::new(),
            retention: None,
            legal_hold: false,
            sse: None,
        };
        self.write(nauka_raft::types::AppCommand::PutObjectVersion {
            bucket: input.bucket,
            key: input.key,
            version: Box::new(version),
        })
        .await?;

        Ok(S3Response::new(PutObjectOutput {
            e_tag: etag.parse().ok(),
            ..Default::default()
        }))
    }

    async fn head_object(
        &self,
        req: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        self.require_bucket(&req.input.bucket)?;
        let s3 = self.state.app.app_state().s3;
        let entry = s3
            .objects
            .get(&(req.input.bucket, req.input.key))
            .ok_or_else(|| s3_error!(NoSuchKey))?;
        let v = entry
            .current_content()
            .ok_or_else(|| s3_error!(NoSuchKey))?;
        Ok(S3Response::new(HeadObjectOutput {
            content_length: Some(v.size as i64),
            e_tag: v.etag.parse().ok(),
            last_modified: Some(Self::timestamp(v.last_modified)),
            content_type: v.content_type.clone(),
            metadata: Some(v.user_metadata.clone().into_iter().collect()),
            ..Default::default()
        }))
    }

    async fn get_object(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        let input = req.input;
        self.require_bucket(&input.bucket)?;
        let s3 = self.state.app.app_state().s3;
        let entry = s3
            .objects
            .get(&(input.bucket.clone(), input.key.clone()))
            .ok_or_else(|| s3_error!(NoSuchKey))?;
        let v = entry
            .current_content()
            .cloned()
            .ok_or_else(|| s3_error!(NoSuchKey))?;

        check_preconditions(
            &v,
            input.if_match.as_ref(),
            input.if_none_match.as_ref(),
            input.if_modified_since.as_ref(),
            input.if_unmodified_since.as_ref(),
        )?;

        // A Range narrows what we reconstruct: only the stripes covering
        // the window are fetched from the cluster.
        let (start, end) = match &input.range {
            None => (0, v.size.saturating_sub(1)),
            Some(r) => resolve_range(r, v.size)?,
        };
        let length = if v.size == 0 { 0 } else { end - start + 1 };
        let partial = input.range.is_some();

        let body = match &v.content {
            None => StreamingBlob::from(Body::from(Vec::<u8>::new())),
            Some(hash) => {
                let manifest = self
                    .state
                    .app
                    .app_state()
                    .manifests
                    .get(hash)
                    .cloned()
                    .ok_or_else(|| s3_error!(NoSuchKey))?;
                let bytes = reconstruct_range(&self.state, &manifest, start, end)
                    .await
                    .map_err(|e| s3_error!(InternalError, "{e:#}"))?;
                StreamingBlob::from(Body::from(bytes))
            }
        };

        Ok(S3Response::new(GetObjectOutput {
            body: Some(body),
            content_length: Some(length as i64),
            content_range: partial.then(|| format!("bytes {start}-{end}/{}", v.size)),
            e_tag: v.etag.parse().ok(),
            last_modified: Some(Self::timestamp(v.last_modified)),
            content_type: v.content_type.clone(),
            metadata: Some(v.user_metadata.clone().into_iter().collect()),
            ..Default::default()
        }))
    }

    async fn copy_object(
        &self,
        req: S3Request<CopyObjectInput>,
    ) -> S3Result<S3Response<CopyObjectOutput>> {
        let input = req.input;
        self.require_bucket(&input.bucket)?;
        let (src_bucket, src_key) = match &input.copy_source {
            CopySource::Bucket { bucket, key, .. } => (bucket.to_string(), key.to_string()),
            // Access points and Outposts are AWS-side routing concepts
            // with no meaning in a self-hosted cluster.
            CopySource::AccessPoint { .. } | CopySource::Outpost { .. } => {
                return Err(s3_error!(NotImplemented, "access point copy sources"))
            }
        };
        let s3 = self.state.app.app_state().s3;
        let source = s3
            .objects
            .get(&(src_bucket, src_key))
            .and_then(|e| e.current_content())
            .cloned()
            .ok_or_else(|| s3_error!(NoSuchKey))?;

        check_preconditions(
            &source,
            input.copy_source_if_match.as_ref(),
            input.copy_source_if_none_match.as_ref(),
            input.copy_source_if_modified_since.as_ref(),
            input.copy_source_if_unmodified_since.as_ref(),
        )?;

        // The copy shares the source's shards: content addressing makes
        // this free, and the derived refcount keeps both alive until the
        // last key referencing them goes.
        // COPY (the default) keeps the source metadata; REPLACE takes it
        // from the request.
        let replace = input
            .metadata_directive
            .as_ref()
            .is_some_and(|d| d.as_str() == MetadataDirective::REPLACE);
        let now = Self::now();
        let copy = nauka_s3::ObjectVersion {
            version_id: "null".into(),
            last_modified: now,
            content_type: if replace {
                input.content_type.clone()
            } else {
                source.content_type.clone()
            },
            user_metadata: if replace {
                input
                    .metadata
                    .clone()
                    .map(|m| m.into_iter().collect())
                    .unwrap_or_default()
            } else {
                source.user_metadata.clone()
            },
            ..source.clone()
        };
        let etag = copy.etag.clone();
        self.write(nauka_raft::types::AppCommand::PutObjectVersion {
            bucket: input.bucket,
            key: input.key,
            version: Box::new(copy),
        })
        .await?;

        Ok(S3Response::new(CopyObjectOutput {
            copy_object_result: Some(CopyObjectResult {
                e_tag: etag.parse().ok(),
                last_modified: Some(Self::timestamp(now)),
                ..Default::default()
            }),
            ..Default::default()
        }))
    }

    // ── Multipart ─────────────────────────────────────────────────────
    // Not an optional feature: `aws s3 cp` switches to multipart above
    // 8 MB and rclone above 200 MB, so without it half the ecosystem
    // breaks on large files. Each part is an ordinary erasure-coded
    // manifest; Complete stitches their references together, which makes
    // completing a 100 GB upload a metadata operation.

    async fn create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        let input = req.input;
        self.require_bucket(&input.bucket)?;
        if !nauka_s3::naming::valid_key(&input.key) {
            return Err(s3_error!(InvalidArgument, "invalid key"));
        }
        let upload_id = uuid_like();
        let upload = nauka_s3::MultipartUpload {
            upload_id: upload_id.clone(),
            bucket: input.bucket.clone(),
            key: input.key.clone(),
            initiated: Self::now(),
            owner: req.credentials.map(|c| c.access_key).unwrap_or_default(),
            content_type: input.content_type.clone(),
            user_metadata: input
                .metadata
                .map(|m| m.into_iter().collect())
                .unwrap_or_default(),
            system_metadata: BTreeMap::new(),
            storage_class: input.storage_class.map(|s| s.as_str().to_owned()),
            tags: BTreeMap::new(),
            sse: None,
            parts: BTreeMap::new(),
        };
        self.write(nauka_raft::types::AppCommand::PutUpload(Box::new(upload)))
            .await?;
        Ok(S3Response::new(CreateMultipartUploadOutput {
            bucket: Some(input.bucket),
            key: Some(input.key),
            upload_id: Some(upload_id),
            ..Default::default()
        }))
    }

    async fn upload_part(
        &self,
        req: S3Request<UploadPartInput>,
    ) -> S3Result<S3Response<UploadPartOutput>> {
        let input = req.input;
        if !self
            .state
            .app
            .app_state()
            .s3
            .uploads
            .contains_key(&input.upload_id)
        {
            return Err(s3_error!(NoSuchUpload));
        }
        let part_number = u32::try_from(input.part_number)
            .ok()
            .filter(|n| (1..=10_000).contains(n))
            .ok_or_else(|| s3_error!(InvalidArgument, "part number must be 1..=10000"))?;

        let tmp = self.state.tmp_dir.join(format!("s3p-{}", uuid_like()));
        let (size, blake, md5) = write_body(input.body, &tmp).await.map_err(|e| {
            let _ = std::fs::remove_file(&tmp);
            s3_error!(InternalError, "{e:#}")
        })?;
        if size == 0 {
            let _ = tokio::fs::remove_file(&tmp).await;
            return Err(s3_error!(InvalidArgument, "an empty part is not allowed"));
        }
        let result = crate::api::dispatch_file(
            &self.state,
            &tmp,
            size,
            blake,
            Some(format!("{}#part{}", input.key, part_number)),
            None,
        )
        .await;
        let _ = tokio::fs::remove_file(&tmp).await;
        let (manifest, _) = result.map_err(|e| s3_error!(InternalError, "{e:#}"))?;

        let etag = nauka_s3::naming::etag_single(&md5);
        // One part at a time: parts arrive concurrently, so the merge
        // belongs in the state machine. Re-uploading a part replaces it,
        // as S3 allows.
        self.write(nauka_raft::types::AppCommand::PutUploadPart {
            upload_id: input.upload_id.clone(),
            part_number,
            part: Box::new(nauka_s3::UploadedPart {
                content: manifest.file_hash,
                size,
                etag: etag.clone(),
                last_modified: Self::now(),
                checksums: BTreeMap::new(),
            }),
        })
        .await?;
        Ok(S3Response::new(UploadPartOutput {
            e_tag: etag.parse().ok(),
            ..Default::default()
        }))
    }

    async fn complete_multipart_upload(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        let input = req.input;
        let upload = self
            .state
            .app
            .app_state()
            .s3
            .uploads
            .get(&input.upload_id)
            .cloned()
            .ok_or_else(|| s3_error!(NoSuchUpload))?;
        let requested = input
            .multipart_upload
            .and_then(|m| m.parts)
            .unwrap_or_default();
        if requested.is_empty() {
            return Err(s3_error!(InvalidRequest, "no parts listed"));
        }

        // The client re-sends the part list. Validate its STRUCTURE first —
        // order, existence, ETags — and only then the size rule. Checking
        // sizes inline lets a small early part mask a missing or
        // misordered later one, so a client debugging its part list gets
        // EntityTooSmall where AWS says InvalidPart.
        let mut previous = 0i32;
        let mut chosen = Vec::with_capacity(requested.len());
        for part in requested.iter() {
            let number = part.part_number.ok_or_else(|| s3_error!(InvalidPart))?;
            if number <= previous {
                return Err(s3_error!(InvalidPartOrder));
            }
            previous = number;
            let stored = u32::try_from(number)
                .ok()
                .and_then(|n| upload.parts.get(&n))
                .ok_or_else(|| s3_error!(InvalidPart))?;
            if let Some(claimed) = &part.e_tag {
                let ours: ETag = stored
                    .etag
                    .parse()
                    .map_err(|_| s3_error!(InternalError, "bad stored etag"))?;
                if !ours.strong_cmp(claimed) {
                    return Err(s3_error!(InvalidPart));
                }
            }
            chosen.push(stored.clone());
        }
        // Every part but the last must be at least 5 MiB — the rule that
        // keeps multipart ETags reproducible across clients.
        const MIN_PART: u64 = 5 * 1024 * 1024;
        if chosen
            .iter()
            .take(chosen.len().saturating_sub(1))
            .any(|p| p.size < MIN_PART)
        {
            return Err(s3_error!(EntityTooSmall));
        }

        // The multipart ETag: MD5 over the concatenated BINARY part
        // digests, suffixed with the count.
        let digests: Vec<[u8; 16]> = chosen
            .iter()
            .filter_map(|p| nauka_s3::naming::md5_from_etag(&p.etag))
            .collect();
        if digests.len() != chosen.len() {
            return Err(s3_error!(InternalError, "unreadable part etag"));
        }
        let etag = nauka_s3::naming::etag_multipart(&digests);
        let total: u64 = chosen.iter().map(|p| p.size).sum();

        // Stitching the parts into one manifest is metadata only: the
        // shards are already placed, so completing a huge upload costs a
        // single Raft write, not a re-upload.
        let mut stripes = Vec::new();
        let state = self.state.app.app_state();
        let mut config = None;
        for part in &chosen {
            let m = state
                .manifests
                .get(&part.content)
                .ok_or_else(|| s3_error!(InternalError, "a part vanished before completion"))?;
            config.get_or_insert(m.config);
            stripes.extend(m.stripes.iter().cloned());
        }
        let joined = nauka_erasure::FileManifest {
            // The assembled object is addressed by the parts it is made
            // of, not by a hash of bytes we never held in one place.
            file_hash: multipart_content_hash(&chosen),
            file_size: total,
            name: Some(upload.key.clone()),
            expires_at: None,
            config: config.unwrap_or(self.state.config),
            stripes,
        };
        let content_hash = joined.file_hash.clone();
        self.state
            .store
            .put_manifest(&joined)
            .map_err(|e| s3_error!(InternalError, "{e}"))?;
        self.write(nauka_raft::types::AppCommand::RegisterManifest(joined))
            .await?;

        let version = nauka_s3::ObjectVersion {
            version_id: "null".into(),
            content: Some(content_hash),
            size: total,
            etag: etag.clone(),
            last_modified: Self::now(),
            content_type: upload.content_type.clone(),
            user_metadata: upload.user_metadata.clone(),
            system_metadata: upload.system_metadata.clone(),
            storage_class: upload.storage_class.clone(),
            tags: upload.tags.clone(),
            checksums: BTreeMap::new(),
            retention: None,
            legal_hold: false,
            sse: upload.sse.clone(),
        };
        self.write(nauka_raft::types::AppCommand::PutObjectVersion {
            bucket: upload.bucket.clone(),
            key: upload.key.clone(),
            version: Box::new(version),
        })
        .await?;
        // Only now does the upload go: while it existed, its parts held
        // references that kept the GC away from their shards.
        self.write(nauka_raft::types::AppCommand::DeleteUpload {
            upload_id: input.upload_id,
        })
        .await?;

        Ok(S3Response::new(CompleteMultipartUploadOutput {
            bucket: Some(upload.bucket),
            key: Some(upload.key),
            e_tag: etag.parse().ok(),
            ..Default::default()
        }))
    }

    async fn abort_multipart_upload(
        &self,
        req: S3Request<AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
        match self
            .state
            .app
            .write(nauka_raft::types::AppCommand::DeleteUpload {
                upload_id: req.input.upload_id,
            })
            .await
        {
            Ok(r) if r.ok => Ok(S3Response::new(AbortMultipartUploadOutput::default())),
            Ok(_) => Err(s3_error!(NoSuchUpload)),
            Err(e) => Err(s3_error!(InternalError, "{e:#}")),
        }
    }

    async fn list_parts(
        &self,
        req: S3Request<ListPartsInput>,
    ) -> S3Result<S3Response<ListPartsOutput>> {
        let upload = self
            .state
            .app
            .app_state()
            .s3
            .uploads
            .get(&req.input.upload_id)
            .cloned()
            .ok_or_else(|| s3_error!(NoSuchUpload))?;
        let after = req.input.part_number_marker.unwrap_or(0);
        let max = req.input.max_parts.unwrap_or(1000).clamp(0, 1000) as usize;
        let mut parts: Vec<Part> = Vec::new();
        let mut truncated = false;
        for (n, p) in upload.parts.iter().filter(|(n, _)| **n as i32 > after) {
            if parts.len() >= max {
                truncated = true;
                break;
            }
            parts.push(Part {
                part_number: Some(*n as i32),
                size: Some(p.size as i64),
                e_tag: p.etag.parse().ok(),
                last_modified: Some(Self::timestamp(p.last_modified)),
                ..Default::default()
            });
        }
        Ok(S3Response::new(ListPartsOutput {
            bucket: Some(upload.bucket),
            key: Some(upload.key),
            upload_id: Some(upload.upload_id),
            parts: Some(parts),
            max_parts: Some(max as i32),
            is_truncated: Some(truncated),
            ..Default::default()
        }))
    }

    async fn list_multipart_uploads(
        &self,
        req: S3Request<ListMultipartUploadsInput>,
    ) -> S3Result<S3Response<ListMultipartUploadsOutput>> {
        let bucket = req.input.bucket;
        self.require_bucket(&bucket)?;
        let prefix = req.input.prefix.clone().unwrap_or_default();
        let uploads: Vec<MultipartUpload> = self
            .state
            .app
            .app_state()
            .s3
            .uploads
            .values()
            .filter(|u| u.bucket == bucket && u.key.starts_with(&prefix))
            .map(|u| MultipartUpload {
                upload_id: Some(u.upload_id.clone()),
                key: Some(u.key.clone()),
                initiated: Some(Self::timestamp(u.initiated)),
                ..Default::default()
            })
            .collect();
        Ok(S3Response::new(ListMultipartUploadsOutput {
            bucket: Some(bucket),
            prefix: Some(prefix),
            uploads: Some(uploads),
            is_truncated: Some(false),
            ..Default::default()
        }))
    }

    async fn delete_objects(
        &self,
        req: S3Request<DeleteObjectsInput>,
    ) -> S3Result<S3Response<DeleteObjectsOutput>> {
        let bucket = req.input.bucket;
        self.require_bucket(&bucket)?;
        let mut deleted = Vec::new();
        let mut errors = Vec::new();
        for obj in req.input.delete.objects {
            // S3 reports per-object outcomes rather than failing the batch,
            // and deleting an absent key counts as success.
            match self
                .state
                .app
                .write(nauka_raft::types::AppCommand::DeleteObjectVersion {
                    bucket: bucket.clone(),
                    key: obj.key.clone(),
                    version_id: obj.version_id.clone().unwrap_or_else(|| "null".into()),
                })
                .await
            {
                Ok(_) => deleted.push(DeletedObject {
                    key: Some(obj.key),
                    version_id: obj.version_id,
                    ..Default::default()
                }),
                Err(e) => errors.push(s3s::dto::Error {
                    key: Some(obj.key),
                    code: Some("InternalError".into()),
                    message: Some(format!("{e:#}")),
                    version_id: obj.version_id,
                }),
            }
        }
        Ok(S3Response::new(DeleteObjectsOutput {
            deleted: Some(deleted),
            errors: Some(errors),
            ..Default::default()
        }))
    }

    async fn delete_object(
        &self,
        req: S3Request<DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        self.require_bucket(&req.input.bucket)?;
        // S3 deletes are idempotent: removing an absent key is a success.
        let _ = self
            .state
            .app
            .write(nauka_raft::types::AppCommand::DeleteObjectVersion {
                bucket: req.input.bucket,
                key: req.input.key,
                version_id: "null".into(),
            })
            .await;
        Ok(S3Response::new(DeleteObjectOutput::default()))
    }

    async fn list_objects_v2(
        &self,
        req: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        let bucket = req.input.bucket;
        self.require_bucket(&bucket)?;
        let s3 = self.state.app.app_state().s3;
        let prefix = req.input.prefix.clone().unwrap_or_default();
        let delimiter = req.input.delimiter.clone();
        let start_after = req
            .input
            .continuation_token
            .clone()
            .or_else(|| req.input.start_after.clone());
        let max_keys = req.input.max_keys.unwrap_or(1000).clamp(0, 1000) as usize;

        let mut contents: Vec<Object> = Vec::new();
        let mut prefixes: std::collections::BTreeSet<String> = Default::default();
        let mut truncated = false;
        let mut next_token = None;

        for ((b, key), entry) in s3.objects.range((bucket.clone(), String::new())..) {
            if *b != bucket {
                break;
            }
            if !key.starts_with(&prefix) {
                continue;
            }
            if start_after.as_ref().is_some_and(|s| key <= s) {
                continue;
            }
            let Some(v) = entry.current_content() else {
                continue;
            };
            // A delimiter rolls everything below it into a common prefix,
            // which is how S3 fakes directories.
            if let Some(d) = &delimiter {
                if let Some(idx) = key[prefix.len()..].find(d.as_str()) {
                    prefixes.insert(key[..prefix.len() + idx + d.len()].to_string());
                    continue;
                }
            }
            if contents.len() >= max_keys {
                truncated = true;
                next_token = Some(key.clone());
                break;
            }
            contents.push(Object {
                key: Some(key.clone()),
                size: Some(v.size as i64),
                e_tag: v.etag.parse().ok(),
                last_modified: Some(Self::timestamp(v.last_modified)),
                storage_class: v
                    .storage_class
                    .clone()
                    .map(ObjectStorageClass::from)
                    .or(Some(ObjectStorageClass::from_static(
                        ObjectStorageClass::STANDARD,
                    ))),
                ..Default::default()
            });
        }

        Ok(S3Response::new(ListObjectsV2Output {
            key_count: Some(contents.len() as i32),
            max_keys: Some(max_keys as i32),
            contents: Some(contents),
            common_prefixes: Some(
                prefixes
                    .into_iter()
                    .map(|p| CommonPrefix { prefix: Some(p) })
                    .collect(),
            ),
            name: Some(bucket),
            prefix: Some(prefix),
            delimiter,
            is_truncated: Some(truncated),
            next_continuation_token: next_token,
            ..Default::default()
        }))
    }
}

/// Streams a request body to `path`, returning its size and both digests.
async fn write_body(
    body: Option<StreamingBlob>,
    path: &std::path::Path,
) -> anyhow::Result<(u64, blake3::Hasher, [u8; 16])> {
    use md5::Digest;
    use tokio::io::AsyncWriteExt;

    let mut file = tokio::fs::File::create(path).await?;
    let mut blake = blake3::Hasher::new();
    let mut md5 = md5::Md5::new();
    let mut size = 0u64;

    if let Some(body) = body {
        use futures::StreamExt;
        let mut stream = body;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|e| anyhow::anyhow!("reading the body: {e}"))?;
            blake.update(&chunk);
            md5.update(&chunk);
            file.write_all(&chunk).await?;
            size += chunk.len() as u64;
        }
    }
    file.flush().await?;
    Ok((size, blake, md5.finalize().into()))
}

fn uuid_like() -> String {
    use rand::Rng;
    let mut b = [0u8; 16];
    rand::thread_rng().fill(&mut b);
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// Builds the S3 HTTP service: SigV4 against the replicated credentials,
/// operations against the Nauka engine.
pub fn service(state: Arc<ApiState>) -> s3s::service::S3Service {
    let mut builder = s3s::service::S3ServiceBuilder::new(NaukaS3::new(state.clone()));
    builder.set_auth(NaukaAuth { state });
    builder.build()
}

/// Serves the S3 endpoint until the process stops.
pub async fn serve(listen: std::net::SocketAddr, state: Arc<ApiState>) -> anyhow::Result<()> {
    let service = service(state);
    let listener = tokio::net::TcpListener::bind(listen).await?;
    tracing::info!("S3 endpoint on http://{listen}");
    // S3Service implements hyper's Service directly, so it is shared as-is.
    let service = Arc::new(service);
    loop {
        let (stream, _) = listener.accept().await?;
        let svc = service.clone();
        tokio::spawn(async move {
            let io = hyper_util::rt::TokioIo::new(stream);
            if let Err(e) = hyper::server::conn::http1::Builder::new()
                .serve_connection(io, ServiceRef(svc))
                .await
            {
                tracing::debug!("S3 connection ended: {e}");
            }
        });
    }
}

/// `hyper` wants a `Service` by value; the S3 service is shared behind an
/// `Arc`, so this hands out a cheap per-connection handle.
#[derive(Clone)]
struct ServiceRef(Arc<s3s::service::S3Service>);

impl hyper::service::Service<hyper::Request<hyper::body::Incoming>> for ServiceRef {
    type Response = hyper::Response<s3s::Body>;
    type Error = s3s::HttpError;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;

    fn call(&self, req: hyper::Request<hyper::body::Incoming>) -> Self::Future {
        let svc = self.0.clone();
        Box::pin(async move { hyper::service::Service::call(svc.as_ref(), req).await })
    }
}

/// Applies the conditional headers S3 defines on reads and copies.
///
/// The order is imposed by RFC 9110 and checked by the conformance suite:
/// `If-Match` first (412 on mismatch), then `If-None-Match` (304 — or 412
/// on a copy source), then the date conditions.
fn check_preconditions(
    v: &nauka_s3::ObjectVersion,
    if_match: Option<&ETagCondition>,
    if_none_match: Option<&ETagCondition>,
    if_modified_since: Option<&Timestamp>,
    if_unmodified_since: Option<&Timestamp>,
) -> S3Result<()> {
    // `s3s` has already parsed the header into a structured condition, so
    // "*" and quoting are handled for us.
    let matches = |cond: &ETagCondition| match cond {
        ETagCondition::Any => true,
        // strong_cmp is the RFC 9110 comparison: a weak ETag never
        // satisfies If-Match.
        ETagCondition::ETag(tag) => v
            .etag
            .parse::<ETag>()
            .is_ok_and(|ours| ours.strong_cmp(tag)),
    };
    if let Some(want) = if_match {
        if !matches(want) {
            return Err(s3_error!(PreconditionFailed));
        }
    }
    if let Some(reject) = if_none_match {
        if matches(reject) {
            return Err(s3_error!(NotModified));
        }
    }
    if let Some(since) = if_unmodified_since {
        if timestamp_secs(since).is_some_and(|s| v.last_modified > s) {
            return Err(s3_error!(PreconditionFailed));
        }
    }
    if let Some(since) = if_modified_since {
        if timestamp_secs(since).is_some_and(|s| v.last_modified <= s) {
            return Err(s3_error!(NotModified));
        }
    }
    Ok(())
}

fn timestamp_secs(t: &Timestamp) -> Option<u64> {
    let odt: time::OffsetDateTime = t.clone().into();
    u64::try_from(odt.unix_timestamp()).ok()
}

/// Resolves an S3 range into inclusive byte offsets, or the 416 S3 returns
/// for a window that starts past the end.
fn resolve_range(range: &Range, size: u64) -> S3Result<(u64, u64)> {
    let (start, end) = match *range {
        Range::Int { first, last } => {
            if first >= size {
                return Err(s3_error!(InvalidRange));
            }
            (first, last.unwrap_or(size - 1).min(size - 1))
        }
        // "bytes=-N": the LAST n bytes.
        Range::Suffix { length } => {
            if length == 0 {
                return Err(s3_error!(InvalidRange));
            }
            (size.saturating_sub(length), size - 1)
        }
    };
    if start > end {
        return Err(s3_error!(InvalidRange));
    }
    Ok((start, end))
}

/// Rebuilds `[start, end]` of an object, fetching only the stripes that
/// cover it — a range read of a 1 GB object touches a few megabytes.
async fn reconstruct_range(
    state: &Arc<ApiState>,
    manifest: &nauka_erasure::FileManifest,
    start: u64,
    end: u64,
) -> anyhow::Result<Vec<u8>> {
    let fetcher = Arc::new(crate::api::Fetcher::new(state.clone()));
    let mut out = Vec::with_capacity((end - start + 1) as usize);
    let mut offset = 0u64;
    for stripe in &manifest.stripes {
        let len = stripe.data_len as u64;
        let stripe_end = offset + len - 1;
        if stripe_end < start {
            offset += len;
            continue;
        }
        if offset > end {
            break;
        }
        let data = crate::api::reconstruct_stripe(&fetcher, stripe, manifest).await?;
        let from = start.saturating_sub(offset) as usize;
        let to = (end.min(stripe_end) - offset) as usize;
        out.extend_from_slice(&data[from..=to]);
        offset += len;
    }
    Ok(out)
}

/// Content hash of an assembled multipart object.
///
/// A normal object is addressed by BLAKE3 over its bytes, but a multipart
/// upload is never held in one place — the parts were hashed separately as
/// they streamed in. Hashing the ordered list of part hashes instead is
/// still content-addressed: the same parts in the same order always yield
/// the same object, so two identical multipart uploads still deduplicate.
fn multipart_content_hash(parts: &[nauka_s3::UploadedPart]) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"nauka-multipart-v1");
    for p in parts {
        hasher.update(p.content.as_bytes());
    }
    hasher.finalize().to_hex().to_string()
}
