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
use s3s::dto::{ObjectVersion as S3ObjectVersion, *};
use s3s::{s3_error, Body, S3Error, S3Request, S3Response, S3Result, S3};

use crate::api::ApiState;

/// Percent-encoding set for keys in ListObjectVersions responses (RFC 3986
/// unreserved plus `/`, everything else encoded — matching what clients
/// url-decode back).
const VERSION_KEY_SET: &percent_encoding::AsciiSet = &percent_encoding::NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~')
    .remove(b'/');

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

    /// URL-encodes a listing value when the client asked for
    /// `encoding-type=url`. S3 percent-encodes Key, Prefix, Delimiter and
    /// the markers (RFC 3986, space as %20 — never `+`), and the client
    /// decodes them back; without it a key containing `+` or a control
    /// character would be ambiguous in the XML.
    fn enc(value: String, encoding: &Option<EncodingType>) -> String {
        match encoding {
            Some(e) if e.as_str() == EncodingType::URL => {
                const SET: &percent_encoding::AsciiSet = &percent_encoding::NON_ALPHANUMERIC
                    .remove(b'-')
                    .remove(b'_')
                    .remove(b'.')
                    .remove(b'~')
                    .remove(b'/');
                percent_encoding::utf8_percent_encode(&value, SET).to_string()
            }
            _ => value,
        }
    }

    /// A bucket's versioning state (Unversioned by default).
    fn versioning_of(&self, bucket: &str) -> nauka_s3::VersioningState {
        self.state
            .app
            .app_state()
            .s3
            .buckets
            .get(bucket)
            .map(|b| b.versioning)
            .unwrap_or_default()
    }

    /// The version id a write should carry: a fresh one in an
    /// Enabled bucket, the literal "null" otherwise (Unversioned or
    /// Suspended both write to the single null version).
    fn version_id_for(state: nauka_s3::VersioningState) -> String {
        match state {
            nauka_s3::VersioningState::Enabled => nauka_s3::new_version_id(),
            _ => "null".into(),
        }
    }

    /// Deletes one object, honouring versioning. Shared by DeleteObject and
    /// the DeleteObjects batch.
    ///
    /// - a specific `version_id` is removed permanently (and if it was a
    ///   delete marker, the response says so);
    /// - without a version id, a versioned bucket does NOT erase anything —
    ///   it lays a *delete marker* on top, which is what makes the deletion
    ///   undoable. Enabled gets a fresh marker version; Suspended replaces
    ///   the null one;
    /// - an unversioned bucket just drops the null version.
    async fn delete_one(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) -> S3Result<DeleteOutcome> {
        if let Some(id) = version_id {
            // Permanent removal of one version. Report whether it was a
            // delete marker (S3 sets x-amz-delete-marker on the response).
            let was_marker = self
                .state
                .app
                .app_state()
                .s3
                .objects
                .get(&(bucket.to_string(), key.to_string()))
                .and_then(|e| e.version(id))
                .map(|v| v.is_delete_marker())
                .unwrap_or(false);
            let _ = self
                .state
                .app
                .write(nauka_raft::types::AppCommand::DeleteObjectVersion {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                    version_id: id.to_string(),
                })
                .await;
            return Ok(DeleteOutcome {
                delete_marker: was_marker,
                version_id: Some(id.to_string()),
            });
        }

        match self.versioning_of(bucket) {
            nauka_s3::VersioningState::Unversioned => {
                // Idempotent: removing an absent key is a success.
                let _ = self
                    .state
                    .app
                    .write(nauka_raft::types::AppCommand::DeleteObjectVersion {
                        bucket: bucket.to_string(),
                        key: key.to_string(),
                        version_id: "null".into(),
                    })
                    .await;
                Ok(DeleteOutcome::default())
            }
            state => {
                // Lay a delete marker on top. Enabled gets a fresh version
                // id; Suspended reuses the null version.
                let marker_id = Self::version_id_for(state);
                let marker = nauka_s3::ObjectVersion {
                    version_id: marker_id.clone(),
                    content: None,
                    delete_marker: true,
                    size: 0,
                    etag: String::new(),
                    last_modified: Self::now(),
                    content_type: None,
                    user_metadata: BTreeMap::new(),
                    system_metadata: BTreeMap::new(),
                    storage_class: None,
                    tags: BTreeMap::new(),
                    checksums: BTreeMap::new(),
                    retention: None,
                    legal_hold: false,
                    sse: None,
                };
                self.write(nauka_raft::types::AppCommand::PutObjectVersion {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                    version: Box::new(marker),
                })
                .await?;
                Ok(DeleteOutcome {
                    delete_marker: true,
                    version_id: (marker_id != "null").then_some(marker_id),
                })
            }
        }
    }

    /// The Owner block S3 attaches to a listed object. Single-tenant: the
    /// object's owner is the credential that created it (or the cluster).
    fn owner_of(&self, _v: &nauka_s3::ObjectVersion) -> Owner {
        Owner {
            display_name: Some("nauka".into()),
            id: Some("nauka".into()),
        }
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
            // In us-east-1 — the region every client defaults to against a
            // custom endpoint — recreating a bucket you already own is a
            // no-op success, not an error. (Other regions answer
            // BucketAlreadyOwnedByYou; a single-tenant cluster is
            // effectively us-east-1.)
            return Ok(S3Response::new(CreateBucketOutput {
                location: Some(format!("/{name}")),
            }));
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
        // S3 stores these headers verbatim and replays them on GET/HEAD.
        let mut system_metadata = BTreeMap::new();
        if let Some(v) = &input.cache_control {
            system_metadata.insert("cache-control".into(), v.clone());
        }
        if let Some(v) = &input.content_disposition {
            system_metadata.insert("content-disposition".into(), v.clone());
        }
        if let Some(v) = &input.content_encoding {
            system_metadata.insert("content-encoding".into(), v.clone());
        }
        if let Some(v) = &input.content_language {
            system_metadata.insert("content-language".into(), v.clone());
        }
        if let Some(v) = &input.expires {
            let odt: time::OffsetDateTime = v.clone().into();
            system_metadata.insert("expires".into(), odt.unix_timestamp().to_string());
        }
        let versioning = self.versioning_of(&input.bucket);
        let version_id = Self::version_id_for(versioning);
        // The x-amz-tagging header sets tags at creation time.
        let tags = match &input.tagging {
            Some(header) => parse_tagging_header(header)?,
            None => BTreeMap::new(),
        };
        let version = nauka_s3::ObjectVersion {
            version_id: version_id.clone(),
            content,
            delete_marker: false,
            size,
            etag: etag.clone(),
            last_modified: Self::now(),
            content_type: input.content_type.map(|v| v.to_string()),
            user_metadata: input
                .metadata
                .map(|m| m.into_iter().collect())
                .unwrap_or_default(),
            system_metadata,
            storage_class: input.storage_class.map(|s| s.as_str().to_owned()),
            tags,
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
            // Only an Enabled bucket surfaces a version id on the write.
            version_id: (versioning == nauka_s3::VersioningState::Enabled).then_some(version_id),
            ..Default::default()
        }))
    }

    async fn put_bucket_versioning(
        &self,
        req: S3Request<PutBucketVersioningInput>,
    ) -> S3Result<S3Response<PutBucketVersioningOutput>> {
        let mut bucket = self.require_bucket(&req.input.bucket)?;
        let status = req
            .input
            .versioning_configuration
            .status
            .as_ref()
            .map(|s| s.as_str());
        bucket.versioning = match status {
            Some(s) if s == BucketVersioningStatus::ENABLED => nauka_s3::VersioningState::Enabled,
            Some(s) if s == BucketVersioningStatus::SUSPENDED => {
                // Suspending keeps existing versions; only new writes go
                // back to the null version. Versioning cannot be turned
                // fully off once enabled, so we never return to Unversioned.
                nauka_s3::VersioningState::Suspended
            }
            _ => return Err(s3_error!(IllegalVersioningConfigurationException)),
        };
        self.write(nauka_raft::types::AppCommand::UpdateBucket {
            name: req.input.bucket,
            bucket: Box::new(bucket),
        })
        .await?;
        Ok(S3Response::new(PutBucketVersioningOutput::default()))
    }

    async fn get_bucket_versioning(
        &self,
        req: S3Request<GetBucketVersioningInput>,
    ) -> S3Result<S3Response<GetBucketVersioningOutput>> {
        let bucket = self.require_bucket(&req.input.bucket)?;
        // An unversioned bucket has never had versioning set: S3 returns an
        // empty Status, not the word "Suspended".
        let status = match bucket.versioning {
            nauka_s3::VersioningState::Unversioned => None,
            nauka_s3::VersioningState::Enabled => Some(BucketVersioningStatus::from_static(
                BucketVersioningStatus::ENABLED,
            )),
            nauka_s3::VersioningState::Suspended => Some(BucketVersioningStatus::from_static(
                BucketVersioningStatus::SUSPENDED,
            )),
        };
        Ok(S3Response::new(GetBucketVersioningOutput {
            status,
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
        let v = resolve_version(entry, req.input.version_id.as_deref())?;
        let sys = |k: &str| v.system_metadata.get(k).cloned();
        let mut resp = S3Response::new(HeadObjectOutput {
            content_length: Some(v.size as i64),
            e_tag: v.etag.parse().ok(),
            last_modified: Some(Self::timestamp(v.last_modified)),
            content_type: v.content_type.clone(),
            cache_control: sys("cache-control"),
            content_disposition: sys("content-disposition"),
            content_encoding: sys("content-encoding"),
            content_language: sys("content-language"),
            expires: sys("expires")
                .and_then(|s| s.parse::<u64>().ok())
                .map(Self::timestamp),
            version_id: versioned_id(v),
            metadata: Some(v.user_metadata.clone().into_iter().collect()),
            ..Default::default()
        });
        // HeadObjectOutput has no tag-count field, so set the header
        // directly, as S3 does.
        if !v.tags.is_empty() {
            if let Ok(val) = v.tags.len().to_string().parse() {
                resp.headers.insert("x-amz-tagging-count", val);
            }
        }
        Ok(resp)
    }

    async fn put_object_tagging(
        &self,
        req: S3Request<PutObjectTaggingInput>,
    ) -> S3Result<S3Response<PutObjectTaggingOutput>> {
        self.require_bucket(&req.input.bucket)?;
        let tags = tag_set_to_map(&req.input.tagging.tag_set, 10)?;
        let resp = self
            .state
            .app
            .write(nauka_raft::types::AppCommand::SetObjectTags {
                bucket: req.input.bucket,
                key: req.input.key,
                version_id: req.input.version_id.clone(),
                tags,
            })
            .await
            .map_err(|e| s3_error!(InternalError, "{e:#}"))?;
        if !resp.ok {
            return Err(s3_error!(NoSuchKey));
        }
        Ok(S3Response::new(PutObjectTaggingOutput {
            version_id: req.input.version_id,
        }))
    }

    async fn get_object_tagging(
        &self,
        req: S3Request<GetObjectTaggingInput>,
    ) -> S3Result<S3Response<GetObjectTaggingOutput>> {
        self.require_bucket(&req.input.bucket)?;
        let s3 = self.state.app.app_state().s3;
        let entry = s3
            .objects
            .get(&(req.input.bucket, req.input.key))
            .ok_or_else(|| s3_error!(NoSuchKey))?;
        let v = resolve_version(entry, req.input.version_id.as_deref())?;
        Ok(S3Response::new(GetObjectTaggingOutput {
            tag_set: map_to_tag_set(&v.tags),
            version_id: versioned_id(v),
        }))
    }

    async fn delete_object_tagging(
        &self,
        req: S3Request<DeleteObjectTaggingInput>,
    ) -> S3Result<S3Response<DeleteObjectTaggingOutput>> {
        self.require_bucket(&req.input.bucket)?;
        let _ = self
            .state
            .app
            .write(nauka_raft::types::AppCommand::SetObjectTags {
                bucket: req.input.bucket,
                key: req.input.key,
                version_id: req.input.version_id.clone(),
                tags: BTreeMap::new(),
            })
            .await;
        Ok(S3Response::new(DeleteObjectTaggingOutput {
            version_id: req.input.version_id,
        }))
    }

    async fn put_bucket_tagging(
        &self,
        req: S3Request<PutBucketTaggingInput>,
    ) -> S3Result<S3Response<PutBucketTaggingOutput>> {
        let mut bucket = self.require_bucket(&req.input.bucket)?;
        bucket.tags = tag_set_to_map(&req.input.tagging.tag_set, 50)?;
        self.write(nauka_raft::types::AppCommand::UpdateBucket {
            name: req.input.bucket,
            bucket: Box::new(bucket),
        })
        .await?;
        Ok(S3Response::new(PutBucketTaggingOutput::default()))
    }

    async fn get_bucket_tagging(
        &self,
        req: S3Request<GetBucketTaggingInput>,
    ) -> S3Result<S3Response<GetBucketTaggingOutput>> {
        let bucket = self.require_bucket(&req.input.bucket)?;
        // S3 answers NoSuchTagSet when a bucket has no tags at all.
        if bucket.tags.is_empty() {
            return Err(s3_error!(NoSuchTagSet, "The bucket has no tags"));
        }
        Ok(S3Response::new(GetBucketTaggingOutput {
            tag_set: map_to_tag_set(&bucket.tags),
        }))
    }

    async fn delete_bucket_tagging(
        &self,
        req: S3Request<DeleteBucketTaggingInput>,
    ) -> S3Result<S3Response<DeleteBucketTaggingOutput>> {
        let mut bucket = self.require_bucket(&req.input.bucket)?;
        bucket.tags.clear();
        self.write(nauka_raft::types::AppCommand::UpdateBucket {
            name: req.input.bucket,
            bucket: Box::new(bucket),
        })
        .await?;
        Ok(S3Response::new(DeleteBucketTaggingOutput::default()))
    }

    async fn get_object_attributes(
        &self,
        req: S3Request<GetObjectAttributesInput>,
    ) -> S3Result<S3Response<GetObjectAttributesOutput>> {
        self.require_bucket(&req.input.bucket)?;
        let s3 = self.state.app.app_state().s3;
        let entry = s3
            .objects
            .get(&(req.input.bucket, req.input.key))
            .ok_or_else(|| s3_error!(NoSuchKey))?;
        let v = resolve_version(entry, req.input.version_id.as_deref())?;
        // GetObjectAttributes reports the ETag WITHOUT quotes, unlike every
        // other operation — a quirk the suite checks explicitly.
        let bare_etag = v.etag.trim_matches('"').to_string();
        Ok(S3Response::new(GetObjectAttributesOutput {
            e_tag: bare_etag.parse().ok(),
            object_size: Some(v.size as i64),
            last_modified: Some(Self::timestamp(v.last_modified)),
            storage_class: Some(StorageClass::from_static(StorageClass::STANDARD)),
            version_id: versioned_id(v),
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
        let v = resolve_version(entry, input.version_id.as_deref())?.clone();

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

        // A response header override lets the client ask GET to echo a
        // different value (?response-cache-control=…), which S3 supports.
        let sys = |k: &str| v.system_metadata.get(k).cloned();
        Ok(S3Response::new(GetObjectOutput {
            body: Some(body),
            content_length: Some(length as i64),
            content_range: partial.then(|| format!("bytes {start}-{end}/{}", v.size)),
            e_tag: v.etag.parse().ok(),
            last_modified: Some(Self::timestamp(v.last_modified)),
            content_type: input
                .response_content_type
                .or_else(|| v.content_type.clone()),
            cache_control: input
                .response_cache_control
                .or_else(|| sys("cache-control")),
            content_disposition: input
                .response_content_disposition
                .or_else(|| sys("content-disposition")),
            content_encoding: input
                .response_content_encoding
                .or_else(|| sys("content-encoding")),
            content_language: input
                .response_content_language
                .or_else(|| sys("content-language")),
            expires: sys("expires")
                .and_then(|s| s.parse::<u64>().ok())
                .map(Self::timestamp),
            version_id: versioned_id(&v),
            tag_count: (!v.tags.is_empty()).then_some(v.tags.len() as i32),
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
        let (src_bucket, src_key, src_version) = match &input.copy_source {
            CopySource::Bucket {
                bucket,
                key,
                version_id,
            } => (
                bucket.to_string(),
                key.to_string(),
                version_id.as_ref().map(|v| v.to_string()),
            ),
            // Access points and Outposts are AWS-side routing concepts
            // with no meaning in a self-hosted cluster.
            CopySource::AccessPoint { .. } | CopySource::Outpost { .. } => {
                return Err(s3_error!(NotImplemented, "access point copy sources"))
            }
        };
        // COPY (the default) keeps the source metadata; REPLACE takes it
        // from the request.
        let replace = input
            .metadata_directive
            .as_ref()
            .is_some_and(|d| d.as_str() == MetadataDirective::REPLACE);
        // Copying a key onto itself is only allowed when it changes the
        // metadata; otherwise S3 rejects it as a pointless request.
        if src_bucket == input.bucket && src_key == input.key && !replace {
            return Err(s3_error!(
                InvalidRequest,
                "This copy request is illegal because it is trying to copy \
                 an object to itself without changing the object's metadata"
            ));
        }
        let s3 = self.state.app.app_state().s3;
        let entry = s3
            .objects
            .get(&(src_bucket, src_key))
            .ok_or_else(|| s3_error!(NoSuchKey))?;
        // Honour the source version: copy the exact version asked for, not
        // whatever is current.
        let source = resolve_version(entry, src_version.as_deref())?.clone();

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
        let now = Self::now();
        let versioning = self.versioning_of(&input.bucket);
        let new_version_id = Self::version_id_for(versioning);
        let copy = nauka_s3::ObjectVersion {
            version_id: new_version_id.clone(),
            delete_marker: false,
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
            version_id: (versioning == nauka_s3::VersioningState::Enabled)
                .then_some(new_version_id),
            copy_source_version_id: src_version,
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
            tags: match &input.tagging {
                Some(h) => parse_tagging_header(h)?,
                None => BTreeMap::new(),
            },
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
        let upload = match self
            .state
            .app
            .app_state()
            .s3
            .uploads
            .get(&input.upload_id)
            .cloned()
        {
            Some(u) => u,
            // Completing an already-completed upload is idempotent in S3:
            // the object exists from the first Complete, so a repeat call
            // returns 200 with that object rather than NoSuchUpload. (The
            // suite's test_multipart_upload calls Complete twice on
            // purpose.)
            None => {
                let existing = self
                    .state
                    .app
                    .app_state()
                    .s3
                    .objects
                    .get(&(input.bucket.clone(), input.key.clone()))
                    .and_then(|e| e.current_content().cloned());
                return match existing {
                    Some(v) => Ok(S3Response::new(CompleteMultipartUploadOutput {
                        bucket: Some(input.bucket),
                        key: Some(input.key),
                        e_tag: v.etag.parse().ok(),
                        ..Default::default()
                    })),
                    None => Err(s3_error!(NoSuchUpload)),
                };
            }
        };
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

        let versioning = self.versioning_of(&upload.bucket);
        let new_version_id = Self::version_id_for(versioning);
        let version = nauka_s3::ObjectVersion {
            version_id: new_version_id.clone(),
            content: Some(content_hash),
            delete_marker: false,
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
            version_id: (versioning == nauka_s3::VersioningState::Enabled)
                .then_some(new_version_id),
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
            // Per-object outcomes, versioning-aware, exactly like the
            // single-object DeleteObject.
            match self
                .delete_one(&bucket, &obj.key, obj.version_id.as_deref())
                .await
            {
                Ok(out) => deleted.push(DeletedObject {
                    key: Some(obj.key),
                    // The version id acted on, and — when a delete marker
                    // was created — its own id, so the client can undo it.
                    version_id: obj.version_id,
                    delete_marker: out.delete_marker.then_some(true),
                    delete_marker_version_id: out.version_id,
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
        let out = self
            .delete_one(
                &req.input.bucket,
                &req.input.key,
                req.input.version_id.as_deref(),
            )
            .await?;
        Ok(S3Response::new(DeleteObjectOutput {
            delete_marker: out.delete_marker.then_some(true),
            version_id: out.version_id,
            ..Default::default()
        }))
    }

    async fn list_objects_v2(
        &self,
        req: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        let bucket = req.input.bucket;
        self.require_bucket(&bucket)?;
        let s3 = self.state.app.app_state().s3;
        let prefix = req.input.prefix.clone().unwrap_or_default();
        // An empty delimiter means "no delimiter" and must not echo back.
        let delimiter = req.input.delimiter.clone().filter(|d| !d.is_empty());
        let encoding = req.input.encoding_type.clone();
        let want_owner = req.input.fetch_owner.unwrap_or(false);
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
            // The continuation token and StartAfter both mean "resume
            // strictly after this key", so a key <= it is skipped.
            if start_after.as_ref().is_some_and(|s| key <= s) {
                continue;
            }
            if entry.current_content().is_none() {
                continue;
            }
            // Emitted keys AND common prefixes both count toward MaxKeys.
            let emitted = contents.len() + prefixes.len();
            // A delimiter rolls everything below it into a common prefix,
            // which is how S3 fakes directories.
            let rolled_prefix = delimiter.as_ref().and_then(|d| {
                key[prefix.len()..]
                    .find(d.as_str())
                    .map(|idx| key[..prefix.len() + idx + d.len()].to_string())
            });
            // A prefix already seen does not consume another slot.
            if let Some(p) = &rolled_prefix {
                if prefixes.contains(p) {
                    continue;
                }
            }
            if emitted >= max_keys {
                // One more matching item exists but does not fit: stop and
                // point the next page at the last item we DID return.
                // MaxKeys=0 is the exception — AWS returns an empty,
                // non-truncated listing for it.
                truncated = max_keys > 0;
                break;
            }
            match rolled_prefix {
                Some(p) => {
                    next_token = Some(p.clone());
                    prefixes.insert(p);
                }
                None => {
                    let v = entry.current_content().unwrap();
                    next_token = Some(key.clone());
                    contents.push(Object {
                        key: Some(Self::enc(key.clone(), &encoding)),
                        size: Some(v.size as i64),
                        e_tag: v.etag.parse().ok(),
                        last_modified: Some(Self::timestamp(v.last_modified)),
                        owner: want_owner.then(|| self.owner_of(v)),
                        storage_class: Some(ObjectStorageClass::from_static(
                            ObjectStorageClass::STANDARD,
                        )),
                        ..Default::default()
                    });
                }
            }
        }
        // The token only matters when the listing is truncated.
        if !truncated {
            next_token = None;
        }

        // KeyCount counts BOTH the returned keys and the rolled-up common
        // prefixes — S3 treats a common prefix as one "key" for the count.
        let key_count = contents.len() + prefixes.len();
        Ok(S3Response::new(ListObjectsV2Output {
            key_count: Some(key_count as i32),
            max_keys: Some(max_keys as i32),
            contents: Some(contents),
            common_prefixes: Some(
                prefixes
                    .into_iter()
                    .map(|p| CommonPrefix {
                        prefix: Some(Self::enc(p, &encoding)),
                    })
                    .collect(),
            ),
            name: Some(bucket),
            prefix: Some(Self::enc(prefix, &encoding)),
            delimiter: delimiter.map(|d| Self::enc(d, &encoding)),
            encoding_type: encoding.clone(),
            // Echoed back verbatim, as S3 does, so a client can correlate.
            continuation_token: req.input.continuation_token.clone(),
            start_after: req.input.start_after.clone(),
            is_truncated: Some(truncated),
            next_continuation_token: next_token,
            ..Default::default()
        }))
    }

    async fn list_objects(
        &self,
        req: S3Request<ListObjectsInput>,
    ) -> S3Result<S3Response<ListObjectsOutput>> {
        // The v1 listing, kept for older clients. Same walk as v2, with the
        // v1 marker/next-marker shape.
        let bucket = req.input.bucket;
        self.require_bucket(&bucket)?;
        let s3 = self.state.app.app_state().s3;
        let prefix = req.input.prefix.clone().unwrap_or_default();
        let delimiter = req.input.delimiter.clone().filter(|d| !d.is_empty());
        let encoding = req.input.encoding_type.clone();
        let marker = req.input.marker.clone().unwrap_or_default();
        let max_keys = req.input.max_keys.unwrap_or(1000).clamp(0, 1000) as usize;

        let mut contents: Vec<Object> = Vec::new();
        let mut prefixes: std::collections::BTreeSet<String> = Default::default();
        let mut truncated = false;
        let mut next_marker = None;

        for ((b, key), entry) in s3.objects.range((bucket.clone(), String::new())..) {
            if *b != bucket {
                break;
            }
            if !key.starts_with(&prefix) || (!marker.is_empty() && key <= &marker) {
                continue;
            }
            let Some(v) = entry.current_content() else {
                continue;
            };
            if let Some(d) = &delimiter {
                if let Some(idx) = key[prefix.len()..].find(d.as_str()) {
                    prefixes.insert(key[..prefix.len() + idx + d.len()].to_string());
                    continue;
                }
            }
            if contents.len() + prefixes.len() >= max_keys {
                // MaxKeys=0 yields an empty, non-truncated listing.
                truncated = max_keys > 0;
                // With a delimiter, NextMarker is the last key returned.
                next_marker = contents.last().and_then(|o| o.key.clone());
                break;
            }
            contents.push(Object {
                key: Some(Self::enc(key.clone(), &encoding)),
                size: Some(v.size as i64),
                e_tag: v.etag.parse().ok(),
                last_modified: Some(Self::timestamp(v.last_modified)),
                storage_class: Some(ObjectStorageClass::from_static(
                    ObjectStorageClass::STANDARD,
                )),
                ..Default::default()
            });
        }
        if !truncated {
            next_marker = None;
        }

        Ok(S3Response::new(ListObjectsOutput {
            contents: Some(contents),
            common_prefixes: Some(
                prefixes
                    .into_iter()
                    .map(|p| CommonPrefix {
                        prefix: Some(Self::enc(p, &encoding)),
                    })
                    .collect(),
            ),
            name: Some(bucket),
            prefix: Some(Self::enc(prefix, &encoding)),
            delimiter: delimiter.map(|d| Self::enc(d, &encoding)),
            marker: Some(marker),
            max_keys: Some(max_keys as i32),
            is_truncated: Some(truncated),
            next_marker,
            encoding_type: encoding.clone(),
            ..Default::default()
        }))
    }

    async fn list_object_versions(
        &self,
        req: S3Request<ListObjectVersionsInput>,
    ) -> S3Result<S3Response<ListObjectVersionsOutput>> {
        // Every version of every key, delete markers included. In an
        // unversioned bucket each key has a single "null" version — which
        // is exactly what the test suite's cleanup relies on to enumerate
        // and delete objects, so this one method unblocks the whole run.
        let bucket = req.input.bucket;
        self.require_bucket(&bucket)?;
        let s3 = self.state.app.app_state().s3;
        let prefix = req.input.prefix.clone().unwrap_or_default();
        let delimiter = req.input.delimiter.clone().filter(|d| !d.is_empty());
        let encoding = req.input.encoding_type.clone();
        let max_keys = req.input.max_keys.unwrap_or(1000).clamp(0, 1000) as usize;

        let mut versions: Vec<S3ObjectVersion> = Vec::new();
        let mut markers: Vec<DeleteMarkerEntry> = Vec::new();
        let mut prefixes: std::collections::BTreeSet<String> = Default::default();
        let mut count = 0usize;
        let mut truncated = false;

        'outer: for ((b, key), entry) in s3.objects.range((bucket.clone(), String::new())..) {
            if *b != bucket || !key.starts_with(&prefix) {
                if *b != bucket {
                    break;
                }
                continue;
            }
            if let Some(d) = &delimiter {
                if let Some(idx) = key[prefix.len()..].find(d.as_str()) {
                    prefixes.insert(key[..prefix.len() + idx + d.len()].to_string());
                    continue;
                }
            }
            for (i, v) in entry.versions.iter().enumerate() {
                if count >= max_keys {
                    truncated = true;
                    break 'outer;
                }
                count += 1;
                let is_latest = i == 0;
                // ListObjectVersions returns URL-encoded keys per the S3
                // spec (clients decode them), unlike ListObjectsV2. Encode
                // so a key with `+` or a space round-trips instead of
                // arriving back with the wrong bytes.
                let out_key =
                    percent_encoding::utf8_percent_encode(key, VERSION_KEY_SET).to_string();
                if v.is_delete_marker() {
                    markers.push(DeleteMarkerEntry {
                        key: Some(out_key.clone()),
                        version_id: Some(v.version_id.clone()),
                        is_latest: Some(is_latest),
                        last_modified: Some(Self::timestamp(v.last_modified)),
                        ..Default::default()
                    });
                } else {
                    versions.push(S3ObjectVersion {
                        key: Some(out_key),
                        version_id: Some(v.version_id.clone()),
                        is_latest: Some(is_latest),
                        size: Some(v.size as i64),
                        e_tag: v.etag.parse().ok(),
                        last_modified: Some(Self::timestamp(v.last_modified)),
                        storage_class: Some(ObjectVersionStorageClass::from_static(
                            ObjectVersionStorageClass::STANDARD,
                        )),
                        ..Default::default()
                    });
                }
            }
        }

        Ok(S3Response::new(ListObjectVersionsOutput {
            versions: Some(versions),
            delete_markers: Some(markers),
            common_prefixes: Some(
                prefixes
                    .into_iter()
                    .map(|p| CommonPrefix { prefix: Some(p) })
                    .collect(),
            ),
            name: Some(bucket),
            prefix: Some(prefix),
            delimiter,
            encoding_type: encoding,
            max_keys: Some(max_keys as i32),
            is_truncated: Some(truncated),
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
/// Converts an S3 `TagSet` into a map, enforcing the S3 limits: at most
/// `max` tags, keys 1–128 characters, values 0–256, and no duplicate keys.
fn tag_set_to_map(tag_set: &[Tag], max: usize) -> S3Result<BTreeMap<String, String>> {
    if tag_set.len() > max {
        return Err(s3_error!(
            InvalidTag,
            "a resource may carry at most {max} tags"
        ));
    }
    let mut map = BTreeMap::new();
    for tag in tag_set {
        let key = tag.key.clone().unwrap_or_default();
        let value = tag.value.clone().unwrap_or_default();
        if key.is_empty() || key.chars().count() > 128 || value.chars().count() > 256 {
            return Err(s3_error!(InvalidTag, "invalid tag key or value"));
        }
        if map.insert(key, value).is_some() {
            return Err(s3_error!(InvalidTag, "duplicate tag key"));
        }
    }
    Ok(map)
}

/// Parses the `x-amz-tagging` header — a url-encoded `k=v&k2=v2` query
/// string — into a tag map, enforcing the same 10-tag object limit.
fn parse_tagging_header(header: &str) -> S3Result<BTreeMap<String, String>> {
    let mut tags: Vec<Tag> = Vec::new();
    for pair in header.split('&').filter(|p| !p.is_empty()) {
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        let decode = |s: &str| {
            percent_encoding::percent_decode_str(s)
                .decode_utf8_lossy()
                .into_owned()
        };
        tags.push(Tag {
            key: Some(decode(k)),
            value: Some(decode(v)),
        });
    }
    tag_set_to_map(&tags, 10)
}

/// Renders a tag map back as an S3 `TagSet`.
fn map_to_tag_set(tags: &BTreeMap<String, String>) -> Vec<Tag> {
    tags.iter()
        .map(|(k, v)| Tag {
            key: Some(k.clone()),
            value: Some(v.clone()),
        })
        .collect()
}

/// The outcome of deleting one object, shaping the DeleteObject /
/// DeleteObjects response fields.
#[derive(Default)]
struct DeleteOutcome {
    /// True when the delete created (or removed) a delete marker.
    delete_marker: bool,
    /// The version id involved: the new marker's id on a versioned delete,
    /// or the removed version's id on a permanent delete.
    version_id: Option<String>,
}

/// Resolves the version a read targets: a specific one when `version_id`
/// is given, otherwise the current content (a delete marker on top reads
/// as absent). A delete marker requested by id is not a body — S3 answers
/// 405 MethodNotAllowed.
fn resolve_version<'a>(
    entry: &'a nauka_s3::ObjectEntry,
    version_id: Option<&str>,
) -> S3Result<&'a nauka_s3::ObjectVersion> {
    match version_id {
        Some(id) => {
            let v = entry.version(id).ok_or_else(|| s3_error!(NoSuchVersion))?;
            if v.is_delete_marker() {
                return Err(s3_error!(MethodNotAllowed));
            }
            Ok(v)
        }
        None => match entry.current() {
            Some(v) if !v.is_delete_marker() => Ok(v),
            // The newest version is a delete marker: the key reads as
            // absent (404), but S3 flags it with x-amz-delete-marker and
            // the marker's version id so a client can tell a deletion from
            // a key that never existed.
            Some(marker) => Err(delete_marker_404(marker)),
            None => Err(s3_error!(NoSuchKey)),
        },
    }
}

/// A 404 that carries `x-amz-delete-marker: true` and the marker's version
/// id, as S3 does when the current version is a delete marker.
fn delete_marker_404(marker: &nauka_s3::ObjectVersion) -> S3Error {
    let mut err = s3_error!(NoSuchKey);
    let mut headers = hyper::HeaderMap::new();
    headers.insert(
        "x-amz-delete-marker",
        hyper::header::HeaderValue::from_static("true"),
    );
    if marker.version_id != "null" {
        if let Ok(id) = marker.version_id.parse::<hyper::header::HeaderValue>() {
            headers.insert("x-amz-version-id", id);
        }
    }
    err.set_headers(headers);
    err
}

/// The version id to surface on a response — `None` for the "null" version
/// of an unversioned object, so an unversioned bucket shows no version id.
fn versioned_id(v: &nauka_s3::ObjectVersion) -> Option<String> {
    (v.version_id != "null").then(|| v.version_id.clone())
}

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
            return Err(not_modified(v));
        }
    }
    if let Some(since) = if_unmodified_since {
        if timestamp_secs(since).is_some_and(|s| v.last_modified > s) {
            return Err(s3_error!(PreconditionFailed));
        }
    }
    if let Some(since) = if_modified_since {
        if timestamp_secs(since).is_some_and(|s| v.last_modified <= s) {
            return Err(not_modified(v));
        }
    }
    Ok(())
}

/// A 304 Not Modified that still carries the object's `ETag` and
/// `Last-Modified` — S3 attaches both, and clients read them off the 304 to
/// keep their cache in sync. A bare 304 (no headers) is what a naive error
/// path produces, and it fails `test_get_object_ifnonematch_good`.
fn not_modified(v: &nauka_s3::ObjectVersion) -> S3Error {
    let mut err = s3_error!(NotModified);
    let mut headers = hyper::HeaderMap::new();
    if let Ok(etag) = v.etag.parse::<hyper::header::HeaderValue>() {
        headers.insert(hyper::header::ETAG, etag);
    }
    let http_date = {
        let odt: time::OffsetDateTime = NaukaS3::timestamp(v.last_modified).clone().into();
        odt.format(&time::format_description::well_known::Rfc2822)
            .ok()
            .and_then(|s| s.parse::<hyper::header::HeaderValue>().ok())
    };
    if let Some(date) = http_date {
        headers.insert(hyper::header::LAST_MODIFIED, date);
    }
    err.set_headers(headers);
    err
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
