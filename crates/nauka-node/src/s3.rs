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
        self.require_bucket(&req.input.bucket)?;
        let s3 = self.state.app.app_state().s3;
        let entry = s3
            .objects
            .get(&(req.input.bucket, req.input.key))
            .ok_or_else(|| s3_error!(NoSuchKey))?;
        let v = entry
            .current_content()
            .cloned()
            .ok_or_else(|| s3_error!(NoSuchKey))?;

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
                let bytes = reconstruct_whole(&self.state, &manifest)
                    .await
                    .map_err(|e| s3_error!(InternalError, "{e:#}"))?;
                StreamingBlob::from(Body::from(bytes))
            }
        };

        Ok(S3Response::new(GetObjectOutput {
            body: Some(body),
            content_length: Some(v.size as i64),
            e_tag: v.etag.parse().ok(),
            last_modified: Some(Self::timestamp(v.last_modified)),
            content_type: v.content_type.clone(),
            metadata: Some(v.user_metadata.clone().into_iter().collect()),
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

/// Rebuilds a whole object from the cluster.
///
/// Buffered for now: S3 GET will stream like `/f/{hash}` does once the
/// core operations are settled — correctness first, then the plumbing.
async fn reconstruct_whole(
    state: &Arc<ApiState>,
    manifest: &nauka_erasure::FileManifest,
) -> anyhow::Result<Vec<u8>> {
    let fetcher = Arc::new(crate::api::Fetcher::new(state.clone()));
    let mut out = Vec::with_capacity(manifest.file_size as usize);
    for stripe in &manifest.stripes {
        let data = crate::api::reconstruct_stripe(&fetcher, stripe, manifest).await?;
        out.extend_from_slice(&data);
    }
    Ok(out)
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
