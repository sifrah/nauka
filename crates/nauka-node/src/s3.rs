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
        // Fail fast when the write provably cannot commit (no leader, or
        // no quorum reachable on the data plane): 503, retryable — never a
        // hang, never a 500. See `ApiState::can_commit_write`.
        if !self.state.can_commit_write() {
            // Counted apart from the mid-flight loss below: this one means
            // the node knew up front it could not commit, which points at
            // the cluster, not at the request.
            crate::telemetry::s3::record_write_rejected(crate::api::NO_QUORUM);
            return Err(Self::unavailable_write_error());
        }
        match self.state.app.write(cmd).await {
            Ok(r) if r.ok => Ok(r.info),
            // A command the state machine deliberately rejected (a real
            // conflict, e.g. a bucket-exists race) is a genuine error.
            Ok(r) => {
                crate::telemetry::s3::record_write_rejected("conflict");
                Err(s3_error!(
                    InternalError,
                    "{}",
                    r.info.unwrap_or_else(|| "refused".into())
                ))
            }
            // Reaching here means the registry did not commit in time
            // (quorum lost mid-flight, the leader went away): an
            // availability failure, not an internal bug — 503, retryable.
            Err(_) => {
                crate::telemetry::s3::record_write_rejected(crate::api::COMMIT_TIMEOUT);
                Err(Self::unavailable_write_error())
            }
        }
    }

    /// 503 for a write that cannot reach quorum right now.
    fn unavailable_write_error() -> S3Error {
        s3_error!(ServiceUnavailable, "{}", crate::api::WRITE_UNAVAILABLE_MSG)
    }

    /// Renders an object-data dispatch failure as an S3 error. The object
    /// path writes its manifest through `dispatch_file`, not through
    /// `write` above, so it needs the same classification: a cluster that
    /// cannot commit is a retryable 503 (and is counted with the same
    /// reasons), anything else is a genuine internal error.
    fn dispatch_error(e: crate::api::DispatchError) -> S3Error {
        match e {
            crate::api::DispatchError::Unavailable(reason) => {
                crate::telemetry::s3::record_write_rejected(reason);
                Self::unavailable_write_error()
            }
            crate::api::DispatchError::Failed(e) => s3_error!(InternalError, "{e:#}"),
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

    /// A local miss is ambiguous: state reads are eventually consistent,
    /// so a just-written key can be committed cluster-wide yet not applied
    /// on this node for a moment (~1s at rest, much longer under leader
    /// churn). Answering NoSuchKey there breaks the read-after-write
    /// pattern every S3 client assumes. So before a GET/HEAD takes the
    /// negative path, catch up with the leader once and look again — hits
    /// stay on the fast local path, only misses pay the round-trip.
    ///
    /// Returns the achieved freshness. Only `Fresh` earns a trusted
    /// NoSuchKey: on `ConfirmedStale` (provably behind, catch-up timed
    /// out — a node healing after a fault) and on `Unknown` (no leader:
    /// an election or a partition, exactly when this node lags the most)
    /// a still-missing key answers 503 SlowDown, not a false NoSuchKey.
    /// S3 clients retry a 503; none of them retries a 404.
    /// Wrapper so every exit is counted — the fast local hit included,
    /// otherwise the freshness series would only ever describe misses and
    /// look alarming on a perfectly healthy node.
    async fn ensure_visible(&self, bucket: &str, key: &str) -> nauka_raft::Freshness {
        let freshness = self.ensure_visible_inner(bucket, key).await;
        crate::telemetry::s3::record_read_freshness(match freshness {
            nauka_raft::Freshness::Fresh => "fresh",
            nauka_raft::Freshness::ConfirmedStale => "confirmed_stale",
            nauka_raft::Freshness::Unknown => "unknown",
        });
        freshness
    }

    async fn ensure_visible_inner(&self, bucket: &str, key: &str) -> nauka_raft::Freshness {
        if self.key_present(bucket, key) {
            return nauka_raft::Freshness::Fresh;
        }
        let freshness = self.state.app.catch_up_with_leader().await;
        // Catch-up proved we applied up to the leader's index — but openraft
        // bumps the applied-index metric a hair before a freshly INSTALLED
        // snapshot is visible in the state machine we read here (a node
        // healing from far behind). While the state machine is still behind
        // that metric, a `Fresh`-but-absent key may just be not-visible-yet,
        // so poll the visibility window briefly before trusting the 404. On
        // a caught-up node `state_lagging` is false immediately, so a
        // genuinely-absent key 404s with no added latency.
        // Fresh means openraft's applied-index metric reached the leader's
        // index — but that metric can lead the state machine we read here by
        // a hair while a freshly-received snapshot finishes installing (a
        // node healing from far behind). While the state machine is still
        // behind that metric, poll the visibility window briefly before
        // trusting a `Fresh`-but-absent negative. On a caught-up node
        // `state_lagging` is false at once, so a genuinely-absent key 404s
        // with no added latency.
        if freshness == nauka_raft::Freshness::Fresh {
            for _ in 0..20 {
                if !self.state.app.state_lagging() || self.key_present(bucket, key) {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(15)).await;
            }
        }
        freshness
    }

    /// Bucket exists locally and holds this key, in the applied state.
    fn key_present(&self, bucket: &str, key: &str) -> bool {
        let s3 = self.state.app.app_state().s3;
        s3.buckets.contains_key(bucket)
            && s3
                .objects
                .contains_key(&(bucket.to_string(), key.to_string()))
    }

    /// The SlowDown error for a negative lookup on a confirmably-stale
    /// node (see [`Self::ensure_visible`]).
    fn stale_read_error() -> S3Error {
        s3_error!(
            SlowDown,
            "this node is catching up with the cluster; retry shortly"
        )
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

    /// The (mode, retain-until-secs) of an object version's Object Lock
    /// retention, if any.
    fn retention_of(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) -> Option<(String, u64)> {
        let s3 = self.state.app.app_state().s3;
        let entry = s3.objects.get(&(bucket.to_string(), key.to_string()))?;
        let v = match version_id {
            Some(id) => entry.version(id)?,
            None => entry.current()?,
        };
        let info: RetentionInfo = serde_json::from_str(v.retention.as_deref()?).ok()?;
        Some((info.mode, info.until))
    }

    /// The `x-amz-expiration` value for a key, if a lifecycle rule will
    /// expire it: `expiry-date="…", rule-id="…"` for the earliest-expiring
    /// Enabled rule with an Expiration action whose filter matches the key.
    /// A days-based expiry lands at the first midnight UTC more than
    /// `days` after the write, as AWS rounds; a date-based one is the date
    /// itself.
    fn expiration_of(
        &self,
        bucket: &str,
        key: &str,
        tags: &BTreeMap<String, String>,
        written: u64,
    ) -> Option<String> {
        const DAY: u64 = 86_400;
        let s3 = self.state.app.app_state().s3;
        let cfg = lifecycle_from_xml(s3.buckets.get(bucket)?.lifecycle.as_deref()?)?;
        let mut best: Option<(u64, &str)> = None;
        for rule in &cfg.rules {
            if rule.status.as_str() != ExpirationStatus::ENABLED {
                continue;
            }
            let Some(exp) = &rule.expiration else {
                continue;
            };
            if !lifecycle_rule_matches(rule, key, tags) {
                continue;
            }
            let when = match (&exp.date, exp.days) {
                (Some(date), _) => match timestamp_secs(date) {
                    Some(s) => s,
                    None => continue,
                },
                (None, Some(days)) => (written / DAY + days as u64 + 1) * DAY,
                // An ExpiredObjectDeleteMarker-only action has no date to
                // announce.
                _ => continue,
            };
            if best.is_none_or(|(b, _)| when < b) {
                best = Some((when, rule.id.as_deref().unwrap_or("")));
            }
        }
        let (when, id) = best?;
        let odt = time::OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(when as i64);
        let date = odt
            .format(&time::format_description::well_known::Rfc2822)
            .ok()?;
        Some(format!("expiry-date=\"{date}\", rule-id=\"{id}\""))
    }

    /// Whether a version is protected from deletion by Object Lock — a
    /// retention still in force, or a legal hold. GOVERNANCE yields to the
    /// bypass header; COMPLIANCE and legal holds never do.
    fn is_locked(&self, bucket: &str, key: &str, version_id: &str, bypass: bool) -> bool {
        let s3 = self.state.app.app_state().s3;
        let Some(entry) = s3.objects.get(&(bucket.to_string(), key.to_string())) else {
            return false;
        };
        let Some(v) = entry.version(version_id) else {
            return false;
        };
        if v.legal_hold {
            return true;
        }
        if let Some((mode, until)) = self.retention_of(bucket, key, Some(version_id)) {
            if until > Self::now() {
                // GOVERNANCE can be bypassed; COMPLIANCE cannot.
                return !(mode == ObjectLockRetentionMode::GOVERNANCE && bypass);
            }
        }
        false
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
        bypass_governance: bool,
    ) -> S3Result<DeleteOutcome> {
        if let Some(id) = version_id {
            // Object Lock: a retained or legally-held version cannot be
            // permanently removed.
            if self.is_locked(bucket, key, id, bypass_governance) {
                return Err(s3_error!(
                    AccessDenied,
                    "the object version is protected by Object Lock"
                ));
            }
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
                    owner: None,
                    acl: None,
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

    /// The Owner block S3 attaches to a listed object: the version's
    /// owner when it has one (with the display name looked up from the
    /// credentials), the cluster placeholder for pre-ownership versions.
    fn owner_of(&self, v: &nauka_s3::ObjectVersion) -> Owner {
        match &v.owner {
            Some(id) => Owner {
                display_name: self.display_name_of(id),
                id: Some(id.clone()),
            },
            None => Owner {
                display_name: Some("nauka".into()),
                id: Some("nauka".into()),
            },
        }
    }

    /// The display name behind a canonical user id — the label its
    /// credential was registered with. Not stored in grants: renaming
    /// the key renames every listing of it.
    fn display_name_of(&self, canonical_id: &str) -> Option<String> {
        self.state
            .app
            .app_state()
            .s3
            .credentials
            .values()
            .find(|c| c.canonical_id() == canonical_id)
            .and_then(|c| c.name.clone())
    }

    /// A bucket's Owner block for ACL responses.
    fn bucket_owner_block(&self, bucket: &nauka_s3::Bucket) -> Owner {
        let id = self.canonical_id_of(&bucket.owner);
        Owner {
            display_name: self.display_name_of(&id),
            id: Some(id),
        }
    }

    /// Converts stored grants to the wire form: groups first (the suite's
    /// comparison relies on it), display names resolved per grantee.
    fn to_s3_grants(&self, grants: &[nauka_s3::acl::AclGrant]) -> Vec<Grant> {
        use nauka_s3::acl::AclGrantee;
        let mut out: Vec<Grant> = Vec::with_capacity(grants.len());
        let (groups, users): (Vec<_>, Vec<_>) = grants
            .iter()
            .partition(|g| matches!(g.grantee, AclGrantee::Group { .. }));
        for g in groups.into_iter().chain(users) {
            let grantee = match &g.grantee {
                AclGrantee::Group { uri } => Grantee {
                    uri: Some(uri.clone()),
                    id: None,
                    display_name: None,
                    email_address: None,
                    type_: Type::from_static(Type::GROUP),
                },
                AclGrantee::Canonical { id } => Grantee {
                    uri: None,
                    id: Some(id.clone()),
                    display_name: self.display_name_of(id),
                    email_address: None,
                    type_: Type::from_static(Type::CANONICAL_USER),
                },
            };
            out.push(Grant {
                grantee: Some(grantee),
                permission: Some(g.permission.clone().into()),
            });
        }
        out
    }

    /// Parses a client-sent AccessControlPolicy into stored grants,
    /// validating each grantee: a canonical id must belong to a known
    /// credential, a group URI must be one of the two real groups, and
    /// email grantees are unresolvable here by construction.
    fn grants_from_acp(&self, acp: &AccessControlPolicy) -> S3Result<Vec<nauka_s3::acl::AclGrant>> {
        use nauka_s3::acl::{AclGrant, ALL_USERS, AUTH_USERS};
        let known = |id: &str| {
            self.state
                .app
                .app_state()
                .s3
                .credentials
                .values()
                .any(|c| c.canonical_id() == id)
        };
        let mut out = Vec::new();
        for grant in acp.grants.iter().flatten() {
            let Some(grantee) = &grant.grantee else {
                return Err(s3_error!(InvalidArgument, "a grant needs a grantee"));
            };
            let Some(permission) = &grant.permission else {
                return Err(s3_error!(InvalidArgument, "a grant needs a permission"));
            };
            let permission = permission.as_str();
            if !matches!(
                permission,
                "READ" | "WRITE" | "READ_ACP" | "WRITE_ACP" | "FULL_CONTROL"
            ) {
                return Err(s3_error!(InvalidArgument, "unknown permission"));
            }
            match grantee.type_.as_str() {
                Type::CANONICAL_USER => {
                    let Some(id) = &grantee.id else {
                        return Err(s3_error!(InvalidArgument, "a CanonicalUser needs an ID"));
                    };
                    if !known(id) {
                        return Err(s3_error!(InvalidArgument, "no such user"));
                    }
                    out.push(AclGrant::canonical(id, permission));
                }
                Type::GROUP => {
                    let uri = grantee.uri.as_deref().unwrap_or_default();
                    if uri != ALL_USERS && uri != AUTH_USERS {
                        return Err(s3_error!(InvalidArgument, "unknown group URI"));
                    }
                    out.push(AclGrant::group(uri, permission));
                }
                Type::AMAZON_CUSTOMER_BY_EMAIL => {
                    return Err(custom_error(
                        "UnresolvableGrantByEmailAddress",
                        hyper::StatusCode::BAD_REQUEST,
                        "email grantees cannot be resolved here",
                    ));
                }
                _ => return Err(s3_error!(InvalidArgument, "unknown grantee type")),
            }
        }
        Ok(out)
    }

    /// The canonical user id behind an access key: what ACLs display and
    /// policy principals match. Falls back to the access key itself for
    /// keys registered without one.
    fn canonical_id_of(&self, access_key: &str) -> String {
        self.state
            .app
            .app_state()
            .s3
            .credentials
            .get(access_key)
            .map(|c| c.canonical_id().to_owned())
            .unwrap_or_else(|| access_key.to_owned())
    }

    /// The owner an object written by this request carries: the id in an
    /// `x-amz-grant-full-control: id=…` header when the writer hands the
    /// object over (the S3 way to give the bucket owner the objects), the
    /// writer's own canonical id otherwise.
    fn object_owner_for(
        &self,
        credentials: Option<&s3s::auth::Credentials>,
        grant_full_control: Option<&str>,
    ) -> Option<String> {
        if let Some(g) = grant_full_control {
            if let Some(id) = g
                .split(',')
                .filter_map(|part| part.trim().strip_prefix("id="))
                .next()
            {
                return Some(id.trim_matches('"').to_owned());
            }
        }
        credentials.map(|c| self.canonical_id_of(&c.access_key))
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
        let object_lock = req.input.object_lock_enabled_for_bucket.unwrap_or(false);
        let bucket = nauka_s3::Bucket {
            created_at: Self::now(),
            owner: req.credentials.map(|c| c.access_key).unwrap_or_default(),
            // The canned ACL, kept for the anonymous-access decision
            // (`public-read` lets unauthenticated reads through).
            acl: req.input.acl.map(|a| a.as_str().to_owned()),
            // Echoed by GetBucketLocation; empty = the null constraint.
            location: req
                .input
                .create_bucket_configuration
                .and_then(|c| c.location_constraint)
                .map(|l| l.as_str().to_owned())
                .filter(|l| !l.is_empty()),
            object_lock_enabled: object_lock,
            // Object Lock requires versioning, so enabling it at creation
            // turns versioning on too, as S3 does.
            versioning: if object_lock {
                nauka_s3::VersioningState::Enabled
            } else {
                nauka_s3::VersioningState::Unversioned
            },
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
        let owned_and_granted = s3.buckets.iter().map(|(name, b)| (name, b.owner.as_str()));
        let visible = match s3.credentials.get(&access_key) {
            Some(c) => c.visible_buckets(owned_and_granted),
            // An unknown key (should not get this far) sees nothing.
            None => Vec::new(),
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
        let mut input = req.input;
        let bucket_meta = self.require_bucket(&input.bucket)?;
        if !nauka_s3::naming::valid_key(&input.key) {
            return Err(s3_error!(InvalidArgument, "invalid key"));
        }
        // A canned ACL on the PUT is expanded and stored on the version —
        // unless the bucket blocks public ACLs and this one is public.
        let owner = self.object_owner_for(
            req.credentials.as_ref(),
            input.grant_full_control.as_deref(),
        );
        let acl = match &input.acl {
            Some(canned) => {
                let canned = canned.as_str();
                let block_public = bucket_meta
                    .public_access_block
                    .as_deref()
                    .and_then(pab_from_xml)
                    .is_some_and(|p| p.block_public_acls.unwrap_or(false));
                if block_public && nauka_s3::acl::canned_is_public(canned) {
                    return Err(s3_error!(AccessDenied, "public ACLs are blocked"));
                }
                let owner_id = owner.clone().unwrap_or_default();
                let bucket_owner = self.canonical_id_of(&bucket_meta.owner);
                nauka_s3::acl::canned_grants(canned, &owner_id, Some(&bucket_owner))
                    .map(|g| nauka_s3::acl::to_json(&g))
            }
            None => None,
        };

        // Server-side encryption, validated before the body is read so a
        // malformed request costs nothing.
        let sse_req = validate_sse_request(
            input.server_side_encryption.as_ref().map(|s| s.as_str()),
            input.ssekms_key_id.as_deref(),
            input.sse_customer_algorithm.as_deref(),
            input.sse_customer_key.as_deref(),
            input.sse_customer_key_md5.as_deref(),
        )?;

        let mut hasher = checksum_hasher_for(&input);
        let mut sse_info = sse_req.info;
        let (content, size, md5, checksums) = if sse_req.customer_key.is_some() {
            // SSE-C: the plaintext never reaches the cluster. It is
            // encrypted here with the customer's key (which is NOT kept),
            // and what gets erasure-coded is the ciphertext. Encryption
            // needs the complete plaintext staged first, so this path
            // keeps the buffered route — it cannot overlap with reception
            // by construction.
            let tmp = self.state.tmp_dir.join(format!("s3-{}", uuid_like()));
            let (size, _blake, md5) = write_body(input.body.take(), &tmp, &mut hasher)
                .await
                .map_err(|e| {
                    let _ = std::fs::remove_file(&tmp);
                    s3_error!(InternalError, "{e:#}")
                })?;
            // A checksum the client sent must match what the body hashes
            // to, before the object is allowed to exist.
            let checksums = match verify_checksums(&input, hasher.finalize()) {
                Ok(c) => c,
                Err(e) => {
                    let _ = tokio::fs::remove_file(&tmp).await;
                    return Err(e);
                }
            };
            // An empty object has no shards to place — pure metadata.
            let content = if size == 0 {
                let _ = tokio::fs::remove_file(&tmp).await;
                None
            } else {
                let key = sse_req.customer_key.clone().expect("branch condition");
                let ct_tmp = self.state.tmp_dir.join(format!("s3-{}", uuid_like()));
                let r = encrypt_to_tmp(key, tmp.clone(), ct_tmp.clone()).await;
                let _ = tokio::fs::remove_file(&tmp).await;
                let (ct_len, _ct_hasher) = match r {
                    Ok(v) => v,
                    Err(e) => {
                        let _ = tokio::fs::remove_file(&ct_tmp).await;
                        return Err(e);
                    }
                };
                if let Some(i) = &mut sse_info {
                    i.segments = vec![ct_len];
                }
                let result =
                    crate::api::dispatch_file(&self.state, &ct_tmp, Some(input.key.clone()), None)
                        .await;
                let _ = tokio::fs::remove_file(&ct_tmp).await;
                let (manifest, _degraded) = result.map_err(Self::dispatch_error)?;
                Some(manifest.file_hash)
            };
            (content, size, md5, checksums)
        } else {
            // The streaming path: the body feeds the elastic buffer while
            // the encoder drains it concurrently — encoding starts on the
            // first complete stripe, not after the last byte. `finish` is
            // only signalled once the client's checksums verify: a
            // mismatch aborts the dispatcher mid-drain and the object
            // never exists (already-written shards are ordinary GC
            // orphans). MD5 still streams here: it is the ETag.
            use md5::Digest;
            let spool_path = self.state.tmp_dir.join(format!("s3-{}", uuid_like()));
            // Spool engages only on a zero RAM grant — see the native door.
            let spool_bound = crate::ingest::fs_available(&self.state.tmp_dir) / 2;
            let (mut tx, rx) = crate::ingest::channel(
                &self.state.ingest_pool,
                crate::api::INGEST_RAM_WANT,
                spool_path,
                spool_bound,
            );
            let dispatch = tokio::spawn(crate::api::dispatch_stream(
                self.state.clone(),
                rx,
                Some(input.key.clone()),
                None,
            ));
            let mut md5 = md5::Md5::new();
            let mut size = 0u64;
            let mut body_err: Option<S3Error> = None;
            if let Some(mut stream) = input.body.take() {
                use futures::StreamExt;
                while let Some(chunk) = stream.next().await {
                    let chunk = match chunk {
                        Ok(c) => c,
                        Err(e) => {
                            body_err = Some(s3_error!(InternalError, "reading the body: {e}"));
                            break;
                        }
                    };
                    md5.update(&chunk);
                    hasher.update(&chunk);
                    size += chunk.len() as u64;
                    if let Err(e) = tx.push(chunk).await {
                        body_err = Some(s3_error!(InternalError, "{e:#}"));
                        break;
                    }
                }
            }
            if let Some(e) = body_err {
                // Dropping the writer unfinished aborts the dispatcher: a
                // truncated stream must never become an object.
                drop(tx);
                let _ = dispatch.await;
                return Err(e);
            }
            let checksums = match verify_checksums(&input, hasher.finalize()) {
                Ok(c) => c,
                Err(e) => {
                    drop(tx);
                    let _ = dispatch.await;
                    return Err(e);
                }
            };
            let content = if size == 0 {
                // Metadata-only object; the dispatcher would refuse an
                // empty stream, so it is never asked to finish.
                drop(tx);
                let _ = dispatch.await;
                None
            } else {
                tx.finish();
                let result = dispatch
                    .await
                    .map_err(|e| s3_error!(InternalError, "dispatch task: {e}"))?;
                let (manifest, _degraded) = result.map_err(Self::dispatch_error)?;
                Some(manifest.file_hash)
            };
            (content, size, md5.finalize().into(), checksums)
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
        // Object Lock can also be set at PUT time via headers.
        let retention = retention_from_headers(
            input.object_lock_mode.as_ref().map(|m| m.as_str()),
            input.object_lock_retain_until_date.as_ref(),
        );
        let legal_hold = input
            .object_lock_legal_hold_status
            .as_ref()
            .is_some_and(|s| s.as_str() == ObjectLockLegalHoldStatus::ON);
        let version = nauka_s3::ObjectVersion {
            version_id: version_id.clone(),
            content,
            delete_marker: false,
            size,
            etag: etag.clone(),
            last_modified: Self::now(),
            // AWS stores `binary/octet-stream` when the client sent no
            // Content-Type, and GET replays it.
            content_type: Some(
                input
                    .content_type
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "binary/octet-stream".into()),
            ),
            user_metadata: input
                .metadata
                .map(|m| m.into_iter().collect())
                .unwrap_or_default(),
            system_metadata,
            storage_class: input.storage_class.map(|s| s.as_str().to_owned()),
            tags,
            checksums,
            retention,
            legal_hold,
            sse: sse_info.as_ref().and_then(SseInfo::to_json),
            owner,
            acl,
        };
        // Announced on the response: the lifecycle rule that will expire
        // this key, decided at write time.
        let expiration = self.expiration_of(
            &input.bucket,
            &input.key,
            &version.tags,
            version.last_modified,
        );
        // The computed checksums echo back on the response, as S3 does.
        let cks = |name: &str| version.checksums.get(name).cloned();
        let output = PutObjectOutput {
            e_tag: etag.parse().ok(),
            expiration,
            checksum_crc32: cks("CRC32"),
            checksum_crc32c: cks("CRC32C"),
            checksum_crc64nvme: cks("CRC64NVME"),
            checksum_sha1: cks("SHA1"),
            checksum_sha256: cks("SHA256"),
            // Only an Enabled bucket surfaces a version id on the write.
            version_id: (versioning == nauka_s3::VersioningState::Enabled).then_some(version_id),
            server_side_encryption: sse_info
                .as_ref()
                .filter(|i| !i.is_customer())
                .map(|i| i.mode.clone().into()),
            ssekms_key_id: sse_info.as_ref().and_then(|i| i.kms_key_id.clone()),
            sse_customer_algorithm: sse_info
                .as_ref()
                .filter(|i| i.is_customer())
                .map(|_| "AES256".into()),
            sse_customer_key_md5: sse_info
                .as_ref()
                .filter(|i| i.is_customer())
                .and_then(|i| i.key_md5.clone()),
            ..Default::default()
        };
        self.write(nauka_raft::types::AppCommand::PutObjectVersion {
            bucket: input.bucket,
            key: input.key,
            version: Box::new(version),
        })
        .await?;

        Ok(S3Response::new(output))
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
                // An Object-Lock bucket requires versioning; it cannot be
                // suspended.
                if bucket.object_lock_enabled {
                    return Err(s3_error!(
                        InvalidBucketState,
                        "Versioning cannot be suspended on a bucket with Object Lock"
                    ));
                }
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

    async fn post_object(
        &self,
        req: S3Request<PostObjectInput>,
    ) -> S3Result<S3Response<PostObjectOutput>> {
        // The browser-upload path. `s3s` has already done the protocol
        // work — multipart/form-data parsing, POST policy expiration and
        // condition checks, the form signature — so what is left is a
        // simplified PutObject fed from form fields.
        let mut input = req.input;
        let bucket_meta = self.require_bucket(&input.bucket)?;
        if !nauka_s3::naming::valid_key(&input.key) {
            return Err(s3_error!(InvalidArgument, "invalid key"));
        }
        let owner = self.object_owner_for(req.credentials.as_ref(), None);
        let acl = match &input.acl {
            Some(canned) => {
                let canned = canned.as_str();
                let block_public = bucket_meta
                    .public_access_block
                    .as_deref()
                    .and_then(pab_from_xml)
                    .is_some_and(|p| p.block_public_acls.unwrap_or(false));
                if block_public && nauka_s3::acl::canned_is_public(canned) {
                    return Err(s3_error!(AccessDenied, "public ACLs are blocked"));
                }
                let owner_id = owner.clone().unwrap_or_default();
                let bucket_owner = self.canonical_id_of(&bucket_meta.owner);
                nauka_s3::acl::canned_grants(canned, &owner_id, Some(&bucket_owner))
                    .map(|g| nauka_s3::acl::to_json(&g))
            }
            None => None,
        };

        // A browser POST can carry the same SSE fields a PUT sends as
        // headers — same validation, same real SSE-C encryption.
        let sse_req = validate_sse_request(
            input.server_side_encryption.as_ref().map(|s| s.as_str()),
            input.ssekms_key_id.as_deref(),
            input.sse_customer_algorithm.as_deref(),
            input.sse_customer_key.as_deref(),
            input.sse_customer_key_md5.as_deref(),
        )?;

        let mut hasher = s3s::checksum::ChecksumHasher::default();
        let tmp = self.state.tmp_dir.join(format!("s3-{}", uuid_like()));
        let (size, blake, md5) = write_body(input.body.take(), &tmp, &mut hasher)
            .await
            .map_err(|e| {
                let _ = std::fs::remove_file(&tmp);
                s3_error!(InternalError, "{e:#}")
            })?;
        let mut sse_info = sse_req.info;
        let (store_path, store_size, _store_hasher) = match (&sse_req.customer_key, size) {
            (Some(key), n) if n > 0 => {
                let ct_tmp = self.state.tmp_dir.join(format!("s3-{}", uuid_like()));
                let r = encrypt_to_tmp(key.clone(), tmp.clone(), ct_tmp.clone()).await;
                let _ = tokio::fs::remove_file(&tmp).await;
                let (ct_len, ct_hasher) = match r {
                    Ok(v) => v,
                    Err(e) => {
                        let _ = tokio::fs::remove_file(&ct_tmp).await;
                        return Err(e);
                    }
                };
                if let Some(i) = &mut sse_info {
                    i.segments = vec![ct_len];
                }
                (ct_tmp, ct_len, ct_hasher)
            }
            _ => (tmp, size, blake),
        };
        let content = if store_size == 0 {
            let _ = tokio::fs::remove_file(&store_path).await;
            None
        } else {
            let result =
                crate::api::dispatch_file(&self.state, &store_path, Some(input.key.clone()), None)
                    .await;
            let _ = tokio::fs::remove_file(&store_path).await;
            let (manifest, _degraded) = result.map_err(Self::dispatch_error)?;
            Some(manifest.file_hash)
        };

        let etag = nauka_s3::naming::etag_single(&md5);
        let versioning = self.versioning_of(&input.bucket);
        let version_id = Self::version_id_for(versioning);
        // The POST form's tagging field is the XML document, not the
        // URL-encoded header PUT uses.
        let tags = match input.tagging.as_deref() {
            Some(xml) => {
                let mut de = s3s::xml::Deserializer::new(xml.as_bytes());
                let tagging: Tagging = s3s::xml::Deserialize::deserialize(&mut de)
                    .map_err(|_| s3_error!(MalformedXML, "invalid tagging document"))?;
                tag_set_to_map(&tagging.tag_set, 10)?
            }
            None => BTreeMap::new(),
        };
        let version = nauka_s3::ObjectVersion {
            version_id: version_id.clone(),
            content,
            delete_marker: false,
            size,
            etag: etag.clone(),
            last_modified: Self::now(),
            content_type: Some(
                input
                    .content_type
                    .clone()
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "binary/octet-stream".into()),
            ),
            user_metadata: input
                .metadata
                .map(|m| m.into_iter().collect())
                .unwrap_or_default(),
            system_metadata: BTreeMap::new(),
            storage_class: input.storage_class.map(|s| s.as_str().to_owned()),
            tags,
            checksums: BTreeMap::new(),
            retention: None,
            legal_hold: false,
            sse: sse_info.as_ref().and_then(SseInfo::to_json),
            owner,
            acl,
        };
        self.write(nauka_raft::types::AppCommand::PutObjectVersion {
            bucket: input.bucket,
            key: input.key,
            version: Box::new(version),
        })
        .await?;
        Ok(S3Response::new(PostObjectOutput {
            e_tag: etag.parse().ok(),
            version_id: (versioning == nauka_s3::VersioningState::Enabled).then_some(version_id),
            server_side_encryption: sse_info
                .as_ref()
                .filter(|i| !i.is_customer())
                .map(|i| i.mode.clone().into()),
            ssekms_key_id: sse_info.as_ref().and_then(|i| i.kms_key_id.clone()),
            sse_customer_algorithm: sse_info
                .as_ref()
                .filter(|i| i.is_customer())
                .map(|_| "AES256".into()),
            sse_customer_key_md5: sse_info
                .as_ref()
                .filter(|i| i.is_customer())
                .and_then(|i| i.key_md5.clone()),
            ..Default::default()
        }))
    }

    async fn get_bucket_location(
        &self,
        req: S3Request<GetBucketLocationInput>,
    ) -> S3Result<S3Response<GetBucketLocationOutput>> {
        // Echo the LocationConstraint recorded at creation. A bucket
        // created without one (every older bucket too) reports the null
        // constraint, S3's us-east-1 convention. Third-party tooling
        // (rclone, warp, Terraform) calls this as a preamble — a 501
        // here breaks them before their first real request.
        let bucket = self.require_bucket(&req.input.bucket)?;
        Ok(S3Response::new(GetBucketLocationOutput {
            location_constraint: bucket.location.map(BucketLocationConstraint::from),
        }))
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
        let freshness = self.ensure_visible(&req.input.bucket, &req.input.key).await;
        self.require_bucket(&req.input.bucket)?;
        let s3 = self.state.app.app_state().s3;
        let entry = s3
            .objects
            .get(&(req.input.bucket.clone(), req.input.key.clone()))
            .ok_or_else(|| match freshness {
                nauka_raft::Freshness::Fresh => s3_error!(NoSuchKey),
                _ => Self::stale_read_error(),
            })?;
        let v = resolve_version(entry, req.input.version_id.as_deref())?;
        // A HEAD of an SSE-C object without (or with the wrong) customer
        // key is a 400, exactly like the GET.
        let sse_info = SseInfo::parse(&v.sse);
        match &sse_info {
            Some(i) if i.is_customer() => {
                require_customer_key(
                    i,
                    req.input.sse_customer_algorithm.as_deref(),
                    req.input.sse_customer_key.as_deref(),
                    req.input.sse_customer_key_md5.as_deref(),
                )?;
            }
            _ => {
                if req.input.sse_customer_key.is_some()
                    || req.input.sse_customer_algorithm.is_some()
                {
                    return Err(s3_error!(
                        InvalidArgument,
                        "the object was not stored with a customer-provided key"
                    ));
                }
            }
        }
        let sys = |k: &str| v.system_metadata.get(k).cloned();
        // Stored checksums come back only when the client opts in.
        let cks = |name: &str| {
            req.input
                .checksum_mode
                .as_ref()
                .filter(|m| m.as_str() == ChecksumMode::ENABLED)
                .and_then(|_| v.checksums.get(name).cloned())
        };
        let mut resp = S3Response::new(HeadObjectOutput {
            expiration: self.expiration_of(
                &req.input.bucket,
                &req.input.key,
                &v.tags,
                v.last_modified,
            ),
            checksum_crc32: cks("CRC32"),
            checksum_crc32c: cks("CRC32C"),
            checksum_crc64nvme: cks("CRC64NVME"),
            checksum_sha1: cks("SHA1"),
            checksum_sha256: cks("SHA256"),
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
            server_side_encryption: sse_info
                .as_ref()
                .filter(|i| !i.is_customer())
                .map(|i| i.mode.clone().into()),
            ssekms_key_id: sse_info.as_ref().and_then(|i| i.kms_key_id.clone()),
            sse_customer_algorithm: sse_info
                .as_ref()
                .filter(|i| i.is_customer())
                .map(|_| "AES256".into()),
            sse_customer_key_md5: sse_info
                .as_ref()
                .filter(|i| i.is_customer())
                .and_then(|i| i.key_md5.clone()),
            ..Default::default()
        });
        // HeadObjectOutput has no tag-count field, so set the header
        // directly, as S3 does.
        if !v.tags.is_empty() {
            if let Ok(val) = v.tags.len().to_string().parse() {
                resp.headers.insert("x-amz-tagging-count", val);
            }
        }
        set_object_lock_headers(&mut resp.headers, v);
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

    async fn put_bucket_lifecycle_configuration(
        &self,
        req: S3Request<PutBucketLifecycleConfigurationInput>,
    ) -> S3Result<S3Response<PutBucketLifecycleConfigurationOutput>> {
        let mut bucket = self.require_bucket(&req.input.bucket)?;
        let mut cfg = req
            .input
            .lifecycle_configuration
            .ok_or_else(|| s3_error!(MalformedXML, "missing lifecycle configuration"))?;
        validate_lifecycle_rules(&mut cfg.rules)?;
        bucket.lifecycle = Some(lifecycle_to_xml(&cfg)?);
        self.write(nauka_raft::types::AppCommand::UpdateBucket {
            name: req.input.bucket,
            bucket: Box::new(bucket),
        })
        .await?;
        Ok(S3Response::new(
            PutBucketLifecycleConfigurationOutput::default(),
        ))
    }

    async fn get_bucket_lifecycle_configuration(
        &self,
        req: S3Request<GetBucketLifecycleConfigurationInput>,
    ) -> S3Result<S3Response<GetBucketLifecycleConfigurationOutput>> {
        let bucket = self.require_bucket(&req.input.bucket)?;
        let cfg = bucket
            .lifecycle
            .as_deref()
            .and_then(lifecycle_from_xml)
            .ok_or_else(|| {
                s3_error!(
                    NoSuchLifecycleConfiguration,
                    "The lifecycle configuration does not exist"
                )
            })?;
        Ok(S3Response::new(GetBucketLifecycleConfigurationOutput {
            rules: Some(cfg.rules),
            ..Default::default()
        }))
    }

    async fn delete_bucket_lifecycle(
        &self,
        req: S3Request<DeleteBucketLifecycleInput>,
    ) -> S3Result<S3Response<DeleteBucketLifecycleOutput>> {
        let mut bucket = self.require_bucket(&req.input.bucket)?;
        // Deleting an absent configuration is still a 204, per S3.
        if bucket.lifecycle.take().is_some() {
            self.write(nauka_raft::types::AppCommand::UpdateBucket {
                name: req.input.bucket,
                bucket: Box::new(bucket),
            })
            .await?;
        }
        Ok(S3Response::new(DeleteBucketLifecycleOutput::default()))
    }

    async fn put_bucket_cors(
        &self,
        req: S3Request<PutBucketCorsInput>,
    ) -> S3Result<S3Response<PutBucketCorsOutput>> {
        let mut bucket = self.require_bucket(&req.input.bucket)?;
        let cfg = req.input.cors_configuration;
        for rule in &cfg.cors_rules {
            for method in &rule.allowed_methods {
                if !matches!(method.as_str(), "GET" | "PUT" | "POST" | "DELETE" | "HEAD") {
                    return Err(s3_error!(
                        InvalidRequest,
                        "AllowedMethod must be GET, PUT, POST, DELETE or HEAD"
                    ));
                }
            }
            // At most one wildcard per origin or header pattern, as AWS
            // enforces.
            let one_star = |s: &str| s.matches('*').count() <= 1;
            if !rule.allowed_origins.iter().all(|o| one_star(o)) {
                return Err(s3_error!(
                    InvalidRequest,
                    "an AllowedOrigin can have at most one wildcard"
                ));
            }
            if let Some(headers) = &rule.allowed_headers {
                if !headers.iter().all(|h| one_star(h)) {
                    return Err(s3_error!(
                        InvalidRequest,
                        "an AllowedHeader can have at most one wildcard"
                    ));
                }
            }
        }
        let mut buf = Vec::new();
        let mut ser = s3s::xml::Serializer::new(&mut buf);
        s3s::xml::Serialize::serialize(&cfg, &mut ser)
            .map_err(|e| s3_error!(InternalError, "serializing the CORS rules: {e}"))?;
        bucket.cors = Some(String::from_utf8(buf).map_err(|e| s3_error!(InternalError, "{e}"))?);
        self.write(nauka_raft::types::AppCommand::UpdateBucket {
            name: req.input.bucket,
            bucket: Box::new(bucket),
        })
        .await?;
        Ok(S3Response::new(PutBucketCorsOutput::default()))
    }

    async fn get_bucket_cors(
        &self,
        req: S3Request<GetBucketCorsInput>,
    ) -> S3Result<S3Response<GetBucketCorsOutput>> {
        let bucket = self.require_bucket(&req.input.bucket)?;
        let cfg = bucket
            .cors
            .as_deref()
            .and_then(cors_from_xml)
            .ok_or_else(|| {
                s3_error!(
                    NoSuchCORSConfiguration,
                    "The CORS configuration does not exist"
                )
            })?;
        Ok(S3Response::new(GetBucketCorsOutput {
            cors_rules: Some(cfg.cors_rules),
        }))
    }

    async fn delete_bucket_cors(
        &self,
        req: S3Request<DeleteBucketCorsInput>,
    ) -> S3Result<S3Response<DeleteBucketCorsOutput>> {
        let mut bucket = self.require_bucket(&req.input.bucket)?;
        // Deleting an absent configuration is still a 204, per S3.
        if bucket.cors.take().is_some() {
            self.write(nauka_raft::types::AppCommand::UpdateBucket {
                name: req.input.bucket,
                bucket: Box::new(bucket),
            })
            .await?;
        }
        Ok(S3Response::new(DeleteBucketCorsOutput::default()))
    }

    async fn put_bucket_policy(
        &self,
        req: S3Request<PutBucketPolicyInput>,
    ) -> S3Result<S3Response<PutBucketPolicyOutput>> {
        let mut bucket = self.require_bucket(&req.input.bucket)?;
        let raw = req.input.policy;
        let policy = match nauka_s3::Policy::parse(&raw) {
            Ok(p) => p,
            Err(nauka_s3::PolicyError::Malformed(m)) => {
                return Err(S3Error::with_message(s3s::S3ErrorCode::MalformedPolicy, m))
            }
            Err(nauka_s3::PolicyError::InvalidArgument(m)) => {
                return Err(s3_error!(InvalidArgument, "{m}"))
            }
        };
        // BlockPublicPolicy does what it says: once set, a policy that
        // would open the bucket to everyone is refused outright.
        if let Some(pab) = bucket.public_access_block.as_deref().and_then(pab_from_xml) {
            if pab.block_public_policy.unwrap_or(false) && policy.is_public() {
                return Err(s3_error!(AccessDenied, "public policies are blocked"));
            }
        }
        // Stored as the raw string: GET must round-trip the exact document.
        bucket.policy = Some(raw);
        self.write(nauka_raft::types::AppCommand::UpdateBucket {
            name: req.input.bucket,
            bucket: Box::new(bucket),
        })
        .await?;
        Ok(S3Response::new(PutBucketPolicyOutput::default()))
    }

    async fn get_bucket_policy(
        &self,
        req: S3Request<GetBucketPolicyInput>,
    ) -> S3Result<S3Response<GetBucketPolicyOutput>> {
        let bucket = self.require_bucket(&req.input.bucket)?;
        let policy = bucket.policy.ok_or_else(|| {
            S3Error::with_message(
                s3s::S3ErrorCode::NoSuchBucketPolicy,
                "The bucket policy does not exist",
            )
        })?;
        Ok(S3Response::new(GetBucketPolicyOutput {
            policy: Some(policy),
        }))
    }

    async fn delete_bucket_policy(
        &self,
        req: S3Request<DeleteBucketPolicyInput>,
    ) -> S3Result<S3Response<DeleteBucketPolicyOutput>> {
        let mut bucket = self.require_bucket(&req.input.bucket)?;
        // Deleting an absent policy is still a 204, like the other
        // bucket subresources.
        if bucket.policy.take().is_some() {
            self.write(nauka_raft::types::AppCommand::UpdateBucket {
                name: req.input.bucket,
                bucket: Box::new(bucket),
            })
            .await?;
        }
        Ok(S3Response::new(DeleteBucketPolicyOutput::default()))
    }

    async fn get_bucket_policy_status(
        &self,
        req: S3Request<GetBucketPolicyStatusInput>,
    ) -> S3Result<S3Response<GetBucketPolicyStatusOutput>> {
        let bucket = self.require_bucket(&req.input.bucket)?;
        // Public = a public canned ACL, or a policy granting to everyone.
        let public_acl = matches!(
            bucket.acl.as_deref(),
            Some("public-read") | Some("public-read-write") | Some("authenticated-read")
        );
        let public_policy = bucket
            .policy
            .as_deref()
            .and_then(|p| nauka_s3::Policy::parse(p).ok())
            .is_some_and(|p| p.is_public());
        Ok(S3Response::new(GetBucketPolicyStatusOutput {
            policy_status: Some(PolicyStatus {
                is_public: Some(public_acl || public_policy),
            }),
        }))
    }

    async fn put_public_access_block(
        &self,
        req: S3Request<PutPublicAccessBlockInput>,
    ) -> S3Result<S3Response<PutPublicAccessBlockOutput>> {
        let mut bucket = self.require_bucket(&req.input.bucket)?;
        let mut buf = Vec::new();
        let mut ser = s3s::xml::Serializer::new(&mut buf);
        s3s::xml::Serialize::serialize(&req.input.public_access_block_configuration, &mut ser)
            .map_err(|e| s3_error!(InternalError, "serializing the configuration: {e}"))?;
        bucket.public_access_block =
            Some(String::from_utf8(buf).map_err(|e| s3_error!(InternalError, "{e}"))?);
        self.write(nauka_raft::types::AppCommand::UpdateBucket {
            name: req.input.bucket,
            bucket: Box::new(bucket),
        })
        .await?;
        Ok(S3Response::new(PutPublicAccessBlockOutput::default()))
    }

    async fn get_public_access_block(
        &self,
        req: S3Request<GetPublicAccessBlockInput>,
    ) -> S3Result<S3Response<GetPublicAccessBlockOutput>> {
        let bucket = self.require_bucket(&req.input.bucket)?;
        let cfg = bucket
            .public_access_block
            .as_deref()
            .and_then(pab_from_xml)
            .ok_or_else(|| {
                custom_error(
                    "NoSuchPublicAccessBlockConfiguration",
                    hyper::StatusCode::NOT_FOUND,
                    "The public access block configuration was not found",
                )
            })?;
        Ok(S3Response::new(GetPublicAccessBlockOutput {
            public_access_block_configuration: Some(cfg),
        }))
    }

    async fn delete_public_access_block(
        &self,
        req: S3Request<DeletePublicAccessBlockInput>,
    ) -> S3Result<S3Response<DeletePublicAccessBlockOutput>> {
        let mut bucket = self.require_bucket(&req.input.bucket)?;
        if bucket.public_access_block.take().is_some() {
            self.write(nauka_raft::types::AppCommand::UpdateBucket {
                name: req.input.bucket,
                bucket: Box::new(bucket),
            })
            .await?;
        }
        Ok(S3Response::new(DeletePublicAccessBlockOutput::default()))
    }

    async fn put_bucket_encryption(
        &self,
        req: S3Request<PutBucketEncryptionInput>,
    ) -> S3Result<S3Response<PutBucketEncryptionOutput>> {
        let mut bucket = self.require_bucket(&req.input.bucket)?;
        let cfg = &req.input.server_side_encryption_configuration;
        for rule in &cfg.rules {
            let Some(default) = &rule.apply_server_side_encryption_by_default else {
                return Err(s3_error!(InvalidArgument, "a rule needs a default"));
            };
            if !matches!(default.sse_algorithm.as_str(), "AES256" | "aws:kms") {
                return Err(s3_error!(InvalidArgument, "unknown SSE algorithm"));
            }
        }
        let mut buf = Vec::new();
        let mut ser = s3s::xml::Serializer::new(&mut buf);
        s3s::xml::Serialize::serialize(cfg, &mut ser)
            .map_err(|e| s3_error!(InternalError, "serializing the configuration: {e}"))?;
        bucket.encryption =
            Some(String::from_utf8(buf).map_err(|e| s3_error!(InternalError, "{e}"))?);
        self.write(nauka_raft::types::AppCommand::UpdateBucket {
            name: req.input.bucket,
            bucket: Box::new(bucket),
        })
        .await?;
        Ok(S3Response::new(PutBucketEncryptionOutput::default()))
    }

    async fn get_bucket_encryption(
        &self,
        req: S3Request<GetBucketEncryptionInput>,
    ) -> S3Result<S3Response<GetBucketEncryptionOutput>> {
        let bucket = self.require_bucket(&req.input.bucket)?;
        let cfg = bucket
            .encryption
            .as_deref()
            .and_then(|xml| {
                let mut de = s3s::xml::Deserializer::new(xml.as_bytes());
                s3s::xml::Deserialize::deserialize(&mut de).ok()
            })
            .ok_or_else(|| {
                custom_error(
                    "ServerSideEncryptionConfigurationNotFoundError",
                    hyper::StatusCode::NOT_FOUND,
                    "The server side encryption configuration was not found",
                )
            })?;
        Ok(S3Response::new(GetBucketEncryptionOutput {
            server_side_encryption_configuration: Some(cfg),
        }))
    }

    async fn delete_bucket_encryption(
        &self,
        req: S3Request<DeleteBucketEncryptionInput>,
    ) -> S3Result<S3Response<DeleteBucketEncryptionOutput>> {
        let mut bucket = self.require_bucket(&req.input.bucket)?;
        // Deleting an absent configuration is still a 204.
        if bucket.encryption.take().is_some() {
            self.write(nauka_raft::types::AppCommand::UpdateBucket {
                name: req.input.bucket,
                bucket: Box::new(bucket),
            })
            .await?;
        }
        Ok(S3Response::new(DeleteBucketEncryptionOutput::default()))
    }

    async fn get_object_acl(
        &self,
        req: S3Request<GetObjectAclInput>,
    ) -> S3Result<S3Response<GetObjectAclOutput>> {
        let bucket = self.require_bucket(&req.input.bucket)?;
        let s3 = self.state.app.app_state().s3;
        let entry = s3
            .objects
            .get(&(req.input.bucket.clone(), req.input.key.clone()))
            .ok_or_else(|| s3_error!(NoSuchKey))?;
        let version = match req.input.version_id.as_deref() {
            Some(id) => entry.version(id).ok_or_else(|| s3_error!(NoSuchVersion))?,
            None => entry
                .current_content()
                .ok_or_else(|| s3_error!(NoSuchKey))?,
        };
        // The object's ACL belongs to the OBJECT owner — not necessarily
        // the bucket owner: an object uploaded by another key stays theirs
        // unless a grant handed it over. Reading it needs ownership or a
        // READ_ACP grant on the object.
        let fallback = self.canonical_id_of(&bucket.owner);
        let owner_id = version.owner.clone().unwrap_or_else(|| fallback.clone());
        let grants = object_grant_list(version, &fallback);
        let requester = req
            .credentials
            .as_ref()
            .map(|c| self.canonical_id_of(&c.access_key));
        let allowed = requester.as_deref() == Some(owner_id.as_str())
            || nauka_s3::acl::grants_allow(&grants, requester.as_deref(), "READ_ACP", false);
        if !allowed {
            return Err(s3_error!(AccessDenied));
        }
        Ok(S3Response::new(GetObjectAclOutput {
            owner: Some(Owner {
                display_name: self.display_name_of(&owner_id),
                id: Some(owner_id),
            }),
            grants: Some(self.to_s3_grants(&grants)),
            ..Default::default()
        }))
    }

    async fn put_object_acl(
        &self,
        req: S3Request<PutObjectAclInput>,
    ) -> S3Result<S3Response<PutObjectAclOutput>> {
        let bucket = self.require_bucket(&req.input.bucket)?;
        let s3 = self.state.app.app_state().s3;
        let entry = s3
            .objects
            .get(&(req.input.bucket.clone(), req.input.key.clone()))
            .ok_or_else(|| s3_error!(NoSuchKey))?;
        let version = match req.input.version_id.as_deref() {
            Some(id) => entry.version(id).ok_or_else(|| s3_error!(NoSuchVersion))?,
            None => entry
                .current_content()
                .ok_or_else(|| s3_error!(NoSuchKey))?,
        };
        let bucket_owner = self.canonical_id_of(&bucket.owner);
        let owner_id = version
            .owner
            .clone()
            .unwrap_or_else(|| bucket_owner.clone());
        let old_grants = object_grant_list(version, &bucket_owner);
        let requester = req
            .credentials
            .as_ref()
            .map(|c| self.canonical_id_of(&c.access_key));
        let allowed = requester.as_deref() == Some(owner_id.as_str())
            || nauka_s3::acl::grants_allow(&old_grants, requester.as_deref(), "WRITE_ACP", false);
        if !allowed {
            return Err(s3_error!(AccessDenied));
        }
        let block_public = bucket
            .public_access_block
            .as_deref()
            .and_then(pab_from_xml)
            .is_some_and(|p| p.block_public_acls.unwrap_or(false));
        let grants = if let Some(canned) = &req.input.acl {
            let canned = canned.as_str();
            if block_public && nauka_s3::acl::canned_is_public(canned) {
                return Err(s3_error!(AccessDenied, "public ACLs are blocked"));
            }
            nauka_s3::acl::canned_grants(canned, &owner_id, Some(&bucket_owner))
                .ok_or_else(|| s3_error!(InvalidArgument, "unknown canned ACL"))?
        } else if let Some(acp) = &req.input.access_control_policy {
            let grants = self.grants_from_acp(acp)?;
            if block_public && nauka_s3::acl::grants_are_public(&grants) {
                return Err(s3_error!(AccessDenied, "public ACLs are blocked"));
            }
            grants
        } else {
            return Err(s3_error!(InvalidArgument, "no ACL in the request"));
        };
        self.write(nauka_raft::types::AppCommand::SetObjectAcl {
            bucket: req.input.bucket,
            key: req.input.key,
            version_id: req.input.version_id.map(|v| v.to_string()),
            acl: Some(nauka_s3::acl::to_json(&grants)),
        })
        .await?;
        Ok(S3Response::new(PutObjectAclOutput::default()))
    }

    async fn get_bucket_acl(
        &self,
        req: S3Request<GetBucketAclInput>,
    ) -> S3Result<S3Response<GetBucketAclOutput>> {
        let bucket = self.require_bucket(&req.input.bucket)?;
        let owner_id = self.canonical_id_of(&bucket.owner);
        let grants = bucket_grant_list(&bucket, &owner_id);
        Ok(S3Response::new(GetBucketAclOutput {
            owner: Some(self.bucket_owner_block(&bucket)),
            grants: Some(self.to_s3_grants(&grants)),
        }))
    }

    async fn put_bucket_acl(
        &self,
        req: S3Request<PutBucketAclInput>,
    ) -> S3Result<S3Response<PutBucketAclOutput>> {
        let mut bucket = self.require_bucket(&req.input.bucket)?;
        let block_public = bucket
            .public_access_block
            .as_deref()
            .and_then(pab_from_xml)
            .is_some_and(|p| p.block_public_acls.unwrap_or(false));
        if let Some(canned) = &req.input.acl {
            let canned = canned.as_str();
            if nauka_s3::acl::canned_grants(canned, "x", Some("x")).is_none() {
                return Err(s3_error!(InvalidArgument, "unknown canned ACL"));
            }
            if block_public && nauka_s3::acl::canned_is_public(canned) {
                return Err(s3_error!(AccessDenied, "public ACLs are blocked"));
            }
            bucket.acl = Some(canned.to_owned());
            bucket.acl_grants = None;
        } else if let Some(acp) = &req.input.access_control_policy {
            let grants = self.grants_from_acp(acp)?;
            if block_public && nauka_s3::acl::grants_are_public(&grants) {
                return Err(s3_error!(AccessDenied, "public ACLs are blocked"));
            }
            bucket.acl = None;
            bucket.acl_grants = Some(nauka_s3::acl::to_json(&grants));
        } else {
            return Err(s3_error!(InvalidArgument, "no ACL in the request"));
        }
        self.write(nauka_raft::types::AppCommand::UpdateBucket {
            name: req.input.bucket,
            bucket: Box::new(bucket),
        })
        .await?;
        Ok(S3Response::new(PutBucketAclOutput::default()))
    }

    async fn put_object_lock_configuration(
        &self,
        req: S3Request<PutObjectLockConfigurationInput>,
    ) -> S3Result<S3Response<PutObjectLockConfigurationOutput>> {
        let mut bucket = self.require_bucket(&req.input.bucket)?;
        // Object Lock requires versioning: the configuration can be set on
        // any versioning-enabled bucket (and enables Object Lock on it),
        // but never on an unversioned or suspended one.
        if bucket.versioning != nauka_s3::VersioningState::Enabled {
            return Err(s3_error!(
                InvalidBucketState,
                "Object Lock requires a versioning-enabled bucket"
            ));
        }
        bucket.object_lock_enabled = true;
        // The configuration's ObjectLockEnabled must literally be "Enabled".
        if let Some(cfg) = &req.input.object_lock_configuration {
            if cfg.object_lock_enabled.as_ref().map(|e| e.as_str())
                != Some(ObjectLockEnabled::ENABLED)
            {
                return Err(s3_error!(MalformedXML, "ObjectLockEnabled must be Enabled"));
            }
        }
        // Validate the default rule: mode must be a known value, and days
        // and years are mutually exclusive and positive.
        if let Some(cfg) = &req.input.object_lock_configuration {
            if let Some(dr) = cfg.rule.as_ref().and_then(|r| r.default_retention.as_ref()) {
                match dr.mode.as_ref().map(|m| m.as_str()) {
                    Some(m)
                        if m == ObjectLockRetentionMode::GOVERNANCE
                            || m == ObjectLockRetentionMode::COMPLIANCE => {}
                    _ => return Err(s3_error!(MalformedXML, "invalid Object Lock mode")),
                }
                if dr.days.is_some() && dr.years.is_some() {
                    return Err(s3_error!(
                        MalformedXML,
                        "days and years are mutually exclusive"
                    ));
                }
                if dr.days.is_some_and(|d| d <= 0) || dr.years.is_some_and(|y| y <= 0) {
                    return Err(custom_error(
                        "InvalidRetentionPeriod",
                        hyper::StatusCode::BAD_REQUEST,
                        "the retention period must be a positive integer",
                    ));
                }
            }
        }
        bucket.object_lock_default = req
            .input
            .object_lock_configuration
            .as_ref()
            .and_then(|c| serde_json::to_string(&RetentionConfig::from_dto(c)).ok());
        self.write(nauka_raft::types::AppCommand::UpdateBucket {
            name: req.input.bucket,
            bucket: Box::new(bucket),
        })
        .await?;
        Ok(S3Response::new(PutObjectLockConfigurationOutput::default()))
    }

    async fn get_object_lock_configuration(
        &self,
        req: S3Request<GetObjectLockConfigurationInput>,
    ) -> S3Result<S3Response<GetObjectLockConfigurationOutput>> {
        let bucket = self.require_bucket(&req.input.bucket)?;
        if !bucket.object_lock_enabled {
            return Err(s3_error!(
                ObjectLockConfigurationNotFoundError,
                "Object Lock is not enabled for this bucket"
            ));
        }
        let cfg: Option<RetentionConfig> = bucket
            .object_lock_default
            .as_deref()
            .and_then(|s| serde_json::from_str(s).ok());
        Ok(S3Response::new(GetObjectLockConfigurationOutput {
            object_lock_configuration: Some(RetentionConfig::to_dto(cfg.as_ref())),
        }))
    }

    async fn put_object_retention(
        &self,
        req: S3Request<PutObjectRetentionInput>,
    ) -> S3Result<S3Response<PutObjectRetentionOutput>> {
        let bucket = self.require_bucket(&req.input.bucket)?;
        // Retention only exists on an Object-Lock bucket.
        if !bucket.object_lock_enabled {
            return Err(s3_error!(
                InvalidRequest,
                "Bucket is missing Object Lock Configuration"
            ));
        }
        let (mode, until) = match &req.input.retention {
            Some(r) => {
                let m = r.mode.as_ref().map(|m| m.as_str().to_string());
                // The mode must be a known value.
                if !matches!(
                    m.as_deref(),
                    Some(ObjectLockRetentionMode::GOVERNANCE)
                        | Some(ObjectLockRetentionMode::COMPLIANCE)
                ) {
                    return Err(s3_error!(MalformedXML, "invalid retention mode"));
                }
                (m, r.retain_until_date.as_ref().and_then(timestamp_secs))
            }
            None => (None, None),
        };
        // Shortening or removing a COMPLIANCE retention is never allowed;
        // a GOVERNANCE one only with the bypass header. Look at what is
        // there already.
        let current = self.retention_of(
            &req.input.bucket,
            &req.input.key,
            req.input.version_id.as_deref(),
        );
        if let Some((cur_mode, cur_until)) = &current {
            let reducing = until.unwrap_or(0) < *cur_until;
            let changing_mode = mode.as_deref() != Some(cur_mode.as_str());
            // A COMPLIANCE retention can neither be shortened nor have its
            // mode changed, ever.
            if cur_mode == ObjectLockRetentionMode::COMPLIANCE && (reducing || changing_mode) {
                return Err(s3_error!(
                    AccessDenied,
                    "a COMPLIANCE retention cannot be shortened or changed"
                ));
            }
            if cur_mode == ObjectLockRetentionMode::GOVERNANCE
                && (reducing || changing_mode)
                && !req.input.bypass_governance_retention.unwrap_or(false)
            {
                return Err(s3_error!(
                    AccessDenied,
                    "shortening or changing a GOVERNANCE retention needs bypass"
                ));
            }
        }
        let serialized = mode.as_ref().map(|m| {
            serde_json::to_string(&RetentionInfo {
                mode: m.clone(),
                until: until.unwrap_or(0),
            })
            .unwrap_or_default()
        });
        let resp = self
            .state
            .app
            .write(nauka_raft::types::AppCommand::SetObjectRetention {
                bucket: req.input.bucket,
                key: req.input.key,
                version_id: req.input.version_id,
                retention: serialized,
            })
            .await
            .map_err(|e| s3_error!(InternalError, "{e:#}"))?;
        if !resp.ok {
            return Err(s3_error!(NoSuchKey));
        }
        Ok(S3Response::new(PutObjectRetentionOutput::default()))
    }

    async fn get_object_retention(
        &self,
        req: S3Request<GetObjectRetentionInput>,
    ) -> S3Result<S3Response<GetObjectRetentionOutput>> {
        let b = self.require_bucket(&req.input.bucket)?;
        if !b.object_lock_enabled {
            return Err(s3_error!(
                InvalidRequest,
                "Bucket is missing Object Lock Configuration"
            ));
        }
        let s3 = self.state.app.app_state().s3;
        let entry = s3
            .objects
            .get(&(req.input.bucket, req.input.key))
            .ok_or_else(|| s3_error!(NoSuchKey))?;
        let v = resolve_version(entry, req.input.version_id.as_deref())?;
        let info: Option<RetentionInfo> = v
            .retention
            .as_deref()
            .and_then(|s| serde_json::from_str(s).ok());
        match info {
            Some(i) => Ok(S3Response::new(GetObjectRetentionOutput {
                retention: Some(ObjectLockRetention {
                    mode: i.mode.parse().ok(),
                    retain_until_date: Some(Self::timestamp(i.until)),
                }),
            })),
            None => Err(s3_error!(
                NoSuchObjectLockConfiguration,
                "the object has no retention"
            )),
        }
    }

    async fn put_object_legal_hold(
        &self,
        req: S3Request<PutObjectLegalHoldInput>,
    ) -> S3Result<S3Response<PutObjectLegalHoldOutput>> {
        let b = self.require_bucket(&req.input.bucket)?;
        if !b.object_lock_enabled {
            return Err(s3_error!(
                InvalidRequest,
                "Bucket is missing Object Lock Configuration"
            ));
        }
        // The status must be ON or OFF.
        let status = req
            .input
            .legal_hold
            .as_ref()
            .and_then(|h| h.status.as_ref());
        match status.map(|s| s.as_str()) {
            Some(ObjectLockLegalHoldStatus::ON) | Some(ObjectLockLegalHoldStatus::OFF) => {}
            _ => return Err(s3_error!(MalformedXML, "invalid legal hold status")),
        }
        let on = status.is_some_and(|s| s.as_str() == ObjectLockLegalHoldStatus::ON);
        let resp = self
            .state
            .app
            .write(nauka_raft::types::AppCommand::SetObjectLegalHold {
                bucket: req.input.bucket,
                key: req.input.key,
                version_id: req.input.version_id,
                on,
            })
            .await
            .map_err(|e| s3_error!(InternalError, "{e:#}"))?;
        if !resp.ok {
            return Err(s3_error!(NoSuchKey));
        }
        Ok(S3Response::new(PutObjectLegalHoldOutput::default()))
    }

    async fn get_object_legal_hold(
        &self,
        req: S3Request<GetObjectLegalHoldInput>,
    ) -> S3Result<S3Response<GetObjectLegalHoldOutput>> {
        let b = self.require_bucket(&req.input.bucket)?;
        if !b.object_lock_enabled {
            return Err(s3_error!(
                InvalidRequest,
                "Bucket is missing Object Lock Configuration"
            ));
        }
        let s3 = self.state.app.app_state().s3;
        let entry = s3
            .objects
            .get(&(req.input.bucket, req.input.key))
            .ok_or_else(|| s3_error!(NoSuchKey))?;
        let v = resolve_version(entry, req.input.version_id.as_deref())?;
        let status = if v.legal_hold {
            ObjectLockLegalHoldStatus::ON
        } else {
            ObjectLockLegalHoldStatus::OFF
        };
        Ok(S3Response::new(GetObjectLegalHoldOutput {
            legal_hold: Some(ObjectLockLegalHold {
                status: ObjectLockLegalHoldStatus::from_static(status).into(),
            }),
        }))
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
        // The stored checksums, when the client asked for that attribute.
        let checksum = req
            .input
            .object_attributes
            .iter()
            .any(|a| a.as_str() == ObjectAttributes::CHECKSUM)
            .then(|| Checksum {
                checksum_crc32: v.checksums.get("CRC32").cloned(),
                checksum_crc32c: v.checksums.get("CRC32C").cloned(),
                checksum_crc64nvme: v.checksums.get("CRC64NVME").cloned(),
                checksum_sha1: v.checksums.get("SHA1").cloned(),
                checksum_sha256: v.checksums.get("SHA256").cloned(),
                ..Default::default()
            });
        Ok(S3Response::new(GetObjectAttributesOutput {
            e_tag: bare_etag.parse().ok(),
            checksum,
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
        let freshness = self.ensure_visible(&input.bucket, &input.key).await;
        self.require_bucket(&input.bucket)?;
        let s3 = self.state.app.app_state().s3;
        let entry = s3
            .objects
            .get(&(input.bucket.clone(), input.key.clone()))
            .ok_or_else(|| match freshness {
                nauka_raft::Freshness::Fresh => s3_error!(NoSuchKey),
                _ => Self::stale_read_error(),
            })?;
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

        // An SSE-C object only opens with the key it was written under;
        // whatever else happens, the cluster alone cannot produce the
        // plaintext.
        let sse_info = SseInfo::parse(&v.sse);
        let customer_key = match &sse_info {
            Some(i) if i.is_customer() => Some(require_customer_key(
                i,
                input.sse_customer_algorithm.as_deref(),
                input.sse_customer_key.as_deref(),
                input.sse_customer_key_md5.as_deref(),
            )?),
            _ => {
                if input.sse_customer_key.is_some() || input.sse_customer_algorithm.is_some() {
                    return Err(s3_error!(
                        InvalidArgument,
                        "the object was not stored with a customer-provided key"
                    ));
                }
                None
            }
        };

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
                match (&customer_key, &sse_info) {
                    (Some(key), Some(info)) => {
                        // The stored bytes are ciphertext: reconstruct all
                        // of it, decrypt stream by stream, then serve the
                        // requested window of the plaintext.
                        let ct = reconstruct_range(
                            &self.state,
                            &manifest,
                            0,
                            manifest.file_size.saturating_sub(1),
                        )
                        .await
                        .map_err(|e| s3_error!(InternalError, "{e:#}"))?;
                        let plain = decrypt_segments(key, &ct, &info.segments)?;
                        let window = plain
                            .get(start as usize..=(end as usize).min(plain.len().saturating_sub(1)))
                            .unwrap_or(&[])
                            .to_vec();
                        StreamingBlob::from(Body::from(window))
                    }
                    _ => {
                        let bytes = reconstruct_range(&self.state, &manifest, start, end)
                            .await
                            .map_err(|e| s3_error!(InternalError, "{e:#}"))?;
                        StreamingBlob::from(Body::from(bytes))
                    }
                }
            }
        };

        // A response header override lets the client ask GET to echo a
        // different value (?response-cache-control=…), which S3 supports.
        let sys = |k: &str| v.system_metadata.get(k).cloned();
        // Stored checksums come back only when the client opts in.
        let cks = |name: &str| {
            input
                .checksum_mode
                .as_ref()
                .filter(|m| m.as_str() == ChecksumMode::ENABLED)
                .and_then(|_| v.checksums.get(name).cloned())
        };
        // Client egress, counted when the response is committed to.
        self.state.egress.add(length);
        let mut resp = S3Response::new(GetObjectOutput {
            expiration: self.expiration_of(&input.bucket, &input.key, &v.tags, v.last_modified),
            checksum_crc32: cks("CRC32"),
            checksum_crc32c: cks("CRC32C"),
            checksum_crc64nvme: cks("CRC64NVME"),
            checksum_sha1: cks("SHA1"),
            checksum_sha256: cks("SHA256"),
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
            server_side_encryption: sse_info
                .as_ref()
                .filter(|i| !i.is_customer())
                .map(|i| i.mode.clone().into()),
            ssekms_key_id: sse_info.as_ref().and_then(|i| i.kms_key_id.clone()),
            sse_customer_algorithm: sse_info
                .as_ref()
                .filter(|i| i.is_customer())
                .map(|_| "AES256".into()),
            sse_customer_key_md5: sse_info
                .as_ref()
                .filter(|i| i.is_customer())
                .and_then(|i| i.key_md5.clone()),
            ..Default::default()
        });
        set_object_lock_headers(&mut resp.headers, &v);
        Ok(resp)
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
        // An SSE-C source is ciphertext this cluster cannot read: copying
        // it needs the customer key on both sides, which is not built.
        if SseInfo::parse(&source.sse).is_some_and(|i| i.is_customer()) {
            return Err(s3_error!(
                NotImplemented,
                "copying an object stored with a customer-provided key"
            ));
        }

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
            // The copy is a new object owned by whoever made it, not by
            // the source's owner.
            owner: self.object_owner_for(req.credentials.as_ref(), None),
            // The copy is a fresh object: it does NOT inherit the
            // source's ACL, it starts private under its new owner.
            acl: None,
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
        // SSE is declared at initiation; parts must then present the same
        // customer key, and the completed object records the mode.
        let sse_req = validate_sse_request(
            input.server_side_encryption.as_ref().map(|s| s.as_str()),
            input.ssekms_key_id.as_deref(),
            input.sse_customer_algorithm.as_deref(),
            input.sse_customer_key.as_deref(),
            input.sse_customer_key_md5.as_deref(),
        )?;
        let upload_id = uuid_like();
        let upload = nauka_s3::MultipartUpload {
            upload_id: upload_id.clone(),
            bucket: input.bucket.clone(),
            key: input.key.clone(),
            initiated: Self::now(),
            owner: req.credentials.map(|c| c.access_key).unwrap_or_default(),
            content_type: Some(
                input
                    .content_type
                    .clone()
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "binary/octet-stream".into()),
            ),
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
            sse: sse_req.info.as_ref().and_then(SseInfo::to_json),
            retention: retention_from_headers(
                input.object_lock_mode.as_ref().map(|m| m.as_str()),
                input.object_lock_retain_until_date.as_ref(),
            ),
            legal_hold: input
                .object_lock_legal_hold_status
                .as_ref()
                .is_some_and(|s| s.as_str() == ObjectLockLegalHoldStatus::ON),
            parts: BTreeMap::new(),
        };
        self.write(nauka_raft::types::AppCommand::PutUpload(Box::new(upload)))
            .await?;
        Ok(S3Response::new(CreateMultipartUploadOutput {
            bucket: Some(input.bucket),
            key: Some(input.key),
            upload_id: Some(upload_id),
            server_side_encryption: sse_req
                .info
                .as_ref()
                .filter(|i| !i.is_customer())
                .map(|i| i.mode.clone().into()),
            ssekms_key_id: sse_req.info.as_ref().and_then(|i| i.kms_key_id.clone()),
            sse_customer_algorithm: sse_req
                .info
                .as_ref()
                .filter(|i| i.is_customer())
                .map(|_| "AES256".into()),
            sse_customer_key_md5: sse_req
                .info
                .as_ref()
                .filter(|i| i.is_customer())
                .and_then(|i| i.key_md5.clone()),
            ..Default::default()
        }))
    }

    async fn upload_part(
        &self,
        req: S3Request<UploadPartInput>,
    ) -> S3Result<S3Response<UploadPartOutput>> {
        let input = req.input;
        let upload_sse = match self.state.app.app_state().s3.uploads.get(&input.upload_id) {
            Some(u) => SseInfo::parse(&u.sse),
            None => return Err(s3_error!(NoSuchUpload)),
        };
        // An SSE-C upload requires every part to arrive with the SAME
        // customer key it was initiated under.
        let customer_key = match &upload_sse {
            Some(i) if i.is_customer() => Some(require_customer_key(
                i,
                input.sse_customer_algorithm.as_deref(),
                input.sse_customer_key.as_deref(),
                input.sse_customer_key_md5.as_deref(),
            )?),
            _ => None,
        };
        let part_number = u32::try_from(input.part_number)
            .ok()
            .filter(|n| (1..=10_000).contains(n))
            .ok_or_else(|| s3_error!(InvalidArgument, "part number must be 1..=10000"))?;

        let tmp = self.state.tmp_dir.join(format!("s3p-{}", uuid_like()));
        let mut hasher = s3s::checksum::ChecksumHasher::default();
        let (size, blake, md5) = write_body(input.body, &tmp, &mut hasher)
            .await
            .map_err(|e| {
                let _ = std::fs::remove_file(&tmp);
                s3_error!(InternalError, "{e:#}")
            })?;
        if size == 0 {
            let _ = tokio::fs::remove_file(&tmp).await;
            return Err(s3_error!(InvalidArgument, "an empty part is not allowed"));
        }
        // SSE-C: what the cluster stores for this part is ciphertext,
        // encrypted with the customer's key. The ETag stays the MD5 of
        // the plaintext, which is what the client compares.
        let (store_path, store_size, _store_hasher, plain_size) = match &customer_key {
            Some(key) => {
                let ct_tmp = self.state.tmp_dir.join(format!("s3p-{}", uuid_like()));
                let r = encrypt_to_tmp(key.clone(), tmp.clone(), ct_tmp.clone()).await;
                let _ = tokio::fs::remove_file(&tmp).await;
                let (ct_len, ct_hasher) = match r {
                    Ok(v) => v,
                    Err(e) => {
                        let _ = tokio::fs::remove_file(&ct_tmp).await;
                        return Err(e);
                    }
                };
                (ct_tmp, ct_len, ct_hasher, Some(size))
            }
            None => (tmp, size, blake, None),
        };
        let result = crate::api::dispatch_file(
            &self.state,
            &store_path,
            Some(format!("{}#part{}", input.key, part_number)),
            None,
        )
        .await;
        let _ = tokio::fs::remove_file(&store_path).await;
        let (manifest, _) = result.map_err(Self::dispatch_error)?;

        let etag = nauka_s3::naming::etag_single(&md5);
        // One part at a time: parts arrive concurrently, so the merge
        // belongs in the state machine. Re-uploading a part replaces it,
        // as S3 allows.
        self.write(nauka_raft::types::AppCommand::PutUploadPart {
            upload_id: input.upload_id.clone(),
            part_number,
            part: Box::new(nauka_s3::UploadedPart {
                content: manifest.file_hash,
                size: store_size,
                plain_size,
                etag: etag.clone(),
                last_modified: Self::now(),
                checksums: BTreeMap::new(),
            }),
        })
        .await?;
        Ok(S3Response::new(UploadPartOutput {
            e_tag: etag.parse().ok(),
            sse_customer_algorithm: customer_key.as_ref().map(|_| "AES256".into()),
            sse_customer_key_md5: upload_sse
                .as_ref()
                .filter(|i| i.is_customer())
                .and_then(|i| i.key_md5.clone()),
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
            .any(|p| p.plain_size.unwrap_or(p.size) < MIN_PART)
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
        // Stored total (ciphertext for SSE-C) sizes the manifest; the
        // plaintext total is what listings and Content-Length announce.
        let total: u64 = chosen.iter().map(|p| p.size).sum();
        let total_plain: u64 = chosen.iter().map(|p| p.plain_size.unwrap_or(p.size)).sum();
        // Each part is an independent encryption stream; the segment
        // lengths let a GET decrypt them back in order.
        let sse = match SseInfo::parse(&upload.sse) {
            Some(mut i) if i.is_customer() => {
                i.segments = chosen.iter().map(|p| p.size).collect();
                i.to_json()
            }
            _ => upload.sse.clone(),
        };

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
            size: total_plain,
            etag: etag.clone(),
            last_modified: Self::now(),
            content_type: upload.content_type.clone(),
            user_metadata: upload.user_metadata.clone(),
            system_metadata: upload.system_metadata.clone(),
            storage_class: upload.storage_class.clone(),
            tags: upload.tags.clone(),
            checksums: BTreeMap::new(),
            retention: upload.retention.clone(),
            legal_hold: upload.legal_hold,
            sse,
            owner: Some(self.canonical_id_of(&upload.owner)),
            acl: None,
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
                // The size a client sees is the bytes it sent — the
                // plaintext, when the part is stored encrypted.
                size: Some(p.plain_size.unwrap_or(p.size) as i64),
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
                .delete_one(
                    &bucket,
                    &obj.key,
                    obj.version_id.as_deref(),
                    req.input.bypass_governance_retention.unwrap_or(false),
                )
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
                    // Surface the real code (AccessDenied on a locked
                    // version), not a blanket InternalError.
                    code: Some(e.code().as_str().to_string()),
                    message: e.message().map(|m| m.to_string()),
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
                req.input.bypass_governance_retention.unwrap_or(false),
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
        // Accepts the `tenant:bucket` form; the flat namespace makes it
        // the same bucket.
        let bucket = tenant_suffix(&req.input.bucket).to_string();
        // S3 listings are strongly consistent: an acked PUT is in the next
        // LIST. Unlike GET/HEAD there is no "miss" to detect — an absent
        // entry is just silently absent — so every listing catches up
        // first (the leader query is batched: a wave of listings shares
        // one round-trip), and a listing served from a confirmably-stale
        // node would silently omit entries, so it answers SlowDown.
        if self.state.app.catch_up_with_leader().await != nauka_raft::Freshness::Fresh {
            return Err(Self::stale_read_error());
        }
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
        let bucket = tenant_suffix(&req.input.bucket).to_string();
        // Strong listing consistency, same as v2.
        if self.state.app.catch_up_with_leader().await != nauka_raft::Freshness::Fresh {
            return Err(Self::stale_read_error());
        }
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
        // Strong listing consistency, same as the object listings.
        if self.state.app.catch_up_with_leader().await != nauka_raft::Freshness::Fresh {
            return Err(Self::stale_read_error());
        }
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
    checksums: &mut s3s::checksum::ChecksumHasher,
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
            checksums.update(&chunk);
            file.write_all(&chunk).await?;
            size += chunk.len() as u64;
        }
    }
    file.flush().await?;
    Ok((size, blake, md5.finalize().into()))
}

/// The checksum hasher a PUT needs: an algorithm is enabled by the
/// declared checksum algorithm or by an explicit value header — computing
/// only what was asked keeps the common no-checksum PUT free.
fn checksum_hasher_for(input: &PutObjectInput) -> s3s::checksum::ChecksumHasher {
    let mut h = s3s::checksum::ChecksumHasher::default();
    let algo = input.checksum_algorithm.as_ref().map(|a| a.as_str());
    if input.checksum_crc32.is_some() || algo == Some(ChecksumAlgorithm::CRC32) {
        h.crc32 = Some(Default::default());
    }
    if input.checksum_crc32c.is_some() || algo == Some(ChecksumAlgorithm::CRC32C) {
        h.crc32c = Some(Default::default());
    }
    if input.checksum_sha1.is_some() || algo == Some(ChecksumAlgorithm::SHA1) {
        h.sha1 = Some(Default::default());
    }
    if input.checksum_sha256.is_some() || algo == Some(ChecksumAlgorithm::SHA256) {
        h.sha256 = Some(Default::default());
    }
    if input.checksum_crc64nvme.is_some() || algo == Some(ChecksumAlgorithm::CRC64NVME) {
        h.crc64nvme = Some(Default::default());
    }
    h
}

/// Verifies every checksum the client sent against the computed ones —
/// a mismatch is BadDigest, as AWS answers — and returns the algorithm →
/// base64 map the object version stores.
fn verify_checksums(
    input: &PutObjectInput,
    computed: Checksum,
) -> S3Result<BTreeMap<String, String>> {
    let pairs = [
        ("CRC32", &input.checksum_crc32, computed.checksum_crc32),
        ("CRC32C", &input.checksum_crc32c, computed.checksum_crc32c),
        (
            "CRC64NVME",
            &input.checksum_crc64nvme,
            computed.checksum_crc64nvme,
        ),
        ("SHA1", &input.checksum_sha1, computed.checksum_sha1),
        ("SHA256", &input.checksum_sha256, computed.checksum_sha256),
    ];
    let mut out = BTreeMap::new();
    for (name, provided, computed) in pairs {
        let Some(computed) = computed else { continue };
        if provided.as_deref().is_some_and(|p| p != computed) {
            return Err(s3_error!(
                BadDigest,
                "The {name} you specified did not match the calculated checksum."
            ));
        }
        out.insert(name.to_string(), computed);
    }
    Ok(out)
}

fn uuid_like() -> String {
    use rand::Rng;
    let mut b = [0u8; 16];
    rand::thread_rng().fill(&mut b);
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// The access decision for a request `s3s` has already authenticated (or
/// found unsigned). Authorization is AWS-shaped:
///
/// 1. the bucket owner (the key that created it) may do anything to it;
/// 2. an explicit per-bucket credential grant opens what it names;
/// 3. the bucket policy is evaluated IAM-style — an Allow admits any
///    principal it names (including anonymous), an explicit Deny is final;
/// 4. anonymously, a `public-read` canned ACL still allows plain reads;
/// 5. everything else is AccessDenied.
///
/// A bucket that does not exist passes through: the operation answers
/// NoSuchBucket, which reveals nothing the name didn't.
struct NaukaAccess {
    state: Arc<ApiState>,
}

/// The IAM action an operation asks for. Bucket subresource ops follow
/// the `s3:{OpName}` convention closely enough that the fallback covers
/// them; the object and listing ops that differ are mapped explicitly.
fn s3_action_of(op: &str) -> String {
    match op {
        "ListObjects" | "ListObjectsV2" | "HeadBucket" => "s3:ListBucket".into(),
        "ListObjectVersions" => "s3:ListBucketVersions".into(),
        "ListMultipartUploads" => "s3:ListBucketMultipartUploads".into(),
        "GetObject" | "HeadObject" | "GetObjectAttributes" => "s3:GetObject".into(),
        // Every stage of writing an object is the same permission — with
        // one caveat the conformance suite checks: they all evaluate
        // against the OBJECT resource, never the bucket.
        "PutObject"
        | "CopyObject"
        | "CreateMultipartUpload"
        | "UploadPart"
        | "UploadPartCopy"
        | "CompleteMultipartUpload" => "s3:PutObject".into(),
        "DeleteObject" | "DeleteObjects" => "s3:DeleteObject".into(),
        "AbortMultipartUpload" => "s3:AbortMultipartUpload".into(),
        "ListParts" => "s3:ListMultipartUploadParts".into(),
        _ => format!("s3:{op}"),
    }
}

/// The coarse grant class an operation needs, for the explicit per-bucket
/// credential grants (`BucketPermission`).
fn grant_class_of(op: &str, is_object: bool) -> nauka_s3::Action {
    if op.starts_with("Get") || op.starts_with("Head") || op.starts_with("List") {
        nauka_s3::Action::Read
    } else if is_object || op == "DeleteObjects" {
        nauka_s3::Action::Write
    } else {
        // Everything else touches the bucket itself or its configuration.
        nauka_s3::Action::Own
    }
}

/// The grant class as a metric label. Bounded by construction, and the
/// cheapest useful split of S3 traffic: reads behave nothing like writes.
fn class_label(action: nauka_s3::Action) -> &'static str {
    match action {
        nauka_s3::Action::Read => "read",
        nauka_s3::Action::Write => "write",
        nauka_s3::Action::Own => "own",
    }
}

/// Strips an RGW-style tenant prefix: `tenant:bucket` and `:bucket` both
/// address `bucket`. Nauka is single-tenant — the syntax is accepted so
/// tenanted clients (and the conformance suite's) can name buckets, but
/// it resolves to the same flat namespace.
fn tenant_suffix(name: &str) -> &str {
    name.rsplit_once(':').map_or(name, |(_, b)| b)
}

/// A bucket's effective grant list: the explicit grants when PutBucketAcl
/// stored some, the expansion of its canned ACL otherwise (private when
/// neither was ever set). `owner_canonical` is the bucket owner's
/// canonical id.
fn bucket_grant_list(
    bucket: &nauka_s3::Bucket,
    owner_canonical: &str,
) -> Vec<nauka_s3::acl::AclGrant> {
    if let Some(g) = bucket
        .acl_grants
        .as_deref()
        .and_then(nauka_s3::acl::from_json)
    {
        return g;
    }
    let canned = bucket.acl.as_deref().unwrap_or("private");
    nauka_s3::acl::canned_grants(canned, owner_canonical, None)
        .or_else(|| nauka_s3::acl::canned_grants("private", owner_canonical, None))
        .unwrap_or_default()
}

/// An object version's effective grant list: its stored ACL, or the
/// private default — its owner (falling back to the bucket owner for
/// pre-ownership versions) with FULL_CONTROL.
fn object_grant_list(
    version: &nauka_s3::ObjectVersion,
    fallback_owner: &str,
) -> Vec<nauka_s3::acl::AclGrant> {
    if let Some(g) = version.acl.as_deref().and_then(nauka_s3::acl::from_json) {
        return g;
    }
    let owner = version.owner.as_deref().unwrap_or(fallback_owner);
    vec![nauka_s3::acl::AclGrant::canonical(owner, "FULL_CONTROL")]
}

/// Bucket names as AWS validates them, plus the `tenant:bucket` form.
struct NaukaNameValidation;

impl s3s::validation::NameValidation for NaukaNameValidation {
    fn validate_bucket_name(&self, name: &str) -> bool {
        match name.rsplit_once(':') {
            Some((_, suffix)) => s3s::path::check_bucket_name(suffix),
            None => s3s::path::check_bucket_name(name),
        }
    }
}

#[async_trait::async_trait]
impl s3s::access::S3Access for NaukaAccess {
    async fn check(&self, cx: &mut s3s::access::S3AccessContext<'_>) -> S3Result<()> {
        use nauka_s3::policy::{Decision, Requester};

        let op = cx.s3_op().name();
        // The only place `s3s` exposes a canonical operation name. Recorded
        // before any early return below, so every routed request is named
        // even when the authorization decision is delegated elsewhere.
        let is_object = matches!(cx.s3_path(), s3s::path::S3Path::Object { .. });
        crate::telemetry::s3::set_op(op, class_label(grant_class_of(op, is_object)));
        // The object-ACL ops authorize in their handlers: their owner is
        // the OBJECT's owner, which only the object itself knows.
        if matches!(op, "GetObjectAcl" | "PutObjectAcl") {
            return Ok(());
        }
        let (bucket_raw, key) = match cx.s3_path() {
            s3s::path::S3Path::Bucket { bucket } => (bucket.to_string(), None),
            s3s::path::S3Path::Object { bucket, key } => {
                (bucket.to_string(), Some(key.to_string()))
            }
            // The service root (ListBuckets): any signed identity, no
            // anonymous listing.
            s3s::path::S3Path::Root => {
                return if cx.credentials().is_some() {
                    Ok(())
                } else {
                    Err(s3_error!(AccessDenied, "Signature is required"))
                }
            }
        };
        let bucket_name = tenant_suffix(&bucket_raw).to_string();
        if op == "CreateBucket" {
            // Any authenticated key may create buckets (it becomes their
            // owner); anonymous creation is never allowed.
            return if cx.credentials().is_some() {
                Ok(())
            } else {
                Err(s3_error!(AccessDenied, "Signature is required"))
            };
        }
        let s3 = self.state.app.app_state().s3;
        let Some(b) = s3.buckets.get(&bucket_name) else {
            return Ok(());
        };

        // SSE headers are write-side only: declaring an encryption on a
        // read is a client error, and `s3s` has no input field to carry
        // it, so it is refused here.
        if matches!(op, "GetObject" | "HeadObject")
            && cx.headers().contains_key("x-amz-server-side-encryption")
        {
            return Err(s3_error!(
                InvalidArgument,
                "x-amz-server-side-encryption is not valid on a read"
            ));
        }

        let is_owner = cx.credentials().is_some_and(|c| b.owner == c.access_key);

        // 1. The bucket policy first: an explicit Deny beats every ACL,
        // grant — and the owner (the suite denies the owner's own
        // unencrypted uploads this way). The one exemption is the policy
        // subresource itself for the owner, so a bad policy can always
        // be repaired, as on AWS.
        let requester_id = cx.credentials().map(|c| {
            s3.credentials
                .get(&c.access_key)
                .map(|cr| cr.canonical_id().to_owned())
                .unwrap_or_else(|| c.access_key.clone())
        });
        let who = match (cx.credentials(), &requester_id) {
            (Some(c), Some(id)) => Requester::Key {
                access_key: &c.access_key,
                user_id: id,
            },
            _ => Requester::Anonymous,
        };
        let policy = b
            .policy
            .as_deref()
            .and_then(|p| nauka_s3::Policy::parse(p).ok());
        let bucket_arn = format!("arn:aws:s3:::{bucket_name}");
        let ctx = policy_context(cx);
        let mut allowed_by_policy = false;
        if let Some(pol) = &policy {
            let resource = match &key {
                Some(k) => format!("arn:aws:s3:::{bucket_name}/{k}"),
                None => bucket_arn.clone(),
            };
            let lockout_proof = is_owner
                && matches!(
                    op,
                    "PutBucketPolicy" | "GetBucketPolicy" | "DeleteBucketPolicy"
                );
            match pol.evaluate(who, &s3_action_of(op), &resource, &ctx) {
                Decision::Deny if !lockout_proof => return Err(s3_error!(AccessDenied)),
                Decision::Deny => {}
                Decision::Allow => allowed_by_policy = true,
                Decision::NoMatch => {}
            }
        }

        // 2. The bucket owner: everything else on their bucket is theirs.
        if is_owner {
            return Ok(());
        }

        // 3. Explicit per-bucket credential grants.
        if let Some(c) = cx.credentials() {
            if let Some(cred) = s3.credentials.get(&c.access_key) {
                if cred.allows(&bucket_name, grant_class_of(op, key.is_some())) {
                    return Ok(());
                }
            }
        }

        // 4. The ACLs. Bucket ACL answers listing (READ), writing keys
        // (WRITE) and the ACL subresource (READ_ACP/WRITE_ACP); reading
        // an object is the OBJECT ACL's call — a public-read bucket does
        // not open a private object, and vice versa.
        let ignore_public = b
            .public_access_block
            .as_deref()
            .and_then(pab_from_xml)
            .is_some_and(|p| p.ignore_public_acls.unwrap_or(false));
        let owner_canonical = s3
            .credentials
            .get(&b.owner)
            .map(|c| c.canonical_id().to_owned())
            .unwrap_or_else(|| b.owner.clone());
        let bucket_grants = bucket_grant_list(b, &owner_canonical);
        let acl_allows = |perm: &str| {
            nauka_s3::acl::grants_allow(
                &bucket_grants,
                requester_id.as_deref(),
                perm,
                ignore_public,
            )
        };
        let bucket_list_via_acl = acl_allows("READ");
        let acl_verdict = match op {
            "ListObjects"
            | "ListObjectsV2"
            | "ListObjectVersions"
            | "HeadBucket"
            | "ListMultipartUploads" => bucket_list_via_acl,
            "GetBucketAcl" => acl_allows("READ_ACP"),
            "PutBucketAcl" => acl_allows("WRITE_ACP"),
            "GetObject" | "HeadObject" | "GetObjectAttributes" => {
                // The current version's own grant list.
                key.as_ref()
                    .and_then(|k| s3.objects.get(&(bucket_name.clone(), k.clone())))
                    .and_then(|e| e.current_content())
                    .is_some_and(|v| {
                        nauka_s3::acl::grants_allow(
                            &object_grant_list(v, &owner_canonical),
                            requester_id.as_deref(),
                            "READ",
                            ignore_public,
                        )
                    })
            }
            // A browser POST addresses the bucket, not a key — same WRITE
            // permission as any other way of creating an object.
            "DeleteObjects" | "PostObject" | "PutObject" => acl_allows("WRITE"),
            // Writing any key — PUT, DELETE, copy destination, all the
            // multipart stages — is the bucket's WRITE permission. Reads
            // of object subresources (tagging, retention…) are not
            // ACL-governed; they fall through to the policy.
            _ if key.is_some()
                && !op.starts_with("Get")
                && !op.starts_with("Head")
                && !op.starts_with("List") =>
            {
                acl_allows("WRITE")
            }
            _ => false,
        };
        if acl_verdict {
            return Ok(());
        }
        if allowed_by_policy {
            return Ok(());
        }

        // 5. The AWS 404-vs-403 rule: a denied read of a key that does
        // not exist answers NoSuchKey when the caller could learn its
        // absence by listing anyway — via the policy or the bucket ACL.
        if let (true, Some(k)) = (matches!(op, "GetObject" | "HeadObject"), &key) {
            let gone = !s3.objects.contains_key(&(bucket_name.clone(), k.clone()));
            if gone {
                let list_via_policy = policy.as_ref().is_some_and(|pol| {
                    let mut lctx = ctx.clone();
                    lctx.insert("s3:prefix".into(), k.clone());
                    pol.evaluate(who, "s3:ListBucket", &bucket_arn, &lctx) == Decision::Allow
                });
                if list_via_policy || bucket_list_via_acl {
                    return Err(s3_error!(NoSuchKey));
                }
            }
        }
        Err(s3_error!(AccessDenied))
    }
}

/// The condition context of one request: only the keys this request
/// actually carries. Values come from the query string (the listing
/// parameters) and the handful of headers policies condition on.
fn policy_context(cx: &s3s::access::S3AccessContext<'_>) -> BTreeMap<String, String> {
    let mut ctx = BTreeMap::new();
    if let Some(q) = cx.uri().query() {
        for pair in q.split('&') {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            let decode = |s: &str| {
                percent_encoding::percent_decode_str(&s.replace('+', " "))
                    .decode_utf8_lossy()
                    .into_owned()
            };
            match k {
                "prefix" => {
                    ctx.insert("s3:prefix".into(), decode(v));
                }
                "delimiter" => {
                    ctx.insert("s3:delimiter".into(), decode(v));
                }
                "max-keys" => {
                    ctx.insert("s3:max-keys".into(), decode(v));
                }
                _ => {}
            }
        }
    }
    let header = |name: &str| {
        cx.headers()
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(String::from)
    };
    if let Some(v) = header("referer") {
        ctx.insert("aws:Referer".into(), v);
    }
    if let Some(v) = header("x-amz-grant-full-control") {
        ctx.insert("s3:x-amz-grant-full-control".into(), v);
    }
    if let Some(v) = header("x-amz-acl") {
        ctx.insert("s3:x-amz-acl".into(), v);
    }
    if let Some(v) = header("x-amz-server-side-encryption") {
        ctx.insert("s3:x-amz-server-side-encryption".into(), v);
    }
    ctx
}

/// Builds the S3 HTTP service: SigV4 against the replicated credentials,
/// operations against the Nauka engine.
pub fn service(state: Arc<ApiState>) -> s3s::service::S3Service {
    let mut builder = s3s::service::S3ServiceBuilder::new(NaukaS3::new(state.clone()));
    builder.set_auth(NaukaAuth {
        state: state.clone(),
    });
    builder.set_access(NaukaAccess { state });
    // AWS naming rules plus the `tenant:bucket` form RGW clients use.
    builder.set_validation(NaukaNameValidation);
    builder.build()
}

/// Serves the S3 endpoint until the process stops.
pub async fn serve(listen: std::net::SocketAddr, state: Arc<ApiState>) -> anyhow::Result<()> {
    let service = Arc::new(service(state.clone()));
    let listener = tokio::net::TcpListener::bind(listen).await?;
    tracing::info!("S3 endpoint on http://{listen}");
    loop {
        let (stream, _) = listener.accept().await?;
        let svc = ServiceRef {
            inner: service.clone(),
            state: state.clone(),
        };
        tokio::spawn(async move {
            let io = hyper_util::rt::TokioIo::new(stream);
            if let Err(e) = hyper::server::conn::http1::Builder::new()
                .serve_connection(io, svc)
                .await
            {
                tracing::debug!("S3 connection ended: {e}");
            }
        });
    }
}

/// The HTTP layer above `s3s`: CORS lives here because it is decided per
/// HTTP request, before (OPTIONS preflight) or after (response headers)
/// the S3 operation — `s3s` never routes either to the service trait.
#[derive(Clone)]
struct ServiceRef {
    inner: Arc<s3s::service::S3Service>,
    state: Arc<ApiState>,
}

impl hyper::service::Service<hyper::Request<hyper::body::Incoming>> for ServiceRef {
    type Response = hyper::Response<s3s::Body>;
    type Error = s3s::HttpError;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;

    fn call(&self, req: hyper::Request<hyper::body::Incoming>) -> Self::Future {
        let svc = self.inner.clone();
        let state = self.state.clone();
        // Everything CORS needs, captured before the request is consumed.
        let bucket = path_bucket(req.uri().path());
        let header = |name: &str| {
            req.headers()
                .get(name)
                .and_then(|v| v.to_str().ok())
                .map(String::from)
        };
        let origin = header("origin");
        let acr_method = header("access-control-request-method");
        let acr_headers = header("access-control-request-headers");
        let is_options = req.method() == hyper::Method::OPTIONS;
        let method = req.method().as_str().to_string();
        let expires_param = req.uri().query().and_then(|q| {
            q.split('&')
                .find_map(|p| p.strip_prefix("X-Amz-Expires="))
                .map(String::from)
        });
        let raw_path = req.uri().path().to_string();
        let raw_query = req.uri().query().map(String::from);
        let host_header = header("host");
        let req_bytes = content_length(req.headers());
        // Every exit below is measured, including the three that never
        // reach an S3 operation. Wrapping the body rather than recording at
        // each `return` keeps that true when a fourth early exit is added.
        Box::pin(crate::telemetry::s3::scoped(async move {
            let started = std::time::Instant::now();
            let result = async move {
                if is_options {
                    // The preflight never reaches an S3 operation: it is
                    // unauthenticated by design and answered from the bucket's
                    // stored CORS rules alone. Named anyway, so preflight volume
                    // does not hide inside the `unknown` series next to the
                    // requests that failed signature verification.
                    crate::telemetry::s3::set_op("Preflight", "read");
                    return Ok(preflight(
                        &state,
                        bucket.as_deref(),
                        origin.as_deref(),
                        acr_method.as_deref(),
                        acr_headers.as_deref(),
                    ));
                }
                // A presigned URL with an out-of-range lifetime is refused as
                // FORBIDDEN, not as a parse error: negative or beyond the
                // 7-day AWS maximum, it never reaches signature verification
                // (`s3s` would answer 400 on the negative case; AWS and the
                // suite say 403).
                if let Some(raw) = &expires_param {
                    const MAX_PRESIGN_SECS: i64 = 604_800;
                    let ok = raw
                        .parse::<i64>()
                        .is_ok_and(|v| v > 0 && v <= MAX_PRESIGN_SECS);
                    if !ok {
                        let mut resp = hyper::Response::new(s3s::Body::from(
                            b"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
                          <Error><Code>AccessDenied</Code>\
                          <Message>invalid X-Amz-Expires</Message></Error>"
                                .to_vec(),
                        ));
                        *resp.status_mut() = hyper::StatusCode::FORBIDDEN;
                        return Ok(resp);
                    }
                }
                // CDN offload: past our egress budget, big presigned GETs are
                // redirected to the member with the most headroom, on a URL
                // we sign for it.
                if method == "GET" {
                    if let Some(resp) = maybe_offload_redirect(
                        &state,
                        &raw_path,
                        raw_query.as_deref(),
                        host_header.as_deref(),
                    ) {
                        return Ok(resp);
                    }
                }
                let mut resp = hyper::service::Service::call(svc.as_ref(), req).await?;
                // POST-object polish over `s3s`'s protocol handling:
                // - an UNMET policy condition is 403 AccessDenied on AWS, but
                //   `s3s` answers 400 InvalidPolicyDocument (same code it uses
                //   for a structurally bad policy, which IS a 400 — the
                //   "Policy condition" message tells the two apart);
                // - the success_action_redirect Location must carry the ETag
                //   WITH its quotes, which `s3s` strips.
                if method == "POST" {
                    if resp.status() == hyper::StatusCode::BAD_REQUEST {
                        let body_bytes = resp.body().bytes();
                        if let Some(b) = body_bytes {
                            let text = String::from_utf8_lossy(&b);
                            if text.contains("<Code>InvalidPolicyDocument</Code>")
                                && (text.contains("Policy condition")
                                    || text.contains("does not match bucket in URL"))
                            {
                                let fixed = text.replace(
                                    "<Code>InvalidPolicyDocument</Code>",
                                    "<Code>AccessDenied</Code>",
                                );
                                *resp.status_mut() = hyper::StatusCode::FORBIDDEN;
                                *resp.body_mut() = s3s::Body::from(fixed.into_bytes());
                            }
                        }
                    }
                    if resp.status() == hyper::StatusCode::SEE_OTHER {
                        let requoted = resp
                            .headers()
                            .get(hyper::header::LOCATION)
                            .and_then(|v| v.to_str().ok())
                            .and_then(|loc| {
                                let (head, etag) = loc.split_once("etag=")?;
                                (!etag.starts_with("%22"))
                                    .then(|| format!("{head}etag=%22{etag}%22"))
                            });
                        if let Some(loc) = requoted {
                            if let Ok(v) = loc.parse() {
                                resp.headers_mut().insert(hyper::header::LOCATION, v);
                            }
                        }
                    }
                }
                // A cross-origin actual request gets the Access-Control-*
                // headers on WHATEVER response the operation produced — a 403
                // from auth still carries them, as AWS does. The method under
                // evaluation is the announced one when present.
                if let (Some(bucket), Some(origin)) = (bucket, origin) {
                    let method = acr_method.as_deref().unwrap_or(&method);
                    if let Some(grant) = cors_grant(&state, &bucket, &origin, method, None) {
                        let h = resp.headers_mut();
                        insert_header(h, "access-control-allow-origin", &grant.allow_origin);
                        insert_header(h, "access-control-allow-methods", method);
                        if let Some(expose) = &grant.expose_headers {
                            insert_header(h, "access-control-expose-headers", expose);
                        }
                        insert_header(h, "vary", "Origin");
                    }
                }
                Ok(resp)
            }
            .await;
            // Read after the POST-object rewriting above: the metric must
            // agree with the status the client actually received, not with
            // the one `s3s` first produced.
            let (status, resp_bytes) = match &result {
                Ok(resp) => (resp.status().as_u16(), content_length(resp.headers())),
                // A transport-level failure never produced a status. From
                // the client's side it is a server failure, so count it as
                // one rather than dropping the request from the metrics.
                Err(_) => (500, None),
            };
            crate::telemetry::s3::record_request(status, started.elapsed(), req_bytes, resp_bytes);
            result
        }))
    }
}

/// A declared `Content-Length`, when there is one. Streaming bodies without
/// the header are simply not counted — better a gap than a wrong number.
fn content_length(headers: &hyper::HeaderMap) -> Option<u64> {
    headers
        .get(hyper::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse().ok())
}

/// The bucket a path-style request addresses: the first path segment.
fn path_bucket(path: &str) -> Option<String> {
    let bucket = path.trim_start_matches('/').split('/').next()?;
    (!bucket.is_empty()).then(|| bucket.to_string())
}

/// The `bucket/key` of a path-style object URL (percent-decoded key).
fn path_object(path: &str) -> Option<(String, String)> {
    let rest = path.trim_start_matches('/');
    let (bucket, key) = rest.split_once('/')?;
    if bucket.is_empty() || key.is_empty() {
        return None;
    }
    let key = percent_encoding::percent_decode_str(key)
        .decode_utf8_lossy()
        .into_owned();
    Some((bucket.to_string(), key))
}

/// Mints a presigned GET URL (SigV4 query auth) for `host` — which need
/// not be THIS node: the credential registry is replicated, so any node
/// can sign a URL that any other node will honour. This is what turns a
/// 302 into a first-class routing tool.
fn presign_get_url(
    host: &str,
    bucket: &str,
    key: &str,
    access_key: &str,
    secret: &str,
    expires_secs: u32,
    now_secs: u64,
) -> String {
    use hmac::Mac;
    type HmacSha256 = hmac::Hmac<sha2::Sha256>;
    let hmac = |k: &[u8], data: &str| -> Vec<u8> {
        let mut m = <HmacSha256 as Mac>::new_from_slice(k).expect("hmac accepts any key length");
        m.update(data.as_bytes());
        m.finalize().into_bytes().to_vec()
    };
    let sha_hex = |data: &str| -> String {
        use sha2::Digest;
        format!("{:x}", sha2::Sha256::digest(data.as_bytes()))
    };
    let odt = time::OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(now_secs as i64);
    let datestamp = format!(
        "{:04}{:02}{:02}",
        odt.year(),
        u8::from(odt.month()),
        odt.day()
    );
    let amz_date = format!(
        "{datestamp}T{:02}{:02}{:02}Z",
        odt.hour(),
        odt.minute(),
        odt.second()
    );
    let scope = format!("{datestamp}/us-east-1/s3/aws4_request");
    // Query-string encoding: RFC 3986 unreserved only.
    const QS: &percent_encoding::AsciiSet = &percent_encoding::NON_ALPHANUMERIC
        .remove(b'-')
        .remove(b'_')
        .remove(b'.')
        .remove(b'~');
    let enc = |s: &str| percent_encoding::utf8_percent_encode(s, QS).to_string();
    // Path encoding: same set, but `/` stays.
    const PS: &percent_encoding::AsciiSet = &percent_encoding::NON_ALPHANUMERIC
        .remove(b'-')
        .remove(b'_')
        .remove(b'.')
        .remove(b'~')
        .remove(b'/');
    let uri = format!(
        "/{bucket}/{}",
        percent_encoding::utf8_percent_encode(key, PS)
    );
    // Sorted by parameter name, as SigV4 canonicalization requires.
    let query = format!(
        "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential={}&X-Amz-Date={amz_date}\
         &X-Amz-Expires={expires_secs}&X-Amz-SignedHeaders=host",
        enc(&format!("{access_key}/{scope}"))
    );
    let canonical = format!("GET\n{uri}\n{query}\nhost:{host}\n\nhost\nUNSIGNED-PAYLOAD");
    let to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        sha_hex(&canonical)
    );
    let k_date = hmac(format!("AWS4{secret}").as_bytes(), &datestamp);
    let k_region = hmac(&k_date, "us-east-1");
    let k_service = hmac(&k_region, "s3");
    let k_signing = hmac(&k_service, "aws4_request");
    let signature: String = hmac(&k_signing, &to_sign)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    format!("http://{host}{uri}?{query}&X-Amz-Signature={signature}")
}

/// Objects below this size are always served directly: a redirect costs
/// a round-trip, which only pays for itself on large transfers.
const REDIRECT_MIN_SIZE: u64 = 8 * 1024 * 1024;

/// CDN offload: when THIS node has exhausted its monthly egress budget,
/// a presigned GET of a large object is answered with a 302 towards a
/// freshly signed URL on the member with the most budget headroom — the
/// egress leaves the right machine, and the client follows without
/// noticing. Serving directly stays the answer whenever no better-funded
/// member exists (deprioritized, never refused), for small objects, and
/// for header-signed SDK requests (they do not re-sign across hosts).
fn maybe_offload_redirect(
    state: &Arc<ApiState>,
    path: &str,
    query: Option<&str>,
    host: Option<&str>,
) -> Option<hyper::Response<s3s::Body>> {
    let query = query?;
    let param = |name: &str| {
        query.split('&').find_map(|p| {
            p.strip_prefix(name)
                .and_then(|v| v.strip_prefix('='))
                .map(|v| {
                    percent_encoding::percent_decode_str(v)
                        .decode_utf8_lossy()
                        .into_owned()
                })
        })
    };
    // Presigned auth in either flavour: SigV4 (X-Amz-Signature +
    // X-Amz-Credential) or SigV2 (Signature + AWSAccessKeyId). Header-
    // signed SDK requests carry neither and are served directly.
    let access_key = match (param("X-Amz-Signature"), param("Signature")) {
        (Some(_), _) => {
            param("X-Amz-Credential").and_then(|c| c.split('/').next().map(str::to_string))
        }
        (None, Some(_)) => param("AWSAccessKeyId"),
        _ => None,
    }?;
    // Only when our own budget is spent.
    let quota = state.egress.quota()?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let (month, served) = state.egress.snapshot(now);
    if served < quota {
        return None;
    }
    let (bucket, key) = path_object(path)?;
    let bucket = tenant_suffix(&bucket).to_string();
    let s3 = state.app.app_state().s3;
    let size = s3
        .objects
        .get(&(bucket.clone(), key.clone()))?
        .current_content()?
        .size;
    if size < REDIRECT_MIN_SIZE {
        return None;
    }
    // The member with the most remaining budget — a node without a
    // ledger is unmetered (infinite headroom). Everyone eligible is
    // strictly better-funded than us (we are at zero), so a redirect can
    // never ping-pong.
    let app_state = state.app.app_state();
    let target = state
        .app
        .members()
        .into_values()
        .filter(|addr| *addr != state.self_id)
        .map(|addr| {
            let ratio = crate::egress::remaining_ratio(app_state.node_egress.get(&addr), &month);
            (addr, ratio)
        })
        .filter(|(_, ratio)| *ratio > 0.0)
        .max_by(|a, b| {
            a.1.partial_cmp(&b.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| b.0.cmp(&a.0))
        })
        .map(|(addr, _)| addr)?;
    // Peer identity is "ip:raft_port"; the S3 endpoint lives on the same
    // ip at the port the CLIENT used to reach us (uniform across a
    // deployment).
    let target_ip = target.rsplit_once(':').map(|(ip, _)| ip)?.to_string();
    let port = host
        .and_then(|h| h.rsplit_once(':').map(|(_, p)| p.to_string()))
        .unwrap_or_else(|| "80".into());
    // Same identity, fresh SigV4 signature the target will honour; its
    // secret comes from the replicated registry.
    let secret = s3.credentials.get(&access_key)?.secret_access_key.clone();
    let url = presign_get_url(
        &format!("{target_ip}:{port}"),
        &bucket,
        &key,
        &access_key,
        &secret,
        300,
        now,
    );
    let mut resp = hyper::Response::new(s3s::Body::from(Vec::new()));
    *resp.status_mut() = hyper::StatusCode::FOUND;
    if let Ok(v) = url.parse() {
        resp.headers_mut().insert(hyper::header::LOCATION, v);
        Some(resp)
    } else {
        None
    }
}

/// What a matched CORS rule grants a request.
struct CorsGrant {
    /// `*` when the rule allowed every origin, the echoed origin otherwise.
    allow_origin: String,
    expose_headers: Option<String>,
    max_age: Option<i32>,
}

/// Finds the first CORS rule of `bucket` matching the request, per the S3
/// rules: the origin matches one of AllowedOrigins (`*` or one-wildcard
/// patterns), the method is in AllowedMethods, and — when the request
/// announces headers — every one is covered by AllowedHeaders.
fn cors_grant(
    state: &Arc<ApiState>,
    bucket: &str,
    origin: &str,
    method: &str,
    requested_headers: Option<&str>,
) -> Option<CorsGrant> {
    let s3 = state.app.app_state().s3;
    let cfg = cors_from_xml(s3.buckets.get(bucket)?.cors.as_deref()?)?;
    for rule in &cfg.cors_rules {
        let matched_origin = rule
            .allowed_origins
            .iter()
            .find(|p| wildcard_matches(p, origin));
        let Some(pattern) = matched_origin else {
            continue;
        };
        if !rule.allowed_methods.iter().any(|m| m == method) {
            continue;
        }
        if let Some(requested) = requested_headers {
            let allowed = rule.allowed_headers.as_deref().unwrap_or(&[]);
            let all_allowed = requested
                .split(',')
                .map(|h| h.trim().to_ascii_lowercase())
                .filter(|h| !h.is_empty())
                .all(|h| {
                    allowed
                        .iter()
                        .any(|p| wildcard_matches(&p.to_ascii_lowercase(), &h))
                });
            if !all_allowed {
                continue;
            }
        }
        return Some(CorsGrant {
            allow_origin: if pattern == "*" {
                "*".into()
            } else {
                origin.to_string()
            },
            expose_headers: rule
                .expose_headers
                .as_ref()
                .filter(|e| !e.is_empty())
                .map(|e| e.join(", ")),
            max_age: rule.max_age_seconds,
        });
    }
    None
}

/// A CORS pattern with at most one `*`: literal match, or the wildcard
/// swallows the middle.
fn wildcard_matches(pattern: &str, value: &str) -> bool {
    match pattern.split_once('*') {
        None => pattern == value,
        Some((prefix, suffix)) => {
            value.len() >= prefix.len() + suffix.len()
                && value.starts_with(prefix)
                && value.ends_with(suffix)
        }
    }
}

/// Answers an OPTIONS preflight from the bucket's CORS rules: 400 when the
/// request is not a preflight at all (no Origin or no announced method),
/// 403 when no rule allows it, 200 with the Access-Control-* headers when
/// one does.
fn preflight(
    state: &Arc<ApiState>,
    bucket: Option<&str>,
    origin: Option<&str>,
    acr_method: Option<&str>,
    acr_headers: Option<&str>,
) -> hyper::Response<s3s::Body> {
    let empty = || s3s::Body::from(Vec::new());
    let status = |code: u16| {
        hyper::Response::builder()
            .status(code)
            .body(empty())
            .unwrap_or_else(|_| hyper::Response::new(empty()))
    };
    let (Some(origin), Some(method)) = (origin, acr_method) else {
        // Not a preflight: a browser always sends both.
        return status(400);
    };
    let Some(bucket) = bucket else {
        return status(403);
    };
    match cors_grant(state, bucket, origin, method, acr_headers) {
        Some(grant) => {
            let mut resp = status(200);
            let h = resp.headers_mut();
            insert_header(h, "access-control-allow-origin", &grant.allow_origin);
            insert_header(h, "access-control-allow-methods", method);
            if let Some(headers) = acr_headers {
                insert_header(h, "access-control-allow-headers", headers);
            }
            if let Some(age) = grant.max_age {
                insert_header(h, "access-control-max-age", &age.to_string());
            }
            insert_header(h, "vary", "Origin");
            resp
        }
        None => status(403),
    }
}

/// Inserts a header, skipping values that cannot be encoded rather than
/// failing the response over a cosmetic addition.
fn insert_header(headers: &mut hyper::HeaderMap, name: &'static str, value: &str) {
    if let Ok(v) = value.parse::<hyper::header::HeaderValue>() {
        headers.insert(name, v);
    }
}

fn cors_from_xml(xml: &str) -> Option<CORSConfiguration> {
    let mut de = s3s::xml::Deserializer::new(xml.as_bytes());
    s3s::xml::Deserialize::deserialize(&mut de).ok()
}

fn pab_from_xml(xml: &str) -> Option<PublicAccessBlockConfiguration> {
    let mut de = s3s::xml::Deserializer::new(xml.as_bytes());
    s3s::xml::Deserialize::deserialize(&mut de).ok()
}

/// SSE metadata stored on a version or an in-flight upload (JSON in the
/// `sse` field). For SSE-C the customer key is NEVER stored — only its
/// MD5 fingerprint, to recognize the right key when it is presented
/// again. The stored bytes are ciphertext the cluster cannot read.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct SseInfo {
    /// "AES256" (SSE-S3), "aws:kms", or "SSE-C".
    mode: String,
    #[serde(default)]
    key_md5: Option<String>,
    #[serde(default)]
    kms_key_id: Option<String>,
    /// Ciphertext segment lengths, one per encryption stream — a single
    /// PUT has one, a multipart object one per part. Decryption walks
    /// them in order.
    #[serde(default)]
    segments: Vec<u64>,
}

impl SseInfo {
    fn parse(raw: &Option<String>) -> Option<Self> {
        raw.as_deref().and_then(|s| serde_json::from_str(s).ok())
    }

    fn to_json(&self) -> Option<String> {
        serde_json::to_string(self).ok()
    }

    fn is_customer(&self) -> bool {
        self.mode == "SSE-C"
    }
}

/// What a write-side request asked for, validated: the SSE mode to
/// record, and the customer key to encrypt with (SSE-C only).
struct SseRequest {
    info: Option<SseInfo>,
    customer_key: Option<nauka_crypto::FileKey>,
}

/// Validates the SSE headers of a PUT/CreateMultipartUpload, with the
/// AWS error surface the suite checks: every malformed or conflicting
/// combination is a 400 InvalidArgument.
fn validate_sse_request(
    sse: Option<&str>,
    kms_key_id: Option<&str>,
    c_alg: Option<&str>,
    c_key: Option<&str>,
    c_md5: Option<&str>,
) -> S3Result<SseRequest> {
    let none = SseRequest {
        info: None,
        customer_key: None,
    };
    if c_alg.is_some() || c_key.is_some() || c_md5.is_some() {
        // SSE-C: exclusive with the server-managed modes.
        if sse.is_some() {
            return Err(s3_error!(
                InvalidArgument,
                "customer-provided keys conflict with x-amz-server-side-encryption"
            ));
        }
        if c_alg != Some("AES256") {
            return Err(s3_error!(
                InvalidArgument,
                "the customer algorithm must be AES256"
            ));
        }
        let Some(key_b64) = c_key else {
            return Err(s3_error!(InvalidArgument, "missing customer key"));
        };
        let key_bytes = data_encoding::BASE64
            .decode(key_b64.as_bytes())
            .map_err(|_| s3_error!(InvalidArgument, "the customer key is not valid base64"))?;
        if key_bytes.len() != 32 {
            return Err(s3_error!(
                InvalidArgument,
                "the customer key must be 256 bits"
            ));
        }
        let Some(md5_b64) = c_md5 else {
            return Err(s3_error!(InvalidArgument, "missing customer key MD5"));
        };
        let digest = {
            use md5::Digest;
            md5::Md5::digest(&key_bytes)
        };
        if data_encoding::BASE64.encode(&digest) != md5_b64 {
            return Err(s3_error!(
                InvalidArgument,
                "the customer key does not match its MD5"
            ));
        }
        let key = nauka_crypto::FileKey::decode(&data_encoding::BASE64URL_NOPAD.encode(&key_bytes))
            .map_err(|_| s3_error!(InvalidArgument, "unusable customer key"))?;
        return Ok(SseRequest {
            info: Some(SseInfo {
                mode: "SSE-C".into(),
                key_md5: Some(md5_b64.to_owned()),
                kms_key_id: None,
                segments: Vec::new(),
            }),
            customer_key: Some(key),
        });
    }
    match sse {
        Some("AES256") => {
            if kms_key_id.is_some() {
                return Err(s3_error!(
                    InvalidArgument,
                    "a KMS key id conflicts with SSE-S3"
                ));
            }
            Ok(SseRequest {
                info: Some(SseInfo {
                    mode: "AES256".into(),
                    key_md5: None,
                    kms_key_id: None,
                    segments: Vec::new(),
                }),
                customer_key: None,
            })
        }
        Some("aws:kms") => {
            let Some(id) = kms_key_id else {
                return Err(s3_error!(InvalidArgument, "aws:kms requires a key id"));
            };
            Ok(SseRequest {
                info: Some(SseInfo {
                    mode: "aws:kms".into(),
                    key_md5: None,
                    kms_key_id: Some(id.to_owned()),
                    segments: Vec::new(),
                }),
                customer_key: None,
            })
        }
        Some(_) => Err(s3_error!(InvalidArgument, "unknown server-side encryption")),
        None => {
            if kms_key_id.is_some() {
                return Err(s3_error!(
                    InvalidArgument,
                    "a KMS key id without x-amz-server-side-encryption"
                ));
            }
            Ok(none)
        }
    }
}

/// Validates the customer-key headers of a read against a stored SSE-C
/// object and returns the key. Everything wrong — missing headers, bad
/// key, a DIFFERENT key than the one the object was written with — is
/// the 400 the suite expects.
fn require_customer_key(
    stored: &SseInfo,
    c_alg: Option<&str>,
    c_key: Option<&str>,
    c_md5: Option<&str>,
) -> S3Result<nauka_crypto::FileKey> {
    let asked = validate_sse_request(None, None, c_alg, c_key, c_md5)?;
    let (Some(info), Some(key)) = (asked.info, asked.customer_key) else {
        return Err(s3_error!(
            InvalidArgument,
            "the object was stored with a customer-provided key; the same key is required"
        ));
    };
    if info.key_md5 != stored.key_md5 {
        return Err(s3_error!(InvalidArgument, "wrong customer key"));
    }
    Ok(key)
}

/// A Write sink that hashes and counts what passes through, so encrypting
/// into a file yields the ciphertext's BLAKE3 and length in one pass.
struct HashingFile {
    file: std::io::BufWriter<std::fs::File>,
    hasher: blake3::Hasher,
    len: u64,
}

impl std::io::Write for HashingFile {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.file.write(buf)?;
        self.hasher.update(&buf[..n]);
        self.len += n as u64;
        Ok(n)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}

/// Encrypts `src` into `dst` with the customer key. Returns the
/// ciphertext length and its BLAKE3 hasher (the content address of what
/// the cluster actually stores).
async fn encrypt_to_tmp(
    key: nauka_crypto::FileKey,
    src: std::path::PathBuf,
    dst: std::path::PathBuf,
) -> S3Result<(u64, blake3::Hasher)> {
    tokio::task::spawn_blocking(move || -> Result<(u64, blake3::Hasher), std::io::Error> {
        let mut input = std::io::BufReader::new(std::fs::File::open(&src)?);
        let mut sink = HashingFile {
            file: std::io::BufWriter::new(std::fs::File::create(&dst)?),
            hasher: blake3::Hasher::new(),
            len: 0,
        };
        nauka_crypto::encrypt(&key, &mut input, &mut sink)
            .map_err(|e| std::io::Error::other(format!("{e}")))?;
        use std::io::Write;
        sink.flush()?;
        Ok((sink.len, sink.hasher))
    })
    .await
    .map_err(|e| s3_error!(InternalError, "{e}"))?
    .map_err(|e| s3_error!(InternalError, "encrypting: {e}"))
}

/// Decrypts a whole ciphertext made of consecutive independent streams
/// (`segments` lengths; a single segment when the list is empty). A
/// failure means the wrong key — surfaced as the 400 the suite expects.
fn decrypt_segments(key: &nauka_crypto::FileKey, ct: &[u8], segments: &[u64]) -> S3Result<Vec<u8>> {
    let one;
    let segs: &[u64] = if segments.is_empty() {
        one = [ct.len() as u64];
        &one
    } else {
        segments
    };
    let mut out = Vec::with_capacity(ct.len());
    let mut off: usize = 0;
    for len in segs {
        let end = off
            .checked_add(*len as usize)
            .filter(|e| *e <= ct.len())
            .ok_or_else(|| s3_error!(InternalError, "corrupt encrypted segments"))?;
        nauka_crypto::decrypt(key, &mut &ct[off..end], &mut out)
            .map_err(|_| s3_error!(InvalidArgument, "wrong customer key"))?;
        off = end;
    }
    Ok(out)
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

/// Validates a lifecycle configuration the way S3 does, generating an id
/// for any rule that lacks one (GET returns the generated ids).
fn validate_lifecycle_rules(rules: &mut [LifecycleRule]) -> S3Result<()> {
    let mut ids = std::collections::BTreeSet::new();
    for rule in rules.iter_mut() {
        let id = rule.id.get_or_insert_with(uuid_like).clone();
        if id.chars().count() > 255 {
            return Err(s3_error!(
                InvalidArgument,
                "a lifecycle rule id is at most 255 characters"
            ));
        }
        if !ids.insert(id) {
            return Err(s3_error!(InvalidArgument, "duplicate lifecycle rule id"));
        }
        let status = rule.status.as_str();
        if status != ExpirationStatus::ENABLED && status != ExpirationStatus::DISABLED {
            return Err(s3_error!(
                MalformedXML,
                "Status must be Enabled or Disabled"
            ));
        }
        if let Some(exp) = &rule.expiration {
            // Exactly one action per Expiration block, as AWS requires.
            let set = [
                exp.days.is_some(),
                exp.date.is_some(),
                exp.expired_object_delete_marker.is_some(),
            ];
            if set.iter().filter(|s| **s).count() != 1 {
                return Err(s3_error!(
                    MalformedXML,
                    "Expiration needs exactly one of Days, Date or ExpiredObjectDeleteMarker"
                ));
            }
            // Days: 0 is legal in a transition rule, not in an expiration.
            if exp.days.is_some_and(|d| d <= 0) {
                return Err(s3_error!(
                    InvalidArgument,
                    "Expiration Days must be a positive integer"
                ));
            }
            if let Some(date) = &exp.date {
                lifecycle_midnight(date)?;
            }
        }
        if let Some(nc) = &rule.noncurrent_version_expiration {
            if nc.noncurrent_days.is_some_and(|d| d <= 0) {
                return Err(s3_error!(
                    InvalidArgument,
                    "NoncurrentDays must be a positive integer"
                ));
            }
        }
        for t in rule.transitions.iter().flatten() {
            if t.days.is_some_and(|d| d < 0) {
                return Err(s3_error!(
                    InvalidArgument,
                    "Transition Days must not be negative"
                ));
            }
            if let Some(date) = &t.date {
                lifecycle_midnight(date)?;
            }
        }
        for t in rule.noncurrent_version_transitions.iter().flatten() {
            if t.noncurrent_days.is_some_and(|d| d < 0) {
                return Err(s3_error!(
                    InvalidArgument,
                    "NoncurrentDays must not be negative"
                ));
            }
        }
    }
    Ok(())
}

/// A lifecycle `Date` must be midnight UTC — AWS rejects any other time.
/// (This is a real rejection path: a malformed client date string like
/// "20200101" reaches us as a valid epoch-seconds timestamp that is not
/// midnight.)
fn lifecycle_midnight(date: &Timestamp) -> S3Result<()> {
    if timestamp_secs(date).is_some_and(|s| s % 86_400 == 0) {
        Ok(())
    } else {
        Err(s3_error!(InvalidArgument, "'Date' must be at midnight GMT"))
    }
}

/// Serializes a lifecycle configuration back to its XML wire form — the
/// storage format, so GET returns exactly the rules PUT accepted.
fn lifecycle_to_xml(cfg: &BucketLifecycleConfiguration) -> S3Result<String> {
    let mut buf = Vec::new();
    let mut ser = s3s::xml::Serializer::new(&mut buf);
    s3s::xml::Serialize::serialize(cfg, &mut ser)
        .map_err(|e| s3_error!(InternalError, "serializing the lifecycle rules: {e}"))?;
    String::from_utf8(buf).map_err(|e| s3_error!(InternalError, "{e}"))
}

fn lifecycle_from_xml(xml: &str) -> Option<BucketLifecycleConfiguration> {
    let mut de = s3s::xml::Deserializer::new(xml.as_bytes());
    s3s::xml::Deserialize::deserialize(&mut de).ok()
}

/// Whether a lifecycle rule's filter selects a key: the prefix (top-level
/// or inside the Filter) matches, and every tag the filter requires is on
/// the object.
fn lifecycle_rule_matches(
    rule: &LifecycleRule,
    key: &str,
    tags: &BTreeMap<String, String>,
) -> bool {
    let filter = rule.filter.as_ref();
    let and = filter.and_then(|f| f.and.as_ref());
    let prefix = rule
        .prefix
        .as_deref()
        .or_else(|| filter.and_then(|f| f.prefix.as_deref()))
        .or_else(|| and.and_then(|a| a.prefix.as_deref()))
        .unwrap_or("");
    if !key.starts_with(prefix) {
        return false;
    }
    let mut required: Vec<&Tag> = Vec::new();
    if let Some(t) = filter.and_then(|f| f.tag.as_ref()) {
        required.push(t);
    }
    if let Some(ts) = and.and_then(|a| a.tags.as_ref()) {
        required.extend(ts.iter());
    }
    required.iter().all(|t| {
        t.key
            .as_ref()
            .is_some_and(|k| tags.get(k) == t.value.as_ref())
    })
}

/// Adds the `x-amz-object-lock-*` headers S3 puts on GET/HEAD of an object
/// carrying a retention or a legal hold. The DTOs lack these fields, so the
/// headers are set directly on the response.
fn set_object_lock_headers(headers: &mut hyper::HeaderMap, v: &nauka_s3::ObjectVersion) {
    if let Some(info) = v
        .retention
        .as_deref()
        .and_then(|s| serde_json::from_str::<RetentionInfo>(s).ok())
    {
        if let Ok(mode) = info.mode.parse() {
            headers.insert("x-amz-object-lock-mode", mode);
        }
        // Retain-until as an RFC 3339 / ISO 8601 timestamp.
        let odt = time::OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(info.until as i64);
        if let Ok(date) = odt
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap_or_default()
            .parse()
        {
            headers.insert("x-amz-object-lock-retain-until-date", date);
        }
    }
    let hold = if v.legal_hold { "ON" } else { "OFF" };
    if let Ok(val) = hold.parse() {
        headers.insert("x-amz-object-lock-legal-hold", val);
    }
}

/// Builds an S3 error with a code s3s does not model as a variant (so it
/// has no default status), giving it an explicit status.
fn custom_error(code: &str, status: hyper::StatusCode, msg: &'static str) -> S3Error {
    use s3s::S3ErrorCode;
    let mut err = S3Error::with_message(S3ErrorCode::Custom(code.into()), msg);
    err.set_status_code(status);
    err
}

/// Builds the serialized retention from the `x-amz-object-lock-mode` and
/// `-retain-until-date` PUT/multipart headers, if both are present.
fn retention_from_headers(mode: Option<&str>, until: Option<&Timestamp>) -> Option<String> {
    let mode = mode?.to_string();
    let until = until.and_then(timestamp_secs)?;
    serde_json::to_string(&RetentionInfo { mode, until }).ok()
}

/// Object Lock retention stored on one object version.
#[derive(serde::Serialize, serde::Deserialize)]
struct RetentionInfo {
    mode: String,
    /// Retain-until, epoch seconds.
    until: u64,
}

/// A bucket's default Object Lock rule, stored serialized.
#[derive(Default, serde::Serialize, serde::Deserialize)]
struct RetentionConfig {
    mode: Option<String>,
    days: Option<i32>,
    years: Option<i32>,
}

impl RetentionConfig {
    fn from_dto(c: &ObjectLockConfiguration) -> Self {
        let dr = c.rule.as_ref().and_then(|r| r.default_retention.as_ref());
        Self {
            mode: dr.and_then(|d| d.mode.as_ref().map(|m| m.as_str().to_string())),
            days: dr.and_then(|d| d.days),
            years: dr.and_then(|d| d.years),
        }
    }

    fn to_dto(cfg: Option<&Self>) -> ObjectLockConfiguration {
        ObjectLockConfiguration {
            object_lock_enabled: ObjectLockEnabled::from_static(ObjectLockEnabled::ENABLED).into(),
            rule: cfg.and_then(|c| {
                c.mode.as_ref().map(|m| ObjectLockRule {
                    default_retention: Some(DefaultRetention {
                        mode: m.parse().ok(),
                        days: c.days,
                        years: c.years,
                    }),
                })
            }),
        }
    }
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
    for (stripe_idx, stripe) in manifest.stripes.iter().enumerate() {
        let len = stripe.data_len as u64;
        let stripe_end = offset + len - 1;
        if stripe_end < start {
            offset += len;
            continue;
        }
        if offset > end {
            break;
        }
        let data = crate::api::reconstruct_stripe(&fetcher, stripe, stripe_idx, manifest).await?;
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
