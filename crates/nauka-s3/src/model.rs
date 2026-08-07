//! The S3 data model, as replicated by Raft.
//!
//! Nauka addresses content by BLAKE3 hash: a file *is* its bytes. S3
//! addresses by `bucket/key`, chosen by the caller and **mutable** — a PUT
//! on an existing key replaces it. Reconciling the two is the whole job of
//! this module:
//!
//! - an [`ObjectVersion`] points at a manifest hash, so identical content
//!   uploaded under ten keys still occupies one set of shards;
//! - because manifests are now shared, a key going away can no longer
//!   delete them. The registry keeps a **reference count** per manifest and
//!   only unregisters (freeing the shards to the GC) when it reaches zero.
//!   Getting that count wrong means either leaked disk or lost data, so it
//!   is derived from the index rather than incremented by hand wherever
//!   possible.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Bucket-level configuration. Everything S3 lets you attach to a bucket
/// lives here; unset fields simply mean "AWS default".
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Bucket {
    /// Seconds since the epoch.
    pub created_at: u64,
    /// The access key that created it (its owner).
    pub owner: String,
    pub versioning: VersioningState,
    /// Raw JSON of the bucket policy, evaluated at request time.
    #[serde(default)]
    pub policy: Option<String>,
    #[serde(default)]
    pub tags: BTreeMap<String, String>,
    /// Serialized `<LifecycleConfiguration>` rules.
    #[serde(default)]
    pub lifecycle: Option<String>,
    #[serde(default)]
    pub cors: Option<String>,
    #[serde(default)]
    pub website: Option<String>,
    #[serde(default)]
    pub encryption: Option<String>,
    #[serde(default)]
    pub notification: Option<String>,
    /// Object Lock cannot be enabled after creation, per S3.
    #[serde(default)]
    pub object_lock_enabled: bool,
    #[serde(default)]
    pub object_lock_default: Option<String>,
    #[serde(default)]
    pub public_access_block: Option<String>,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
pub enum VersioningState {
    /// Never enabled: a PUT overwrites, and versions carry the literal id
    /// "null" as AWS does.
    #[default]
    Unversioned,
    Enabled,
    /// Was enabled then suspended: existing versions survive, new writes
    /// land on the "null" version again.
    Suspended,
}

/// One version of one key. A delete marker is a version too — that is how
/// S3 makes deletion undoable in a versioned bucket.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ObjectVersion {
    /// "null" in an unversioned bucket, otherwise a generated id.
    pub version_id: String,
    /// Manifest hash holding the bytes. `None` for a delete marker.
    pub content: Option<String>,
    pub size: u64,
    /// S3 ETag *with* its quotes, as it goes on the wire. A single-part
    /// object uses the MD5 of the content; a multipart one uses
    /// `md5(concat(md5(part)…))-<count>`, which is why we compute MD5
    /// alongside BLAKE3 at ingest.
    pub etag: String,
    pub last_modified: u64,
    #[serde(default)]
    pub content_type: Option<String>,
    /// `x-amz-meta-*` headers, minus the prefix.
    #[serde(default)]
    pub user_metadata: BTreeMap<String, String>,
    /// Headers S3 stores verbatim and replays on GET (Cache-Control,
    /// Content-Disposition, Content-Encoding, Content-Language, Expires).
    #[serde(default)]
    pub system_metadata: BTreeMap<String, String>,
    #[serde(default)]
    pub storage_class: Option<String>,
    #[serde(default)]
    pub tags: BTreeMap<String, String>,
    /// Additional checksums (CRC32, CRC32C, SHA1, SHA256) keyed by
    /// algorithm name.
    #[serde(default)]
    pub checksums: BTreeMap<String, String>,
    #[serde(default)]
    pub retention: Option<String>,
    #[serde(default)]
    pub legal_hold: bool,
    /// SSE algorithm applied, if any.
    #[serde(default)]
    pub sse: Option<String>,
}

impl ObjectVersion {
    pub fn is_delete_marker(&self) -> bool {
        self.content.is_none()
    }
}

/// Every version of a key, newest first. Never empty: an entry with no
/// versions is removed from the index instead.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ObjectEntry {
    pub versions: Vec<ObjectVersion>,
}

impl ObjectEntry {
    /// The version a plain GET resolves to: the newest one, delete marker
    /// included (which is what makes a deleted key read as absent).
    pub fn current(&self) -> Option<&ObjectVersion> {
        self.versions.first()
    }

    /// The newest version that actually holds bytes.
    pub fn current_content(&self) -> Option<&ObjectVersion> {
        self.versions.first().filter(|v| !v.is_delete_marker())
    }

    pub fn version(&self, id: &str) -> Option<&ObjectVersion> {
        self.versions.iter().find(|v| v.version_id == id)
    }
}

/// An in-flight multipart upload. Parts are ordinary manifests, so an
/// abandoned upload costs shards until it is aborted — which the lifecycle
/// rules and the GC take care of.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MultipartUpload {
    pub upload_id: String,
    pub bucket: String,
    pub key: String,
    pub initiated: u64,
    pub owner: String,
    #[serde(default)]
    pub content_type: Option<String>,
    #[serde(default)]
    pub user_metadata: BTreeMap<String, String>,
    #[serde(default)]
    pub system_metadata: BTreeMap<String, String>,
    #[serde(default)]
    pub storage_class: Option<String>,
    #[serde(default)]
    pub tags: BTreeMap<String, String>,
    #[serde(default)]
    pub sse: Option<String>,
    /// part number → part, ordered.
    #[serde(default)]
    pub parts: BTreeMap<u32, UploadedPart>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UploadedPart {
    /// Manifest hash of the part's bytes.
    pub content: String,
    pub size: u64,
    /// Quoted MD5 of this part — Complete concatenates these to build the
    /// final ETag, and the client sends them back for verification.
    pub etag: String,
    pub last_modified: u64,
    #[serde(default)]
    pub checksums: BTreeMap<String, String>,
}

/// A set of credentials. The secret never leaves the cluster; SigV4 proves
/// possession without transmitting it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Credential {
    pub access_key_id: String,
    pub secret_access_key: String,
    #[serde(default)]
    pub name: Option<String>,
    pub created_at: u64,
    /// None = full access (the cluster owner). Otherwise the buckets this
    /// key may touch, with per-bucket permissions.
    #[serde(default)]
    pub buckets: Option<BTreeMap<String, BucketPermission>>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct BucketPermission {
    pub read: bool,
    pub write: bool,
    pub owner: bool,
}

/// The whole S3 view, materialized by the Raft state machine.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct S3State {
    pub buckets: BTreeMap<String, Bucket>,
    /// (bucket, key) → versions.
    pub objects: BTreeMap<(String, String), ObjectEntry>,
    pub uploads: BTreeMap<String, MultipartUpload>,
    pub credentials: BTreeMap<String, Credential>,
}

impl S3State {
    /// How many live references point at a manifest hash: object versions
    /// plus multipart parts. The single source of truth for "may the GC
    /// have these shards?" — computed, never incremented, so it cannot
    /// drift.
    pub fn refcount(&self, content_hash: &str) -> usize {
        let in_objects = self
            .objects
            .values()
            .flat_map(|e| &e.versions)
            .filter(|v| v.content.as_deref() == Some(content_hash))
            .count();
        let in_uploads = self
            .uploads
            .values()
            .flat_map(|u| u.parts.values())
            .filter(|p| p.content == content_hash)
            .count();
        in_objects + in_uploads
    }

    /// Every manifest hash the S3 layer still needs. The GC unregisters
    /// anything else.
    pub fn live_content(&self) -> std::collections::BTreeSet<String> {
        let mut live = std::collections::BTreeSet::new();
        for entry in self.objects.values() {
            for v in &entry.versions {
                if let Some(h) = &v.content {
                    live.insert(h.clone());
                }
            }
        }
        for upload in self.uploads.values() {
            for p in upload.parts.values() {
                live.insert(p.content.clone());
            }
        }
        live
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn version(id: &str, content: Option<&str>) -> ObjectVersion {
        ObjectVersion {
            version_id: id.into(),
            content: content.map(String::from),
            size: 3,
            etag: "\"abc\"".into(),
            last_modified: 0,
            content_type: None,
            user_metadata: Default::default(),
            system_metadata: Default::default(),
            storage_class: None,
            tags: Default::default(),
            checksums: Default::default(),
            retention: None,
            legal_hold: false,
            sse: None,
        }
    }

    #[test]
    fn deduplicated_content_is_counted_once_per_reference() {
        // The same bytes under two keys: one manifest, two references.
        // Deleting one key must NOT free the shards of the other.
        let mut s = S3State::default();
        s.objects.insert(
            ("b".into(), "a.txt".into()),
            ObjectEntry {
                versions: vec![version("null", Some("HASH"))],
            },
        );
        s.objects.insert(
            ("b".into(), "b.txt".into()),
            ObjectEntry {
                versions: vec![version("null", Some("HASH"))],
            },
        );
        assert_eq!(s.refcount("HASH"), 2);

        s.objects.remove(&("b".into(), "a.txt".into()));
        assert_eq!(s.refcount("HASH"), 1);
        assert!(s.live_content().contains("HASH"), "still referenced");

        s.objects.remove(&("b".into(), "b.txt".into()));
        assert_eq!(s.refcount("HASH"), 0);
        assert!(!s.live_content().contains("HASH"), "now collectable");
    }

    #[test]
    fn multipart_parts_hold_references_too() {
        // An in-flight upload keeps its parts alive even though no object
        // points at them yet.
        let mut s = S3State::default();
        let mut parts = BTreeMap::new();
        parts.insert(
            1,
            UploadedPart {
                content: "PART".into(),
                size: 5,
                etag: "\"x\"".into(),
                last_modified: 0,
                checksums: Default::default(),
            },
        );
        s.uploads.insert(
            "u1".into(),
            MultipartUpload {
                upload_id: "u1".into(),
                bucket: "b".into(),
                key: "k".into(),
                initiated: 0,
                owner: "AK".into(),
                content_type: None,
                user_metadata: Default::default(),
                system_metadata: Default::default(),
                storage_class: None,
                tags: Default::default(),
                sse: None,
                parts,
            },
        );
        assert_eq!(s.refcount("PART"), 1);
        assert!(s.live_content().contains("PART"));

        // Aborting the upload releases them.
        s.uploads.remove("u1");
        assert_eq!(s.refcount("PART"), 0);
    }

    #[test]
    fn a_delete_marker_hides_content_without_freeing_it() {
        // Versioned delete: the newest version is a marker, so a plain GET
        // sees nothing — but the bytes must stay, since the old version is
        // still addressable by version-id.
        let entry = ObjectEntry {
            versions: vec![version("v2", None), version("v1", Some("HASH"))],
        };
        assert!(entry.current().unwrap().is_delete_marker());
        assert!(entry.current_content().is_none(), "GET must 404");
        assert!(entry.version("v1").is_some(), "still reachable by id");

        let mut s = S3State::default();
        s.objects.insert(("b".into(), "k".into()), entry);
        assert_eq!(s.refcount("HASH"), 1, "the old version still holds it");
    }
}
