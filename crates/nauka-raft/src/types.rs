//! Consensus types: commands applied to the replicated state machine.

use std::collections::BTreeMap;
use std::io::Cursor;

use nauka_erasure::FileManifest;
use openraft::BasicNode;
use serde::{Deserialize, Serialize};

pub type NodeId = u64;

openraft::declare_raft_types!(
    /// Nauka's openraft configuration.
    pub TypeConfig:
        D = AppCommand,
        R = AppResponse,
        NodeId = NodeId,
        Node = BasicNode,
);

/// Commands replicated by Raft. Shard bytes NEVER go through the consensus
/// log — only metadata does; shards travel directly over the QUIC transport.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AppCommand {
    /// Registers a file in the cluster's replicated registry.
    RegisterManifest(FileManifest),
    /// Removes a file from the registry (the GC will purge its shards).
    UnregisterManifest { file_hash: String },
    /// Declares a node's disk capacity (the weight used by weighted
    /// placement). Keyed by announced address — the same identity placement
    /// uses.
    UpdateNodeStats { addr: String, capacity_bytes: u64 },
    /// Publishes a node's Vivaldi network coordinates: placement uses them to
    /// spread the shards of a single stripe geographically.
    UpdateNodeCoord {
        addr: String,
        coord: nauka_cluster::vivaldi::Coord,
    },
    /// Publishes a node's egress usage for the current calendar month,
    /// with its declared monthly budget. The read path prefers pulling
    /// shards from nodes with budget to spare — the flow-side twin of the
    /// capacity weight on the storage side. Self-declared: only the node
    /// itself writes its record.
    UpdateNodeEgress { addr: String, egress: NodeEgress },
    /// Bans a hash: the file leaves the registry, the API refuses to serve it
    /// (410) and the GC purges its shards. Lets us honor a takedown report or
    /// a legal request without ever reading the content.
    BanHash { file_hash: String, reason: String },
    /// Lifts a ban (misjudgment, decision overturned).
    UnbanHash { file_hash: String },

    // ── S3 layer ──────────────────────────────────────────────────────
    // The S3 view (buckets, keys, credentials) is replicated exactly like
    // the manifest registry: every node answers S3 requests, and any of
    // them can be the endpoint.
    /// Creates a set of S3 credentials.
    PutCredential(nauka_s3::Credential),
    /// Revokes a set of credentials by access key id.
    DeleteCredential { access_key_id: String },
    /// Creates a bucket. Refused if the name is taken.
    CreateBucket {
        name: String,
        bucket: Box<nauka_s3::Bucket>,
    },
    /// Replaces a bucket's configuration (policy, lifecycle, CORS…).
    UpdateBucket {
        name: String,
        bucket: Box<nauka_s3::Bucket>,
    },
    /// Deletes a bucket. Refused unless it is empty, as S3 requires.
    DeleteBucket { name: String },
    /// Adds a version to a key (a plain PUT in an unversioned bucket
    /// replaces the single "null" version).
    PutObjectVersion {
        bucket: String,
        key: String,
        version: Box<nauka_s3::ObjectVersion>,
    },
    /// Removes a specific version. In a versioned bucket a plain DELETE
    /// adds a delete marker instead (a `PutObjectVersion` with no content).
    DeleteObjectVersion {
        bucket: String,
        key: String,
        version_id: String,
    },
    /// Replaces the tag set on one object version (the current version when
    /// `version_id` is None). Tags live on the version, not the content, so
    /// this never touches shards.
    SetObjectTags {
        bucket: String,
        key: String,
        version_id: Option<String>,
        tags: BTreeMap<String, String>,
    },
    /// Sets (or clears) the Object Lock retention on one object version.
    /// `retention` is the serialized mode+until; `None` clears it.
    SetObjectRetention {
        bucket: String,
        key: String,
        version_id: Option<String>,
        retention: Option<String>,
    },
    /// Sets the Object Lock legal hold on one object version.
    SetObjectLegalHold {
        bucket: String,
        key: String,
        version_id: Option<String>,
        on: bool,
    },
    /// Replaces the ACL on one object version (the current version when
    /// `version_id` is None). `acl` is the serialized grant list; `None`
    /// restores the private default.
    SetObjectAcl {
        bucket: String,
        key: String,
        version_id: Option<String>,
        acl: Option<String>,
    },
    /// Registers an in-flight multipart upload.
    PutUpload(Box<nauka_s3::MultipartUpload>),
    /// Adds ONE part to an existing upload.
    ///
    /// Not `PutUpload` with a modified copy: clients upload parts in
    /// parallel (boto3 does by default), so read-modify-write of the whole
    /// upload loses every part but the last to land — the upload then
    /// fails at completion with InvalidPart. Merging a single part inside
    /// the state machine, where the log serializes it, is the only correct
    /// form.
    PutUploadPart {
        upload_id: String,
        part_number: u32,
        part: Box<nauka_s3::UploadedPart>,
    },
    /// Forgets a multipart upload (completed or aborted); its parts lose
    /// their references and the GC reclaims what nothing else holds.
    DeleteUpload { upload_id: String },

    // ── APPEND-ONLY BELOW ─────────────────────────────────────────────
    // The log is bincode, which encodes an enum by its VARIANT INDEX.
    // Inserting a variant anywhere above shifts every later index, so an
    // old log entry deserializes as the wrong command — a live cluster
    // whose binary was upgraded then crashes replaying its own log
    // (learned the hard way). New variants go HERE, at the end, forever.
    /// Marks a node as DRAINING (disabled=true) or active again. A
    /// disabled node stays a full member — it votes, serves reads, keeps
    /// its registry — but leaves the placement view: every shard it holds
    /// gains a new owner elsewhere, the scrubbers migrate them, and its
    /// own GC releases each one once the new owner proves possession. Its
    /// store drains to zero while the cluster never dips below full
    /// redundancy; `node remove` is then instant and safe.
    SetNodeDisabled { addr: String, disabled: bool },
}

/// One node's egress ledger for one calendar month, self-declared and
/// replicated so every node can weigh its read routing the same way.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct NodeEgress {
    /// Calendar month the counter belongs to, as `"YYYY-MM"` (UTC). A
    /// record from a previous month reads as "fresh budget".
    pub month: String,
    /// Bytes served to clients (S3 + native HTTP) during `month`.
    pub served_bytes: u64,
    /// Declared monthly budget; `None` = unmetered (never deprioritized).
    pub quota_bytes: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AppResponse {
    pub ok: bool,
    pub info: Option<String>,
}

/// State materialized by the state machine: the file registry and the
/// capacities declared by the nodes.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AppState {
    pub manifests: BTreeMap<String, FileManifest>,
    /// Disk capacity declared per node (address → bytes). Used as the weight
    /// for weighted placement; absent = default capacity.
    #[serde(default)]
    pub node_capacities: BTreeMap<String, u64>,
    /// Network coordinates declared per node (address → Vivaldi position).
    #[serde(default)]
    pub node_coords: BTreeMap<String, nauka_cluster::vivaldi::Coord>,
    /// Monthly egress ledger per node (address → month, served, budget).
    #[serde(default)]
    pub node_egress: BTreeMap<String, NodeEgress>,
    /// Banned hashes (hash → reason): never served, never re-accepted.
    #[serde(default)]
    pub banned: BTreeMap<String, String>,
    /// The S3 view: buckets, keys, in-flight uploads, credentials.
    #[serde(default)]
    pub s3: nauka_s3::S3State,

    // ── APPEND-ONLY BELOW ─────────────────────────────────────────────
    // Snapshots are bincode, which is positional and NOT self-describing:
    // `#[serde(default)]` does nothing on read. An old snapshot has no
    // bytes for a field added here, so a naive load hits EOF. New fields
    // go LAST, and the snapshot loader (see `store.rs`) falls back to the
    // previous shape and upgrades — after one snapshot cycle the state is
    // all new-format. Never insert above this line.
    /// Nodes currently draining (see [`AppCommand::SetNodeDisabled`]):
    /// excluded from placement, still full members. Keyed by advertised
    /// address.
    #[serde(default)]
    pub disabled: std::collections::BTreeSet<String>,
}

/// The AppState shape BEFORE `disabled` was appended — every field of
/// [`AppState`] except the trailing additions. An old bincode snapshot
/// deserializes into this, and [`AppState::from_legacy`] upgrades it.
/// A new legacy struct is minted each time [`AppState`] grows a trailing
/// field, capturing the previous shape.
#[derive(Deserialize)]
pub struct AppStateLegacyV0 {
    pub manifests: BTreeMap<String, FileManifest>,
    #[serde(default)]
    pub node_capacities: BTreeMap<String, u64>,
    #[serde(default)]
    pub node_coords: BTreeMap<String, nauka_cluster::vivaldi::Coord>,
    #[serde(default)]
    pub node_egress: BTreeMap<String, NodeEgress>,
    #[serde(default)]
    pub banned: BTreeMap<String, String>,
    #[serde(default)]
    pub s3: nauka_s3::S3State,
}

impl AppState {
    /// Deserialize a bincode snapshot, tolerating the pre-`disabled`
    /// format: try the current shape first, fall back to the legacy one
    /// and fill the new fields with their defaults.
    pub fn from_snapshot_bytes(data: &[u8]) -> Result<Self, bincode::Error> {
        match bincode::deserialize::<AppState>(data) {
            Ok(s) => Ok(s),
            Err(_) => {
                let v0: AppStateLegacyV0 = bincode::deserialize(data)?;
                Ok(AppState {
                    manifests: v0.manifests,
                    node_capacities: v0.node_capacities,
                    node_coords: v0.node_coords,
                    node_egress: v0.node_egress,
                    banned: v0.banned,
                    s3: v0.s3,
                    disabled: Default::default(),
                })
            }
        }
    }
}

/// Admin requests addressed to a node (outside the Raft log).
#[derive(Debug, Serialize, Deserialize)]
pub enum AdminRequest {
    /// Initializes the cluster with these members (once, on a single node).
    Init(BTreeMap<NodeId, String>),
    /// Adds a node as a learner (catches up on the log without voting).
    AddLearner { id: NodeId, addr: String },
    /// Changes the set of voting members.
    ChangeMembership(Vec<NodeId>),
    /// Writes a command via the leader (redirected if needed).
    Write(AppCommand),
    /// Cluster view: leader, members, log state.
    Metrics,
    /// List of the manifests in the replicated registry.
    ListManifests,
    /// The S3 view (buckets, credentials, uploads). Secrets included: the
    /// channel is mTLS between cluster members, and the CLI needs them to
    /// show what exists — never exposed over HTTP.
    S3State,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum AdminResponse {
    Ok(AppResponse),
    /// This node is not the leader; retry on `leader`.
    ForwardTo {
        leader: Option<(NodeId, String)>,
    },
    Metrics {
        id: NodeId,
        leader: Option<NodeId>,
        members: BTreeMap<NodeId, String>,
        last_applied: Option<u64>,
        /// Declared capacities (address → bytes) — placement's weighted view,
        /// so that clients place shards the same way the cluster does.
        #[serde(default)]
        capacities: BTreeMap<String, u64>,
    },
    Manifests(Vec<String>),
    S3State(Box<nauka_s3::S3State>),
    Err(String),
}

#[cfg(test)]
mod snapshot_compat_tests {
    use super::*;

    // A snapshot written by a PRE-`disabled` binary must load into the
    // current AppState — the exact failure that crash-looped a live
    // cluster when the field was first added mid-struct without a fallback.
    #[test]
    fn legacy_snapshot_loads_with_disabled_defaulted() {
        // Serialize the OLD shape (no `disabled` field), as an old binary
        // would have persisted it.
        let mut legacy = AppStateLegacyV0 {
            manifests: BTreeMap::new(),
            node_capacities: BTreeMap::new(),
            node_coords: BTreeMap::new(),
            node_egress: BTreeMap::new(),
            banned: BTreeMap::new(),
            s3: Default::default(),
        };
        legacy.node_capacities.insert("10.0.0.1:7311".into(), 42);
        legacy.banned.insert("deadbeef".into(), "report".into());
        // AppStateLegacyV0 has no Serialize; mint identical bytes from an
        // AppState carrying only the legacy fields, which is byte-for-byte
        // what the old binary wrote (disabled is the trailing addition).
        let old_equiv = AppState {
            manifests: legacy.manifests.clone(),
            node_capacities: legacy.node_capacities.clone(),
            node_coords: legacy.node_coords.clone(),
            node_egress: legacy.node_egress.clone(),
            banned: legacy.banned.clone(),
            s3: legacy.s3.clone(),
            disabled: Default::default(),
        };
        // Truncate the trailing empty-BTreeSet length prefix to simulate a
        // snapshot that predates the field entirely.
        let full = bincode::serialize(&old_equiv).unwrap();
        let no_disabled = bincode::serialize(&legacy_bytes_source(&old_equiv)).unwrap();
        assert!(
            no_disabled.len() < full.len(),
            "legacy bytes must be shorter"
        );

        let loaded = AppState::from_snapshot_bytes(&no_disabled).unwrap();
        assert_eq!(loaded.node_capacities.get("10.0.0.1:7311"), Some(&42));
        assert_eq!(
            loaded.banned.get("deadbeef").map(String::as_str),
            Some("report")
        );
        assert!(loaded.disabled.is_empty());
    }

    // A current-shape snapshot round-trips through the tolerant loader.
    #[test]
    fn current_snapshot_round_trips() {
        let mut s = AppState::default();
        s.disabled.insert("2.2.2.2:7311".into());
        let bytes = bincode::serialize(&s).unwrap();
        let back = AppState::from_snapshot_bytes(&bytes).unwrap();
        assert!(back.disabled.contains("2.2.2.2:7311"));
    }

    // Helper: a struct with exactly the legacy fields, so its bincode
    // output is what a pre-`disabled` binary produced.
    #[derive(serde::Serialize)]
    struct LegacyBytes<'a> {
        manifests: &'a BTreeMap<String, FileManifest>,
        node_capacities: &'a BTreeMap<String, u64>,
        node_coords: &'a BTreeMap<String, nauka_cluster::vivaldi::Coord>,
        node_egress: &'a BTreeMap<String, NodeEgress>,
        banned: &'a BTreeMap<String, String>,
        s3: &'a nauka_s3::S3State,
    }
    fn legacy_bytes_source(s: &AppState) -> LegacyBytes<'_> {
        LegacyBytes {
            manifests: &s.manifests,
            node_capacities: &s.node_capacities,
            node_coords: &s.node_coords,
            node_egress: &s.node_egress,
            banned: &s.banned,
            s3: &s.s3,
        }
    }
}
