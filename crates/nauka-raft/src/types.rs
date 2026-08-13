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
    /// Creates or replaces an organisation — the engine's unit of
    /// contract: a client APPLICATION (never an end user), suspendable as
    /// a whole. Replicated so every node can check it locally.
    UpsertOrg { name: String, record: OrgRecord },
    /// Removes an organisation. Refused while any space still belongs to
    /// it — deleting a customer must be an explicit, space-by-space act.
    DeleteOrg { name: String },
    /// Creates or replaces a storage space within an organisation. The
    /// name is the full `org/space` path; the record carries the space's
    /// own policies. Refused if the organisation does not exist.
    UpsertSpace { name: String, record: SpaceRecord },
    /// Removes a space.
    DeleteSpace { name: String },
    /// Registers a public key on a space. The private half is generated
    /// client-side and NEVER transmitted: the replicated state only ever
    /// holds verification material — a compromised node can check
    /// signatures, not mint them.
    AddSpaceKey { space: String, key: SpaceKey },
    /// Removes a key from a space (rotation, or a leaked frontend key).
    /// Signatures made with it die cluster-wide within one replication
    /// round-trip.
    RemoveSpaceKey { space: String, public_key: [u8; 32] },
}

/// What a space key is allowed to do. bincode is positional: new roles
/// go at the END, forever.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum SpaceKeyRole {
    /// May only sign READ links. The role for exposed surfaces (web
    /// frontends): leaked, it can hand out temporary downloads, never
    /// write or destroy.
    Signer,
    /// Full rights on the space: authenticated uploads and deletes, and
    /// everything a signer can do. Keep it on backends.
    Admin,
}

/// One Ed25519 public key registered on a space.
///
/// APPEND-ONLY: embedded in bincode snapshots — new fields go last, and
/// growing this struct mints a legacy loader shape.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SpaceKey {
    /// The Ed25519 verifying key, raw 32 bytes.
    pub public_key: [u8; 32],
    pub role: SpaceKeyRole,
    /// Human handle for rotation ("backend", "web-2026") — unique within
    /// the space.
    pub name: String,
}

/// An organisation: the engine's client. Applications (a file-sharing
/// product, a WebDAV gateway…) are organisations; their own end users
/// never appear in the engine — that boundary is what keeps this state
/// small enough to replicate everywhere. Keyed by name in
/// [`AppState::orgs`].
///
/// APPEND-ONLY: this struct is embedded in bincode snapshots — new fields
/// go last, and each growth mints a legacy loader shape in `types.rs`.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct OrgRecord {
    /// A suspended org answers nothing: every space under it goes dark on
    /// every node within one replication round-trip.
    pub suspended: bool,
    /// Optional cap on the sum of its spaces' logical bytes. `None` =
    /// uncapped.
    pub quota_bytes: Option<u64>,
}

/// A storage space: the operational unit inside an organisation, keyed by
/// its full `org/name` path in [`AppState::spaces`]. Spaces are counted in
/// DOZENS per org (split by usage: uploads, thumbnails, archives…), never
/// one per end user — anything that scales with the client's customer
/// base belongs in the client's database, not in this replicated state.
///
/// APPEND-ONLY: same snapshot rule as [`OrgRecord`].
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct SpaceRecord {
    /// The owning organisation (also the prefix of the map key; kept in
    /// the record so a space is self-describing).
    pub org: String,
    /// A suspended space refuses reads and writes; its signed links die
    /// cluster-wide.
    pub suspended: bool,
    /// `true` = files referenced by this space are served bare, no
    /// signature (direct links). Default: private.
    pub public_read: bool,
    /// Storage cap in logical bytes (sum of referenced file sizes).
    pub quota_bytes: Option<u64>,
    /// Monthly egress cap in bytes; past it, reads are throttled hard
    /// rather than cut.
    pub egress_quota_bytes: Option<u64>,
    /// Default per-connection read rate in bytes/s (applies where a
    /// signed link does not carry its own).
    pub rate_default: Option<u64>,
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
    /// Organisations — the engine's clients (name → record).
    #[serde(default)]
    pub orgs: BTreeMap<String, OrgRecord>,
    /// Storage spaces (`org/name` → record).
    #[serde(default)]
    pub spaces: BTreeMap<String, SpaceRecord>,
    /// Public keys per space (`org/name` → keys). A separate trailing map
    /// rather than a field inside [`SpaceRecord`]: growing the record
    /// would have invalidated every snapshot holding one, while a new
    /// top-level map only costs the usual legacy loader shape.
    #[serde(default)]
    pub space_keys: BTreeMap<String, Vec<SpaceKey>>,
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

/// The AppState shape BEFORE `orgs`/`spaces` were appended (i.e. with
/// `disabled` as the last field) — what every v0.5.2x binary persisted.
#[derive(Deserialize)]
pub struct AppStateLegacyV1 {
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
    #[serde(default)]
    pub disabled: std::collections::BTreeSet<String>,
}

/// The AppState shape BEFORE `space_keys` was appended (orgs/spaces
/// present) — what the AUTH-1 build persisted.
#[derive(Deserialize)]
pub struct AppStateLegacyV2 {
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
    #[serde(default)]
    pub disabled: std::collections::BTreeSet<String>,
    #[serde(default)]
    pub orgs: BTreeMap<String, OrgRecord>,
    #[serde(default)]
    pub spaces: BTreeMap<String, SpaceRecord>,
}

impl AppState {
    /// Deserialize a bincode snapshot, tolerating every previous shape:
    /// try the current one first, then each legacy shape from newest to
    /// oldest, filling the missing trailing fields with their defaults.
    pub fn from_snapshot_bytes(data: &[u8]) -> Result<Self, bincode::Error> {
        if let Ok(s) = bincode::deserialize::<AppState>(data) {
            return Ok(s);
        }
        if let Ok(v2) = bincode::deserialize::<AppStateLegacyV2>(data) {
            return Ok(AppState {
                manifests: v2.manifests,
                node_capacities: v2.node_capacities,
                node_coords: v2.node_coords,
                node_egress: v2.node_egress,
                banned: v2.banned,
                s3: v2.s3,
                disabled: v2.disabled,
                orgs: v2.orgs,
                spaces: v2.spaces,
                space_keys: Default::default(),
            });
        }
        if let Ok(v1) = bincode::deserialize::<AppStateLegacyV1>(data) {
            return Ok(AppState {
                manifests: v1.manifests,
                node_capacities: v1.node_capacities,
                node_coords: v1.node_coords,
                node_egress: v1.node_egress,
                banned: v1.banned,
                s3: v1.s3,
                disabled: v1.disabled,
                orgs: Default::default(),
                spaces: Default::default(),
                space_keys: Default::default(),
            });
        }
        let v0: AppStateLegacyV0 = bincode::deserialize(data)?;
        Ok(AppState {
            manifests: v0.manifests,
            node_capacities: v0.node_capacities,
            node_coords: v0.node_coords,
            node_egress: v0.node_egress,
            banned: v0.banned,
            s3: v0.s3,
            disabled: Default::default(),
            orgs: Default::default(),
            spaces: Default::default(),
            space_keys: Default::default(),
        })
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
            orgs: Default::default(),
            spaces: Default::default(),
            space_keys: Default::default(),
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

    // A snapshot written by a v0.5.2x binary (`disabled` present, no
    // orgs/spaces) must load with empty org and space maps.
    #[test]
    fn v1_snapshot_loads_with_empty_orgs() {
        let mut s = AppState::default();
        s.disabled.insert("2.2.2.2:7311".into());
        s.node_capacities.insert("10.0.0.1:7311".into(), 7);
        let v1_bytes = bincode::serialize(&LegacyBytesV1 {
            manifests: &s.manifests,
            node_capacities: &s.node_capacities,
            node_coords: &s.node_coords,
            node_egress: &s.node_egress,
            banned: &s.banned,
            s3: &s.s3,
            disabled: &s.disabled,
        })
        .unwrap();
        assert!(v1_bytes.len() < bincode::serialize(&s).unwrap().len());

        let loaded = AppState::from_snapshot_bytes(&v1_bytes).unwrap();
        assert!(loaded.disabled.contains("2.2.2.2:7311"));
        assert_eq!(loaded.node_capacities.get("10.0.0.1:7311"), Some(&7));
        assert!(loaded.orgs.is_empty());
        assert!(loaded.spaces.is_empty());
    }

    // A current snapshot carrying orgs and spaces round-trips intact.
    #[test]
    fn orgs_and_spaces_round_trip() {
        let mut s = AppState::default();
        s.orgs.insert(
            "yogfile".into(),
            OrgRecord {
                suspended: false,
                quota_bytes: Some(1 << 40),
            },
        );
        s.spaces.insert(
            "yogfile/uploads".into(),
            SpaceRecord {
                org: "yogfile".into(),
                suspended: false,
                public_read: false,
                quota_bytes: None,
                egress_quota_bytes: Some(1 << 42),
                rate_default: None,
            },
        );
        let bytes = bincode::serialize(&s).unwrap();
        let back = AppState::from_snapshot_bytes(&bytes).unwrap();
        assert_eq!(back.orgs.get("yogfile"), s.orgs.get("yogfile"));
        assert_eq!(
            back.spaces.get("yogfile/uploads"),
            s.spaces.get("yogfile/uploads")
        );
    }

    // Helper: the exact field set a v0.5.2x binary serialized.
    #[derive(serde::Serialize)]
    struct LegacyBytesV1<'a> {
        manifests: &'a BTreeMap<String, FileManifest>,
        node_capacities: &'a BTreeMap<String, u64>,
        node_coords: &'a BTreeMap<String, nauka_cluster::vivaldi::Coord>,
        node_egress: &'a BTreeMap<String, NodeEgress>,
        banned: &'a BTreeMap<String, String>,
        s3: &'a nauka_s3::S3State,
        disabled: &'a std::collections::BTreeSet<String>,
    }

    // A snapshot written by the AUTH-1 build (orgs/spaces present, no
    // space_keys) must load with an empty key map.
    #[test]
    fn v2_snapshot_loads_with_empty_keys() {
        let mut s = AppState::default();
        s.orgs.insert("yogfile".into(), Default::default());
        s.spaces.insert(
            "yogfile/uploads".into(),
            SpaceRecord {
                org: "yogfile".into(),
                ..Default::default()
            },
        );
        let v2_bytes = bincode::serialize(&LegacyBytesV2 {
            manifests: &s.manifests,
            node_capacities: &s.node_capacities,
            node_coords: &s.node_coords,
            node_egress: &s.node_egress,
            banned: &s.banned,
            s3: &s.s3,
            disabled: &s.disabled,
            orgs: &s.orgs,
            spaces: &s.spaces,
        })
        .unwrap();
        assert!(v2_bytes.len() < bincode::serialize(&s).unwrap().len());

        let loaded = AppState::from_snapshot_bytes(&v2_bytes).unwrap();
        assert!(loaded.orgs.contains_key("yogfile"));
        assert!(loaded.spaces.contains_key("yogfile/uploads"));
        assert!(loaded.space_keys.is_empty());
    }

    // Keys round-trip through the tolerant loader, bytes intact.
    #[test]
    fn space_keys_round_trip() {
        let mut s = AppState::default();
        s.space_keys.insert(
            "yogfile/uploads".into(),
            vec![SpaceKey {
                public_key: [7u8; 32],
                role: SpaceKeyRole::Admin,
                name: "backend".into(),
            }],
        );
        let bytes = bincode::serialize(&s).unwrap();
        let back = AppState::from_snapshot_bytes(&bytes).unwrap();
        let keys = back.space_keys.get("yogfile/uploads").unwrap();
        assert_eq!(keys[0].public_key, [7u8; 32]);
        assert_eq!(keys[0].role, SpaceKeyRole::Admin);
        assert_eq!(keys[0].name, "backend");
    }

    // Helper: the exact field set the AUTH-1 build serialized.
    #[derive(serde::Serialize)]
    struct LegacyBytesV2<'a> {
        manifests: &'a BTreeMap<String, FileManifest>,
        node_capacities: &'a BTreeMap<String, u64>,
        node_coords: &'a BTreeMap<String, nauka_cluster::vivaldi::Coord>,
        node_egress: &'a BTreeMap<String, NodeEgress>,
        banned: &'a BTreeMap<String, String>,
        s3: &'a nauka_s3::S3State,
        disabled: &'a std::collections::BTreeSet<String>,
        orgs: &'a BTreeMap<String, OrgRecord>,
        spaces: &'a BTreeMap<String, SpaceRecord>,
    }
}
