//! Consensus types: commands applied to the replicated state machine.

use std::collections::BTreeMap;
use std::io::Cursor;

use nauka_erasure::FileManifest;
use openraft::BasicNode;
use serde::{Deserialize, Serialize};

pub type NodeId = u64;

openraft::declare_raft_types!(
    /// yogfile's openraft configuration.
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
    /// Banned hashes (hash → reason): never served, never re-accepted.
    #[serde(default)]
    pub banned: BTreeMap<String, String>,
    /// The S3 view: buckets, keys, in-flight uploads, credentials.
    #[serde(default)]
    pub s3: nauka_s3::S3State,
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
