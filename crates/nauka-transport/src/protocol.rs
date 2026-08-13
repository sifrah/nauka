//! Inter-node protocol: messages serialized with bincode, framed by a
//! little-endian u32 length, one request/response exchange per bidirectional
//! QUIC stream.

use nauka_erasure::FileManifest;
use serde::{Deserialize, Serialize};

/// Safety net: max size of a message (1 MiB shard + headroom, manifests).
pub const MAX_MESSAGE_SIZE: u32 = 64 * 1024 * 1024;

/// ALPN of the Nauka cluster protocol.
pub const ALPN: &[u8] = b"nauka/0";

#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    Ping,
    /// Stores a shard on the remote node (idempotent, content-addressed).
    PutShard(Vec<u8>),
    /// Fetches a shard by hash.
    GetShard(String),
    /// Does the node hold this shard?
    HasShard(String),
    /// Proof of possession: `blake3(nonce ‖ shard bytes)`. Unlike `HasShard`,
    /// it cannot be answered without actually re-reading the bytes (the nonce
    /// is drawn at random by the challenger every time).
    ProveShard {
        hash: String,
        nonce: [u8; 32],
    },
    /// Replicates a manifest on the remote node.
    PutManifest(FileManifest),
    /// Fetches a manifest by file hash.
    GetManifest(String),
    /// Raft (openraft) message: bincode payload, opaque to the transport.
    Raft(RaftRpc),
    // ── APPEND-ONLY BELOW ── bincode is positional: inserting a variant
    // above this line breaks the wire protocol between mixed versions
    // during a rolling deploy. An old peer answers an unknown variant with
    // `Error("unreadable request")`, which callers must treat as a refusal.
    /// Proof of possession PLUS an ownership claim: the proof is the same
    /// `blake3(nonce ‖ bytes)` as [`Request::ProveShard`], and `owner` in
    /// the response says whether the peer considers itself an owner of the
    /// shard under ITS OWN current placement view. The rebalancing GC
    /// requires both: a bare proof is a snapshot in time, and two holders
    /// with crossed views can each prove to the other and both delete —
    /// zero copies left (lost a live file to exactly that race on
    /// 2026-08-12). A node that claims ownership never releases the shard
    /// in the same pass, so requiring the claim makes mutual deletion
    /// impossible by construction.
    ProveShardOwned {
        hash: String,
        nonce: [u8; 32],
    },
    /// Asks a peer for a DECODED stripe from its local cache (cooperative
    /// regional cache: one transfer from a close neighbor instead of k
    /// shard fetches from far owners). Answered from cache only — a miss
    /// is a miss, the peer never reconstructs on the asker's behalf
    /// (that would let one busy region chain-drag another).
    GetCachedStripe {
        file_hash: String,
        stripe_idx: u32,
    },
}

/// Consensus RPCs, carried as-is; only the nauka-raft layer knows how to
/// deserialize them.
#[derive(Debug, Serialize, Deserialize)]
pub enum RaftRpc {
    AppendEntries(Vec<u8>),
    Vote(Vec<u8>),
    InstallSnapshot(Vec<u8>),
    /// Admin/client command (init, add-learner, change-membership,
    /// client-write, metrics) — handled by the local node.
    Admin(Vec<u8>),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    Pong,
    /// Hash of the stored shard.
    PutShardOk(String),
    /// `None` if the shard is missing or corrupt on the remote node.
    Shard(Option<Vec<u8>>),
    Has(bool),
    /// Answer to the challenge: `None` if the shard is missing or corrupt.
    Proof(Option<[u8; 32]>),
    PutManifestOk,
    Manifest(Option<FileManifest>),
    /// Raft response: opaque bincode payload.
    Raft(Vec<u8>),
    /// Application-level error on the server side.
    Error(String),
    // ── APPEND-ONLY BELOW ── same positional-bincode rule as `Request`.
    /// Answer to [`Request::ProveShardOwned`]: the proof (`None` if the
    /// shard is missing or corrupt) and whether the peer claims ownership
    /// of it under its own view.
    ProofOwned {
        proof: Option<[u8; 32]>,
        owner: bool,
    },
    /// The cached stripe, or None on a cache miss. The caller MUST verify
    /// integrity (re-encode and match the manifest's shard hashes): the
    /// transport authenticates the peer, not the bytes.
    CachedStripe(Option<Vec<u8>>),
}

#[derive(Debug, thiserror::Error)]
pub enum WireError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("message too large: {0} bytes (max {MAX_MESSAGE_SIZE})")]
    TooLarge(u32),
    #[error("serialization: {0}")]
    Codec(#[from] bincode::Error),
    #[error("quic stream: {0}")]
    Stream(String),
}

/// Writes a framed message to an outgoing QUIC stream.
pub async fn write_message<T: Serialize>(
    send: &mut quinn::SendStream,
    msg: &T,
) -> Result<(), WireError> {
    let payload = bincode::serialize(msg)?;
    let len = u32::try_from(payload.len()).map_err(|_| WireError::TooLarge(u32::MAX))?;
    if len > MAX_MESSAGE_SIZE {
        return Err(WireError::TooLarge(len));
    }
    send.write_all(&len.to_le_bytes())
        .await
        .map_err(|e| WireError::Stream(e.to_string()))?;
    send.write_all(&payload)
        .await
        .map_err(|e| WireError::Stream(e.to_string()))?;
    // After the write, not before: a message that failed halfway never
    // reached the wire in full, and counting it would inflate throughput
    // exactly when the link is broken.
    crate::telemetry::record_wire_bytes(crate::telemetry::OUT, payload.len());
    Ok(())
}

/// Reads a framed message from an incoming QUIC stream.
pub async fn read_message<T: serde::de::DeserializeOwned>(
    recv: &mut quinn::RecvStream,
) -> Result<T, WireError> {
    let mut len_buf = [0u8; 4];
    recv.read_exact(&mut len_buf)
        .await
        .map_err(|e| WireError::Stream(e.to_string()))?;
    let len = u32::from_le_bytes(len_buf);
    if len > MAX_MESSAGE_SIZE {
        return Err(WireError::TooLarge(len));
    }
    let mut payload = vec![0u8; len as usize];
    recv.read_exact(&mut payload)
        .await
        .map_err(|e| WireError::Stream(e.to_string()))?;
    // The bytes arrived; whether they deserialize is a separate question,
    // and a truncated or corrupt payload still cost the link its bandwidth.
    crate::telemetry::record_wire_bytes(crate::telemetry::IN, payload.len());
    Ok(bincode::deserialize(&payload)?)
}
