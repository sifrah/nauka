//! Transport telemetry: the labels and the recording sites for everything
//! that crosses the QUIC layer.
//!
//! Recording goes through the `metrics` facade, which compiles to a no-op
//! until a recorder is installed — so a library, a bench or a test that
//! never installs one pays nothing for the instrumentation below.
//!
//! The module is named `telemetry` rather than `metrics` on purpose: a
//! local module of the latter name would shadow the crate and make every
//! `metrics::` path here ambiguous.
//!
//! Histogram buckets are NOT configured here. The exporter matches on the
//! metric-name suffix (`_seconds`, `_bytes`), so a well-named metric
//! inherits the right ladder and stays consistent with the rest of the
//! process.
//!
//! ## Label discipline
//!
//! Every label below is a closed set: `op` is one variant name of
//! [`Request`], and `result`/`reason`/`direction` are `&'static str`
//! constants. Nothing derived from user data — never a shard hash, never a
//! file hash — may become a label: one series per stored object would
//! destroy the exporter long before it helped anyone.

use std::time::Duration;

use crate::protocol::Request;

/// Outbound: this node called a peer.
pub const OUT: &str = "out";
/// Inbound: a peer called this node.
pub const IN: &str = "in";

/// Register the HELP/TYPE text of every transport metric.
///
/// Called once by the binary that installs the recorder. Describing a
/// metric that is never recorded is harmless — the description alone
/// creates no series.
pub fn describe() {
    metrics::describe_counter!(
        "nauka_transport_requests_total",
        "Peer RPCs, by operation, outcome and direction. direction=out is measured at the caller, direction=in at the server that served it — a cluster-wide sum over both double-counts every exchange."
    );
    metrics::describe_histogram!(
        "nauka_transport_request_duration_seconds",
        "Round-trip time of an outbound peer RPC, measured at the caller: stream open, request written, response read. Timed-out requests are recorded at the timeout."
    );
    metrics::describe_counter!(
        "nauka_transport_connections_total",
        "QUIC connections established or refused, by direction and outcome. Every outbound call builds a fresh endpoint — there is no pool, so this counts attempts, not a steady state."
    );
    metrics::describe_counter!(
        "nauka_transport_connection_closes_total",
        "Inbound connections that ended, by how they ended. Anything other than the peer closing cleanly is a churn or fault signal."
    );
    metrics::describe_histogram!(
        "nauka_transport_wire_bytes",
        "Size of a framed protocol message, payload only (the 4-byte length prefix is excluded), by direction."
    );
}

/// The operation label for a request.
///
/// A variant name, not a payload: `Request::GetShard(hash)` is `get_shard`
/// for every hash in existence. Raft RPCs collapse to a single `raft` —
/// the consensus plane has its own instrumentation, and splitting the
/// sub-variants here would only duplicate it.
pub fn op(req: &Request) -> &'static str {
    match req {
        Request::Ping => "ping",
        Request::PutShard(_) => "put_shard",
        Request::GetShard(_) => "get_shard",
        Request::HasShard(_) => "has_shard",
        Request::ProveShard { .. } => "prove_shard",
        Request::PutManifest(_) => "put_manifest",
        Request::GetManifest(_) => "get_manifest",
        Request::Raft(_) => "raft",
    }
}

/// Outcome of one RPC, and the reason the three failure modes are kept
/// apart: a timeout is a slow or wedged peer, a transport error is a broken
/// connection, and `peer_error` is a peer that answered perfectly well with
/// a refusal — an application fault, not a network one. An operator fixes
/// each of the three somewhere else entirely.
///
/// `TIMEOUT`, `TRANSPORT` and `PEER_ERROR` only ever appear on outbound
/// requests; a server sees neither its caller's deadline nor a broken path
/// (those show up as a connection that goes away, under
/// `nauka_transport_connection_closes_total`), so inbound requests are only
/// ever `OK` or `ERROR`.
pub mod result {
    pub const OK: &str = "ok";
    pub const TIMEOUT: &str = "timeout";
    pub const TRANSPORT: &str = "transport";
    pub const PEER_ERROR: &str = "peer_error";
    pub const ERROR: &str = "error";
}

/// Record a finished RPC. `elapsed` is only meaningful for [`OUT`], where
/// the caller owns the whole round trip; inbound requests are counted but
/// not timed, so the histogram keeps one unambiguous meaning.
pub fn record_request(direction: &'static str, op: &'static str, result: &'static str) {
    metrics::counter!(
        "nauka_transport_requests_total",
        "op" => op,
        "result" => result,
        "direction" => direction,
    )
    .increment(1);
}

/// Latency of one outbound RPC, timeouts included (recorded at the
/// timeout, which is the honest lower bound on what the caller waited).
pub fn record_request_duration(op: &'static str, elapsed: Duration) {
    metrics::histogram!("nauka_transport_request_duration_seconds", "op" => op)
        .record(elapsed.as_secs_f64());
}

/// Outcome of a connection attempt. Outbound uses `OK`/`TIMEOUT`/`ERROR`
/// (a handshake that ran past [`crate::client`]'s connect deadline is not
/// the same failure as one the peer refused); inbound uses
/// `ACCEPTED`/`REJECTED`, which is the split the accept loops already make.
pub mod conn {
    pub const OK: &str = "ok";
    pub const TIMEOUT: &str = "timeout";
    pub const ERROR: &str = "error";
    pub const ACCEPTED: &str = "accepted";
    pub const REJECTED: &str = "rejected";
}

/// A connection attempt that finished, one way or the other.
pub fn record_connection(direction: &'static str, result: &'static str) {
    metrics::counter!(
        "nauka_transport_connections_total",
        "direction" => direction,
        "result" => result,
    )
    .increment(1);
}

/// How an inbound connection ended. See [`close`] for the values.
pub fn record_close(reason: &'static str) {
    metrics::counter!("nauka_transport_connection_closes_total", "reason" => reason).increment(1);
}

/// Reasons an inbound connection stopped serving streams.
pub mod close {
    /// The peer closed the connection from the application layer — the
    /// normal end of a `PeerClient` that has been dropped.
    pub const APPLICATION: &str = "application_closed";
    /// The peer closed it at the QUIC layer.
    pub const CONNECTION: &str = "connection_closed";
    /// No traffic for the idle timeout: the peer vanished without saying
    /// goodbye, which is what a crash or a partition looks like.
    pub const TIMED_OUT: &str = "timed_out";
    /// Anything else — a protocol violation, a broken path.
    pub const ERROR: &str = "error";
}

/// Bytes of one framed message. The payload only: the length prefix is a
/// constant 4 bytes and adding it would just skew every bucket.
pub fn record_wire_bytes(direction: &'static str, bytes: usize) {
    metrics::histogram!("nauka_transport_wire_bytes", "direction" => direction)
        .record(bytes as f64);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_request_variant_has_a_bounded_op_label() {
        let manifest = nauka_erasure::FileManifest {
            file_hash: String::new(),
            file_size: 0,
            name: None,
            expires_at: None,
            config: nauka_erasure::ErasureConfig::default(),
            stripes: Vec::new(),
        };
        // Exhaustive by construction: `op` matches without a wildcard, so a
        // new Request variant fails to compile rather than silently
        // landing in a catch-all bucket.
        let cases = [
            (Request::Ping, "ping"),
            (Request::PutShard(vec![1, 2, 3]), "put_shard"),
            (Request::GetShard("deadbeef".into()), "get_shard"),
            (Request::HasShard("deadbeef".into()), "has_shard"),
            (
                Request::ProveShard {
                    hash: "deadbeef".into(),
                    nonce: [0u8; 32],
                },
                "prove_shard",
            ),
            (Request::PutManifest(manifest), "put_manifest"),
            (Request::GetManifest("deadbeef".into()), "get_manifest"),
            (
                Request::Raft(crate::protocol::RaftRpc::Vote(Vec::new())),
                "raft",
            ),
        ];
        for (req, want) in cases {
            assert_eq!(op(&req), want);
        }
    }

    #[test]
    fn op_labels_never_carry_the_payload() {
        // The regression this guards: `format!("{req:?}")` as a label would
        // mint one series per shard hash and take the exporter down.
        let hash = "b3f1c0de".repeat(8);
        assert_eq!(op(&Request::GetShard(hash.clone())), "get_shard");
        assert!(!op(&Request::GetShard(hash)).contains("b3f1"));
    }

    #[test]
    fn recording_without_a_recorder_is_inert() {
        // No recorder is installed in unit tests; every site must still be
        // safe to call. Telemetry may never be the reason a request fails.
        record_request(OUT, "ping", result::OK);
        record_request_duration("ping", Duration::from_millis(1));
        record_connection(IN, conn::ACCEPTED);
        record_close(close::TIMED_OUT);
        record_wire_bytes(OUT, 4096);
    }
}
