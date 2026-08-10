//! Storage attestation: verify that a peer REALLY holds the shards
//! placement makes it responsible for.
//!
//! `has_shard` is declarative — a node can answer "yes" while its disk has
//! been wiped or silently corrupted. Two complementary proof mechanisms:
//!
//! 1. **Nonce challenge** (`ProveShard`): the peer returns
//!    `blake3(nonce ‖ bytes)`, unpredictable and non-replayable. Verifiable
//!    only if the verifier holds the bytes — which is exactly the GC's
//!    case, as it now demands this proof before releasing its copy (see
//!    healer.rs).
//!
//! 2. **Sampling audit** (this module): in steady state each shard has only
//!    ONE holder — nobody else has the bytes to verify a challenge. So the
//!    auditor samples shards the peer OWNS according to placement,
//!    downloads them, and checks their hash against the manifest. Since
//!    storage is content-addressed, cheating means producing bytes with an
//!    imposed BLAKE3 — a preimage. Cost: `SAMPLE_PER_PEER` x 1 MiB per peer
//!    per pass, bounded and tunable.
//!
//! A "missing" answer for a shard the peer owns according to placement is
//! the useful signal: either its scrubber is lagging (transient), or the
//! node has lost data (persistent → alert).

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
use nauka_store::ShardStore;
use nauka_transport::PeerClient;
use rand::{RngCore, SeedableRng};
use tracing::{info, warn};

/// Shards downloaded and verified per peer per pass.
pub const SAMPLE_PER_PEER: usize = 3;

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct AuditReport {
    /// Verifications attempted.
    pub challenged: usize,
    /// Downloaded shards whose hash matches the manifest.
    pub proved: usize,
    /// The peer could not supply the shard (missing or corrupted on its
    /// side).
    pub missing: usize,
    /// The peer supplied bytes with the WRONG hash — a serious anomaly
    /// (impossible short of a bug or malice, the transport checks too).
    pub failed: usize,
    /// Unreachable peers (not counted as faults).
    pub unreachable: usize,
}

/// Expected proof of possession for a nonce challenge.
pub fn expected_proof(nonce: &[u8; 32], data: &[u8]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(nonce);
    hasher.update(data);
    *hasher.finalize().as_bytes()
}

/// One audit pass: for each peer, samples shards it owns according to
/// placement and verifies that it really holds them.
pub async fn audit_once(
    store: &Arc<ShardStore>,
    self_id: &str,
    all_nodes: &[(String, u64)],
) -> Result<AuditReport> {
    audit_once_geo(store, self_id, all_nodes, &Default::default()).await
}

/// Geo-aware variant (see [`crate::placement::stripe_owners_geo`]).
pub async fn audit_once_geo(
    store: &Arc<ShardStore>,
    self_id: &str,
    all_nodes: &[(String, u64)],
    coords: &crate::placement::CoordMap,
) -> Result<AuditReport> {
    let mut report = AuditReport::default();
    let node_refs: Vec<(&str, u64)> = all_nodes.iter().map(|(n, w)| (n.as_str(), *w)).collect();

    // Who owns what, according to the locally known manifests.
    let mut owned_by_peer: HashMap<&str, Vec<(String, String)>> = HashMap::new();
    for file_hash in store.list_manifests()? {
        let manifest = store.get_manifest(&file_hash)?;
        for (si, stripe) in manifest.stripes.iter().enumerate() {
            let stripe_owners = crate::placement::stripe_owners_geo(
                &manifest.file_hash,
                si,
                stripe.shard_hashes.len(),
                &node_refs,
                coords,
            );
            for (i, hash) in stripe.shard_hashes.iter().enumerate() {
                let owner = stripe_owners[i];
                if owner != self_id {
                    if let Some((node, _)) = node_refs.iter().find(|(n, _)| *n == owner) {
                        owned_by_peer
                            .entry(node)
                            .or_default()
                            .push((hash.clone(), manifest.file_hash.clone()));
                    }
                }
            }
        }
    }

    let mut rng = rand::rngs::StdRng::from_entropy();
    for (peer, owned) in owned_by_peer {
        if owned.is_empty() {
            continue;
        }
        let Ok(addr) = peer.parse::<SocketAddr>() else {
            continue;
        };
        let Ok(client) = PeerClient::connect(addr).await else {
            report.unreachable += 1;
            continue;
        };

        for _ in 0..SAMPLE_PER_PEER.min(owned.len()) {
            let (shard_hash, file) = &owned[(rng.next_u64() % owned.len() as u64) as usize];
            report.challenged += 1;
            match client.get_shard(shard_hash).await {
                Ok(Some(data)) if nauka_erasure::hash_bytes(&data) == *shard_hash => {
                    report.proved += 1;
                }
                Ok(Some(_)) => {
                    warn!(peer = %peer, shard = %shard_hash, file = %file,
                          "AUDIT: bytes with the wrong hash — the peer does not hold what it serves");
                    report.failed += 1;
                }
                Ok(None) => {
                    // Missing although placement assigns it to this peer:
                    // its scrubber should heal it — watch if it persists.
                    report.missing += 1;
                }
                Err(e) => {
                    warn!(peer = %peer, "audit interrupted: {e}");
                    report.challenged -= 1;
                    report.unreachable += 1;
                    break;
                }
            }
        }
    }

    if report.failed > 0 {
        warn!(
            "audit: {} verification(s) FAILED out of {} — a peer is serving invalid bytes",
            report.failed, report.challenged
        );
    } else if report.challenged > 0 {
        info!(
            "audit: {}/{} possessions proved, {} missing",
            report.proved, report.challenged, report.missing
        );
    }
    crate::telemetry::record_audit_report(
        report.challenged as u64,
        report.proved as u64,
        report.missing as u64,
        report.failed as u64,
        report.unreachable as u64,
    );
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proof_binds_nonce_and_content() {
        let n1 = [1u8; 32];
        let n2 = [2u8; 32];
        // Same content, different nonce → different proof (no replay).
        assert_ne!(expected_proof(&n1, b"data"), expected_proof(&n2, b"data"));
        // Same nonce, different content → different proof.
        assert_ne!(expected_proof(&n1, b"data"), expected_proof(&n1, b"datb"));
        // Deterministic.
        assert_eq!(expected_proof(&n1, b"data"), expected_proof(&n1, b"data"));
    }
}
