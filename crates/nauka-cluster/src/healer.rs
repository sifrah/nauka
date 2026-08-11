//! Self-healing: every node periodically checks that it really holds the
//! shards placement makes it responsible for. A missing or corrupted shard
//! is regenerated with Reed-Solomon from the rest of the cluster, without
//! human intervention.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{bail, Result};
use nauka_erasure::{decode_stripe, encode_stripe, FileManifest};
use nauka_store::ShardStore;
use nauka_transport::PeerClient;
use tracing::{info, warn};

#[derive(Debug, Default)]
pub struct HealReport {
    pub shards_checked: usize,
    pub shards_healed: usize,
    pub shards_unrecoverable: usize,
}

#[derive(Debug, Default)]
pub struct GcReport {
    pub shards_released: usize,
    pub shards_kept: usize,
    /// Shards purged because no live file references them any more
    /// (file deleted, expired or banned).
    pub orphans_purged: usize,
    /// Local manifests removed because they are absent from the replicated
    /// registry.
    pub manifests_purged: usize,
}

/// An unreferenced shard younger than this is NOT an orphan: it is an
/// upload in flight. Shards land on their owners stripe by stripe and the
/// manifest is only registered once the whole file is dispatched, so a
/// slow upload keeps shards unreferenced for its entire duration. Purging
/// them mid-flight destroys the file while the client is told "200 OK"
/// (observed on a 5-node WAN cluster: a 21-second upload lost 93 of its
/// 125 stripes to the GC). The grace must exceed any plausible upload
/// duration; disk held by a genuinely abandoned upload is reclaimed one
/// hour later.
pub const ORPHAN_GRACE: std::time::Duration = std::time::Duration::from_secs(3600);

/// Purge of deleted files: removes the local manifests absent from the
/// replicated registry, then the shards no live manifest references any
/// more.
///
/// `live_manifests` is the authoritative list (the Raft registry). The
/// purge happens ONLY if that list is trustworthy: a node that has just
/// started and has not received the state yet must erase nothing, hence
/// the `registry_ready` parameter. `orphan_grace` is how long a shard may
/// sit unreferenced before it is considered an orphan — production passes
/// [`ORPHAN_GRACE`], tests shorten it.
pub fn purge_deleted(
    store: &Arc<ShardStore>,
    live_manifests: &std::collections::BTreeSet<String>,
    registry_ready: bool,
    orphan_grace: std::time::Duration,
) -> Result<GcReport> {
    let mut report = GcReport::default();
    if !registry_ready {
        return Ok(report);
    }

    // 1. Local manifests no longer in the registry → deleted.
    for local in store.list_manifests()? {
        if !live_manifests.contains(&local) {
            store.delete_manifest(&local)?;
            report.manifests_purged += 1;
        }
    }

    // 2. Shards that none of the remaining local manifests reference.
    let mut referenced: std::collections::BTreeSet<String> = Default::default();
    for file_hash in store.list_manifests()? {
        let manifest = store.get_manifest(&file_hash)?;
        for stripe in &manifest.stripes {
            for hash in &stripe.shard_hashes {
                referenced.insert(hash.clone());
            }
        }
    }
    for shard in store.list_shards()? {
        if referenced.contains(&shard) {
            continue;
        }
        // Only a shard that has been unreferenced for a while is an orphan.
        // A young one belongs to an upload still in flight — its manifest
        // does not exist yet. `None` (unreadable age) counts as young:
        // when in doubt, never delete.
        match store.shard_age(&shard) {
            Some(age) if age >= orphan_grace => {
                store.delete_shard(&shard)?;
                report.orphans_purged += 1;
            }
            _ => report.shards_kept += 1,
        }
    }
    crate::telemetry::record_gc_report(
        0,
        report.orphans_purged as u64,
        report.manifests_purged as u64,
    );
    Ok(report)
}

/// Rebalancing GC: releases the local shards this node no longer owns (the
/// cluster view changed — node added/removed).
///
/// Maximum caution: a shard is deleted only if ALL of its current owners
/// (a shard may be referenced by several manifests) supply a PROOF of
/// possession — `blake3(nonce ‖ bytes)`, checked against our local copy —
/// and not a mere `has_shard` claim. An unreachable owner, or one without
/// a proof → we keep the shard. Shards unknown to the local manifests are
/// never touched.
pub async fn gc_once(
    store: &Arc<ShardStore>,
    self_id: &str,
    all_nodes: &[(String, u64)],
) -> Result<GcReport> {
    gc_once_geo(store, self_id, all_nodes, &Default::default()).await
}

/// Geo-aware variant (see [`crate::placement::stripe_owners_geo`]).
pub async fn gc_once_geo(
    store: &Arc<ShardStore>,
    self_id: &str,
    all_nodes: &[(String, u64)],
    coords: &crate::placement::CoordMap,
) -> Result<GcReport> {
    let node_refs: Vec<(&str, u64)> = all_nodes.iter().map(|(n, w)| (n.as_str(), *w)).collect();

    // shard → owners (across all manifests).
    let mut owners: HashMap<String, std::collections::BTreeSet<String>> = HashMap::new();
    for file_hash in store.list_manifests()? {
        let manifest = store.get_manifest(&file_hash)?;
        for (si, stripe) in manifest.stripes.iter().enumerate() {
            let stripe_owners = crate::placement::stripe_owners_geo(
                crate::placement::stripe_key_of(stripe),
                si,
                stripe.shard_hashes.len(),
                &node_refs,
                coords,
            );
            for (i, hash) in stripe.shard_hashes.iter().enumerate() {
                owners
                    .entry(hash.clone())
                    .or_default()
                    .insert(stripe_owners[i].to_string());
            }
        }
    }

    let mut peers: HashMap<String, Option<PeerClient>> = HashMap::new();
    let mut report = GcReport::default();
    for shard in store.list_shards()? {
        let Some(shard_owners) = owners.get(&shard) else {
            // Orphan shard (deregistered file?): out of scope for v1.
            continue;
        };
        if shard_owners.iter().any(|o| o == self_id) {
            continue; // still ours, nothing to do
        }
        // We still hold the bytes, so we can DEMAND a proof of possession
        // before releasing them.
        let Ok(local_data) = store.get_shard(&shard) else {
            continue;
        };
        let mut all_confirmed = true;
        for owner in shard_owners {
            let client = peer_once(&mut peers, owner).await;
            let proved = match client {
                Some(c) => {
                    let nonce: [u8; 32] = rand::random();
                    match c.prove_shard(&shard, nonce).await {
                        Ok(Some(proof)) => {
                            proof == crate::audit::expected_proof(&nonce, &local_data)
                        }
                        _ => false,
                    }
                }
                None => false,
            };
            if !proved {
                all_confirmed = false;
                break;
            }
        }
        if all_confirmed {
            store.delete_shard(&shard)?;
            report.shards_released += 1;
        } else {
            report.shards_kept += 1;
        }
    }
    crate::telemetry::record_gc_report(report.shards_released as u64, 0, 0);
    Ok(report)
}

/// A full scrub pass over every locally known manifest.
///
/// `self_id` is this node's advertised address, `all_nodes` the cluster
/// view (this node included) — both must be consistent with what the other
/// nodes use, so that placement converges.
pub async fn scrub_once(
    store: &Arc<ShardStore>,
    self_id: &str,
    all_nodes: &[(String, u64)],
) -> Result<HealReport> {
    scrub_once_geo(store, self_id, all_nodes, &Default::default()).await
}

/// Geo-aware variant (see [`crate::placement::stripe_owners_geo`]).
pub async fn scrub_once_geo(
    store: &Arc<ShardStore>,
    self_id: &str,
    all_nodes: &[(String, u64)],
    coords: &crate::placement::CoordMap,
) -> Result<HealReport> {
    let node_refs: Vec<(&str, u64)> = all_nodes.iter().map(|(n, w)| (n.as_str(), *w)).collect();
    let mut peers: HashMap<String, Option<PeerClient>> = HashMap::new();
    let mut report = HealReport::default();

    for file_hash in store.list_manifests()? {
        let manifest = store.get_manifest(&file_hash)?;
        for (stripe_idx, shard_idx, shard_hash) in
            crate::placement::shards_owned_by_geo(&manifest, self_id, &node_refs, coords)
        {
            report.shards_checked += 1;
            // get_shard verifies the hash: missing OR corrupted → we heal.
            if store.get_shard(shard_hash).is_ok() {
                continue;
            }
            match heal_shard(
                store, &manifest, stripe_idx, shard_idx, &node_refs, self_id, &mut peers,
            )
            .await
            {
                Ok(()) => {
                    info!(
                        file = %file_hash, stripe = stripe_idx, shard = shard_idx,
                        "shard healed"
                    );
                    report.shards_healed += 1;
                }
                Err(e) => {
                    warn!(
                        file = %file_hash, stripe = stripe_idx, shard = shard_idx,
                        "unrecoverable for now: {e}"
                    );
                    report.shards_unrecoverable += 1;
                }
            }
        }
    }
    // Recorded here, at the single implementation both entry points funnel
    // through, so every caller — consensus ticker, static loop, tests — is
    // counted exactly once.
    crate::telemetry::record_heal_report(
        report.shards_checked as u64,
        report.shards_healed as u64,
        report.shards_unrecoverable as u64,
    );
    Ok(report)
}

/// Connection to a peer, attempted at most ONCE per pass.
///
/// `peers` doubles as a negative cache: a key present with `None` means
/// "tried, unreachable — do not try again this pass". Retrying per shard
/// multiplied the connect timeout by the number of shards and turned one
/// dead node into a maintenance loop that never came back around.
async fn peer_once<'a>(
    peers: &'a mut HashMap<String, Option<PeerClient>>,
    node: &str,
) -> Option<&'a PeerClient> {
    if !peers.contains_key(node) {
        let client = match node.parse::<SocketAddr>() {
            Ok(addr) => PeerClient::connect(addr).await.ok(),
            Err(_) => None,
        };
        peers.insert(node.to_string(), client);
    }
    peers.get(node).and_then(|c| c.as_ref())
}

/// Regenerates one specific shard: collects ≥ k shards of the stripe
/// (local + peers), decodes the original data, re-encodes the stripe and
/// stores the missing shard. The manifest hashes guarantee that the healed
/// shard is identical to the original.
async fn heal_shard(
    store: &Arc<ShardStore>,
    manifest: &FileManifest,
    stripe_idx: usize,
    shard_idx: usize,
    all_nodes: &[(&str, u64)],
    self_id: &str,
    peers: &mut HashMap<String, Option<PeerClient>>,
) -> Result<()> {
    let meta = &manifest.stripes[stripe_idx];
    let mut slots: Vec<Option<Vec<u8>>> = Vec::with_capacity(meta.shard_hashes.len());

    for hash in &meta.shard_hashes {
        // Local store first, otherwise ask the other nodes (the nominal
        // owner first, then the rest).
        if let Ok(data) = store.get_shard(hash) {
            slots.push(Some(data));
            continue;
        }
        let mut found = None;
        let mut candidates = crate::placement::rank_nodes(hash, all_nodes);
        candidates.retain(|n| *n != self_id);
        for (node, _) in all_nodes.iter().filter(|(n, _)| *n != self_id) {
            if !candidates.contains(node) {
                candidates.push(node);
            }
        }
        for node in candidates {
            if let Some(c) = peer_once(peers, node).await {
                if let Ok(Some(data)) = c.get_shard(hash).await {
                    found = Some(data);
                    break;
                }
            }
        }
        slots.push(found);
    }

    // decode_stripe discards corrupted shards and requires ≥ k valid ones.
    let stripe_data = decode_stripe(slots, meta, &manifest.config)?;
    let shards = encode_stripe(&stripe_data, &manifest.config)?;
    let shard = &shards[shard_idx];
    if shard.hash != meta.shard_hashes[shard_idx] {
        bail!("re-encoded shard inconsistent with the manifest");
    }
    store.put_shard(&shard.data)?;
    Ok(())
}
