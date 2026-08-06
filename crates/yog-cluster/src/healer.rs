//! Auto-healing : chaque nœud vérifie périodiquement qu'il détient bien les
//! shards dont le placement le rend responsable. Un shard manquant ou
//! corrompu est régénéré par Reed-Solomon depuis le reste du cluster, sans
//! intervention humaine.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{bail, Result};
use tracing::{info, warn};
use yog_erasure::{decode_stripe, encode_stripe, FileManifest};
use yog_store::ShardStore;
use yog_transport::PeerClient;

#[derive(Debug, Default)]
pub struct HealReport {
    pub shards_checked: usize,
    pub shards_healed: usize,
    pub shards_unrecoverable: usize,
}

/// Une passe de scrub complète sur tous les manifests connus localement.
///
/// `self_id` est l'adresse annoncée de ce nœud, `all_nodes` la vue du cluster
/// (ce nœud inclus) — les deux doivent être cohérents avec ce que les autres
/// nœuds utilisent pour que le placement converge.
pub async fn scrub_once(
    store: &Arc<ShardStore>,
    self_id: &str,
    all_nodes: &[String],
) -> Result<HealReport> {
    let node_refs: Vec<&str> = all_nodes.iter().map(String::as_str).collect();
    let mut peers: HashMap<String, Option<PeerClient>> = HashMap::new();
    let mut report = HealReport::default();

    for file_hash in store.list_manifests()? {
        let manifest = store.get_manifest(&file_hash)?;
        for (stripe_idx, shard_idx, shard_hash) in
            crate::placement::shards_owned_by(&manifest, self_id, &node_refs)
        {
            report.shards_checked += 1;
            // get_shard vérifie le hash : manquant OU corrompu → on répare.
            if store.get_shard(shard_hash).is_ok() {
                continue;
            }
            match heal_shard(store, &manifest, stripe_idx, shard_idx, &node_refs, self_id, &mut peers)
                .await
            {
                Ok(()) => {
                    info!(
                        file = %file_hash, stripe = stripe_idx, shard = shard_idx,
                        "shard régénéré"
                    );
                    report.shards_healed += 1;
                }
                Err(e) => {
                    warn!(
                        file = %file_hash, stripe = stripe_idx, shard = shard_idx,
                        "irréparable pour l'instant: {e}"
                    );
                    report.shards_unrecoverable += 1;
                }
            }
        }
    }
    Ok(report)
}

/// Régénère un shard précis : collecte ≥ k shards de la stripe (local +
/// peers), décode les données originales, ré-encode la stripe et stocke le
/// shard manquant. Les hashes du manifest garantissent que le shard régénéré
/// est identique à l'original.
async fn heal_shard(
    store: &Arc<ShardStore>,
    manifest: &FileManifest,
    stripe_idx: usize,
    shard_idx: usize,
    all_nodes: &[&str],
    self_id: &str,
    peers: &mut HashMap<String, Option<PeerClient>>,
) -> Result<()> {
    let meta = &manifest.stripes[stripe_idx];
    let mut slots: Vec<Option<Vec<u8>>> = Vec::with_capacity(meta.shard_hashes.len());

    for hash in &meta.shard_hashes {
        // D'abord le store local, sinon on demande aux autres nœuds
        // (le propriétaire théorique en premier, puis les autres).
        if let Ok(data) = store.get_shard(hash) {
            slots.push(Some(data));
            continue;
        }
        let mut found = None;
        let mut candidates = crate::placement::rank_nodes(hash, all_nodes);
        candidates.retain(|n| *n != self_id);
        for node in all_nodes.iter().filter(|n| **n != self_id) {
            if !candidates.contains(node) {
                candidates.push(node);
            }
        }
        for node in candidates {
            let client = peers.entry(node.to_string()).or_insert_with(|| None);
            if client.is_none() {
                if let Ok(addr) = node.parse::<SocketAddr>() {
                    *client = PeerClient::connect(addr).await.ok();
                }
            }
            if let Some(c) = client {
                if let Ok(Some(data)) = c.get_shard(hash).await {
                    found = Some(data);
                    break;
                }
            }
        }
        slots.push(found);
    }

    // decode_stripe écarte les shards corrompus et exige ≥ k valides.
    let stripe_data = decode_stripe(slots, meta, &manifest.config)?;
    let shards = encode_stripe(&stripe_data, &manifest.config)?;
    let shard = &shards[shard_idx];
    if shard.hash != meta.shard_hashes[shard_idx] {
        bail!("shard ré-encodé incohérent avec le manifest");
    }
    store.put_shard(&shard.data)?;
    Ok(())
}
