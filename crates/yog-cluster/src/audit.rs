//! Attestation de stockage : vérifier qu'un pair détient RÉELLEMENT les
//! shards dont le placement le rend responsable.
//!
//! `has_shard` est déclaratif — un nœud peut répondre « oui » alors que le
//! disque a été vidé ou silencieusement corrompu. Deux mécanismes de
//! preuve, complémentaires :
//!
//! 1. **Challenge par nonce** (`ProveShard`) : le pair renvoie
//!    `blake3(nonce ‖ octets)`, imprévisible et non rejouable. Vérifiable
//!    seulement si le vérificateur détient les octets — c'est exactement le
//!    cas du GC, qui exige désormais cette preuve avant de libérer sa
//!    copie (voir healer.rs).
//!
//! 2. **Audit par échantillonnage** (ce module) : en régime permanent,
//!    chaque shard n'a qu'UN détenteur — personne d'autre n'a les octets
//!    pour vérifier un challenge. L'auditeur échantillonne donc des shards
//!    que le pair POSSÈDE selon le placement, les télécharge, et vérifie
//!    leur hash contre le manifest. Le stockage étant content-addressed,
//!    tricher = produire des octets ayant un BLAKE3 imposé — une préimage.
//!    Coût : `SAMPLE_PER_PEER` × 1 Mio par pair et par passe, borné et
//!    réglable.
//!
//! Un « absent » sur un shard que le pair possède selon le placement est le
//! signal utile : soit son scrubber est en retard (transitoire), soit le
//! nœud a perdu des données (persistant → alerte).

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
use rand::{RngCore, SeedableRng};
use tracing::{info, warn};
use yog_store::ShardStore;
use yog_transport::PeerClient;

/// Shards téléchargés et vérifiés par pair et par passe.
pub const SAMPLE_PER_PEER: usize = 3;

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct AuditReport {
    /// Vérifications tentées.
    pub challenged: usize,
    /// Shards téléchargés dont le hash correspond au manifest.
    pub proved: usize,
    /// Le pair n'a pas pu fournir le shard (absent ou corrompu chez lui).
    pub missing: usize,
    /// Le pair a fourni des octets au MAUVAIS hash — anomalie sérieuse
    /// (impossible sans bug ou malveillance, le transport vérifie aussi).
    pub failed: usize,
    /// Pairs injoignables (non comptés comme fautes).
    pub unreachable: usize,
}

/// Preuve de détention attendue pour un challenge par nonce.
pub fn expected_proof(nonce: &[u8; 32], data: &[u8]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(nonce);
    hasher.update(data);
    *hasher.finalize().as_bytes()
}

/// Une passe d'audit : pour chaque pair, échantillonne des shards qui lui
/// appartiennent selon le placement et vérifie qu'il les détient vraiment.
pub async fn audit_once(
    store: &Arc<ShardStore>,
    self_id: &str,
    all_nodes: &[(String, u64)],
) -> Result<AuditReport> {
    audit_once_geo(store, self_id, all_nodes, &Default::default()).await
}

/// Variante géo-consciente (voir [`crate::placement::stripe_owners_geo`]).
pub async fn audit_once_geo(
    store: &Arc<ShardStore>,
    self_id: &str,
    all_nodes: &[(String, u64)],
    coords: &crate::placement::CoordMap,
) -> Result<AuditReport> {
    let mut report = AuditReport::default();
    let node_refs: Vec<(&str, u64)> = all_nodes.iter().map(|(n, w)| (n.as_str(), *w)).collect();

    // Qui possède quoi, d'après les manifests connus localement.
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
        let Ok(addr) = peer.parse::<SocketAddr>() else { continue };
        let Ok(client) = PeerClient::connect(addr).await else {
            report.unreachable += 1;
            continue;
        };

        for _ in 0..SAMPLE_PER_PEER.min(owned.len()) {
            let (shard_hash, file) = &owned[(rng.next_u64() % owned.len() as u64) as usize];
            report.challenged += 1;
            match client.get_shard(shard_hash).await {
                Ok(Some(data)) if yog_erasure::hash_bytes(&data) == *shard_hash => {
                    report.proved += 1;
                }
                Ok(Some(_)) => {
                    warn!(peer = %peer, shard = %shard_hash, file = %file,
                          "AUDIT: octets au mauvais hash — le pair ne détient pas ce qu'il sert");
                    report.failed += 1;
                }
                Ok(None) => {
                    // Absent alors que le placement le lui attribue : son
                    // scrubber devrait le régénérer — à surveiller si ça dure.
                    report.missing += 1;
                }
                Err(e) => {
                    warn!(peer = %peer, "audit interrompu: {e}");
                    report.challenged -= 1;
                    report.unreachable += 1;
                    break;
                }
            }
        }
    }

    if report.failed > 0 {
        warn!(
            "audit: {} vérification(s) en ÉCHEC sur {} — un pair sert des octets invalides",
            report.failed, report.challenged
        );
    } else if report.challenged > 0 {
        info!(
            "audit: {}/{} détentions prouvées, {} absentes",
            report.proved, report.challenged, report.missing
        );
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proof_binds_nonce_and_content() {
        let n1 = [1u8; 32];
        let n2 = [2u8; 32];
        // Même contenu, nonce différent → preuve différente (pas de rejeu).
        assert_ne!(expected_proof(&n1, b"data"), expected_proof(&n2, b"data"));
        // Même nonce, contenu différent → preuve différente.
        assert_ne!(expected_proof(&n1, b"data"), expected_proof(&n1, b"datb"));
        // Déterministe.
        assert_eq!(expected_proof(&n1, b"data"), expected_proof(&n1, b"data"));
    }
}
