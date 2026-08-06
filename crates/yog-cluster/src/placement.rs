//! Placement déterministe des shards : rendezvous hashing (HRW).
//!
//! Chaque nœud du cluster calcule le même placement à partir des mêmes
//! entrées (manifest + liste de nœuds), sans aucune coordination : « qui
//! doit détenir ce shard ? » est une pure fonction.
//!
//! Par stripe, les nœuds sont classés par blake3(node_id ‖ stripe_key), et
//! le shard i va au nœud de rang i % n. Deux shards d'une même stripe ne
//! tombent donc sur le même nœud que si n < k+m, et le classement change
//! de stripe en stripe → la charge s'étale naturellement sur le cluster.

use yog_erasure::FileManifest;

/// Classe les nœuds pour une clé donnée (ordre HRW, score décroissant).
pub fn rank_nodes<'a>(key: &str, nodes: &[&'a str]) -> Vec<&'a str> {
    let mut scored: Vec<(blake3::Hash, &str)> = nodes
        .iter()
        .map(|n| {
            let mut hasher = blake3::Hasher::new();
            hasher.update(n.as_bytes());
            hasher.update(b"\0");
            hasher.update(key.as_bytes());
            (hasher.finalize(), *n)
        })
        .collect();
    scored.sort_by(|a, b| b.0.as_bytes().cmp(a.0.as_bytes()));
    scored.into_iter().map(|(_, n)| n).collect()
}

/// Nœud responsable du shard `shard_idx` de la stripe `stripe_idx` d'un fichier.
pub fn shard_owner<'a>(
    file_hash: &str,
    stripe_idx: usize,
    shard_idx: usize,
    nodes: &[&'a str],
) -> &'a str {
    let ranked = rank_nodes(&format!("{file_hash}/{stripe_idx}"), nodes);
    ranked[shard_idx % ranked.len()]
}

/// Pour un manifest : la liste (stripe_idx, shard_idx, shard_hash) des shards
/// dont `node` est responsable.
pub fn shards_owned_by<'m>(
    manifest: &'m FileManifest,
    node: &str,
    nodes: &[&str],
) -> Vec<(usize, usize, &'m str)> {
    let mut out = Vec::new();
    for (si, stripe) in manifest.stripes.iter().enumerate() {
        for (i, hash) in stripe.shard_hashes.iter().enumerate() {
            if shard_owner(&manifest.file_hash, si, i, nodes) == node {
                out.push((si, i, hash.as_str()));
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use yog_erasure::{encode_file, ErasureConfig};

    #[test]
    fn deterministic_and_stable() {
        let nodes = ["a:1", "b:1", "c:1"];
        let r1 = rank_nodes("clef", &nodes);
        let r2 = rank_nodes("clef", &nodes);
        assert_eq!(r1, r2);
        // L'ordre d'entrée des nœuds ne change pas le classement.
        let shuffled = ["c:1", "a:1", "b:1"];
        assert_eq!(rank_nodes("clef", &shuffled), r1);
    }

    #[test]
    fn same_stripe_shards_spread_across_nodes() {
        let nodes = ["a:1", "b:1", "c:1", "d:1", "e:1", "f:1"];
        // 6 nœuds, 6 shards par stripe → tous sur des nœuds distincts.
        let owners: std::collections::HashSet<_> =
            (0..6).map(|i| shard_owner("fichier", 0, i, &nodes)).collect();
        assert_eq!(owners.len(), 6);
    }

    #[test]
    fn ownership_partitions_all_shards() {
        let nodes = ["a:1", "b:1", "c:1"];
        let cfg = ErasureConfig { data_shards: 4, parity_shards: 2, shard_size: 256 };
        let data = vec![7u8; 3000];
        let (manifest, _) = encode_file(&data, &cfg).unwrap();

        let total: usize = nodes
            .iter()
            .map(|n| shards_owned_by(&manifest, n, &nodes).len())
            .sum();
        let expected: usize = manifest.stripes.iter().map(|s| s.shard_hashes.len()).sum();
        assert_eq!(total, expected, "chaque shard a exactement un propriétaire");
    }
}
