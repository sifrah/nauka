//! Placement déterministe des shards : rendezvous hashing PONDÉRÉ (WRH).
//!
//! Chaque nœud du cluster calcule le même placement à partir des mêmes
//! entrées (manifest + vue pondérée des nœuds), sans aucune coordination :
//! « qui doit détenir ce shard ? » est une pure fonction.
//!
//! Le poids d'un nœud est sa capacité disque (déclarée dans l'état Raft) :
//! un nœud de 10 To reçoit ~10× plus de shards qu'un nœud de 1 To, et tous
//! convergent vers le même pourcentage de remplissage. Score WRH classique
//! `-poids / ln(h)` avec h uniforme dérivé de blake3(node ‖ clé).
//!
//! DÉTERMINISME : le placement doit être identique bit à bit sur toutes les
//! plateformes. Les fonctions transcendantes (`f64::ln`) dépendent de la
//! libm du système — on utilise donc un ln maison n'employant que des
//! opérations IEEE 754 de base (+,-,×,÷), strictement reproductibles.

use yog_erasure::FileManifest;

/// Vue pondérée du cluster : (identité du nœud, poids > 0).
/// Le poids est en unités arbitraires mais cohérentes (octets de capacité).
pub type WeightedNode<'a> = (&'a str, u64);

/// Capacité par défaut d'un nœud qui n'a pas encore déclaré la sienne
/// (100 Gio) — le temps d'un tick de déclaration, il participe sainement.
pub const DEFAULT_CAPACITY: u64 = 100 * 1024 * 1024 * 1024;

/// ln(x) déterministe pour x ∈ (0, 1), en opérations de base uniquement.
/// Précision ~1e-7 relative — largement assez : seule la COHÉRENCE du
/// classement entre nœuds compte, pas la précision absolue.
fn det_ln(x: f64) -> f64 {
    const LN2: f64 = 0.693_147_180_559_945_3;
    let bits = x.to_bits();
    let e = ((bits >> 52) & 0x7ff) as i64 - 1023;
    // Mantisse ramenée dans [1, 2).
    let m = f64::from_bits((bits & 0x000f_ffff_ffff_ffff) | 0x3ff0_0000_0000_0000);
    // ln(m) par série d'artanh, |z| ≤ 1/3.
    let z = (m - 1.0) / (m + 1.0);
    let z2 = z * z;
    let ln_m = 2.0 * z
        * (1.0 + z2 / 3.0 + z2 * z2 / 5.0 + z2 * z2 * z2 / 7.0 + z2 * z2 * z2 * z2 / 9.0);
    ln_m + (e as f64) * LN2
}

/// Score WRH d'un nœud pour une clé. Plus grand = mieux classé.
fn score(node: &str, key: &str, weight: u64) -> f64 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(node.as_bytes());
    hasher.update(b"\0");
    hasher.update(key.as_bytes());
    let h = u64::from_le_bytes(hasher.finalize().as_bytes()[..8].try_into().unwrap());
    // h uniforme → x ∈ (0, 1), jamais exactement 0 ni 1.
    let x = (h as f64 + 0.5) * (1.0 / 18_446_744_073_709_551_616.0);
    -(weight.max(1) as f64) / det_ln(x)
}

/// Classe les nœuds pour une clé donnée (score WRH décroissant ; égalité
/// départagée par l'identité pour un ordre total stable).
pub fn rank_nodes<'a>(key: &str, nodes: &[WeightedNode<'a>]) -> Vec<&'a str> {
    let mut scored: Vec<(f64, &str)> =
        nodes.iter().map(|(n, w)| (score(n, key, *w), *n)).collect();
    scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal).then(a.1.cmp(b.1)));
    scored.into_iter().map(|(_, n)| n).collect()
}

/// Nœud responsable du shard `shard_idx` de la stripe `stripe_idx` d'un fichier.
pub fn shard_owner<'a>(
    file_hash: &str,
    stripe_idx: usize,
    shard_idx: usize,
    nodes: &[WeightedNode<'a>],
) -> &'a str {
    let ranked = rank_nodes(&format!("{file_hash}/{stripe_idx}"), nodes);
    ranked[shard_idx % ranked.len()]
}

/// Pour un manifest : la liste (stripe_idx, shard_idx, shard_hash) des shards
/// dont `node` est responsable.
pub fn shards_owned_by<'m>(
    manifest: &'m FileManifest,
    node: &str,
    nodes: &[WeightedNode<'_>],
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

/// Vue uniforme (tous à poids égal) — mode statique et tests.
pub fn uniform<'a>(nodes: &[&'a str]) -> Vec<WeightedNode<'a>> {
    nodes.iter().map(|n| (*n, 1)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use yog_erasure::{encode_file, ErasureConfig};

    #[test]
    fn deterministic_and_stable() {
        let nodes = uniform(&["a:1", "b:1", "c:1"]);
        let r1 = rank_nodes("clef", &nodes);
        let r2 = rank_nodes("clef", &nodes);
        assert_eq!(r1, r2);
        // L'ordre d'entrée des nœuds ne change pas le classement.
        let shuffled = uniform(&["c:1", "a:1", "b:1"]);
        assert_eq!(rank_nodes("clef", &shuffled), r1);
    }

    #[test]
    fn same_stripe_shards_spread_across_nodes() {
        let nodes = uniform(&["a:1", "b:1", "c:1", "d:1", "e:1", "f:1"]);
        // 6 nœuds, 6 shards par stripe → tous sur des nœuds distincts.
        let owners: std::collections::HashSet<_> =
            (0..6).map(|i| shard_owner("fichier", 0, i, &nodes)).collect();
        assert_eq!(owners.len(), 6);
    }

    #[test]
    fn ownership_partitions_all_shards() {
        let nodes = uniform(&["a:1", "b:1", "c:1"]);
        let cfg = ErasureConfig { data_shards: 4, parity_shards: 2, shard_size: 256 };
        let data = vec![7u8; 3000];
        let (manifest, _) = encode_file(&data, &cfg).unwrap();

        let total: usize = nodes
            .iter()
            .map(|(n, _)| shards_owned_by(&manifest, n, &nodes).len())
            .sum();
        let expected: usize = manifest.stripes.iter().map(|s| s.shard_hashes.len()).sum();
        assert_eq!(total, expected, "chaque shard a exactement un propriétaire");
    }

    #[test]
    fn weights_drive_proportional_selection() {
        // La probabilité d'être EN TÊTE du classement est proportionnelle
        // au poids — c'est ce qui pilote la sélection du sous-ensemble
        // hébergeur quand n > k+m, et l'attribution des shards
        // « supplémentaires » sinon.
        let nodes: Vec<WeightedNode> =
            vec![("a:1", 100), ("b:1", 100), ("c:1", 200), ("d:1", 400)];
        let mut counts = std::collections::HashMap::new();
        for f in 0..4000 {
            let top = rank_nodes(&format!("fichier-{f}"), &nodes)[0];
            *counts.entry(top).or_insert(0usize) += 1;
        }
        let share = |n: &str| counts[n] as f64 / 4000.0;
        // Parts attendues: a=b=12,5 %, c=25 %, d=50 %.
        assert!((share("a:1") - 0.125).abs() < 0.03, "a: {}", share("a:1"));
        assert!((share("b:1") - 0.125).abs() < 0.03, "b: {}", share("b:1"));
        assert!((share("c:1") - 0.25).abs() < 0.04, "c: {}", share("c:1"));
        assert!((share("d:1") - 0.50).abs() < 0.05, "d: {}", share("d:1"));
    }

    #[test]
    fn anti_affinity_beats_capacity_on_small_clusters() {
        // Avec n ≤ k+m, chaque stripe couvre tous les nœuds presque
        // uniformément QUELS QUE SOIENT les poids : un gros nœud qui
        // concentrerait > m shards d'une stripe deviendrait un point de
        // défaillance unique. Durabilité d'abord, capacité ensuite.
        let nodes: Vec<WeightedNode> = vec![("a:1", 1), ("b:1", 1), ("c:1", 1000)];
        for f in 0..50 {
            let mut counts = std::collections::HashMap::new();
            for i in 0..6 {
                let owner = shard_owner(&format!("fichier-{f}"), 0, i, &nodes);
                *counts.entry(owner).or_insert(0usize) += 1;
            }
            // 6 shards sur 3 nœuds : exactement 2 chacun, toujours.
            assert!(counts.values().all(|c| *c == 2), "répartition {counts:?}");
        }
    }

    #[test]
    fn weight_change_moves_minimal_shards() {
        // Doubler le poids d'un nœud ne doit PAS rebrasser tout le cluster :
        // seuls les shards qui migrent VERS lui changent de propriétaire.
        let before: Vec<WeightedNode> = vec![("a:1", 100), ("b:1", 100), ("c:1", 100)];
        let after: Vec<WeightedNode> = vec![("a:1", 200), ("b:1", 100), ("c:1", 100)];
        let mut moved = 0usize;
        let mut moved_elsewhere = 0usize;
        let total = 3000usize;
        for f in 0..total {
            let key = format!("f-{f}");
            let o1 = shard_owner(&key, 0, 0, &before);
            let o2 = shard_owner(&key, 0, 0, &after);
            if o1 != o2 {
                moved += 1;
                if o2 != "a:1" {
                    moved_elsewhere += 1;
                }
            }
        }
        // Part de a: 1/3 → 1/2, donc ~1/6 du total doit migrer, vers a
        // exclusivement.
        assert_eq!(moved_elsewhere, 0, "des shards ont migré entre nœuds inchangés");
        let frac = moved as f64 / total as f64;
        assert!((frac - 1.0 / 6.0).abs() < 0.05, "migration: {frac}");
    }
}
