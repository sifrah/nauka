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

use nauka_erasure::FileManifest;

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

/// Distance (ms) en dessous de laquelle deux nœuds sont jugés « voisins » :
/// même datacenter ou même région, donc probablement corrélés en panne.
pub const NEARBY_MS: f64 = 15.0;

/// Placement GÉO-CONSCIENT : comme [`shard_owner`], mais les shards d'une
/// même stripe sont poussés vers des nœuds réseau-distants les uns des
/// autres. Un fichier survit ainsi à la perte d'une région entière, pas
/// seulement d'une machine.
///
/// Le classement WRH pondéré reste la base (capacité, déterminisme,
/// migration minimale) ; on n'y applique qu'un **réordonnancement local** :
/// pour le shard i, on écarte les candidats trop proches des nœuds déjà
/// retenus dans cette stripe, tant qu'il reste des alternatives. Sans
/// coordonnées fiables, le comportement est exactement celui d'avant.
///
/// `coords` : position par nœud, telle que répliquée dans l'état Raft.
pub fn stripe_owners_geo<'a>(
    file_hash: &str,
    stripe_idx: usize,
    shard_count: usize,
    nodes: &[WeightedNode<'a>],
    coords: &std::collections::BTreeMap<String, crate::vivaldi::Coord>,
) -> Vec<&'a str> {
    let ranked = rank_nodes(&format!("{file_hash}/{stripe_idx}"), nodes);
    let n = ranked.len();
    let mut chosen: Vec<&str> = Vec::with_capacity(shard_count);

    for i in 0..shard_count {
        // Fenêtre de candidats : le titulaire WRH et les suivants. On ne
        // regarde jamais au-delà d'un tour complet pour rester stable.
        let base = i % n;
        let mut pick = ranked[base];
        // Ne pas déplacer un shard vers un nœud déjà utilisé plus de fois
        // que nécessaire : on ne considère que des candidats de même
        // « couche » (même quotient i/n), ce qui préserve l'anti-affinité
        // et l'équilibre de charge du WRH.
        let layer = i / n;
        let candidates: Vec<&str> = (0..n)
            .map(|off| ranked[(base + off) % n])
            .filter(|c| {
                // Un candidat n'est éligible que s'il n'a pas déjà pris
                // plus de shards que la couche courante ne l'autorise.
                chosen.iter().filter(|x| **x == *c).count() <= layer
            })
            .collect();

        if let Some(best) = candidates.iter().copied().max_by(|a, b| {
            let sa = min_distance_to(a, &chosen, coords);
            let sb = min_distance_to(b, &chosen, coords);
            sa.partial_cmp(&sb).unwrap_or(std::cmp::Ordering::Equal).then(b.cmp(a))
        }) {
            // On ne dévie du titulaire WRH que si ça éloigne réellement :
            // sinon on garde le placement nominal (stabilité).
            let nominal = min_distance_to(pick, &chosen, coords);
            let improved = min_distance_to(best, &chosen, coords);
            if improved > nominal && nominal < NEARBY_MS {
                pick = best;
            }
        }
        chosen.push(pick);
    }
    chosen
}

/// Distance du candidat au plus proche des nœuds déjà choisis.
/// `f64::MAX` si aucune comparaison n'est possible (pas de coordonnées
/// fiables, ou premier shard) — le candidat est alors neutre.
fn min_distance_to(
    candidate: &str,
    chosen: &[&str],
    coords: &std::collections::BTreeMap<String, crate::vivaldi::Coord>,
) -> f64 {
    let Some(c) = coords.get(candidate).filter(|c| c.is_settled()) else {
        return f64::MAX;
    };
    let mut min = f64::MAX;
    for other in chosen {
        if let Some(o) = coords.get(*other).filter(|o| o.is_settled()) {
            let d = c.distance(o);
            if d < min {
                min = d;
            }
        }
    }
    min
}


/// Type des coordonnées telles que répliquées dans l'état Raft.
pub type CoordMap = std::collections::BTreeMap<String, crate::vivaldi::Coord>;

/// Variante géo-consciente de [`shards_owned_by`] : mêmes garanties, mais
/// les shards d'une stripe sont écartés géographiquement quand des
/// coordonnées fiables existent.
pub fn shards_owned_by_geo<'m>(
    manifest: &'m FileManifest,
    node: &str,
    nodes: &[WeightedNode<'_>],
    coords: &CoordMap,
) -> Vec<(usize, usize, &'m str)> {
    let mut out = Vec::new();
    for (si, stripe) in manifest.stripes.iter().enumerate() {
        let owners = stripe_owners_geo(
            &manifest.file_hash,
            si,
            stripe.shard_hashes.len(),
            nodes,
            coords,
        );
        for (i, hash) in stripe.shard_hashes.iter().enumerate() {
            if owners[i] == node {
                out.push((si, i, hash.as_str()));
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use nauka_erasure::{encode_file, ErasureConfig};

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

#[cfg(test)]
mod geo_tests {
    use super::*;
    use crate::vivaldi::Coord;
    use std::collections::BTreeMap;

    /// Deux régions : 3 nœuds à Paris, 3 à Miami (RTT intra ~2 ms,
    /// inter ~90 ms).
    fn two_regions() -> (Vec<(String, u64)>, BTreeMap<String, Coord>) {
        let mut coords = BTreeMap::new();
        let mut nodes = Vec::new();
        for (i, (name, x)) in [
            ("par-1", 0.0), ("par-2", 1.0), ("par-3", 2.0),
            ("mia-1", 90.0), ("mia-2", 91.0), ("mia-3", 92.0),
        ]
        .iter()
        .enumerate()
        {
            let _ = i;
            nodes.push((name.to_string(), 1u64));
            coords.insert(
                name.to_string(),
                Coord { vec: [*x, 0.0], height: 1.0, error: 0.05 },
            );
        }
        (nodes, coords)
    }

    #[test]
    fn geo_placement_spreads_across_regions() {
        let (nodes, coords) = two_regions();
        let refs: Vec<WeightedNode> = nodes.iter().map(|(n, w)| (n.as_str(), *w)).collect();

        // Sur beaucoup de stripes, compte celles dont les 6 shards sont
        // tous dans la même région (le pire cas pour la durabilité).
        let mut geo_mono = 0;
        let mut plain_mono = 0;
        for s in 0..200 {
            let geo = stripe_owners_geo("fichier", s, 6, &refs, &coords);
            let plain: Vec<&str> =
                (0..6).map(|i| shard_owner("fichier", s, i, &refs)).collect();
            let par = |v: &Vec<&str>| v.iter().filter(|n| n.starts_with("par")).count();
            if par(&geo) == 6 || par(&geo) == 0 {
                geo_mono += 1;
            }
            if par(&plain) == 6 || par(&plain) == 0 {
                plain_mono += 1;
            }
        }
        assert_eq!(geo_mono, 0, "aucune stripe ne doit tenir dans une seule région");
        let _ = plain_mono;

        // Et chaque stripe doit toucher les deux régions de façon
        // équilibrée (3/3 avec 6 nœuds et 6 shards).
        for s in 0..50 {
            let geo = stripe_owners_geo("f2", s, 6, &refs, &coords);
            let par = geo.iter().filter(|n| n.starts_with("par")).count();
            assert_eq!(par, 3, "stripe {s} déséquilibrée: {geo:?}");
        }
    }

    #[test]
    fn geo_placement_preserves_load_balance() {
        // Chaque nœud doit rester également chargé.
        let (nodes, coords) = two_regions();
        let refs: Vec<WeightedNode> = nodes.iter().map(|(n, w)| (n.as_str(), *w)).collect();
        let mut counts: std::collections::HashMap<&str, usize> = Default::default();
        for s in 0..600 {
            for owner in stripe_owners_geo("charge", s, 6, &refs, &coords) {
                *counts.entry(owner).or_default() += 1;
            }
        }
        let total: usize = counts.values().sum();
        assert_eq!(total, 600 * 6);
        for (node, c) in &counts {
            let share = *c as f64 / total as f64;
            assert!(
                (share - 1.0 / 6.0).abs() < 0.02,
                "{node} déséquilibré: {:.1}%",
                share * 100.0
            );
        }
    }

    #[test]
    fn without_coordinates_behaviour_is_unchanged() {
        let (nodes, _) = two_regions();
        let refs: Vec<WeightedNode> = nodes.iter().map(|(n, w)| (n.as_str(), *w)).collect();
        let empty = BTreeMap::new();
        for s in 0..20 {
            let geo = stripe_owners_geo("x", s, 6, &refs, &empty);
            let plain: Vec<&str> = (0..6).map(|i| shard_owner("x", s, i, &refs)).collect();
            assert_eq!(geo, plain, "sans coordonnées, le placement doit être le WRH nominal");
        }
    }

    #[test]
    fn single_region_falls_back_gracefully() {
        // Tous les nœuds proches : rien à optimiser, pas de crash, charge
        // toujours équilibrée.
        let mut coords = BTreeMap::new();
        let names = ["a", "b", "c"];
        for (i, n) in names.iter().enumerate() {
            coords.insert(
                n.to_string(),
                Coord { vec: [i as f64, 0.0], height: 1.0, error: 0.05 },
            );
        }
        let refs: Vec<WeightedNode> = names.iter().map(|n| (*n, 1u64)).collect();
        for s in 0..30 {
            let owners = stripe_owners_geo("y", s, 6, &refs, &coords);
            assert_eq!(owners.len(), 6);
            for n in &names {
                assert_eq!(owners.iter().filter(|o| *o == n).count(), 2);
            }
        }
    }
}
