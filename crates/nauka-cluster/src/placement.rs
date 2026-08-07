//! Deterministic shard placement: WEIGHTED rendezvous hashing (WRH).
//!
//! Every node in the cluster computes the same placement from the same
//! inputs (manifest + weighted node view), with zero coordination: "who
//! should hold this shard?" is a pure function.
//!
//! A node's weight is its disk capacity (declared in the Raft state): a
//! 10 TB node receives ~10x more shards than a 1 TB node, and all of them
//! converge to the same fill percentage. Classic WRH score
//! `-weight / ln(h)` with h uniform, derived from blake3(node ‖ key).
//!
//! DETERMINISM: placement must be bit-for-bit identical on every platform.
//! Transcendental functions (`f64::ln`) depend on the system libm — so we
//! use an in-house ln that relies only on basic IEEE 754 operations
//! (+,-,x,/), which are strictly reproducible.

use nauka_erasure::FileManifest;

/// Weighted cluster view: (node identity, weight > 0).
/// The weight is in arbitrary but consistent units (capacity bytes).
pub type WeightedNode<'a> = (&'a str, u64);

/// Default capacity for a node that has not declared its own yet
/// (100 GiB) — it takes part sanely until the next declaration tick.
pub const DEFAULT_CAPACITY: u64 = 100 * 1024 * 1024 * 1024;

/// Deterministic ln(x) for x ∈ (0, 1), using basic operations only.
/// Relative accuracy ~1e-7 — more than enough: only the CONSISTENCY of the
/// ranking across nodes matters, not absolute precision.
fn det_ln(x: f64) -> f64 {
    const LN2: f64 = 0.693_147_180_559_945_3;
    let bits = x.to_bits();
    let e = ((bits >> 52) & 0x7ff) as i64 - 1023;
    // Mantissa brought back into [1, 2).
    let m = f64::from_bits((bits & 0x000f_ffff_ffff_ffff) | 0x3ff0_0000_0000_0000);
    // ln(m) via the artanh series, |z| ≤ 1/3.
    let z = (m - 1.0) / (m + 1.0);
    let z2 = z * z;
    let ln_m = 2.0 * z
        * (1.0 + z2 / 3.0 + z2 * z2 / 5.0 + z2 * z2 * z2 / 7.0 + z2 * z2 * z2 * z2 / 9.0);
    ln_m + (e as f64) * LN2
}

/// WRH score of a node for a key. Higher = ranked better.
fn score(node: &str, key: &str, weight: u64) -> f64 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(node.as_bytes());
    hasher.update(b"\0");
    hasher.update(key.as_bytes());
    let h = u64::from_le_bytes(hasher.finalize().as_bytes()[..8].try_into().unwrap());
    // h uniform → x ∈ (0, 1), never exactly 0 nor 1.
    let x = (h as f64 + 0.5) * (1.0 / 18_446_744_073_709_551_616.0);
    -(weight.max(1) as f64) / det_ln(x)
}

/// Ranks the nodes for a given key (decreasing WRH score; ties broken by
/// identity for a stable total order).
pub fn rank_nodes<'a>(key: &str, nodes: &[WeightedNode<'a>]) -> Vec<&'a str> {
    let mut scored: Vec<(f64, &str)> =
        nodes.iter().map(|(n, w)| (score(n, key, *w), *n)).collect();
    scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal).then(a.1.cmp(b.1)));
    scored.into_iter().map(|(_, n)| n).collect()
}

/// Node responsible for shard `shard_idx` of stripe `stripe_idx` of a file.
pub fn shard_owner<'a>(
    file_hash: &str,
    stripe_idx: usize,
    shard_idx: usize,
    nodes: &[WeightedNode<'a>],
) -> &'a str {
    let ranked = rank_nodes(&format!("{file_hash}/{stripe_idx}"), nodes);
    ranked[shard_idx % ranked.len()]
}

/// For a manifest: the list of (stripe_idx, shard_idx, shard_hash) shards
/// that `node` is responsible for.
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

/// Uniform view (every node equally weighted) — static mode and tests.
pub fn uniform<'a>(nodes: &[&'a str]) -> Vec<WeightedNode<'a>> {
    nodes.iter().map(|n| (*n, 1)).collect()
}

/// Distance (ms) below which two nodes are considered "nearby": same
/// datacenter or same region, hence likely to share a correlated failure.
pub const NEARBY_MS: f64 = 15.0;

/// GEO-AWARE placement: like [`shard_owner`], but the shards of a single
/// stripe are pushed towards nodes that are network-distant from one
/// another. A file then survives the loss of a whole region, not just of a
/// single machine.
///
/// Weighted WRH ranking remains the foundation (capacity, determinism,
/// minimal migration); we only apply a **local reordering** on top: for
/// shard i, candidates too close to the nodes already picked for this
/// stripe are set aside, as long as alternatives remain. Without reliable
/// coordinates, the behaviour is exactly what it was before.
///
/// `coords`: per-node position, as replicated in the Raft state.
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
        // Candidate window: the WRH holder and the ones after it. We never
        // look beyond a full turn, so as to stay stable.
        let base = i % n;
        let mut pick = ranked[base];
        // Never move a shard to a node already used more times than
        // necessary: only candidates from the same "layer" (same quotient
        // i/n) are considered, which preserves WRH's anti-affinity and
        // load balance.
        let layer = i / n;
        let candidates: Vec<&str> = (0..n)
            .map(|off| ranked[(base + off) % n])
            .filter(|c| {
                // A candidate is eligible only if it has not already taken
                // more shards than the current layer allows.
                chosen.iter().filter(|x| **x == *c).count() <= layer
            })
            .collect();

        if let Some(best) = candidates.iter().copied().max_by(|a, b| {
            let sa = min_distance_to(a, &chosen, coords);
            let sb = min_distance_to(b, &chosen, coords);
            sa.partial_cmp(&sb).unwrap_or(std::cmp::Ordering::Equal).then(b.cmp(a))
        }) {
            // Deviate from the WRH holder only if it actually increases
            // the spread; otherwise keep the nominal placement (stability).
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

/// Distance from the candidate to the nearest already-chosen node.
/// `f64::MAX` when no comparison is possible (no reliable coordinates, or
/// first shard) — the candidate is then neutral.
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


/// Type of the coordinates as replicated in the Raft state.
pub type CoordMap = std::collections::BTreeMap<String, crate::vivaldi::Coord>;

/// Geo-aware variant of [`shards_owned_by`]: same guarantees, but the
/// shards of a stripe are spread geographically whenever reliable
/// coordinates exist.
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
        let r1 = rank_nodes("key", &nodes);
        let r2 = rank_nodes("key", &nodes);
        assert_eq!(r1, r2);
        // The input order of the nodes does not change the ranking.
        let shuffled = uniform(&["c:1", "a:1", "b:1"]);
        assert_eq!(rank_nodes("key", &shuffled), r1);
    }

    #[test]
    fn same_stripe_shards_spread_across_nodes() {
        let nodes = uniform(&["a:1", "b:1", "c:1", "d:1", "e:1", "f:1"]);
        // 6 nodes, 6 shards per stripe → all on distinct nodes.
        let owners: std::collections::HashSet<_> =
            (0..6).map(|i| shard_owner("file", 0, i, &nodes)).collect();
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
        assert_eq!(total, expected, "every shard has exactly one owner");
    }

    #[test]
    fn weights_drive_proportional_selection() {
        // The probability of ranking FIRST is proportional to the weight —
        // this is what drives the selection of the hosting subset when
        // n > k+m, and the assignment of the "extra" shards otherwise.
        let nodes: Vec<WeightedNode> =
            vec![("a:1", 100), ("b:1", 100), ("c:1", 200), ("d:1", 400)];
        let mut counts = std::collections::HashMap::new();
        for f in 0..4000 {
            let top = rank_nodes(&format!("file-{f}"), &nodes)[0];
            *counts.entry(top).or_insert(0usize) += 1;
        }
        let share = |n: &str| counts[n] as f64 / 4000.0;
        // Expected shares: a=b=12.5%, c=25%, d=50%.
        assert!((share("a:1") - 0.125).abs() < 0.03, "a: {}", share("a:1"));
        assert!((share("b:1") - 0.125).abs() < 0.03, "b: {}", share("b:1"));
        assert!((share("c:1") - 0.25).abs() < 0.04, "c: {}", share("c:1"));
        assert!((share("d:1") - 0.50).abs() < 0.05, "d: {}", share("d:1"));
    }

    #[test]
    fn anti_affinity_beats_capacity_on_small_clusters() {
        // With n ≤ k+m, every stripe covers all nodes almost uniformly
        // WHATEVER the weights are: a large node concentrating > m shards
        // of a stripe would become a single point of failure. Durability
        // first, capacity second.
        let nodes: Vec<WeightedNode> = vec![("a:1", 1), ("b:1", 1), ("c:1", 1000)];
        for f in 0..50 {
            let mut counts = std::collections::HashMap::new();
            for i in 0..6 {
                let owner = shard_owner(&format!("file-{f}"), 0, i, &nodes);
                *counts.entry(owner).or_insert(0usize) += 1;
            }
            // 6 shards over 3 nodes: exactly 2 each, always.
            assert!(counts.values().all(|c| *c == 2), "distribution {counts:?}");
        }
    }

    #[test]
    fn weight_change_moves_minimal_shards() {
        // Doubling a node's weight must NOT reshuffle the whole cluster:
        // only the shards migrating TOWARDS it change owner.
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
        // a's share goes 1/3 → 1/2, so ~1/6 of the total must migrate, and
        // exclusively towards a.
        assert_eq!(moved_elsewhere, 0, "shards migrated between unchanged nodes");
        let frac = moved as f64 / total as f64;
        assert!((frac - 1.0 / 6.0).abs() < 0.05, "migration: {frac}");
    }
}

#[cfg(test)]
mod geo_tests {
    use super::*;
    use crate::vivaldi::Coord;
    use std::collections::BTreeMap;

    /// Two regions: 3 nodes in Paris, 3 in Miami (intra-region RTT ~2 ms,
    /// inter-region ~90 ms).
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

        // Over many stripes, count those whose 6 shards all land in the
        // same region (the worst case for durability).
        let mut geo_mono = 0;
        let mut plain_mono = 0;
        for s in 0..200 {
            let geo = stripe_owners_geo("file", s, 6, &refs, &coords);
            let plain: Vec<&str> =
                (0..6).map(|i| shard_owner("file", s, i, &refs)).collect();
            let par = |v: &Vec<&str>| v.iter().filter(|n| n.starts_with("par")).count();
            if par(&geo) == 6 || par(&geo) == 0 {
                geo_mono += 1;
            }
            if par(&plain) == 6 || par(&plain) == 0 {
                plain_mono += 1;
            }
        }
        assert_eq!(geo_mono, 0, "no stripe may fit inside a single region");
        let _ = plain_mono;

        // And every stripe must reach both regions in a balanced way
        // (3/3 with 6 nodes and 6 shards).
        for s in 0..50 {
            let geo = stripe_owners_geo("f2", s, 6, &refs, &coords);
            let par = geo.iter().filter(|n| n.starts_with("par")).count();
            assert_eq!(par, 3, "stripe {s} unbalanced: {geo:?}");
        }
    }

    #[test]
    fn geo_placement_preserves_load_balance() {
        // Every node must stay equally loaded.
        let (nodes, coords) = two_regions();
        let refs: Vec<WeightedNode> = nodes.iter().map(|(n, w)| (n.as_str(), *w)).collect();
        let mut counts: std::collections::HashMap<&str, usize> = Default::default();
        for s in 0..600 {
            for owner in stripe_owners_geo("load", s, 6, &refs, &coords) {
                *counts.entry(owner).or_default() += 1;
            }
        }
        let total: usize = counts.values().sum();
        assert_eq!(total, 600 * 6);
        for (node, c) in &counts {
            let share = *c as f64 / total as f64;
            assert!(
                (share - 1.0 / 6.0).abs() < 0.02,
                "{node} unbalanced: {:.1}%",
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
            assert_eq!(geo, plain, "without coordinates, placement must be nominal WRH");
        }
    }

    #[test]
    fn single_region_falls_back_gracefully() {
        // All nodes close together: nothing to optimise, no crash, load
        // still balanced.
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
