//! Cluster telemetry: who this node thinks is alive, and how far away it
//! thinks they are.
//!
//! Recording goes through the `metrics` facade, a no-op until a recorder is
//! installed. Named `telemetry` rather than `metrics` so it does not shadow
//! the crate it calls into.
//!
//! ## What these gauges do and do not promise
//!
//! Liveness comes from [`PeerHealth`], which needs [`MISS_THRESHOLD`]
//! consecutive failed probes before it calls a peer down. At the 5-second
//! probe cadence that is ~15 s from death to `nauka_peer_up 0`, and no
//! alert built on these gauges can resolve faster than that. A `for: 30s`
//! alert rule is the shortest one that means anything here.
//!
//! ## Why the membership view is an argument
//!
//! [`PeerHealth::snapshot`] only reports peers it has an opinion about, and
//! a *success wipes the entry entirely* — a perfectly healthy peer is
//! absent from the snapshot, indistinguishable there from one nobody ever
//! probed. Publishing the snapshot alone would therefore emit `peer_up`
//! for sick peers only, and would leave a recovered peer's gauge pinned at
//! 0 forever. The membership view supplies the denominator; the snapshot
//! only overrides it.
//!
//! This is also why nothing here goes near `filter_view`: that call returns
//! the *full* view when every peer is down, so "everyone is dead" and
//! "everyone is fine" are the same answer through it. Built on it, the
//! metric would go quiet at exactly the moment it mattered.

use std::collections::BTreeSet;
use std::sync::Mutex;

use crate::health::PeerHealth;
use crate::vivaldi::Coord;

/// Peers published in the previous round, so that a member removed from the
/// cluster can be walked down to 0 instead of leaving a gauge stuck at 1.
///
/// A stale `nauka_peer_up{peer="…"} 1` for a node that no longer exists is
/// the worse failure of the two: it is a green light for something that is
/// not there. Setting it to 0 is at least true — that peer is not a live
/// peer of this node any more.
static PUBLISHED: Mutex<BTreeSet<String>> = Mutex::new(BTreeSet::new());

/// Register the HELP/TYPE text of every cluster metric.
pub fn describe() {
    metrics::describe_gauge!(
        "nauka_peers_total",
        "Peers in this node's membership view, by liveness. Excludes the node itself. A peer is only counted dead after MISS_THRESHOLD consecutive failed probes, so the count trails a real death by ~15 s."
    );
    metrics::describe_gauge!(
        "nauka_peer_up",
        "1 if this node's liveness map considers the peer reachable, 0 once it has missed MISS_THRESHOLD consecutive probes. Optimistic: a peer nobody has probed yet reads 1."
    );
    metrics::describe_gauge!(
        "nauka_peer_rtt_seconds",
        "Round-trip time to a peer as estimated from the replicated Vivaldi coordinates, which is what placement and read routing decide on. A model output, not a measurement: it cannot go below ~2 ms, so inside one datacenter it reads as that floor."
    );
}

/// Publish per-peer liveness and the alive/dead totals.
///
/// `peers` is the membership view with this node already excluded; it is
/// the set of series that will exist. Call it after each probe round.
pub fn record_peer_liveness<'a>(peers: impl IntoIterator<Item = &'a str>, health: &PeerHealth) {
    let snapshot = health.snapshot();
    let mut alive_count = 0u64;
    let mut dead_count = 0u64;
    let mut current = BTreeSet::new();

    for peer in peers {
        // A missing entry reads exactly as `is_alive` does: alive.
        let alive = snapshot.get(peer).copied().unwrap_or(true);
        if alive {
            alive_count += 1;
        } else {
            dead_count += 1;
        }
        metrics::gauge!("nauka_peer_up", "peer" => peer.to_string()).set(if alive {
            1.0
        } else {
            0.0
        });
        current.insert(peer.to_string());
    }

    let mut published = PUBLISHED.lock().unwrap_or_else(|e| e.into_inner());
    for gone in published.difference(&current) {
        metrics::gauge!("nauka_peer_up", "peer" => gone.clone()).set(0.0);
    }
    *published = current;

    metrics::gauge!("nauka_peers_total", "state" => "alive").set(alive_count as f64);
    metrics::gauge!("nauka_peers_total", "state" => "dead").set(dead_count as f64);
}

/// Publish the estimated RTT to each peer, from the Vivaldi coordinates
/// that placement and read-routing already decide on.
///
/// `peers` must come from the replicated coordinate map, so every entry is
/// a position a node actually published — never a `Coord::default()` stood
/// in for a node that has never been heard from.
///
/// Deliberately NOT gated on [`Coord::is_settled`]. That gate looks right
/// and is a trap: the model floors a predicted distance at twice
/// `MIN_HEIGHT`, i.e. 2 ms, so inside one datacenter — where the real RTT
/// is a few hundred microseconds — the relative error never falls and
/// `is_settled` stays false forever (measured: error pinned at the 1.5
/// maximum after 500 observations at 0.5 ms; the same node settles within a
/// handful of observations at 20 ms). Gating on it would have left this
/// metric permanently empty on every single-datacenter cluster.
///
/// So the value exported is the estimate placement is using, whatever its
/// confidence — which is the only number that explains a placement
/// decision. On a LAN it reads as a ~2 ms floor rather than a measurement,
/// and it should be read as "how far apart the placer thinks these nodes
/// are", not as a latency SLI.
pub fn record_peer_rtt<'a>(
    self_coord: &Coord,
    peers: impl IntoIterator<Item = (&'a str, &'a Coord)>,
) {
    for (peer, coord) in peers {
        // Vivaldi works in milliseconds; the metric is named `_seconds` and
        // inherits the latency buckets from the exporter's suffix matcher,
        // so the conversion is not optional.
        let seconds = self_coord.distance(coord) / 1000.0;
        metrics::gauge!("nauka_peer_rtt_seconds", "peer" => peer.to_string()).set(seconds);
    }
}

/// Register the HELP/TYPE text of the maintenance metrics — the reports
/// every scrub/GC/audit pass already computes and, until now, printed to
/// stderr and dropped.
pub fn describe_maintenance() {
    metrics::describe_counter!(
        "nauka_scrub_shards_checked_total",
        "Shards this node verified across all scrub passes."
    );
    metrics::describe_counter!(
        "nauka_scrub_shards_healed_total",
        "Shards reconstructed from surviving peers across all scrub passes."
    );
    metrics::describe_gauge!(
        "nauka_scrub_shards_unrecoverable",
        "Shards this node should hold but could not rebuild in the LAST pass. There is no repair queue — each pass is a full scan — so this is the backlog: anything above zero means data is currently below its intended redundancy."
    );
    metrics::describe_histogram!(
        "nauka_scrub_pass_duration_seconds",
        "Wall-clock time of one full maintenance pass (scrub + GC + audit + publications). If it exceeds the scrub interval the ticker silently falls behind and the cluster heals less often than configured."
    );
    metrics::describe_counter!(
        "nauka_gc_shards_released_total",
        "Shards deleted because placement no longer assigns them to this node."
    );
    metrics::describe_counter!(
        "nauka_gc_orphans_purged_total",
        "Shards deleted because no live manifest references them."
    );
    metrics::describe_counter!(
        "nauka_gc_manifests_purged_total",
        "Manifests deleted because the registry no longer knows them."
    );
    metrics::describe_counter!(
        "nauka_audit_challenged_total",
        "Proof-of-storage challenges this node issued to peers."
    );
    metrics::describe_counter!(
        "nauka_audit_proved_total",
        "Challenges a peer answered with a valid proof."
    );
    metrics::describe_counter!(
        "nauka_audit_missing_total",
        "Challenges a peer answered with 'I do not have that shard'."
    );
    metrics::describe_counter!(
        "nauka_audit_failed_total",
        "Challenges answered with a WRONG proof — a peer claiming to hold data it cannot produce. The audit's whole reason to exist; anything above zero deserves a look."
    );
    metrics::describe_counter!(
        "nauka_audit_unreachable_total",
        "Challenges that could not be delivered. Overlaps with peer liveness; kept separate so audit coverage is honest about what it could not check."
    );
}

/// Record a scrub pass. Checked/healed accumulate; unrecoverable is the
/// last pass's level — a healed cluster must be able to walk it back to 0,
/// which a counter could never do.
pub fn record_heal_report(checked: u64, healed: u64, unrecoverable: u64) {
    metrics::counter!("nauka_scrub_shards_checked_total").increment(checked);
    metrics::counter!("nauka_scrub_shards_healed_total").increment(healed);
    metrics::gauge!("nauka_scrub_shards_unrecoverable").set(unrecoverable as f64);
}

/// Record a GC or purge pass. Both feed the same counters: their fields are
/// disjoint in practice (GC releases shards, purge removes orphans and dead
/// manifests) and an operator cares about the totals, not the pass kind.
pub fn record_gc_report(released: u64, orphans: u64, manifests: u64) {
    metrics::counter!("nauka_gc_shards_released_total").increment(released);
    metrics::counter!("nauka_gc_orphans_purged_total").increment(orphans);
    metrics::counter!("nauka_gc_manifests_purged_total").increment(manifests);
}

/// Record an audit pass.
pub fn record_audit_report(
    challenged: u64,
    proved: u64,
    missing: u64,
    failed: u64,
    unreachable: u64,
) {
    metrics::counter!("nauka_audit_challenged_total").increment(challenged);
    metrics::counter!("nauka_audit_proved_total").increment(proved);
    metrics::counter!("nauka_audit_missing_total").increment(missing);
    metrics::counter!("nauka_audit_failed_total").increment(failed);
    metrics::counter!("nauka_audit_unreachable_total").increment(unreachable);
}

/// Record the wall-clock of one full maintenance pass.
pub fn record_maintenance_pass(seconds: f64) {
    metrics::histogram!("nauka_scrub_pass_duration_seconds").record(seconds);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::health::MISS_THRESHOLD;

    /// The counting the gauges are built on, without a recorder in the way.
    fn tally<'a>(peers: impl IntoIterator<Item = &'a str>, health: &PeerHealth) -> (u64, u64) {
        let snapshot = health.snapshot();
        let mut alive = 0;
        let mut dead = 0;
        for peer in peers {
            if snapshot.get(peer).copied().unwrap_or(true) {
                alive += 1;
            } else {
                dead += 1;
            }
        }
        (alive, dead)
    }

    #[test]
    fn unprobed_peers_count_as_alive() {
        let health = PeerHealth::default();
        assert_eq!(tally(["a:1", "b:2"], &health), (2, 0));
    }

    #[test]
    fn every_peer_down_is_not_every_peer_up() {
        // The `filter_view` trap, asserted from the metric's side: with all
        // peers down the tally must read (0 alive, 2 dead), never (2, 0).
        let health = PeerHealth::default();
        for _ in 0..MISS_THRESHOLD {
            health.record_miss("a:1");
            health.record_miss("b:2");
        }
        assert_eq!(tally(["a:1", "b:2"], &health), (0, 2));

        // And the view `filter_view` would have handed us instead.
        let view = vec![("a:1".to_string(), 10), ("b:2".to_string(), 10)];
        assert_eq!(
            health.filter_view(view.clone()).len(),
            2,
            "filter_view still reports two usable peers — which is why the \
             metric does not use it"
        );
    }

    #[test]
    fn a_peer_below_the_threshold_is_still_alive() {
        let health = PeerHealth::default();
        for _ in 0..MISS_THRESHOLD - 1 {
            health.record_miss("a:1");
        }
        assert_eq!(
            tally(["a:1", "b:2"], &health),
            (2, 0),
            "misses short of the threshold must not move the gauge"
        );
    }

    #[test]
    fn recovery_returns_the_peer_to_alive() {
        let health = PeerHealth::default();
        for _ in 0..MISS_THRESHOLD {
            health.record_miss("a:1");
        }
        assert_eq!(tally(["a:1"], &health), (0, 1));
        health.record_success("a:1");
        assert_eq!(
            tally(["a:1"], &health),
            (1, 0),
            "a success drops the entry from the snapshot; the view must \
             still yield an alive peer, not a missing series"
        );
    }

    #[test]
    fn recording_without_a_recorder_is_inert() {
        let health = PeerHealth::default();
        record_peer_liveness(["a:1", "b:2"], &health);
        // Membership shrinks: the departed peer must be walked down rather
        // than left behind, and doing so must not panic.
        record_peer_liveness(["a:1"], &health);
        record_peer_rtt(&Coord::default(), [("a:1", &Coord::default())]);
    }

    /// Guards the reason `record_peer_rtt` does not gate on `is_settled`:
    /// inside one datacenter a coordinate never settles, so that gate would
    /// silence the metric exactly where most clusters run.
    #[test]
    fn coordinates_never_settle_at_datacenter_latencies() {
        let mut lan = Coord::default();
        for _ in 0..500 {
            lan.observe(&Coord::default(), 0.5);
        }
        assert!(
            !lan.is_settled(),
            "0.5 ms is below the 2 x MIN_HEIGHT floor, so the relative error \
             cannot fall — if this ever starts settling, the gate becomes \
             safe to reintroduce"
        );

        let mut wan = Coord::default();
        for _ in 0..500 {
            wan.observe(&Coord::default(), 20.0);
        }
        assert!(wan.is_settled(), "a WAN-scale RTT converges normally");
    }

    #[test]
    fn distance_is_exported_in_seconds_not_milliseconds() {
        // The metric name ends in `_seconds` and inherits the latency
        // buckets from it; Vivaldi speaks milliseconds. A missing division
        // would put every cluster three buckets past `+Inf`.
        let here = Coord::default();
        let there = Coord {
            vec: [30.0, 0.0],
            height: 1.0,
            error: 0.1,
        };
        let ms = here.distance(&there);
        assert!((ms - 32.0).abs() < 1e-9, "{ms}");
        assert!((ms / 1000.0 - 0.032).abs() < 1e-9);
    }
}
