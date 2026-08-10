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
        "Round-trip time to a peer as estimated by its Vivaldi coordinates. Absent while either coordinate is still unsettled — an unconverged estimate is noise, not a measurement."
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
/// Only settled coordinates are exported. A fresh node starts at the
/// maximum error with a placeholder position; publishing that distance
/// would put a confident-looking number on a guess, and the reader has no
/// way to tell it apart from a measurement.
pub fn record_peer_rtt<'a>(
    self_coord: &Coord,
    peers: impl IntoIterator<Item = (&'a str, &'a Coord)>,
) {
    if !self_coord.is_settled() {
        return;
    }
    for (peer, coord) in peers {
        if !coord.is_settled() {
            continue;
        }
        // Vivaldi works in milliseconds; the metric is named `_seconds` and
        // inherits the latency buckets from the exporter's suffix matcher,
        // so the conversion is not optional.
        let seconds = self_coord.distance(coord) / 1000.0;
        metrics::gauge!("nauka_peer_rtt_seconds", "peer" => peer.to_string()).set(seconds);
    }
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

    #[test]
    fn unsettled_coordinates_are_not_exported() {
        let fresh = Coord::default();
        assert!(
            !fresh.is_settled(),
            "a default coordinate is a guess, and must stay out of the metric"
        );
    }
}
