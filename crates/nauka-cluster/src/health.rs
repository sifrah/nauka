//! Cluster liveness, fed by a background pinger.
//!
//! Deciding who is alive is deliberately NOT Raft's job: membership guards
//! identity, votes and quorum, and must stay stable through a reboot. This
//! map only steers *placement* — where new shards go, which owners the
//! scrubber repairs towards. A member marked down keeps its identity and
//! its vote; it merely stops receiving shards until it answers pings again.
//!
//! The map is optimistic: a peer nobody has probed yet counts as alive
//! (a fresh node must not start with an empty world), and a single
//! successful exchange clears any accumulated misses.

use std::collections::BTreeMap;
use std::sync::Mutex;

/// Consecutive failed probes before a peer is considered down. With the
/// 5-second probe cadence this detects a death in ~15 s — fast enough to
/// stop routing uploads at it, slow enough to ride out one lost datagram.
pub const MISS_THRESHOLD: u32 = 3;

#[derive(Default)]
pub struct PeerHealth {
    misses: Mutex<BTreeMap<String, u32>>,
}

impl PeerHealth {
    pub fn record_success(&self, addr: &str) {
        self.misses.lock().unwrap().remove(addr);
    }

    pub fn record_miss(&self, addr: &str) {
        let mut m = self.misses.lock().unwrap();
        *m.entry(addr.to_string()).or_insert(0) += 1;
    }

    pub fn is_alive(&self, addr: &str) -> bool {
        self.misses
            .lock()
            .unwrap()
            .get(addr)
            .is_none_or(|n| *n < MISS_THRESHOLD)
    }

    /// Liveness of every peer the map has an opinion about — `false` once
    /// the misses reached the threshold. A peer nobody has probed yet is
    /// simply ABSENT from the result: the map stays optimistic, and the
    /// caller reads a missing entry exactly as `is_alive` does, alive.
    ///
    /// This reads the raw counters on purpose. `filter_view` cannot answer
    /// the question — it returns the full view when everyone is down, so
    /// "all peers dead" and "all peers alive" are the same answer there.
    pub fn snapshot(&self) -> BTreeMap<String, bool> {
        self.misses
            .lock()
            .unwrap()
            .iter()
            .map(|(addr, n)| (addr.clone(), *n < MISS_THRESHOLD))
            .collect()
    }

    /// The placement view restricted to peers currently answering. If the
    /// filter would empty the view — this node partitioned from everyone —
    /// the full view is returned instead: behaving like the liveness map
    /// does not exist degrades to the old semantics, never to "place
    /// nothing anywhere".
    pub fn filter_view(&self, view: Vec<(String, u64)>) -> Vec<(String, u64)> {
        let alive: Vec<(String, u64)> = view
            .iter()
            .filter(|(addr, _)| self.is_alive(addr))
            .cloned()
            .collect();
        if alive.is_empty() {
            view
        } else {
            alive
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_peers_are_alive() {
        let h = PeerHealth::default();
        assert!(h.is_alive("1.2.3.4:7311"));
    }

    #[test]
    fn down_after_threshold_up_after_one_success() {
        let h = PeerHealth::default();
        for _ in 0..MISS_THRESHOLD - 1 {
            h.record_miss("a:1");
            assert!(h.is_alive("a:1"), "below the threshold: still alive");
        }
        h.record_miss("a:1");
        assert!(!h.is_alive("a:1"));
        h.record_success("a:1");
        assert!(h.is_alive("a:1"), "one success clears the slate");
    }

    #[test]
    fn snapshot_reports_only_probed_peers_and_agrees_with_is_alive() {
        let h = PeerHealth::default();
        assert!(h.snapshot().is_empty(), "nothing probed yet: no opinion");

        h.record_miss("a:1");
        assert_eq!(
            h.snapshot().get("a:1"),
            Some(&true),
            "one miss is not death"
        );
        for _ in 1..MISS_THRESHOLD {
            h.record_miss("a:1");
        }
        assert_eq!(h.snapshot().get("a:1"), Some(&false));
        assert!(!h.snapshot().contains_key("b:2"), "unprobed stays unknown");
        assert_eq!(h.snapshot().get("a:1").copied(), Some(h.is_alive("a:1")));

        h.record_success("a:1");
        assert!(
            !h.snapshot().contains_key("a:1"),
            "a success forgets the peer entirely — back to optimistic"
        );
    }

    #[test]
    fn snapshot_distinguishes_everyone_down_from_everyone_up() {
        // The trap `filter_view` sets: with every peer down it returns the
        // full view, which is indistinguishable from every peer being up.
        let h = PeerHealth::default();
        let view = vec![("a:1".to_string(), 10), ("b:2".to_string(), 10)];
        for _ in 0..MISS_THRESHOLD {
            h.record_miss("a:1");
            h.record_miss("b:2");
        }
        assert_eq!(h.filter_view(view.clone()), view, "the trap");
        assert_eq!(
            h.snapshot(),
            BTreeMap::from([("a:1".to_string(), false), ("b:2".to_string(), false)])
        );
    }

    #[test]
    fn filtered_view_drops_the_dead_but_never_empties() {
        let h = PeerHealth::default();
        let view = vec![("a:1".to_string(), 10), ("b:2".to_string(), 10)];
        for _ in 0..MISS_THRESHOLD {
            h.record_miss("b:2");
        }
        assert_eq!(h.filter_view(view.clone()), vec![("a:1".to_string(), 10)]);

        // Everyone down (we are partitioned): full view, old semantics.
        for _ in 0..MISS_THRESHOLD {
            h.record_miss("a:1");
        }
        assert_eq!(h.filter_view(view.clone()), view);
    }
}
