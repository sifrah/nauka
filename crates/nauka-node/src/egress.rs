//! Egress metering and monthly budgets.
//!
//! Storage placement balances a STOCK: bytes on disk against declared
//! capacity. This module balances the matching FLOW: bytes served to
//! clients against a declared monthly budget (`NAUKA_EGRESS_QUOTA`).
//! Same pattern end to end — the node measures itself, publishes the
//! counter into the replicated state, and every node derives the same
//! routing preference from the same inputs, with zero coordination.
//!
//! The counter is intent, not acknowledgement: bytes are counted when a
//! response is committed to, so a client that disconnects mid-download
//! still consumed its slice of budget. Over a month the difference is
//! noise, and over-counting is the safe direction for a budget.
//!
//! A node past its budget is DEPRIORITIZED, never refused: correctness
//! (serving the file) always beats economy. The budget shifts load while
//! alternatives exist and yields when they don't.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use nauka_raft::types::NodeEgress;

/// The calendar month of a unix timestamp, as `"YYYY-MM"` (UTC) — the
/// window budgets reset on, matching how providers bill.
pub fn month_key(unix_secs: u64) -> String {
    let odt = time::OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(unix_secs as i64);
    format!("{:04}-{:02}", odt.year(), u8::from(odt.month()))
}

/// Parses a human byte size: plain bytes ("1500000"), decimal units
/// ("500GB", "20TB", "1.5TB"), or binary units ("1TiB", "512MiB").
/// Case-insensitive, optional whitespace before the unit.
pub fn parse_size(s: &str) -> Option<u64> {
    let s = s.trim();
    let split = s
        .find(|c: char| !(c.is_ascii_digit() || c == '.'))
        .unwrap_or(s.len());
    let (num, unit) = s.split_at(split);
    let value: f64 = num.parse().ok()?;
    if !value.is_finite() || value < 0.0 {
        return None;
    }
    let mult: u64 = match unit.trim().to_ascii_lowercase().as_str() {
        "" | "b" => 1,
        "kb" => 1_000,
        "mb" => 1_000_000,
        "gb" => 1_000_000_000,
        "tb" => 1_000_000_000_000,
        "pb" => 1_000_000_000_000_000,
        "kib" => 1 << 10,
        "mib" => 1 << 20,
        "gib" => 1 << 30,
        "tib" => 1u64 << 40,
        "pib" => 1u64 << 50,
        _ => return None,
    };
    let bytes = value * mult as f64;
    if bytes > u64::MAX as f64 {
        return None;
    }
    Some(bytes as u64)
}

/// This node's live egress counter for the current month. Cheap to bump
/// from any response path; the maintenance ticker snapshots and publishes
/// it into the replicated state.
pub struct EgressMeter {
    served: AtomicU64,
    /// Month the counter belongs to; crossing into a new month resets it.
    month: Mutex<String>,
    quota: Option<u64>,
}

impl EgressMeter {
    pub fn new(quota: Option<u64>, now_secs: u64) -> Self {
        Self {
            served: AtomicU64::new(0),
            month: Mutex::new(month_key(now_secs)),
            quota,
        }
    }

    pub fn quota(&self) -> Option<u64> {
        self.quota
    }

    /// Counts bytes committed to a client response.
    pub fn add(&self, bytes: u64) {
        self.served.fetch_add(bytes, Ordering::Relaxed);
    }

    /// The (month, served) pair as of `now`, rolling the counter over to
    /// zero when the calendar month changed since the last look.
    pub fn snapshot(&self, now_secs: u64) -> (String, u64) {
        let current = month_key(now_secs);
        let mut month = self.month.lock().unwrap_or_else(|p| p.into_inner());
        if *month != current {
            *month = current.clone();
            self.served.store(0, Ordering::Relaxed);
        }
        (current, self.served.load(Ordering::Relaxed))
    }

    /// Adopts a replicated record from a previous run of this node, so a
    /// restart mid-month does not zero the ledger. Only grows the counter,
    /// and only within the same month.
    pub fn seed(&self, record: &NodeEgress, now_secs: u64) {
        let (month, served) = self.snapshot(now_secs);
        if record.month == month && record.served_bytes > served {
            self.served.store(record.served_bytes, Ordering::Relaxed);
        }
    }
}

/// The remaining-budget ratio a routing decision sorts by. Unmetered,
/// unknown, or previous-month records read as infinite headroom; an
/// exhausted budget reads as 0.
pub fn remaining_ratio(record: Option<&NodeEgress>, current_month: &str) -> f64 {
    let Some(r) = record else {
        return f64::INFINITY;
    };
    let Some(quota) = r.quota_bytes else {
        return f64::INFINITY;
    };
    if r.month != current_month {
        // Stale record: their month rolled over, budget is fresh again.
        return f64::INFINITY;
    }
    if quota == 0 {
        return 0.0;
    }
    (quota.saturating_sub(r.served_bytes)) as f64 / quota as f64
}

/// Orders the shard slots of a stripe by preference: local shards first
/// (no network at all), then holders with the most budget headroom, ties
/// broken by slot index so data shards win over parity at equal standing
/// (cheaper decode) and the order stays deterministic.
pub fn rank_slots(holder_ratios: &[(bool, f64)]) -> Vec<usize> {
    let mut idx: Vec<usize> = (0..holder_ratios.len()).collect();
    idx.sort_by(|&a, &b| {
        let (la, ra) = holder_ratios[a];
        let (lb, rb) = holder_ratios[b];
        lb.cmp(&la) // local first
            .then(rb.partial_cmp(&ra).unwrap_or(std::cmp::Ordering::Equal))
            .then(a.cmp(&b))
    });
    idx
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(month: &str, served: u64, quota: Option<u64>) -> NodeEgress {
        NodeEgress {
            month: month.into(),
            served_bytes: served,
            quota_bytes: quota,
        }
    }

    #[test]
    fn month_key_is_utc_calendar() {
        assert_eq!(month_key(0), "1970-01");
        // 2026-08-08 ~10:00 UTC.
        assert_eq!(month_key(1_786_528_800), "2026-08");
        // Last second of a month vs first of the next.
        assert_eq!(month_key(1_756_684_799), "2025-08");
        assert_eq!(month_key(1_756_684_800), "2025-09");
    }

    #[test]
    fn sizes_parse_like_humans_write_them() {
        assert_eq!(parse_size("123"), Some(123));
        assert_eq!(parse_size("500GB"), Some(500_000_000_000));
        assert_eq!(parse_size("20TB"), Some(20_000_000_000_000));
        assert_eq!(parse_size("1.5TB"), Some(1_500_000_000_000));
        assert_eq!(parse_size("1TiB"), Some(1 << 40));
        assert_eq!(parse_size(" 512 MiB "), Some(512 << 20));
        assert_eq!(parse_size("0"), Some(0));
        assert_eq!(parse_size("garbage"), None);
        assert_eq!(parse_size("-5GB"), None);
        assert_eq!(parse_size("10XB"), None);
    }

    #[test]
    fn the_meter_counts_and_rolls_over_at_month_boundaries() {
        let aug = 1_786_528_800; // 2026-08
        let m = EgressMeter::new(Some(1_000), aug);
        m.add(400);
        m.add(100);
        assert_eq!(m.snapshot(aug), ("2026-08".into(), 500));
        // Same month later: still counting.
        m.add(1);
        assert_eq!(m.snapshot(aug + 3600).1, 501);
        // Next month: fresh counter.
        let sep = aug + 31 * 86_400;
        assert_eq!(m.snapshot(sep), ("2026-09".into(), 0));
        m.add(7);
        assert_eq!(m.snapshot(sep).1, 7);
    }

    #[test]
    fn seeding_survives_restarts_but_not_months() {
        let aug = 1_786_528_800;
        let m = EgressMeter::new(Some(1_000), aug);
        m.add(10);
        // The replicated record remembers more than the fresh counter.
        m.seed(&rec("2026-08", 800, Some(1_000)), aug);
        assert_eq!(m.snapshot(aug).1, 800);
        // A record smaller than the live counter never shrinks it.
        m.seed(&rec("2026-08", 300, Some(1_000)), aug);
        assert_eq!(m.snapshot(aug).1, 800);
        // A record from another month is ignored.
        let m2 = EgressMeter::new(None, aug);
        m2.seed(&rec("2026-07", 900, Some(1_000)), aug);
        assert_eq!(m2.snapshot(aug).1, 0);
    }

    #[test]
    fn remaining_ratio_reads_budgets_the_safe_way() {
        let now = "2026-08";
        assert_eq!(remaining_ratio(None, now), f64::INFINITY);
        assert_eq!(
            remaining_ratio(Some(&rec("2026-08", 500, None)), now),
            f64::INFINITY,
            "no quota = unmetered"
        );
        assert_eq!(
            remaining_ratio(Some(&rec("2026-07", 999, Some(1_000))), now),
            f64::INFINITY,
            "last month's exhaustion is this month's fresh budget"
        );
        assert_eq!(
            remaining_ratio(Some(&rec("2026-08", 250, Some(1_000))), now),
            0.75
        );
        assert_eq!(
            remaining_ratio(Some(&rec("2026-08", 2_000, Some(1_000))), now),
            0.0
        );
        assert_eq!(remaining_ratio(Some(&rec("2026-08", 0, Some(0))), now), 0.0);
    }

    #[test]
    fn slot_ranking_prefers_local_then_headroom_then_data_shards() {
        // Slots: 0-1 data on remote nodes, 2 parity on an unmetered node,
        // 3 parity held locally.
        let ranked = rank_slots(&[
            (false, 0.10),          // 0: data, nearly exhausted holder
            (false, 0.90),          // 1: data, plenty left
            (false, f64::INFINITY), // 2: parity, unmetered
            (true, 0.0),            // 3: parity but LOCAL — free, wins outright
        ]);
        assert_eq!(ranked, vec![3, 2, 1, 0]);

        // Ties fall back to slot order: data before parity.
        let tied = rank_slots(&[(false, 1.0), (false, 1.0), (false, 1.0)]);
        assert_eq!(tied, vec![0, 1, 2]);
    }
}
