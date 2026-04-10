//! PD reconciler — removes zombie PD members that stay unhealthy for >5 minutes.
//!
//! PD members from failed joins can linger as unhealthy phantoms, breaking
//! quorum or preventing new joins. Previously these were only cleaned during
//! the next `hypervisor join`. This reconciler catches them continuously.
//!
//! Runs as a pre-flight step (before TiKV connect) because a zombie member
//! can prevent the TiKV client from connecting at all.

use std::collections::HashMap;
use std::net::Ipv6Addr;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use nauka_hypervisor::controlplane::pd_client::PdClient;

/// How long a member must be continuously unhealthy before removal.
const ZOMBIE_THRESHOLD: Duration = Duration::from_secs(5 * 60);

/// Pure logic: returns true if a member first seen at `first_seen` has exceeded
/// the zombie `threshold` relative to `now`.
fn should_cleanup_member(first_seen: Instant, now: Instant, threshold: Duration) -> bool {
    now.duration_since(first_seen) >= threshold
}

/// Pure logic: update the tracking map with the current set of unhealthy member IDs.
///
/// - Members that are no longer unhealthy are removed (health recovered).
/// - Newly-unhealthy members are inserted with timestamp `now`.
///
/// Returns the list of (member_id, name) pairs that have exceeded the threshold.
fn update_tracker_and_find_zombies(
    tracker: &mut HashMap<u64, Instant>,
    current_unhealthy: &HashMap<u64, String>,
    now: Instant,
    threshold: Duration,
) -> Vec<(u64, String)> {
    // Remove entries for members that recovered (no longer in current_unhealthy)
    tracker.retain(|id, _| current_unhealthy.contains_key(id));

    // Insert newly-unhealthy members
    for &id in current_unhealthy.keys() {
        tracker.entry(id).or_insert(now);
    }

    // Collect members that exceeded the threshold
    current_unhealthy
        .iter()
        .filter_map(|(&id, name)| {
            let first_seen = tracker.get(&id)?;
            if should_cleanup_member(*first_seen, now, threshold) {
                Some((id, name.clone()))
            } else {
                None
            }
        })
        .collect()
}

/// Tracks when each unhealthy member was first seen.
static UNHEALTHY_SINCE: Mutex<Option<HashMap<u64, Instant>>> = Mutex::new(None);

/// Clean up zombie PD members that have been unhealthy for >5 minutes.
///
/// Called every reconcile cycle, before TiKV connect. Returns the number
/// of members removed.
pub fn cleanup_zombie_members(mesh_ipv6: &Ipv6Addr) -> usize {
    let client = PdClient::from_mesh(mesh_ipv6);

    // Query PD health endpoint
    let health = match client.get_health() {
        Ok(h) => h,
        Err(_) => return 0, // PD unreachable — nothing to do
    };

    let now = Instant::now();

    // Collect current unhealthy member IDs
    let mut current_unhealthy: HashMap<u64, String> = HashMap::new();

    for entry in &health {
        if entry.member_id == 0 {
            continue;
        }

        if !entry.healthy {
            current_unhealthy.insert(entry.member_id, entry.name.clone());
        }
    }

    if current_unhealthy.is_empty() {
        // All healthy — clear tracker
        if let Ok(mut guard) = UNHEALTHY_SINCE.lock() {
            if let Some(tracker) = guard.as_mut() {
                tracker.clear();
            }
        }
        return 0;
    }

    // Update the tracking map
    let mut guard = match UNHEALTHY_SINCE.lock() {
        Ok(g) => g,
        Err(_) => return 0,
    };
    let tracker = guard.get_or_insert_with(HashMap::new);

    let zombies =
        update_tracker_and_find_zombies(tracker, &current_unhealthy, now, ZOMBIE_THRESHOLD);

    // Log members still waiting
    for (&id, name) in &current_unhealthy {
        if !zombies.iter().any(|(zid, _)| *zid == id) {
            if let Some(first_seen) = tracker.get(&id) {
                let remaining = ZOMBIE_THRESHOLD
                    .checked_sub(now.duration_since(*first_seen))
                    .unwrap_or_default();
                tracing::debug!(
                    member_id = id,
                    name = name.as_str(),
                    remaining_secs = remaining.as_secs(),
                    "unhealthy PD member tracked, waiting for threshold"
                );
            }
        }
    }

    let mut removed = 0;

    for (member_id, name) in &zombies {
        tracing::warn!(
            member_id,
            name = name.as_str(),
            "removing zombie PD member (unhealthy >5 min)"
        );

        match client.delete_member_by_id(*member_id) {
            Ok(()) => {
                tracing::warn!(member_id, name = name.as_str(), "zombie PD member removed");
                tracker.remove(member_id);
                removed += 1;
            }
            Err(_) => {
                tracing::error!(
                    member_id,
                    name = name.as_str(),
                    "failed to remove zombie PD member"
                );
            }
        }
    }

    removed
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn member_unhealthy_below_threshold_not_cleaned() {
        let mut tracker = HashMap::new();
        let now = Instant::now();
        let threshold = Duration::from_secs(5 * 60);

        let mut unhealthy = HashMap::new();
        unhealthy.insert(1, "pd-node1".to_string());

        // First call: member just appeared unhealthy
        let zombies = update_tracker_and_find_zombies(&mut tracker, &unhealthy, now, threshold);
        assert!(zombies.is_empty(), "member just seen should not be cleaned");

        // Simulate 1 minute later — still below 5-min threshold
        let later = now + Duration::from_secs(60);
        let zombies = update_tracker_and_find_zombies(&mut tracker, &unhealthy, later, threshold);
        assert!(
            zombies.is_empty(),
            "member unhealthy for 1 min should not be cleaned"
        );
    }

    #[test]
    fn member_unhealthy_above_threshold_cleaned() {
        let mut tracker = HashMap::new();
        let now = Instant::now();
        let threshold = Duration::from_secs(5 * 60);

        let mut unhealthy = HashMap::new();
        unhealthy.insert(42, "pd-zombie".to_string());

        // First seen
        let _ = update_tracker_and_find_zombies(&mut tracker, &unhealthy, now, threshold);

        // 6 minutes later — exceeds threshold
        let later = now + Duration::from_secs(6 * 60);
        let zombies = update_tracker_and_find_zombies(&mut tracker, &unhealthy, later, threshold);
        assert_eq!(zombies.len(), 1);
        assert_eq!(zombies[0].0, 42);
        assert_eq!(zombies[0].1, "pd-zombie");
    }

    #[test]
    fn member_recovers_resets_timer() {
        let mut tracker = HashMap::new();
        let now = Instant::now();
        let threshold = Duration::from_secs(5 * 60);

        let mut unhealthy = HashMap::new();
        unhealthy.insert(7, "pd-flaky".to_string());

        // First seen at t=0
        let _ = update_tracker_and_find_zombies(&mut tracker, &unhealthy, now, threshold);

        // 3 minutes later — still unhealthy
        let t1 = now + Duration::from_secs(3 * 60);
        let zombies = update_tracker_and_find_zombies(&mut tracker, &unhealthy, t1, threshold);
        assert!(zombies.is_empty());

        // Member recovers — remove from unhealthy set
        let healthy: HashMap<u64, String> = HashMap::new();
        let _ = update_tracker_and_find_zombies(&mut tracker, &healthy, t1, threshold);
        assert!(
            !tracker.contains_key(&7),
            "recovered member should be removed from tracker"
        );

        // Member becomes unhealthy again at t=3min
        let t2 = now + Duration::from_secs(3 * 60 + 1);
        let _ = update_tracker_and_find_zombies(&mut tracker, &unhealthy, t2, threshold);

        // 3 more minutes (total 6 min from start, but only 3 from re-appearance)
        let t3 = now + Duration::from_secs(6 * 60 + 1);
        let zombies = update_tracker_and_find_zombies(&mut tracker, &unhealthy, t3, threshold);
        assert!(
            zombies.is_empty(),
            "timer should have reset; only 3 min since re-appearance"
        );

        // 5 more minutes from re-appearance — now exceeds threshold
        let t4 = t2 + Duration::from_secs(5 * 60);
        let zombies = update_tracker_and_find_zombies(&mut tracker, &unhealthy, t4, threshold);
        assert_eq!(zombies.len(), 1);
    }

    #[test]
    fn should_cleanup_member_boundary() {
        let start = Instant::now();

        // Exactly at threshold — should cleanup
        assert!(should_cleanup_member(
            start,
            start + Duration::from_secs(300),
            Duration::from_secs(300)
        ));

        // 1 second before threshold — should not
        assert!(!should_cleanup_member(
            start,
            start + Duration::from_secs(299),
            Duration::from_secs(300)
        ));

        // Well past threshold — should cleanup
        assert!(should_cleanup_member(
            start,
            start + Duration::from_secs(600),
            Duration::from_secs(300)
        ));
    }
}
