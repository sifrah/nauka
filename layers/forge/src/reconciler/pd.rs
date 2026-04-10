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

    // Remove entries for members that are now healthy or gone
    tracker.retain(|id, _| current_unhealthy.contains_key(id));

    // Add newly-unhealthy members
    for &id in current_unhealthy.keys() {
        tracker.entry(id).or_insert(now);
    }

    // Find members that exceeded the threshold
    let zombies: Vec<(u64, String)> = current_unhealthy
        .iter()
        .filter_map(|(&id, name)| {
            let first_seen = tracker.get(&id)?;
            if now.duration_since(*first_seen) >= ZOMBIE_THRESHOLD {
                Some((id, name.clone()))
            } else {
                let remaining = ZOMBIE_THRESHOLD
                    .checked_sub(now.duration_since(*first_seen))
                    .unwrap_or_default();
                tracing::debug!(
                    member_id = id,
                    name = name.as_str(),
                    remaining_secs = remaining.as_secs(),
                    "unhealthy PD member tracked, waiting for threshold"
                );
                None
            }
        })
        .collect();

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
