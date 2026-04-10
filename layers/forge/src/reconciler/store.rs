//! TiKV store reconciler — offlines stores whose fabric peer is unreachable.
//!
//! When a fabric peer has been unreachable for more than 60 seconds, the TiKV
//! store running on that peer is moved offline via the PD API. This prevents
//! stuck regions caused by PD waiting for a store that will never return.

use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use nauka_hypervisor::controlplane;
use nauka_hypervisor::fabric;
use nauka_state::LocalDb;

use crate::types::{ReconcileContext, ReconcileResult};

/// How long a peer must be unreachable before we offline its store (seconds).
const UNREACHABLE_THRESHOLD_SECS: u64 = 60;

/// Pure logic: returns true if the store address matches any of the expected
/// peer addresses. Both sides use the `[ipv6]:port` format.
fn match_store_to_peer(store_addr: &str, peer_addrs: &[String]) -> bool {
    peer_addrs.iter().any(|a| a == store_addr)
}

/// Pure logic: returns true if a store with the given `state_name` is eligible
/// for deregistration. Only "Up" stores should be offlined; Tombstone, Offline,
/// and Disconnected stores are already handled by PD.
fn is_store_eligible_for_deregister(state_name: &str) -> bool {
    state_name == "Up"
}

/// Pure logic: returns true if a peer has been unreachable long enough based on
/// its reference timestamp (`last_handshake` or `added_at`) and the current time.
fn is_peer_unreachable_past_threshold(ref_time: u64, now: u64, threshold: u64) -> bool {
    now.saturating_sub(ref_time) > threshold
}

pub struct StoreReconciler;

#[async_trait::async_trait]
impl super::Reconciler for StoreReconciler {
    fn name(&self) -> &str {
        "store"
    }

    async fn reconcile(&self, ctx: &ReconcileContext) -> anyhow::Result<ReconcileResult> {
        let mut result = ReconcileResult::new("store");

        // Load fabric state to get peer list
        let local_db = LocalDb::open("hypervisor")?;
        let state = match fabric::state::FabricState::load(&local_db)
            .map_err(|e| anyhow::anyhow!("{e}"))?
        {
            Some(s) => s,
            None => return Ok(result), // not initialized
        };

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // Find peers that have been unreachable for >60s
        let unreachable_peers: Vec<_> = state
            .peers
            .peers
            .iter()
            .filter(|p| p.status == fabric::peer::PeerStatus::Unreachable)
            .filter(|p| {
                // Use last_handshake as the reference point for how long the peer
                // has been unreachable. If last_handshake is 0, use added_at instead.
                let ref_time = if p.last_handshake > 0 {
                    p.last_handshake
                } else {
                    p.added_at
                };
                is_peer_unreachable_past_threshold(ref_time, now, UNREACHABLE_THRESHOLD_SECS)
            })
            .collect();

        if unreachable_peers.is_empty() {
            return Ok(result);
        }

        // Build the set of TiKV addresses for unreachable peers
        let unreachable_addrs: Vec<String> = unreachable_peers
            .iter()
            .map(|p| format!("[{}]:{}", p.mesh_ipv6, controlplane::TIKV_PORT))
            .collect();

        // Get store list from PD
        let pd_url = format!(
            "http://[{}]:{}",
            ctx.mesh_ipv6,
            controlplane::PD_CLIENT_PORT,
        );
        let stores_url = format!("{pd_url}/pd/api/v1/stores");
        let output = match Command::new("curl")
            .args(["-sf", "--max-time", "5", &stores_url])
            .output()
        {
            Ok(o) if o.status.success() => o,
            _ => return Ok(result), // PD not reachable, skip
        };

        let body: serde_json::Value = match serde_json::from_slice(&output.stdout) {
            Ok(v) => v,
            Err(_) => return Ok(result),
        };

        let stores = match body["stores"].as_array() {
            Some(s) => s,
            None => return Ok(result),
        };

        result.desired = stores
            .iter()
            .filter(|s| {
                is_store_eligible_for_deregister(s["store"]["state_name"].as_str().unwrap_or(""))
            })
            .count();

        // For each "Up" store whose address matches an unreachable peer, offline it
        for store in stores {
            let addr = store["store"]["address"].as_str().unwrap_or("");
            let state_name = store["store"]["state_name"].as_str().unwrap_or("");
            let store_id = store["store"]["id"].as_u64().unwrap_or(0);

            if !is_store_eligible_for_deregister(state_name) || store_id == 0 {
                continue;
            }

            if !match_store_to_peer(addr, &unreachable_addrs) {
                continue;
            }

            // Find the peer name for logging
            let peer_name = unreachable_peers
                .iter()
                .find(|p| format!("[{}]:{}", p.mesh_ipv6, controlplane::TIKV_PORT) == addr)
                .map(|p| p.name.as_str())
                .unwrap_or("unknown");

            tracing::warn!(
                store_id,
                addr,
                peer = peer_name,
                "offlining TiKV store — peer unreachable >60s"
            );

            let delete_url = format!("{pd_url}/pd/api/v1/store/{store_id}");
            match Command::new("curl")
                .args(["-sf", "-X", "DELETE", "--max-time", "5", &delete_url])
                .output()
            {
                Ok(o) if o.status.success() => {
                    result.deleted += 1;
                }
                _ => {
                    result.failed += 1;
                    result
                        .errors
                        .push(format!("failed to offline store {store_id} ({addr})"));
                }
            }
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn store_address_matches_peer() {
        let peer_addrs = vec![
            "[fd12:3456:789a::1]:20160".to_string(),
            "[fd12:3456:789a::2]:20160".to_string(),
        ];

        assert!(match_store_to_peer(
            "[fd12:3456:789a::1]:20160",
            &peer_addrs
        ));
        assert!(match_store_to_peer(
            "[fd12:3456:789a::2]:20160",
            &peer_addrs
        ));
    }

    #[test]
    fn store_address_no_match() {
        let peer_addrs = vec!["[fd12:3456:789a::1]:20160".to_string()];

        // Different address
        assert!(!match_store_to_peer(
            "[fd12:3456:789a::99]:20160",
            &peer_addrs
        ));
        // Different port
        assert!(!match_store_to_peer(
            "[fd12:3456:789a::1]:20161",
            &peer_addrs
        ));
        // Empty peer list
        assert!(!match_store_to_peer("[fd12:3456:789a::1]:20160", &[]));
    }

    #[test]
    fn store_up_is_eligible() {
        assert!(is_store_eligible_for_deregister("Up"));
    }

    #[test]
    fn store_tombstone_not_eligible() {
        assert!(!is_store_eligible_for_deregister("Tombstone"));
    }

    #[test]
    fn store_offline_not_eligible() {
        assert!(!is_store_eligible_for_deregister("Offline"));
        assert!(!is_store_eligible_for_deregister("Disconnected"));
        assert!(!is_store_eligible_for_deregister(""));
    }

    #[test]
    fn peer_unreachable_past_threshold() {
        let now = 1000;
        let threshold = 60;

        // last_handshake was 120s ago — well past 60s threshold
        assert!(is_peer_unreachable_past_threshold(880, now, threshold));

        // last_handshake was 61s ago — just past threshold
        assert!(is_peer_unreachable_past_threshold(939, now, threshold));
    }

    #[test]
    fn peer_unreachable_within_threshold() {
        let now = 1000;
        let threshold = 60;

        // last_handshake was 30s ago — within threshold
        assert!(!is_peer_unreachable_past_threshold(970, now, threshold));

        // last_handshake was exactly 60s ago — not past (> not >=)
        assert!(!is_peer_unreachable_past_threshold(940, now, threshold));
    }

    #[test]
    fn peer_unreachable_zero_ref_time() {
        // ref_time = 0 (never handshaked), now = 100, threshold = 60
        // saturating_sub(0) = 100 > 60 → unreachable
        assert!(is_peer_unreachable_past_threshold(0, 100, 60));
    }
}
