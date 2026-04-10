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
                now.saturating_sub(ref_time) > UNREACHABLE_THRESHOLD_SECS
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
            .filter(|s| s["store"]["state_name"].as_str() == Some("Up"))
            .count();

        // For each "Up" store whose address matches an unreachable peer, offline it
        for store in stores {
            let addr = store["store"]["address"].as_str().unwrap_or("");
            let state_name = store["store"]["state_name"].as_str().unwrap_or("");
            let store_id = store["store"]["id"].as_u64().unwrap_or(0);

            if state_name != "Up" || store_id == 0 {
                continue;
            }

            if !unreachable_addrs.iter().any(|a| a == addr) {
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
