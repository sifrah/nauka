//! Control plane operations — high-level orchestration.
//!
//! Called by the hypervisor handler during init/join/leave/status.
//! Orchestrates TiUP install → config → systemd → health check.
//!
//! PD (Raft consensus) member count is configurable (1, 3, 5, 7) via
//! `--max-pd-members` on init (default: 3). Nodes beyond the PD limit
//! run TiKV only (storage), connecting to existing PD.

use std::net::Ipv6Addr;

use nauka_core::error::NaukaError;
use nauka_core::ui;
use nauka_state::EmbeddedDb;

use super::service::{self, PdConfig, TikvConfig};

/// Bootstrap a new single-node TiKV cluster.
///
/// Uses `steps` to report progress (consumes 4 steps).
pub fn bootstrap(
    node_name: &str,
    mesh_ipv6: &Ipv6Addr,
    steps: &ui::Steps,
) -> Result<(), NaukaError> {
    tracing::info!(node_name, %mesh_ipv6, "controlplane bootstrap starting");

    steps.set("Installing control plane");
    service::ensure_installed()?;
    steps.inc();

    steps.set("Starting control plane");
    let pd_cfg = PdConfig {
        name: node_name.to_string(),
        mesh_ipv6: *mesh_ipv6,
        initial_cluster: format!("{node_name}=http://[{mesh_ipv6}]:{}", super::PD_PEER_PORT),
        initial_cluster_state: "new".to_string(),
    };

    let tikv_cfg = TikvConfig {
        mesh_ipv6: *mesh_ipv6,
        pd_endpoints: vec![format!("http://[{mesh_ipv6}]:{}", super::PD_CLIENT_PORT)],
    };

    service::install(&pd_cfg, &tikv_cfg, None)?;
    service::enable_and_start()?;
    steps.inc();

    steps.set("Waiting for PD");
    service::wait_pd_ready(mesh_ipv6, 30)?;
    steps.inc();

    steps.set("Waiting for TiKV");
    service::wait_tikv_ready(mesh_ipv6, 60)?;

    // Single-node bootstrap: set max-replicas=1 so regions stay healthy
    // even without additional stores. PD defaults to 3 which is wrong
    // for a single-node cluster.
    let pd_url = format!("http://[{}]:{}", mesh_ipv6, super::PD_CLIENT_PORT);
    let _ = service::adjust_max_replicas(&pd_url, 1);

    steps.inc();

    Ok(())
}

/// Join an existing TiKV cluster.
///
/// Uses `steps` to report progress (consumes 4 steps).
///
/// PD scaling strategy — **never run an even number of PD members**
/// (even counts have no fault-tolerance advantage and risk split-brain):
///
/// - Total nodes < next_odd(current_pd_count): TiKV only
/// - Total nodes == next_odd(current_pd_count): scale PD atomically
///
/// Default (max_pd_members=3):
/// - `peer_count` 1 (2nd node): TiKV only, PD stays single-node
/// - `peer_count` 2 (3rd node): scale PD 1→3 atomically
/// - `peer_count` ≥3 (4th+ node): TiKV only
///
/// With max_pd_members=5:
/// - `peer_count` 1: TiKV only
/// - `peer_count` 2: scale PD 1→3
/// - `peer_count` 3,4: TiKV only (already at 3 PD members)
/// - `peer_count` 4: scale PD 3→5
/// - `peer_count` ≥5: TiKV only
///
/// `all_peer_infos` provides (name, mesh_ipv6) for all known peers so we
/// can tell node 2 to start its PD when this is node 3.
pub fn join(
    node_name: &str,
    mesh_ipv6: &Ipv6Addr,
    existing_pd_endpoints: &[String],
    peer_count: usize,
    all_peer_infos: &[(&str, Ipv6Addr)],
    max_pd_members: usize,
    steps: &ui::Steps,
) -> Result<(), NaukaError> {
    if existing_pd_endpoints.is_empty() {
        return Err(NaukaError::precondition(
            "no PD endpoints available. Cannot join control plane.",
        ));
    }

    tracing::info!(node_name, %mesh_ipv6, peer_count, max_pd_members, "controlplane join starting");

    steps.set("Installing control plane");
    service::ensure_installed()?;
    steps.inc();

    let primary_pd = &existing_pd_endpoints[0];

    // Determine if this node should trigger PD scaling.
    // We count total nodes = peer_count + 1 (self).
    // Current PD count comes from how many PD members the cluster has.
    // Scaling triggers: total_nodes reaches 3 (1→3), 5 (3→5), 7 (5→7).
    //
    // The key invariant: never have an even number of PD members.
    // peer_count == N-1 for the Nth node (0-indexed peer list excludes self).
    let should_scale = should_scale_pd(peer_count, max_pd_members);

    if should_scale {
        let total_nodes = peer_count + 1;
        let target = total_nodes.min(max_pd_members);
        let current = if target == 3 { 1 } else { target - 2 };
        steps.set(&format!(
            "Scaling PD {current}→{target} (adding 2 PD members atomically)"
        ));
        scale_pd(
            node_name,
            mesh_ipv6,
            existing_pd_endpoints,
            primary_pd,
            all_peer_infos,
        )?;
    } else {
        steps.set("Starting TiKV only");
        join_tikv_only(node_name, mesh_ipv6, existing_pd_endpoints)?;
    }
    steps.inc();

    // Scale up max-replicas now that we have more stores.
    // min(active_stores, max_pd_members) — never exceed Raft group size.
    let active = service::count_active_stores(&existing_pd_endpoints[0]);
    let target_replicas = active.min(max_pd_members);
    if target_replicas > 0 {
        let _ = service::adjust_max_replicas(&existing_pd_endpoints[0], target_replicas);
    }

    steps.set("Control plane ready");
    steps.inc();

    Ok(())
}

/// Determine whether this joining node should trigger PD scaling.
///
/// PD scaling happens at odd-numbered total node counts: 3, 5, 7.
/// `peer_count` is the number of peers (excluding self), so total = peer_count + 1.
///
/// Returns true if total nodes == 3, 5, or 7 and that count <= max_pd_members.
fn should_scale_pd(peer_count: usize, max_pd_members: usize) -> bool {
    let total_nodes = peer_count + 1; // including self
                                      // Scaling triggers at odd totals: 3, 5, 7
                                      // (when total == 3, we scale 1→3; when total == 5, we scale 3→5; etc.)
    total_nodes >= 3 && total_nodes % 2 == 1 && total_nodes <= max_pd_members
}

/// Scale PD by adding this node as a new PD member.
///
/// Called when the cluster reaches an odd total node count (3, 5, 7)
/// that is within the configured max_pd_members. This node joins PD
/// in --join mode, connecting to the existing cluster.
///
/// We accept a brief even-member window during the scale-up, but add
/// rollback on failure to prevent phantom members that break quorum.
fn scale_pd(
    node_name: &str,
    mesh_ipv6: &Ipv6Addr,
    existing_pd_endpoints: &[String],
    primary_pd: &str,
    _all_peer_infos: &[(&str, Ipv6Addr)],
) -> Result<(), NaukaError> {
    wait_mesh_connectivity(primary_pd, 30)?;

    let self_peer_url = format!("http://[{mesh_ipv6}]:{}", super::PD_PEER_PORT);

    let pd_cfg = PdConfig {
        name: node_name.to_string(),
        mesh_ipv6: *mesh_ipv6,
        initial_cluster: format!("{node_name}={self_peer_url}"),
        initial_cluster_state: "join".to_string(),
    };

    let mut pd_endpoints: Vec<String> = existing_pd_endpoints.to_vec();
    let self_endpoint = format!("http://[{mesh_ipv6}]:{}", super::PD_CLIENT_PORT);
    if !pd_endpoints.contains(&self_endpoint) {
        pd_endpoints.push(self_endpoint);
    }

    let tikv_cfg = TikvConfig {
        mesh_ipv6: *mesh_ipv6,
        pd_endpoints,
    };

    service::install(&pd_cfg, &tikv_cfg, Some(primary_pd))?;
    service::enable_and_start()?;

    // If PD or TiKV fails to become ready, rollback: deregister the PD
    // member we just added to prevent a phantom member that breaks quorum.
    if let Err(e) = service::wait_pd_ready(mesh_ipv6, 120) {
        tracing::error!("PD failed to become ready, rolling back member registration");
        rollback_pd_member(mesh_ipv6, primary_pd);
        return Err(e);
    }

    if let Err(e) = service::wait_tikv_ready(mesh_ipv6, 120) {
        tracing::error!("TiKV failed to become ready, rolling back PD member");
        rollback_pd_member(mesh_ipv6, primary_pd);
        return Err(e);
    }

    Ok(())
}

/// Remove our PD member from the cluster via a remote PD endpoint.
/// Used as rollback when join_with_pd() fails after PD was started.
fn rollback_pd_member(mesh_ipv6: &Ipv6Addr, remote_pd_url: &str) {
    // Stop local PD+TiKV first to prevent further Raft disruption
    let _ = service::stop();

    // Ask the remote PD (which should still have quorum with the other
    // existing members) to remove our member.
    let client = super::pd_client::PdClient::new(vec![remote_pd_url.to_string()]);

    let members = match client.get_members() {
        Ok(m) => m,
        Err(_) => {
            tracing::warn!("rollback: cannot reach remote PD to deregister member");
            return;
        }
    };

    let our_ip = mesh_ipv6.to_string();
    for member in &members {
        let peer_urls_joined = member.peer_urls.join(",");
        if peer_urls_joined.contains(&our_ip) && member.member_id > 0 {
            tracing::warn!(
                member_id = member.member_id,
                "rollback: removing our PD member from cluster"
            );
            let _ = client.delete_member_by_id(member.member_id);
        }
    }
}

/// Join with TiKV only (for nodes not triggering PD scaling).
fn join_tikv_only(
    _node_name: &str,
    mesh_ipv6: &Ipv6Addr,
    existing_pd_endpoints: &[String],
) -> Result<(), NaukaError> {
    wait_mesh_connectivity(&existing_pd_endpoints[0], 30)?;

    let tikv_cfg = TikvConfig {
        mesh_ipv6: *mesh_ipv6,
        pd_endpoints: existing_pd_endpoints.to_vec(),
    };

    // Install TiKV only (no PD unit)
    service::install_tikv_only(&tikv_cfg)?;

    // Start TiKV only
    service::start_tikv_only()?;

    // Wait for TiKV to register with existing PD
    // Use first PD endpoint's IP for health check
    let pd_ip = extract_ipv6_from_endpoint(&existing_pd_endpoints[0]);
    if let Some(ip) = pd_ip {
        service::wait_tikv_ready(&ip, 60)?;
    }

    Ok(())
}

/// Get current cluster status.
pub fn status(mesh_ipv6: &Ipv6Addr) -> Result<service::ClusterStatus, NaukaError> {
    service::cluster_status(mesh_ipv6)
}

/// Start the control plane services.
pub fn start() -> Result<(), NaukaError> {
    if !service::is_installed() {
        return Err(NaukaError::precondition(
            "control plane not installed. Run 'nauka hypervisor init' first.",
        ));
    }
    service::start()
}

/// Stop the control plane services.
pub fn stop() -> Result<(), NaukaError> {
    service::stop()
}

/// Restart the control plane services.
pub fn restart() -> Result<(), NaukaError> {
    service::restart()
}

/// Uninstall the control plane. Deregisters TiKV store, adjusts replicas,
/// waits for region drain, then removes PD member.
///
/// **Idempotent**: each step checks whether it has already been completed
/// (e.g., store already tombstoned, PD member already removed) and skips
/// accordingly. Safe to call again after a crash mid-leave.
pub async fn leave_with_mesh(mesh_ipv6: &Ipv6Addr) -> Result<(), NaukaError> {
    let pd_url = format!("http://[{}]:{}", mesh_ipv6, super::PD_CLIENT_PORT);
    let our_addr = format!("[{}]:{}", mesh_ipv6, super::TIKV_PORT);

    // 1. Find our store ID (if still registered). If already gone, skip drain.
    let store_id = service::find_store_id_via_pd(&our_addr, &pd_url);

    if let Some(sid) = store_id {
        // 2. Deregister TiKV store (marks it as Offline → PD starts draining)
        tracing::info!(store_id = sid, "deregistering TiKV store");
        let _ = service::deregister_store(mesh_ipv6);

        // 3. Adjust max-replicas so Raft can still form quorums after we leave
        let active = service::count_active_stores(&pd_url);
        let remaining = if active > 0 { active - 1 } else { 0 };
        if remaining > 0 {
            // Load max_pd_members from state, fall back to default
            let max_pd = load_max_pd_members()
                .await
                .unwrap_or(super::DEFAULT_MAX_PD_MEMBERS);
            let target = remaining.min(max_pd);
            let _ = service::adjust_max_replicas(&pd_url, target);
        }

        // 4. Wait for regions to drain off this store (5 min, poll every 5s)
        let _ = service::wait_store_regions_drained(&pd_url, sid, 300);
    } else {
        tracing::info!("store already deregistered or not found, skipping drain");
    }

    // 5. Deregister PD member — idempotent: skip if already removed
    deregister_pd_member_idempotent(mesh_ipv6, &pd_url).await;

    service::uninstall()
}

/// Load `max_pd_members` from the local fabric state, if available.
async fn load_max_pd_members() -> Option<usize> {
    let db = EmbeddedDb::open_default().await.ok()?;
    let state = crate::fabric::state::FabricState::load(&db)
        .await
        .ok()
        .flatten();
    let _ = db.shutdown().await;
    state.map(|s| s.max_pd_members)
}

/// Read the local peer list from the fabric state (best-effort, empty on error).
async fn load_peer_ipv6s() -> Vec<Ipv6Addr> {
    let db = match EmbeddedDb::open_default().await {
        Ok(db) => db,
        Err(_) => return Vec::new(),
    };
    let state = crate::fabric::state::FabricState::load(&db)
        .await
        .ok()
        .flatten();
    let _ = db.shutdown().await;
    state
        .map(|s| s.peers.peers.iter().map(|p| p.mesh_ipv6).collect())
        .unwrap_or_default()
}

/// Deregister PD member with idempotent handling.
/// Tries local PD first, then falls back to remote peers.
/// If the member is already gone, treats as success.
async fn deregister_pd_member_idempotent(mesh_ipv6: &Ipv6Addr, pd_url: &str) {
    // Check if our PD member even exists before trying to remove it
    if !service::pd_member_exists(mesh_ipv6, pd_url) {
        // Try remote PDs — maybe local PD is already down
        let found_remotely = try_remote_pd_member_check(mesh_ipv6).await;
        if !found_remotely {
            tracing::info!("PD member already removed, skipping deregistration");
            return;
        }
    }

    let local_ok = service::deregister_pd_member(mesh_ipv6).is_ok() && service::pd_is_active();

    if !local_ok {
        // Local PD is down — try to deregister via a remote PD.
        for peer_ipv6 in load_peer_ipv6s().await {
            let remote_url = format!("http://[{}]:{}", peer_ipv6, super::PD_CLIENT_PORT);
            tracing::info!(%remote_url, "leave: trying remote PD for member deregistration");
            rollback_pd_member(mesh_ipv6, &remote_url);
        }
    }
}

/// Check if our PD member exists via any remote peer's PD.
async fn try_remote_pd_member_check(mesh_ipv6: &Ipv6Addr) -> bool {
    for peer_ipv6 in load_peer_ipv6s().await {
        let remote_url = format!("http://[{}]:{}", peer_ipv6, super::PD_CLIENT_PORT);
        if service::pd_member_exists(mesh_ipv6, &remote_url) {
            return true;
        }
    }
    false
}

/// Uninstall without deregistration (fallback).
pub fn leave() -> Result<(), NaukaError> {
    service::uninstall()
}

/// Wait for mesh connectivity to a PD endpoint (HTTP health check).
fn wait_mesh_connectivity(pd_url: &str, timeout_secs: u64) -> Result<(), NaukaError> {
    let client = super::pd_client::PdClient::new(vec![pd_url.to_string()]);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(timeout_secs);

    while std::time::Instant::now() < deadline {
        if client.ping() {
            // Also clean up any zombie PD members before joining
            cleanup_zombie_members(pd_url);
            return Ok(());
        }
        std::thread::sleep(std::time::Duration::from_secs(2));
    }

    Err(NaukaError::timeout("mesh connectivity to PD", timeout_secs))
}

/// Remove unhealthy PD members (zombies from failed joins).
fn cleanup_zombie_members(pd_url: &str) {
    let client = super::pd_client::PdClient::new(vec![pd_url.to_string()]);

    let health = match client.get_health() {
        Ok(h) => h,
        Err(_) => return,
    };

    for entry in &health {
        // Remove unhealthy members with no name (zombie from failed join)
        if !entry.healthy && entry.name.is_empty() && entry.member_id > 0 {
            tracing::info!(member_id = entry.member_id, "removing zombie PD member");
            let _ = client.delete_member_by_id(entry.member_id);
        }
    }
}

/// Extract IPv6 from endpoint like "http://[fd01::1]:2379"
fn extract_ipv6_from_endpoint(endpoint: &str) -> Option<Ipv6Addr> {
    let s = endpoint.strip_prefix("http://[")?;
    let s = s.split(']').next()?;
    s.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::service::{PdConfig, TikvConfig};
    use super::*;

    #[test]
    fn bootstrap_config_single_node() {
        let ip: Ipv6Addr = "fd01::1".parse().unwrap();
        let pd_cfg = PdConfig {
            name: "node-1".into(),
            mesh_ipv6: ip,
            initial_cluster: format!("node-1=http://[{ip}]:2380"),
            initial_cluster_state: "new".into(),
        };
        let conf = service::generate_pd_conf(&pd_cfg, false);
        assert!(conf.contains("initial-cluster-state = \"new\""));
    }

    #[test]
    fn join_config_format() {
        let ip: Ipv6Addr = "fd01::2".parse().unwrap();
        let pd_cfg = PdConfig {
            name: "node-2".into(),
            mesh_ipv6: ip,
            initial_cluster: "node-2=http://[fd01::2]:2380".into(),
            initial_cluster_state: "join".into(),
        };
        let conf = service::generate_pd_conf(&pd_cfg, true);
        assert!(!conf.contains("initial-cluster"));
        assert!(conf.contains("node-2"));
    }

    #[test]
    fn tikv_multi_pd_endpoints() {
        let cfg = TikvConfig {
            mesh_ipv6: "fd01::1".parse().unwrap(),
            pd_endpoints: vec![
                "http://[fd01::1]:2379".into(),
                "http://[fd01::2]:2379".into(),
            ],
        };
        let conf = service::generate_tikv_conf(&cfg);
        assert!(conf.contains("fd01::1"));
        assert!(conf.contains("fd01::2"));
    }

    #[test]
    fn extract_ipv6_works() {
        let ip = extract_ipv6_from_endpoint("http://[fd01::1]:2379");
        assert_eq!(ip, Some("fd01::1".parse().unwrap()));
    }

    #[test]
    fn extract_ipv6_invalid() {
        assert!(extract_ipv6_from_endpoint("http://1.2.3.4:2379").is_none());
    }

    #[test]
    fn default_max_pd_members() {
        assert_eq!(super::super::DEFAULT_MAX_PD_MEMBERS, 3);
    }

    #[test]
    fn should_scale_pd_default_3() {
        // Default max_pd_members=3
        // peer_count=0 (1st node = init, not join) — N/A
        // peer_count=1 (2nd node, total=2) — no scale
        assert!(!should_scale_pd(1, 3));
        // peer_count=2 (3rd node, total=3) — scale 1→3
        assert!(should_scale_pd(2, 3));
        // peer_count=3 (4th node, total=4) — no scale
        assert!(!should_scale_pd(3, 3));
        // peer_count=4 (5th node, total=5) — no scale (max is 3)
        assert!(!should_scale_pd(4, 3));
    }

    #[test]
    fn should_scale_pd_max_5() {
        // max_pd_members=5
        // peer_count=1 (total=2) — no
        assert!(!should_scale_pd(1, 5));
        // peer_count=2 (total=3) — scale 1→3
        assert!(should_scale_pd(2, 5));
        // peer_count=3 (total=4) — no (even)
        assert!(!should_scale_pd(3, 5));
        // peer_count=4 (total=5) — scale 3→5
        assert!(should_scale_pd(4, 5));
        // peer_count=5 (total=6) — no
        assert!(!should_scale_pd(5, 5));
        // peer_count=6 (total=7) — no (max is 5)
        assert!(!should_scale_pd(6, 5));
    }

    #[test]
    fn should_scale_pd_max_7() {
        // max_pd_members=7
        assert!(should_scale_pd(2, 7)); // total=3 → scale 1→3
        assert!(!should_scale_pd(3, 7)); // total=4 → no
        assert!(should_scale_pd(4, 7)); // total=5 → scale 3→5
        assert!(!should_scale_pd(5, 7)); // total=6 → no
        assert!(should_scale_pd(6, 7)); // total=7 → scale 5→7
        assert!(!should_scale_pd(7, 7)); // total=8 → no
    }

    #[test]
    fn should_scale_pd_max_1() {
        // max_pd_members=1 — never scale beyond init node
        assert!(!should_scale_pd(1, 1));
        assert!(!should_scale_pd(2, 1));
        assert!(!should_scale_pd(10, 1));
    }
}
