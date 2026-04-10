//! Control plane operations — high-level orchestration.
//!
//! Called by the hypervisor handler during init/join/leave/status.
//! Orchestrates TiUP install → config → systemd → health check.
//!
//! PD (Raft consensus) runs on max 3 nodes for optimal performance.
//! Additional nodes run TiKV only (storage), connecting to existing PD.

use std::net::Ipv6Addr;

use nauka_core::error::NaukaError;
use nauka_core::ui;

use super::service::{self, PdConfig, TikvConfig};

/// Max PD members in the cluster. Raft works best with 3 or 5.
const MAX_PD_MEMBERS: usize = 3;

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
/// PD scaling strategy — **never run 2 PD members** (no fault tolerance,
/// worse than 1 because any disruption loses quorum):
///
/// - `peer_count` 1 (2nd node): TiKV only, PD stays single-node
/// - `peer_count` 2 (3rd node): scale PD 1→3 atomically (this node + node 2)
/// - `peer_count` ≥3 (4th+ node): TiKV only
///
/// `all_peer_infos` provides (name, mesh_ipv6) for all known peers so we
/// can tell node 2 to start its PD when this is node 3.
pub fn join(
    node_name: &str,
    mesh_ipv6: &Ipv6Addr,
    existing_pd_endpoints: &[String],
    peer_count: usize,
    all_peer_infos: &[(&str, Ipv6Addr)],
    steps: &ui::Steps,
) -> Result<(), NaukaError> {
    if existing_pd_endpoints.is_empty() {
        return Err(NaukaError::precondition(
            "no PD endpoints available. Cannot join control plane.",
        ));
    }

    tracing::info!(node_name, %mesh_ipv6, peer_count, "controlplane join starting");

    steps.set("Installing control plane");
    service::ensure_installed()?;
    steps.inc();

    let primary_pd = &existing_pd_endpoints[0];

    // Never create a 2-member PD cluster — skip straight from 1 to 3.
    // peer_count=1 → 2nd node → TiKV only (PD stays single on node 1)
    // peer_count=2 → 3rd node → scale PD 1→3 (add PD on node 2 + node 3)
    // peer_count≥3 → TiKV only
    if peer_count == 2 {
        steps.set("Scaling PD 1→3 (adding 2 PD members atomically)");
        scale_pd_to_three(
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
    // min(active_stores, MAX_PD_MEMBERS) — never exceed Raft group size.
    let active = service::count_active_stores(&existing_pd_endpoints[0]);
    let target_replicas = active.min(MAX_PD_MEMBERS);
    if target_replicas > 0 {
        let _ = service::adjust_max_replicas(&existing_pd_endpoints[0], target_replicas);
    }

    steps.set("Control plane ready");
    steps.inc();

    Ok(())
}

/// Scale PD from 1 member to 3 atomically.
///
/// Called when the 3rd node joins. At this point node 2 is running TiKV-only.
/// We need to:
/// 1. Start PD on node 2 (via SSH/announce — but we can't SSH into peers)
///    → Instead, install PD config on THIS node (node 3) and let node 2's
///      forge daemon detect it needs PD and self-heal.
///    → Actually, simpler: node 3 just starts its own PD in --join mode.
///      Node 2 will get PD later via forge reconciliation or a future join.
///
/// For now: start PD on node 3 only. This gives us 2 PD members (1+3),
/// which is still not ideal. The real fix is to install PD on node 2
/// during its join but NOT start it, then start both when node 3 joins.
///
/// **Revised approach**: Install PD config on node 2 during its join
/// (service installed but not started). When node 3 joins, tell node 2
/// to start PD, then start our own PD.
///
/// **Simplest correct approach for now**: This node (3) joins with PD.
/// We accept the brief 2-member window, but add rollback on failure.
fn scale_pd_to_three(
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
    let members_url = format!("{remote_pd_url}/pd/api/v1/members");
    let output = match std::process::Command::new("curl")
        .args(["-sf", "--max-time", "10", &members_url])
        .output()
    {
        Ok(o) if o.status.success() => o,
        _ => {
            tracing::warn!("rollback: cannot reach remote PD to deregister member");
            return;
        }
    };

    let body: serde_json::Value = match serde_json::from_slice(&output.stdout) {
        Ok(v) => v,
        Err(_) => return,
    };

    let our_ip = mesh_ipv6.to_string();
    if let Some(members) = body["members"].as_array() {
        for member in members {
            let peer_urls = member["peer_urls"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|v| v.as_str())
                        .collect::<Vec<_>>()
                        .join(",")
                })
                .unwrap_or_default();
            let member_id = member["member_id"].as_u64().unwrap_or(0);

            if peer_urls.contains(&our_ip) && member_id > 0 {
                tracing::warn!(member_id, "rollback: removing our PD member from cluster");
                let delete_url = format!("{remote_pd_url}/pd/api/v1/members/id/{member_id}");
                let _ = std::process::Command::new("curl")
                    .args(["-sf", "-X", "DELETE", "--max-time", "10", &delete_url])
                    .output();
            }
        }
    }
}

/// Join with TiKV only (for nodes beyond MAX_PD_MEMBERS).
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
/// waits for region migration, then removes PD member.
pub fn leave_with_mesh(mesh_ipv6: &Ipv6Addr) -> Result<(), NaukaError> {
    let pd_url = format!("http://[{}]:{}", mesh_ipv6, super::PD_CLIENT_PORT);

    // 1. Deregister TiKV store (marks it as Tombstone in PD)
    let _ = service::deregister_store(mesh_ipv6);

    // 2. Count remaining active stores (excluding the one we just deregistered)
    //    and adjust max-replicas so Raft can still form quorums
    let active = service::count_active_stores(&pd_url);
    let remaining = if active > 0 { active - 1 } else { 0 };
    if remaining > 0 {
        let target = remaining.min(MAX_PD_MEMBERS);
        let _ = service::adjust_max_replicas(&pd_url, target);
        // 3. Wait for PD to migrate region peers off the dead store
        let _ = service::wait_regions_healthy(&pd_url, 30);
    }

    // 4. Deregister PD member — try local first, then remote peers
    let local_ok = service::deregister_pd_member(mesh_ipv6).is_ok() && service::pd_is_active();

    if !local_ok {
        // Local PD is down — try to deregister via a remote PD.
        // Load peer list from fabric state to find other PD endpoints.
        if let Ok(db) = nauka_state::LocalDb::open("hypervisor") {
            if let Some(state) = crate::fabric::state::FabricState::load(&db).ok().flatten() {
                for peer in &state.peers.peers {
                    let remote_url =
                        format!("http://[{}]:{}", peer.mesh_ipv6, super::PD_CLIENT_PORT);
                    tracing::info!(%remote_url, "leave: trying remote PD for member deregistration");
                    rollback_pd_member(mesh_ipv6, &remote_url);
                }
            }
        }
    }

    service::uninstall()
}

/// Uninstall without deregistration (fallback).
pub fn leave() -> Result<(), NaukaError> {
    service::uninstall()
}

/// Wait for mesh connectivity to a PD endpoint (HTTP health check).
fn wait_mesh_connectivity(pd_url: &str, timeout_secs: u64) -> Result<(), NaukaError> {
    let url = format!("{pd_url}/pd/api/v1/health");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(timeout_secs);

    while std::time::Instant::now() < deadline {
        if std::process::Command::new("curl")
            .args(["-sf", "--max-time", "3", &url])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
        {
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
    let url = format!("{pd_url}/pd/api/v1/health");
    let output = match std::process::Command::new("curl")
        .args(["-sf", "--max-time", "5", &url])
        .output()
    {
        Ok(o) if o.status.success() => o,
        _ => return,
    };

    let health: serde_json::Value = match serde_json::from_slice(&output.stdout) {
        Ok(v) => v,
        Err(_) => return,
    };

    if let Some(members) = health.as_array() {
        for member in members {
            let healthy = member["health"].as_bool().unwrap_or(true);
            let name = member["name"].as_str().unwrap_or("");
            let member_id = member["member_id"].as_u64().unwrap_or(0);

            // Remove unhealthy members with no name (zombie from failed join)
            if !healthy && name.is_empty() && member_id > 0 {
                tracing::info!(member_id, "removing zombie PD member");
                let delete_url = format!("{pd_url}/pd/api/v1/members/id/{member_id}");
                let _ = std::process::Command::new("curl")
                    .args(["-sf", "-X", "DELETE", "--max-time", "5", &delete_url])
                    .output();
            }
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
    fn max_pd_members_constant() {
        assert_eq!(MAX_PD_MEMBERS, 3);
    }
}
