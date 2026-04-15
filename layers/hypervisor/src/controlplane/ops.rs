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
/// Uses `steps` to report progress (consumes 5 steps).
///
/// The fifth step — "Applying cluster schemas" — was added in P2.7
/// (sifrah/nauka#211) per ADR 0004 (sifrah/nauka#210): the bootstrap
/// node applies the cluster `.surql` schemas to TiKV exactly once,
/// after PD/TiKV are healthy and before the storage region config is
/// published. Joining nodes do NOT apply the schemas — they assume
/// the cluster already has them.
pub async fn bootstrap(
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

    // ── Step 5: Apply cluster schemas (P2.7, sifrah/nauka#211) ──────
    //
    // Per ADR 0004 (sifrah/nauka#210), the bootstrap node applies the
    // cluster `.surql` schemas exactly once, after PD/TiKV are up and
    // before the storage region config is published. Idempotent thanks
    // to `IF NOT EXISTS` on every `DEFINE`, so a re-run during a retry
    // is safe. The schemas live in nauka-state via `include_str!`, so
    // there is no runtime filesystem dependency on the source tree.
    //
    // The PD endpoint list is built the same way `controlplane::connect`
    // builds it — `pd_endpoints_for` produces the canonical
    // `http://[<ipv6>]:<port>` strings that `EmbeddedDb::open_tikv`
    // expects. On a single-node bootstrap there is exactly one address
    // (the local mesh IPv6), so the slice is a one-element window onto
    // `mesh_ipv6`.
    steps.set("Applying cluster schemas");
    let pd_addresses = std::slice::from_ref(mesh_ipv6);
    let pd_endpoints = nauka_state::pd_endpoints_for(pd_addresses, super::PD_CLIENT_PORT);
    let pd_refs: Vec<&str> = pd_endpoints.iter().map(String::as_str).collect();
    let cluster_db = nauka_state::EmbeddedDb::open_tikv(&pd_refs)
        .await
        .map_err(|e| NaukaError::internal(format!("connect TiKV for schema apply: {e}")))?;
    nauka_state::apply_cluster_schemas(&cluster_db)
        .await
        .map_err(|e| NaukaError::internal(format!("apply cluster schemas: {e}")))?;
    // Drop the EmbeddedDb so the SDK client gets cleaned up before
    // bootstrap returns; the next caller will reconnect via
    // `controlplane::connect()`. The TiKV branch of `shutdown` is a
    // no-op on the local filesystem — the router drains its in-flight
    // gRPC calls as the `Surreal<Db>` is dropped — so any failure here
    // is not actionable for the bootstrap flow and we swallow it.
    let _ = cluster_db.shutdown().await;
    steps.inc();

    Ok(())
}

/// Join an existing TiKV cluster.
///
/// Uses `steps` to report progress (consumes 4 steps).
///
/// Schema handling — per ADR 0004 (sifrah/nauka#210) joining nodes do
/// **not** apply the cluster `.surql` schemas. They assume the
/// bootstrap node (the one that ran `nauka hypervisor init`) already
/// applied them as part of its own bootstrap, and they read/write
/// the existing tables on the shared TiKV cluster through the
/// already-deployed schema. Running DDL from every joining node would
/// be wasteful, racey under partial partitions, and hostile to future
/// data-backfill migrations — see the ADR for the full rationale.
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
    all_peer_infos: &[(&str, Ipv6Addr, u16)],
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

/// Scale PD from N to N+2 atomically.
///
/// Called when the joining node pushes the cluster over an odd-total
/// threshold (3, 5, 7) still within `max_pd_members`. Because we never
/// want an even PD member count, we must promote the joining node **and**
/// all pre-existing TiKV-only peers in a single operation.
///
/// Flow (for the 1→3 case, which is the common one):
/// 1. Identify TiKV-only peers (peers not currently in `existing_pd_endpoints`).
/// 2. For each TiKV-only peer, send a `PromoteToPd` message over the
///    announce protocol. The peer installs its PD unit in --join mode,
///    which registers it with the bootstrap PD via PD's internal join
///    protocol. We poll `existing_pd_endpoints[0]` until the peer appears
///    in the member list, with a hard timeout.
/// 3. Only after every peer has become a PD member do we install and
///    start our own PD. This preserves the invariant that the member
///    count transitions 1 → 2 → 3 as quickly as possible (though we
///    still briefly cross 2 members — unavoidable without a multi-raft
///    joint-consensus primitive, but the scale-up is now driven by us
///    rather than left half-done).
/// 4. Rollback: if our own PD fails, we deregister ourselves from PD.
///    Peers promoted earlier stay as PD members — they're healthy and
///    the cluster is simply at N+1 members instead of N+2. Operators
///    can retry the failed node's join.
fn scale_pd(
    node_name: &str,
    mesh_ipv6: &Ipv6Addr,
    _existing_pd_endpoints: &[String],
    primary_pd: &str,
    all_peer_infos: &[(&str, Ipv6Addr, u16)],
) -> Result<(), NaukaError> {
    wait_mesh_connectivity(primary_pd, 30)?;

    // Query the primary PD for the authoritative current member list.
    // The caller's `existing_pd_endpoints` is built from the local peer
    // list and contains one entry per peer — including TiKV-only nodes
    // whose PD doesn't actually exist. Using that list would make us
    // miss the very peers we need to promote. The PD /members API
    // returns only nodes that are real Raft members.
    let pd_client = super::pd_client::PdClient::new(vec![primary_pd.to_string()]);
    let current_members = pd_client.get_members().map_err(|e| {
        NaukaError::internal(format!("scale_pd: fetch PD members: {e}"))
    })?;

    let existing_pd_ips: std::collections::HashSet<Ipv6Addr> = current_members
        .iter()
        .flat_map(|m| m.peer_urls.iter())
        .filter_map(|url| extract_ipv6_from_endpoint(url))
        .collect();

    // The real PD endpoints (client URLs) that existing members advertise.
    // We hand these to promoted peers so their PromoteToPd request carries
    // an accurate "join against these PDs" list for the TiKV config rewrite.
    let real_pd_endpoints: Vec<String> = current_members
        .iter()
        .flat_map(|m| m.client_urls.iter().cloned())
        .collect();

    let to_promote: Vec<(String, Ipv6Addr, u16)> = all_peer_infos
        .iter()
        .filter(|(_, ip, _)| !existing_pd_ips.contains(ip))
        .map(|(name, ip, port)| ((*name).to_string(), *ip, *port))
        .collect();

    // Build the post-scale PD endpoint list (existing + promoted + self).
    // Used to rewrite the TiKV config on this node so it sees every PD.
    let mut full_pd_endpoints = real_pd_endpoints.clone();
    for (_, ip, _) in &to_promote {
        let ep = format!("http://[{ip}]:{}", super::PD_CLIENT_PORT);
        if !full_pd_endpoints.contains(&ep) {
            full_pd_endpoints.push(ep);
        }
    }
    let self_endpoint = format!("http://[{mesh_ipv6}]:{}", super::PD_CLIENT_PORT);
    if !full_pd_endpoints.contains(&self_endpoint) {
        full_pd_endpoints.push(self_endpoint.clone());
    }

    // Step 1: promote each TiKV-only peer to a full PD member.
    // Sequential on purpose — PD's add-member path is cheap but we want
    // deterministic ordering so rollback reasoning is tractable.
    for (peer_name, peer_ip, peer_wg_port) in &to_promote {
        tracing::info!(
            peer = %peer_name,
            %peer_ip,
            "scale_pd: requesting PD promotion"
        );

        let announce_addr = format!(
            "[{peer_ip}]:{}",
            *peer_wg_port + super::super::fabric::announce::ANNOUNCE_PORT_OFFSET
        );
        let promote_msg = super::super::fabric::peering::PromoteToPd {
            target_name: peer_name.clone(),
            primary_pd_url: primary_pd.to_string(),
            pd_endpoints: real_pd_endpoints.clone(),
            requested_by: node_name.to_string(),
        };

        // We're invoked from an async handler via a sync call chain, so
        // we block on the announce send using the current tokio runtime.
        // block_in_place requires a multi-threaded runtime — the CLI
        // uses `#[tokio::main]` which defaults to that.
        let send_result = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(
                super::super::fabric::announce::send_promote_to_pd(&announce_addr, &promote_msg),
            )
        });

        if let Err(e) = send_result {
            return Err(NaukaError::internal(format!(
                "failed to deliver PromoteToPd to {peer_name}: {e}"
            )));
        }

        // Wait for PD to report the new member. member_exists() does a
        // GET /members against the primary PD.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
        loop {
            if pd_client.member_exists(peer_ip) {
                break;
            }
            if std::time::Instant::now() >= deadline {
                return Err(NaukaError::timeout(
                    &format!("PD promotion of {peer_name}"),
                    120,
                ));
            }
            std::thread::sleep(std::time::Duration::from_secs(2));
        }
        tracing::info!(peer = %peer_name, "scale_pd: peer promoted to PD");
    }

    // Step 2: install and start our own PD (join mode).
    let self_peer_url = format!("http://[{mesh_ipv6}]:{}", super::PD_PEER_PORT);
    let pd_cfg = PdConfig {
        name: node_name.to_string(),
        mesh_ipv6: *mesh_ipv6,
        initial_cluster: format!("{node_name}={self_peer_url}"),
        initial_cluster_state: "join".to_string(),
    };
    let tikv_cfg = TikvConfig {
        mesh_ipv6: *mesh_ipv6,
        pd_endpoints: full_pd_endpoints,
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

/// Promote this node from TiKV-only to full PD+TiKV.
///
/// Invoked by the announce listener when another node (the joining one
/// that's driving a PD scale-up) sends us a `PromoteToPd` message. We
/// rewrite our control-plane configs (PD in --join mode, TiKV with the
/// new full PD endpoint list) and start PD. TiKV is already running and
/// re-discovers the new PD members at the next heartbeat.
pub fn promote_self_to_pd(
    node_name: &str,
    mesh_ipv6: &Ipv6Addr,
    primary_pd: &str,
    existing_pd_endpoints: &[String],
) -> Result<(), NaukaError> {
    let self_peer_url = format!("http://[{mesh_ipv6}]:{}", super::PD_PEER_PORT);
    let pd_cfg = PdConfig {
        name: node_name.to_string(),
        mesh_ipv6: *mesh_ipv6,
        initial_cluster: format!("{node_name}={self_peer_url}"),
        initial_cluster_state: "join".to_string(),
    };

    // TiKV needs the full post-scale PD list. We include existing + self;
    // any other peer being promoted in the same scale-up will appear via
    // PD's own discovery once they join.
    let mut full_pd = existing_pd_endpoints.to_vec();
    let self_client = format!("http://[{mesh_ipv6}]:{}", super::PD_CLIENT_PORT);
    if !full_pd.contains(&self_client) {
        full_pd.push(self_client);
    }
    let tikv_cfg = TikvConfig {
        mesh_ipv6: *mesh_ipv6,
        pd_endpoints: full_pd,
    };

    service::install(&pd_cfg, &tikv_cfg, Some(primary_pd))?;
    service::enable_and_start()?;
    service::wait_pd_ready(mesh_ipv6, 120)?;

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

/// Resolve the set of real, currently-reachable PD endpoints for the TiKV
/// SDK, given a naive per-peer candidate list.
///
/// The caller (post-join storage setup) builds `candidates` by mapping
/// every fabric peer to `http://[<mesh_ipv6>]:2379`. That list contains:
///  - real PD members (good),
///  - synthesized URLs for TiKV-only peers (bad: no PD listening on 2379),
///  - possibly real PDs whose WireGuard peer propagation hasn't caught up
///    on this node yet (typical during rapid joins — the new node
///    received all peers via the join response, but an existing peer may
///    not have received the PeerAnnounce broadcast yet).
///
/// Passing such a list straight to `open_tikv` wastes its 10 s
/// per-endpoint timeout on every bad entry, and in the common case the
/// SDK picks a bad one first and the whole join fails with
/// `open_tikv: handshake to [<ip>]:2379 timed out after 10s`
/// (sifrah/nauka#293).
///
/// This helper solves it in two passes:
///
/// 1. Find any one reachable endpoint in `candidates` (polling with a
///    short interval until `max_wait_secs` elapses). As soon as one
///    responds, query it for the authoritative PD member list via
///    `/pd/api/v1/members`.
/// 2. Wait until every member in that authoritative list is also
///    reachable, up to the remaining deadline. Return whatever subset
///    became reachable — never less than the one seed endpoint, and
///    usually the full set.
///
/// Returns an empty vec only if no endpoint ever answers within the
/// deadline. The caller must handle that as a hard error.
pub fn wait_reachable_pds(candidates: &[String], max_wait_secs: u64) -> Vec<String> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(max_wait_secs);

    // Pass 1: find a single reachable candidate and use it to fetch the
    // real member list. We loop because a freshly-joined node may need
    // a few seconds for WG to carry the first packet to even one peer.
    let mut seed: Option<(String, Vec<String>)> = None;
    while seed.is_none() && std::time::Instant::now() < deadline {
        for url in candidates {
            let client = super::pd_client::PdClient::new(vec![url.clone()]);
            if !client.ping() {
                continue;
            }
            match client.get_members() {
                Ok(members) => {
                    let real: Vec<String> = members
                        .iter()
                        .flat_map(|m| m.client_urls.iter().cloned())
                        .collect();
                    tracing::debug!(
                        seed = %url,
                        members = real.len(),
                        "wait_reachable_pds: fetched authoritative member list"
                    );
                    seed = Some((url.clone(), real));
                    break;
                }
                Err(e) => {
                    tracing::debug!(
                        endpoint = %url,
                        error = %e,
                        "wait_reachable_pds: ping ok but get_members failed, trying next"
                    );
                }
            }
        }
        if seed.is_none() {
            std::thread::sleep(std::time::Duration::from_secs(1));
        }
    }

    let (seed_url, real_endpoints) = match seed {
        Some(s) => s,
        None => {
            tracing::warn!(
                candidates_count = candidates.len(),
                "wait_reachable_pds: no PD endpoint answered within deadline"
            );
            return Vec::new();
        }
    };

    // Pass 2: wait for every real member to be reachable. The seed is
    // already known-good, so we add it straight to the reachable set.
    let mut pending: Vec<String> = real_endpoints
        .iter()
        .filter(|e| *e != &seed_url)
        .cloned()
        .collect();
    let mut reachable: Vec<String> = vec![seed_url];

    while !pending.is_empty() && std::time::Instant::now() < deadline {
        pending.retain(|url: &String| {
            let client = super::pd_client::PdClient::new(vec![url.clone()]);
            if client.ping() {
                reachable.push(url.clone());
                false
            } else {
                true
            }
        });
        if !pending.is_empty() && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_secs(1));
        }
    }

    if !pending.is_empty() {
        tracing::warn!(
            unreachable = ?pending,
            reachable_count = reachable.len(),
            "wait_reachable_pds: deadline reached, proceeding with reachable subset"
        );
    }
    reachable
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
