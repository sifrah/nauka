//! Fabric operations — high-level orchestration.
//!
//! These are the public entry points that the hypervisor handler calls.
//! Each function orchestrates the lower-level modules (mesh, service, state, wg).

use nauka_core::error::NaukaError;
use nauka_core::ui;
use nauka_state::LayerDb;

use super::mesh::{self, HypervisorIdentity, MeshIdentity};
use super::peer::PeerList;
use super::service;
use super::state::FabricState;
use super::wg;

/// Result of a successful fabric init.
pub struct InitResult {
    pub mesh: MeshIdentity,
    pub hypervisor: HypervisorIdentity,
    pub secret_masked: String,
    pub pin: String,
}

/// Configuration for fabric init.
pub struct InitConfig<'a> {
    pub node_name: &'a str,
    pub region: &'a str,
    pub zone: &'a str,
    pub port: u16,
    pub network_mode: super::backend::NetworkMode,
    pub fabric_interface: &'a str,
    pub endpoint: Option<String>,
    pub ipv6_block: Option<String>,
    pub ipv4_public: Option<String>,
}

/// Initialize a new mesh.
///
/// Uses `steps` to report progress (consumes 2 steps).
pub fn init(
    db: &LayerDb,
    cfg: &InitConfig<'_>,
    steps: &ui::Steps,
) -> Result<InitResult, NaukaError> {
    tracing::info!(
        node_name = cfg.node_name, region = cfg.region, zone = cfg.zone,
        port = cfg.port, %cfg.network_mode, fabric_interface = cfg.fabric_interface,
        "fabric init starting"
    );

    // Check not already initialized
    if FabricState::exists(db).map_err(|e| NaukaError::internal(e.to_string()))? {
        return Err(NaukaError::conflict(
            "hypervisor",
            cfg.node_name,
            "already initialized. Run 'nauka hypervisor leave' first.",
        ));
    }

    // Create backend from mode
    steps.set("Installing network");
    let backend = super::backend::create_backend(cfg.network_mode);
    backend.ensure_installed()?;
    steps.inc();

    // Create identities
    steps.set("Creating mesh");
    let (mesh_id, secret) = mesh::create_mesh();
    let hv = mesh::create_hypervisor(&mesh::CreateHypervisorConfig {
        name: cfg.node_name,
        region: cfg.region,
        zone: cfg.zone,
        port: cfg.port,
        endpoint: cfg.endpoint.clone(),
        fabric_interface: cfg.fabric_interface,
        mesh_prefix: &mesh_id.prefix,
        ipv6_block: cfg.ipv6_block.clone(),
        ipv4_public: cfg.ipv4_public.clone(),
    })?;

    // Setup network via backend
    backend.setup(&hv.wg_private_key, cfg.port, &hv.mesh_ipv6, &[])?;

    // Persist state
    let secret_str = secret.to_string();
    let state = FabricState {
        mesh: mesh_id.clone(),
        hypervisor: hv.clone(),
        secret: secret_str.clone(),
        peers: PeerList::new(),
        network_mode: cfg.network_mode,
        node_state: super::state::NodeState::default(),
    };
    state
        .save(db)
        .map_err(|e| NaukaError::internal(e.to_string()))?;
    steps.inc();

    let secret_masked = if secret_str.len() >= 14 {
        format!(
            "{}...{}",
            &secret_str[..10],
            &secret_str[secret_str.len() - 4..]
        )
    } else {
        "***".to_string()
    };

    // Derive a PIN from the secret for peering
    let pin = secret.derive_pin();

    Ok(InitResult {
        mesh: mesh_id,
        hypervisor: hv,
        secret_masked,
        pin,
    })
}

/// Start a peering listener to accept join requests.
///
/// Also starts background tasks for:
/// - Health check loop (monitors peer reachability via WG handshakes)
/// - Announce listener (receives peer announcements from other nodes)
///
/// Blocks until timeout or Ctrl+C. Opens DB per-request (no long-lived lock).
pub async fn listen_for_peers(
    pin: &str,
    peering_port: u16,
    timeout_secs: u64,
) -> Result<usize, NaukaError> {
    let bind_addr = format!("[::]:{peering_port}")
        .parse()
        .map_err(|_| NaukaError::internal("invalid bind address"))?;

    let timeout = std::time::Duration::from_secs(timeout_secs);

    let db_opener = || {
        let dir = nauka_core::process::nauka_dir();
        let _ = std::fs::create_dir_all(&dir);
        nauka_state::LayerDb::open("hypervisor").map_err(|e| NaukaError::internal(e.to_string()))
    };

    // Start health check loop in background
    let health_db_opener = || {
        let dir = nauka_core::process::nauka_dir();
        let _ = std::fs::create_dir_all(&dir);
        nauka_state::LayerDb::open("hypervisor").map_err(|e| NaukaError::internal(e.to_string()))
    };
    tokio::spawn(async move {
        super::health::run_loop(
            health_db_opener,
            super::health::DEFAULT_INTERVAL_SECS,
            super::health::DEFAULT_STALE_THRESHOLD_SECS,
        )
        .await;
    });

    // Start announce listener in background (peering_port + 1 = announce port)
    let announce_port = peering_port + 1;
    let announce_addr: std::net::SocketAddr = format!("[::]:{announce_port}")
        .parse()
        .map_err(|_| NaukaError::internal("invalid announce bind address"))?;
    let announce_db_opener = || {
        let dir = nauka_core::process::nauka_dir();
        let _ = std::fs::create_dir_all(&dir);
        nauka_state::LayerDb::open("hypervisor").map_err(|e| NaukaError::internal(e.to_string()))
    };
    tokio::spawn(async move {
        if let Err(e) = super::announce::listen(announce_db_opener, announce_addr).await {
            tracing::warn!(error = %e, "announce listener stopped");
        }
    });

    // Start periodic mesh reconciliation in background.
    // Every 30s, re-read the full peer list and announce every peer to every
    // other peer. This is idempotent (known peers are skipped) and guarantees
    // full mesh convergence even after concurrent joins.
    let reconcile_wg_port = peering_port - 1; // wg_port = peering_port - 1
    tokio::spawn(async move {
        // Initial delay — let the first joins settle
        tokio::time::sleep(std::time::Duration::from_secs(30)).await;
        loop {
            reconcile_mesh(reconcile_wg_port).await;
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
        }
    });

    // Main peering listener (blocks)
    super::peering_server::listen(db_opener, pin, bind_addr, timeout, 0).await
}

/// Reconciliation interval in seconds (matches the sleep in the spawned task).
const RECONCILE_INTERVAL_SECS: u64 = 30;

/// Periodic mesh reconciliation.
///
/// Only announces peers whose `added_at` falls within the last two
/// reconciliation intervals — recently joined nodes that other peers
/// may not have learned about yet. This keeps the cost proportional
/// to the join rate (O(new × n)) instead of the total mesh size (O(n²)).
///
/// Idempotent — known peers are skipped by the announce handler.
async fn reconcile_mesh(wg_port: u16) {
    let db = {
        let dir = nauka_core::process::nauka_dir();
        let _ = std::fs::create_dir_all(&dir);
        match nauka_state::LayerDb::open("hypervisor") {
            Ok(db) => db,
            Err(_) => return,
        }
    };
    let state = match FabricState::load(&db).ok().flatten() {
        Some(s) => s,
        None => return,
    };

    if state.peers.len() < 2 {
        return; // nothing to reconcile with 0 or 1 peer
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    // Only announce peers added in the last 2 intervals (with margin).
    let recent_cutoff = now.saturating_sub(RECONCILE_INTERVAL_SECS * 2);

    let recent_peers: Vec<_> = state
        .peers
        .peers
        .iter()
        .filter(|p| p.added_at >= recent_cutoff)
        .collect();

    if recent_peers.is_empty() {
        return; // no new peers to announce
    }

    let mut total_sent = 0usize;
    for peer in &recent_peers {
        let info = super::peering::PeerInfo {
            name: peer.name.clone(),
            region: peer.region.clone(),
            zone: peer.zone.clone(),
            wg_public_key: peer.wg_public_key.clone(),
            wg_port: peer.wg_port,
            endpoint: peer.endpoint.clone(),
            mesh_ipv6: peer.mesh_ipv6,
        };
        let targets: Vec<_> = state
            .peers
            .peers
            .iter()
            .filter(|p| p.wg_public_key != peer.wg_public_key)
            .cloned()
            .collect();
        let (ok, _) =
            super::announce::broadcast_new_peer(&info, &state.hypervisor.name, &targets, wg_port)
                .await;
        total_sent += ok;
    }

    if total_sent > 0 {
        tracing::info!(
            announcements = total_sent,
            recent_peers = recent_peers.len(),
            "mesh reconciliation complete"
        );
    }
}

/// Result of a successful fabric join.
pub struct JoinResult {
    pub hypervisor: HypervisorIdentity,
    pub peer_count: usize,
}

/// Configuration for fabric join.
pub struct JoinConfig<'a> {
    pub target: &'a str,
    pub node_name: &'a str,
    pub region: &'a str,
    pub zone: &'a str,
    pub port: u16,
    pub pin: Option<&'a str>,
    pub network_mode: super::backend::NetworkMode,
    pub ipv6_block: Option<String>,
    pub ipv4_public: Option<String>,
}

/// Join an existing cluster.
///
/// Uses `steps` to report progress (consumes 2 steps).
pub async fn join(
    db: &LayerDb,
    cfg: &JoinConfig<'_>,
    steps: &ui::Steps,
) -> Result<JoinResult, NaukaError> {
    // Check not already initialized
    if FabricState::exists(db).map_err(|e| NaukaError::internal(e.to_string()))? {
        return Err(NaukaError::conflict(
            "hypervisor",
            cfg.node_name,
            "already initialized. Run 'nauka hypervisor leave' first.",
        ));
    }

    // Ensure backend is installed
    steps.set("Installing network");
    let backend = super::backend::create_backend(cfg.network_mode);
    backend.ensure_installed()?;
    steps.inc();

    // Build join request and connect
    steps.set("Joining mesh");
    // We need a temporary keypair to send in the request
    let (wg_private, wg_public) = nauka_core::crypto::generate_wg_keypair();

    let trace_id = nauka_core::logging::generate_trace_id();
    tracing::info!(trace_id = %trace_id, target = cfg.target, "sending join request");

    let request = super::peering::JoinRequest {
        name: cfg.node_name.to_string(),
        region: cfg.region.to_string(),
        zone: cfg.zone.to_string(),
        wg_public_key: wg_public.clone(),
        wg_port: cfg.port,
        endpoint: None, // will be discovered by the target
        pin: cfg.pin.map(|s| s.to_string()),
        trace_id: Some(trace_id),
    };

    // TCP peering exchange
    let response = super::peering_client::join(cfg.target, request).await?;

    // Extract mesh info from response
    let secret_str = response
        .secret
        .ok_or_else(|| NaukaError::internal("join response missing secret"))?;
    let prefix = response
        .prefix
        .ok_or_else(|| NaukaError::internal("join response missing prefix"))?;

    // Derive our mesh IPv6 from the prefix + our public key
    use base64::Engine as _;
    let pub_bytes = base64::engine::general_purpose::STANDARD
        .decode(&wg_public)
        .map_err(|e| NaukaError::internal(format!("invalid WireGuard key: {e}")))?;
    let mesh_ipv6 = nauka_core::addressing::derive_node_address(&prefix, &pub_bytes);

    // Validate our identity
    nauka_core::validate::name(cfg.node_name)?;
    nauka_core::validate::region(cfg.region)?;
    nauka_core::validate::zone(cfg.zone)?;
    nauka_core::validate::port(cfg.port)?;

    // Build hypervisor identity
    let runtime = if std::path::Path::new("/dev/kvm").exists() {
        "kvm".to_string()
    } else {
        "container".to_string()
    };

    let hv = HypervisorIdentity {
        id: nauka_core::id::HypervisorId::generate(),
        name: cfg.node_name.to_string(),
        region: cfg.region.to_string(),
        zone: cfg.zone.to_string(),
        wg_private_key: wg_private.clone(),
        wg_public_key: wg_public,
        wg_port: cfg.port,
        endpoint: None,
        fabric_interface: String::new(),
        mesh_ipv6,
        runtime,
        ipv6_block: cfg.ipv6_block.clone(),
        ipv4_public: cfg.ipv4_public.clone(),
    };

    // Use mesh ID from the accepting node (consistent across cluster)
    let mesh_id_str = response
        .mesh_id
        .unwrap_or_else(|| nauka_core::id::MeshId::generate().to_string());
    let mesh_id = MeshIdentity {
        id: mesh_id_str
            .parse()
            .unwrap_or_else(|_| nauka_core::id::MeshId::generate()),
        prefix,
    };

    // Extract target IP for endpoint discovery
    let target_ip = cfg
        .target
        .split(':')
        .next()
        .unwrap_or(cfg.target)
        .to_string();

    // Build peer list from response (acceptor + existing peers)
    let mut peers = PeerList::new();
    if let Some(acceptor) = &response.acceptor {
        // If acceptor didn't set endpoint, use target IP + acceptor's WG port
        let endpoint = acceptor
            .endpoint
            .clone()
            .or_else(|| Some(format!("{}:{}", target_ip, acceptor.wg_port)));
        let _ = peers.add(super::peer::Peer::new(
            acceptor.name.clone(),
            acceptor.region.clone(),
            acceptor.zone.clone(),
            acceptor.wg_public_key.clone(),
            acceptor.wg_port,
            endpoint,
            acceptor.mesh_ipv6,
        ));
    }
    for p in &response.peers {
        let _ = peers.add(super::peer::Peer::new(
            p.name.clone(),
            p.region.clone(),
            p.zone.clone(),
            p.wg_public_key.clone(),
            p.wg_port,
            p.endpoint.clone(),
            p.mesh_ipv6,
        ));
    }

    let peer_count = peers.len();

    // Build backend peer configs
    let backend_peers: Vec<super::backend::BackendPeer> = peers
        .peers
        .iter()
        .map(|p| super::backend::BackendPeer {
            public_key: p.wg_public_key.clone(),
            endpoint: p.endpoint.clone(),
            mesh_ipv6: p.mesh_ipv6,
            keepalive_secs: 25,
        })
        .collect();

    // Setup network via backend (install + start)
    backend.setup(&wg_private, cfg.port, &mesh_ipv6, &backend_peers)?;

    // Persist state
    let state = FabricState {
        mesh: mesh_id,
        hypervisor: hv.clone(),
        secret: secret_str,
        peers,
        network_mode: cfg.network_mode,
        node_state: super::state::NodeState::default(),
    };
    state
        .save(db)
        .map_err(|e| NaukaError::internal(e.to_string()))?;
    steps.inc();

    Ok(JoinResult {
        hypervisor: hv,
        peer_count,
    })
}

/// Get the current fabric status.
pub struct StatusResult {
    pub hypervisor_name: String,
    pub hypervisor_id: String,

    pub region: String,
    pub zone: String,
    pub mesh_ipv6: String,
    pub state: String,
    pub service_installed: bool,
    pub service_active: bool,
    pub wg_interface_up: bool,
    pub peer_count: usize,
    pub active_peers: usize,
    pub wg_port: u16,
    pub wg_peer_count: usize,
    pub rx_bytes: u64,
    pub tx_bytes: u64,
}

pub fn status(db: &LayerDb) -> Result<StatusResult, NaukaError> {
    let state = FabricState::load(db)
        .map_err(|e| NaukaError::internal(e.to_string()))?
        .ok_or_else(|| {
            NaukaError::precondition("not initialized. Run 'nauka hypervisor init' first.")
        })?;

    let svc_installed = service::is_installed();
    let svc_active = service::is_active();
    let wg_up = wg::interface_exists();

    let fabric_state = if !svc_active || !wg_up {
        if svc_installed {
            "stopped"
        } else {
            "not installed"
        }
    } else if state.node_state == super::state::NodeState::Draining {
        "draining"
    } else {
        "available"
    };

    let (wg_port, wg_peer_count, rx, tx) = if wg_up {
        match wg::get_status() {
            Ok(s) => (s.listen_port, s.peer_count, s.rx_bytes, s.tx_bytes),
            Err(_) => (0, 0, 0, 0),
        }
    } else {
        (0, 0, 0, 0)
    };

    Ok(StatusResult {
        hypervisor_name: state.hypervisor.name,
        hypervisor_id: state.hypervisor.id.to_string(),
        region: state.hypervisor.region,
        zone: state.hypervisor.zone,
        mesh_ipv6: state.hypervisor.mesh_ipv6.to_string(),
        state: fabric_state.to_string(),
        service_installed: svc_installed,
        service_active: svc_active,
        wg_interface_up: wg_up,
        peer_count: state.peers.len(),
        active_peers: state.peers.active_count(),
        wg_port,
        wg_peer_count,
        rx_bytes: rx,
        tx_bytes: tx,
    })
}

/// Start the fabric network service.
pub fn start(db: &LayerDb) -> Result<(), NaukaError> {
    let state = FabricState::load(db)
        .map_err(|e| NaukaError::internal(e.to_string()))?
        .ok_or_else(|| {
            NaukaError::precondition("not initialized. Run 'nauka hypervisor init' first.")
        })?;

    let backend = super::backend::create_backend(state.network_mode);

    // If the interface is missing (e.g., `ip link del nauka0`), or exists
    // but lost its IPv6 address (e.g., `ip link set nauka0 down && up`),
    // do a full service restart to restore the complete mesh config.
    let needs_restart = if !backend.is_up() {
        tracing::info!("interface missing — recreating from saved state");
        true
    } else if !has_mesh_ipv6(&state.hypervisor.mesh_ipv6) {
        tracing::info!("mesh IPv6 address missing — restarting service");
        true
    } else {
        false
    };

    if needs_restart {
        let _ = backend.stop();
        return backend.start();
    }

    if backend.is_active() {
        return Ok(()); // already running, idempotent
    }
    backend.start()
}

/// Check if the expected mesh IPv6 address is assigned to nauka0.
fn has_mesh_ipv6(expected: &std::net::Ipv6Addr) -> bool {
    let output = match std::process::Command::new("ip")
        .args(["-6", "addr", "show", "dev", "nauka0"])
        .output()
    {
        Ok(o) => o,
        Err(_) => return false,
    };
    let stdout = String::from_utf8_lossy(&output.stdout);
    stdout.contains(&expected.to_string())
}

/// Stop the fabric network service.
pub fn stop(db: &LayerDb) -> Result<(), NaukaError> {
    let state = match FabricState::load(db).map_err(|e| NaukaError::internal(e.to_string()))? {
        Some(s) => s,
        None => return Ok(()), // not initialized, nothing to stop
    };
    let backend = super::backend::create_backend(state.network_mode);
    if !backend.is_active() {
        return Ok(());
    }
    backend.stop()
}

/// Configuration for updating hypervisor fields.
pub struct UpdateConfig {
    pub ipv6_block: Option<String>,
    pub ipv4_public: Option<String>,
    pub name: Option<String>,
}

/// Update mutable hypervisor fields on a live node.
pub fn update(db: &LayerDb, cfg: &UpdateConfig) -> Result<HypervisorIdentity, NaukaError> {
    let mut state = FabricState::load(db)
        .map_err(|e| NaukaError::internal(e.to_string()))?
        .ok_or_else(|| {
            NaukaError::precondition("not initialized. Run 'nauka hypervisor init' first.")
        })?;

    let mut changed = false;

    if let Some(ref block) = cfg.ipv6_block {
        if !block.contains('/') {
            return Err(NaukaError::validation(
                "ipv6-block must be a CIDR (e.g., 2a01:4f8:c012:abcd::/64)",
            ));
        }
        let prefix = block.split('/').next().unwrap_or_default();
        if prefix.parse::<std::net::Ipv6Addr>().is_err() {
            return Err(NaukaError::validation("ipv6-block: invalid IPv6 address"));
        }
        state.hypervisor.ipv6_block = Some(block.clone());
        changed = true;
    }

    if let Some(ref ip) = cfg.ipv4_public {
        if ip.parse::<std::net::Ipv4Addr>().is_err() {
            return Err(NaukaError::validation("ipv4-public: invalid IPv4 address"));
        }
        state.hypervisor.ipv4_public = Some(ip.clone());
        changed = true;
    }

    if let Some(ref name) = cfg.name {
        nauka_core::validate::name(name)?;
        state.hypervisor.name = name.clone();
        changed = true;
    }

    if !changed {
        return Err(NaukaError::validation(
            "no fields to update. Pass --ipv6-block, --ipv4-public, or --name",
        ));
    }

    state
        .save(db)
        .map_err(|e| NaukaError::internal(e.to_string()))?;

    tracing::info!(node = state.hypervisor.name.as_str(), "hypervisor updated");

    Ok(state.hypervisor)
}

/// Set node scheduling state to draining (maintenance mode).
/// Persists the state and broadcasts the change to all peers.
pub async fn drain(db: &LayerDb) -> Result<(), NaukaError> {
    let mut state = FabricState::load(db)
        .map_err(|e| NaukaError::internal(e.to_string()))?
        .ok_or_else(|| {
            NaukaError::precondition("not initialized. Run 'nauka hypervisor init' first.")
        })?;

    if state.node_state == super::state::NodeState::Draining {
        return Err(NaukaError::conflict(
            "hypervisor",
            &state.hypervisor.name,
            "already draining",
        ));
    }

    state.node_state = super::state::NodeState::Draining;
    state
        .save(db)
        .map_err(|e| NaukaError::internal(e.to_string()))?;

    tracing::info!(
        node = state.hypervisor.name.as_str(),
        "node set to draining"
    );

    // Broadcast state change to peers (best-effort)
    if !state.peers.is_empty() {
        let change = super::peering::StateChange {
            name: state.hypervisor.name.clone(),
            wg_public_key: state.hypervisor.wg_public_key.clone(),
            node_state: super::state::NodeState::Draining,
        };
        let peers: Vec<_> = state.peers.peers.clone();
        let (ok, fail) = super::announce::broadcast_state_change(&change, &peers).await;
        tracing::info!(successes = ok, failures = fail, "drain broadcast complete");
    }

    Ok(())
}

/// Set node scheduling state back to available (exit maintenance mode).
/// Persists the state and broadcasts the change to all peers.
pub async fn enable(db: &LayerDb) -> Result<(), NaukaError> {
    let mut state = FabricState::load(db)
        .map_err(|e| NaukaError::internal(e.to_string()))?
        .ok_or_else(|| {
            NaukaError::precondition("not initialized. Run 'nauka hypervisor init' first.")
        })?;

    if state.node_state == super::state::NodeState::Available {
        return Err(NaukaError::conflict(
            "hypervisor",
            &state.hypervisor.name,
            "already available",
        ));
    }

    state.node_state = super::state::NodeState::Available;
    state
        .save(db)
        .map_err(|e| NaukaError::internal(e.to_string()))?;

    tracing::info!(
        node = state.hypervisor.name.as_str(),
        "node set to available"
    );

    // Broadcast state change to peers (best-effort)
    if !state.peers.is_empty() {
        let change = super::peering::StateChange {
            name: state.hypervisor.name.clone(),
            wg_public_key: state.hypervisor.wg_public_key.clone(),
            node_state: super::state::NodeState::Available,
        };
        let peers: Vec<_> = state.peers.peers.clone();
        let (ok, fail) = super::announce::broadcast_state_change(&change, &peers).await;
        tracing::info!(successes = ok, failures = fail, "enable broadcast complete");
    }

    Ok(())
}

/// Number of removal broadcast attempts before giving up.
const LEAVE_BROADCAST_ATTEMPTS: usize = 4;
/// Delay between removal broadcast retries.
const LEAVE_RETRY_DELAY: std::time::Duration = std::time::Duration::from_secs(3);

/// Leave the cluster — notify peers, uninstall service, remove state.
pub async fn leave(db: &LayerDb) -> Result<(), NaukaError> {
    // Notify peers before tearing down (best-effort, over mesh).
    // Retry once so transient failures don't leave peers thinking we're still around.
    if let Some(state) = FabricState::load(db).ok().flatten() {
        if !state.peers.is_empty() {
            let peers: Vec<_> = state.peers.peers.clone();
            let remaining = peers.clone();

            for attempt in 1..=LEAVE_BROADCAST_ATTEMPTS {
                let (ok, fail) = super::announce::broadcast_peer_remove(
                    &state.hypervisor.name,
                    &state.hypervisor.wg_public_key,
                    &remaining,
                    state.hypervisor.wg_port,
                )
                .await;
                tracing::info!(
                    attempt,
                    successes = ok,
                    failures = fail,
                    "peer removal broadcast"
                );

                if fail == 0 || attempt == LEAVE_BROADCAST_ATTEMPTS {
                    break;
                }

                // Retry only the peers that failed
                // broadcast_peer_remove doesn't return which failed, so wait and retry all remaining
                tokio::time::sleep(LEAVE_RETRY_DELAY).await;
            }
        }

        let backend = super::backend::create_backend(state.network_mode);
        let _ = backend.teardown();
    } else {
        // Fallback: try WG teardown anyway
        let _ = service::uninstall();
    }

    // Delete state
    FabricState::delete(db).map_err(|e| NaukaError::internal(e.to_string()))?;

    Ok(())
}
