//! Fabric operations — high-level orchestration.
//!
//! These are the public entry points that the hypervisor handler calls.
//! Each function orchestrates the lower-level modules (mesh, service, state, wg).

use nauka_core::error::NaukaError;
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
}

pub fn init(db: &LayerDb, cfg: &InitConfig<'_>) -> Result<InitResult, NaukaError> {
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
    let backend = super::backend::create_backend(cfg.network_mode);
    backend.ensure_installed()?;

    // Create identities
    let (mesh_id, secret) = mesh::create_mesh();
    let hv = mesh::create_hypervisor(
        cfg.node_name,
        cfg.region,
        cfg.zone,
        cfg.port,
        cfg.endpoint.clone(),
        cfg.fabric_interface,
        &mesh_id.prefix,
    )?;

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
    };
    state
        .save(db)
        .map_err(|e| NaukaError::internal(e.to_string()))?;

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
    let bind_addr = format!("0.0.0.0:{peering_port}")
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
    let announce_addr: std::net::SocketAddr = format!("0.0.0.0:{announce_port}")
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

    // Main peering listener (blocks)
    super::peering_server::listen(db_opener, pin, bind_addr, timeout, 0).await
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
}

/// Join an existing cluster.
///
/// 1. TCP connect to target → peering exchange
/// 2. Receive mesh secret + peer list
/// 3. Create hypervisor identity from received mesh prefix
/// 4. Setup network via backend
/// 5. Persist state
pub async fn join(db: &LayerDb, cfg: &JoinConfig<'_>) -> Result<JoinResult, NaukaError> {
    // Check not already initialized
    if FabricState::exists(db).map_err(|e| NaukaError::internal(e.to_string()))? {
        return Err(NaukaError::conflict(
            "hypervisor",
            cfg.node_name,
            "already initialized. Run 'nauka hypervisor leave' first.",
        ));
    }

    // Ensure backend is installed
    let backend = super::backend::create_backend(cfg.network_mode);
    backend.ensure_installed()?;

    // Build join request
    // We need a temporary keypair to send in the request
    let (wg_private, wg_public) = nauka_core::crypto::generate_wg_keypair();

    let request = super::peering::JoinRequest {
        name: cfg.node_name.to_string(),
        region: cfg.region.to_string(),
        zone: cfg.zone.to_string(),
        wg_public_key: wg_public.clone(),
        wg_port: cfg.port,
        endpoint: None, // will be discovered by the target
        pin: cfg.pin.map(|s| s.to_string()),
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
    let target_ip = cfg.target.split(':').next().unwrap_or(cfg.target).to_string();

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
    };
    state
        .save(db)
        .map_err(|e| NaukaError::internal(e.to_string()))?;

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

    let fabric_state = if svc_active && wg_up {
        "available"
    } else if svc_installed {
        "stopped"
    } else {
        "not installed"
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
    if backend.is_active() {
        return Ok(()); // already running, idempotent
    }
    backend.start()
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

/// Leave the cluster — uninstall service, remove state.
pub fn leave(db: &LayerDb) -> Result<(), NaukaError> {
    // Get backend from state (if available)
    if let Some(state) = FabricState::load(db).ok().flatten() {
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
