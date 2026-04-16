//! Split between one-shot setup (`init_hypervisor`, `join_hypervisor`,
//! `leave_hypervisor`) and the long-running service loop (`run_daemon`).
//!
//! The CLI calls the setup functions, which return quickly after
//! persisting state. The systemd unit runs `run_daemon` which loads that
//! state and stays up.

use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use nauka_state::{node_id_from_key, Database, RaftNode, TlsConfig};
use serde::Deserialize;
use surrealdb::types::SurrealValue;
use tokio::signal::unix::{signal, SignalKind};

use crate::mesh::{
    certs, generate_pin, join_mesh, mesh_listener, whoami, Mesh, MeshError, MeshId, MeshState,
    PeerInfo, DEFAULT_JOIN_PORT,
};
use crate::mesh::reconciler;

/// Read the snapshot threshold from `NAUKA_SNAPSHOT_THRESHOLD` if set,
/// otherwise fall back to the production default. Lets ops tune (or CI
/// tests force) how often Raft snapshots fire.
fn snapshot_threshold() -> u64 {
    std::env::var("NAUKA_SNAPSHOT_THRESHOLD")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(nauka_state::raft::SNAPSHOT_THRESHOLD)
}

#[derive(Deserialize, SurrealValue)]
struct EndpointRow {
    endpoint: Option<String>,
}

#[derive(Clone)]
pub struct SetupConfig {
    pub interface_name: String,
    pub listen_port: u16,
    pub join_port: u16,
}

impl Default for SetupConfig {
    fn default() -> Self {
        Self {
            interface_name: "nauka0".into(),
            listen_port: 51820,
            join_port: DEFAULT_JOIN_PORT,
        }
    }
}

pub struct InitSummary {
    pub mesh_id: MeshId,
    pub public_key: String,
    pub address: String,
    pub pin: String,
    pub raft_addr: String,
}

pub struct JoinSummary {
    pub mesh_id: MeshId,
    pub public_key: String,
    pub address: String,
    pub raft_addr: String,
}

fn build_tls(state: &MeshState) -> Option<TlsConfig> {
    let (ca, cert, key) = match (&state.ca_cert, &state.tls_cert, &state.tls_key) {
        (Some(ca), Some(cert), Some(key)) => (ca, cert, key),
        _ => return None,
    };
    match TlsConfig::new(ca, cert, key) {
        Ok(tls) => Some(tls),
        Err(e) => {
            eprintln!("  ! tls config failed: {e}");
            None
        }
    }
}

/// One-shot: generate a fresh mesh, persist state + CA + PIN, initialize
/// the Raft cluster as a single-node voter, register self in the
/// hypervisor table. Returns summary for the CLI to print.
///
/// Does NOT start the peering listener / reconciler / refresh task — those
/// run in `run_daemon` under systemd.
pub async fn init_hypervisor(
    db: Arc<Database>,
    config: SetupConfig,
) -> Result<InitSummary, MeshError> {
    let mut mesh = Mesh::new(
        config.interface_name.clone(),
        config.listen_port,
        None,
        None,
        None,
    )?;
    mesh.up()?;

    let (ca_cert, ca_key) = certs::generate_ca()?;
    let (tls_cert, tls_key) = certs::sign_node_cert(&ca_cert, &ca_key)?;
    let pin = generate_pin();

    let mut state = mesh.to_state();
    state.ca_cert = Some(ca_cert);
    state.ca_key = Some(ca_key);
    state.tls_cert = Some(tls_cert);
    state.tls_key = Some(tls_key);
    state.peering_pin = Some(pin.clone());
    state.save(&db).await?;

    let tls = build_tls(&state);
    let node_id = node_id_from_key(mesh.public_key());
    let raft_addr = format!("[{}]:4001", mesh.address().address);
    let own_pk = mesh.public_key().to_string();

    // Init the cluster as a single-node voter. No Raft server needed — with
    // one voter, client_write commits locally; the daemon brings the server
    // up later.
    let raft = RaftNode::new_with_snapshot_threshold(
        node_id,
        db.clone(),
        tls,
        snapshot_threshold(),
    )
    .await
    .map_err(|e| MeshError::State(e.to_string()))?;

    // Start the Raft server before initializing the cluster. Even though a
    // single-node cluster doesn't use RPC, openraft's internal task setup
    // appears to need the server task alive for vote persistence to
    // complete cleanly.
    let server_handle = raft.start_server(raft_addr.clone()).await;

    raft.init_cluster(&raft_addr)
        .await
        .map_err(|e| MeshError::State(e.to_string()))?;

    // Let the initial vote + blank entry fully apply before we write.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let surql = format!(
        "CREATE hypervisor SET \
         public_key = '{own_pk}', node_id = {node_id}, address = '{addr}', \
         endpoint = NONE, allowed_ips = ['{addr}'], keepalive = 25, \
         raft_addr = '{raft_addr}'",
        node_id = node_id as i64,
        addr = mesh.address(),
    );
    raft.write(surql)
        .await
        .map_err(|e| MeshError::State(format!("register self: {e}")))?;

    // Tear the in-process Raft down cleanly so the systemd-launched daemon
    // can acquire the SurrealKV LOCK file — otherwise it crash-loops on
    // "Database is already locked by another process".
    server_handle.abort();
    raft.raft
        .shutdown()
        .await
        .map_err(|e| MeshError::State(format!("raft shutdown: {e}")))?;
    drop(raft);

    Ok(InitSummary {
        mesh_id: mesh.mesh_id().clone(),
        public_key: own_pk,
        address: mesh.address().to_string(),
        pin,
        raft_addr,
    })
}

/// One-shot: contact an existing node, receive the mesh config + TLS cert,
/// persist state. Does NOT start the daemon — leave that to systemd.
pub async fn join_hypervisor(
    db: Arc<Database>,
    host: &str,
    pin: &str,
    config: SetupConfig,
) -> Result<JoinSummary, MeshError> {
    let (mesh, bootstrap_peers, tls_certs) = join_mesh(
        host,
        pin,
        config.interface_name.clone(),
        config.listen_port,
        config.join_port,
    )?;

    let mut state = mesh.to_state();
    if let Some(ref certs) = tls_certs {
        state.ca_cert = Some(certs.ca_cert.clone());
        state.tls_cert = Some(certs.tls_cert.clone());
        state.tls_key = Some(certs.tls_key.clone());
    }
    state.peering_pin = None; // joiners do not accept further joins (v1)
    state.save(&db).await?;

    write_bootstrap_peers(&db, &bootstrap_peers).await;

    let raft_addr = format!("[{}]:4001", mesh.address().address);
    Ok(JoinSummary {
        mesh_id: mesh.mesh_id().clone(),
        public_key: mesh.public_key().to_string(),
        address: mesh.address().to_string(),
        raft_addr,
    })
}

async fn write_bootstrap_peers(db: &Database, peers: &[PeerInfo]) {
    for p in peers {
        let canonical_pk = match defguard_wireguard_rs::key::Key::from_str(&p.public_key) {
            Ok(k) => k.to_string(),
            Err(_) => {
                eprintln!("  ! bootstrap peer: invalid public_key, skipping");
                continue;
            }
        };
        let endpoint: SocketAddr = match p.endpoint.parse() {
            Ok(a) => a,
            Err(_) => {
                eprintln!("  ! bootstrap peer {canonical_pk}: invalid endpoint, skipping");
                continue;
            }
        };
        let mesh_addr_mask: defguard_wireguard_rs::net::IpAddrMask = match p.mesh_address.parse() {
            Ok(m) => m,
            Err(_) => {
                eprintln!("  ! bootstrap peer {canonical_pk}: invalid mesh_address, skipping");
                continue;
            }
        };
        let surql = format!(
            "CREATE hypervisor SET \
             public_key = '{canonical_pk}', node_id = {node_id}, address = '{mesh_addr_mask}', \
             endpoint = '{endpoint}', allowed_ips = ['{mesh_addr_mask}'], keepalive = 25, \
             raft_addr = '[{ip}]:4001'",
            node_id = node_id_from_key(&canonical_pk) as i64,
            ip = mesh_addr_mask.address,
        );
        let _ = db.query(&surql).await;
    }
}

/// Teardown helper called after the daemon is already stopped. Opens the
/// DB (which is unlocked now), wipes local state, removes the WG
/// interface, and removes the systemd unit file. The CLI is responsible
/// for the IPC leave notification and the systemctl stop that precede
/// this call.
pub async fn leave_hypervisor(interface_name: &str) -> Result<(), MeshError> {
    let db = Arc::new(
        Database::open(None)
            .await
            .map_err(|e| MeshError::State(e.to_string()))?,
    );
    let _ = MeshState::delete(&db).await;
    drop(db);

    let _ = Mesh::down_interface(interface_name);
    crate::systemd::remove_unit_file()?;
    println!("hypervisor left mesh — systemd unit removed, local state wiped");
    Ok(())
}

/// Long-running service entrypoint — executed by systemd.
pub async fn run_daemon(db: Arc<Database>) -> Result<(), MeshError> {
    let state = MeshState::load(&db).await?;
    let mut mesh = Mesh::from_state(&state)?;
    mesh.up()?; // idempotent — if the interface already exists, configure it

    let tls = build_tls(&state);
    let own_pk = mesh.public_key().to_string();
    let node_id = node_id_from_key(&own_pk);
    let raft_addr = format!("[{}]:4001", mesh.address().address);

    println!("nauka hypervisor daemon");
    println!("  interface:  {}", mesh.interface_name());
    println!("  mesh:       {}", mesh.mesh_id());
    println!("  address:    {}", mesh.address());
    println!("  public key: {}", mesh.public_key());
    println!("  port:       {}", mesh.listen_port());
    println!("  raft:       {raft_addr}");
    println!("  tls:        {}", if tls.is_some() { "enabled" } else { "disabled" });
    println!(
        "  peering:    {}",
        if state.peering_pin.is_some() { "accepting joins" } else { "closed" }
    );

    let raft_node = RaftNode::new_with_snapshot_threshold(
        node_id,
        db.clone(),
        tls,
        snapshot_threshold(),
    )
    .await
    .map_err(|e| MeshError::State(e.to_string()))?;
    let _raft_server = raft_node.start_server(raft_addr).await;
    let raft = Arc::new(raft_node);

    let db2 = db.clone();
    let iface = state.interface_name.clone();
    let own_pk2 = own_pk.clone();

    let reconciler_handle = tokio::spawn(async move {
        reconciler::run(&db2, &iface, &own_pk2).await;
    });

    let listener_handle = tokio::spawn(mesh_listener(
        Some(raft.clone()),
        db.clone(),
        mesh.mesh_id().clone(),
        mesh.keypair().clone(),
        mesh.address().to_string(),
        state.interface_name.clone(),
        state.listen_port,
        state.peering_pin.clone(),
        DEFAULT_JOIN_PORT,
        state.ca_cert.clone(),
        state.ca_key.clone(),
    ));

    let refresh_handle = tokio::spawn(refresh_own_endpoint(
        db.clone(),
        raft.clone(),
        own_pk.clone(),
        state.listen_port,
    ));

    // systemd sends SIGTERM on `systemctl stop`; Ctrl+C is SIGINT in
    // foreground mode. Handle either.
    let mut sigint =
        signal(SignalKind::interrupt()).map_err(|e| MeshError::State(e.to_string()))?;
    let mut sigterm =
        signal(SignalKind::terminate()).map_err(|e| MeshError::State(e.to_string()))?;
    tokio::select! {
        _ = sigint.recv() => eprintln!("\n  received SIGINT"),
        _ = sigterm.recv() => eprintln!("\n  received SIGTERM"),
    }

    listener_handle.abort();
    reconciler_handle.abort();
    refresh_handle.abort();
    mesh.down()?;
    println!("daemon stopped (state preserved)");
    Ok(())
}

/// Discover our public endpoint via `whoami` to a peer, then, if it differs
/// from what's currently in the hypervisor table, UPDATE via Raft so every
/// node's reconciler can refresh its WG peer endpoint for us.
async fn refresh_own_endpoint(
    db: Arc<Database>,
    raft: Arc<RaftNode>,
    own_pk: String,
    listen_port: u16,
) {
    tokio::time::sleep(Duration::from_secs(3)).await;

    let peer_ip = match pick_peer_ip(&db, &own_pk).await {
        Ok(Some(ip)) => ip,
        Ok(None) => {
            eprintln!("  endpoint refresh: no peer with endpoint yet — skipping");
            return;
        }
        Err(e) => {
            eprintln!("  endpoint refresh: pick peer failed: {e}");
            return;
        }
    };

    let observed_ip = match whoami(&peer_ip, DEFAULT_JOIN_PORT).await {
        Ok(ip) => ip,
        Err(e) => {
            eprintln!("  endpoint refresh: whoami {peer_ip} failed: {e}");
            return;
        }
    };
    let new_endpoint = SocketAddr::new(observed_ip, listen_port).to_string();

    let current_ep = match read_own_endpoint(&db, &own_pk).await {
        Ok(ep) => ep,
        Err(e) => {
            eprintln!("  endpoint refresh: read own endpoint failed: {e}");
            return;
        }
    };
    if current_ep.as_deref() == Some(new_endpoint.as_str()) {
        println!("  endpoint refresh: {new_endpoint} (unchanged)");
        return;
    }

    println!(
        "  endpoint refresh: {:?} -> {new_endpoint}",
        current_ep.as_deref().unwrap_or("NONE")
    );
    let surql = format!(
        "UPDATE hypervisor SET endpoint = '{new_endpoint}' WHERE public_key = '{own_pk}'"
    );
    match raft.write(surql).await {
        Ok(_) => println!("  endpoint refresh: propagated via Raft"),
        Err(e) => eprintln!("  endpoint refresh: raft write failed: {e}"),
    }
}

async fn pick_peer_ip(db: &Database, own_pk: &str) -> Result<Option<String>, MeshError> {
    #[derive(Deserialize, SurrealValue)]
    struct PeerRow {
        public_key: String,
        endpoint: Option<String>,
    }
    let peers: Vec<PeerRow> = db
        .query_take("SELECT public_key, endpoint FROM hypervisor")
        .await
        .map_err(|e| MeshError::State(e.to_string()))?;
    for p in peers {
        if p.public_key == own_pk {
            continue;
        }
        if let Some(ep) = p.endpoint {
            if let Ok(sa) = ep.parse::<SocketAddr>() {
                return Ok(Some(sa.ip().to_string()));
            }
        }
    }
    Ok(None)
}

#[derive(Deserialize, SurrealValue)]
pub struct HypervisorSummary {
    pub public_key: String,
    pub address: String,
    pub endpoint: Option<String>,
}

pub async fn list_hypervisors(db: &Database) -> Result<Vec<HypervisorSummary>, MeshError> {
    db.query_take(
        "SELECT public_key, address, endpoint FROM hypervisor ORDER BY public_key",
    )
    .await
    .map_err(|e| MeshError::State(e.to_string()))
}

async fn read_own_endpoint(db: &Database, own_pk: &str) -> Result<Option<String>, MeshError> {
    let rows: Vec<EndpointRow> = db
        .query_take(&format!(
            "SELECT endpoint FROM hypervisor WHERE public_key = '{own_pk}'"
        ))
        .await
        .map_err(|e| MeshError::State(e.to_string()))?;
    Ok(rows.into_iter().next().and_then(|r| r.endpoint))
}
