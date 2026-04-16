use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use nauka_state::{node_id_from_key, Database, RaftNode, TlsConfig};
use serde::Deserialize;
use surrealdb::types::SurrealValue;
use tokio::signal;

use crate::mesh::{
    certs, generate_pin, join_mesh, mesh_listener, whoami, KeyPair, Mesh, MeshError, MeshId,
    MeshState, DEFAULT_JOIN_PORT,
};

use crate::mesh::reconciler;

#[derive(Deserialize, SurrealValue)]
struct EndpointRow {
    endpoint: Option<String>,
}

pub struct DaemonConfig {
    pub interface_name: String,
    pub listen_port: u16,
    pub mesh_id: Option<MeshId>,
    pub keypair: Option<KeyPair>,
    pub peering_pin: Option<String>,
    pub join_port: u16,
}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self {
            interface_name: "nauka0".into(),
            listen_port: 51820,
            mesh_id: None,
            keypair: None,
            peering_pin: None,
            join_port: DEFAULT_JOIN_PORT,
        }
    }
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

pub async fn run_daemon(db: Arc<Database>, config: DaemonConfig) -> Result<(), MeshError> {
    let mut mesh = Mesh::new(
        config.interface_name.clone(),
        config.listen_port,
        config.mesh_id,
        config.keypair,
        None,
    )?;
    mesh.up()?;

    // Generate mesh CA + node TLS cert
    let (ca_cert, ca_key) = certs::generate_ca()?;
    let (tls_cert, tls_key) = certs::sign_node_cert(&ca_cert, &ca_key)?;

    let mut state = mesh.to_state();
    state.ca_cert = Some(ca_cert.clone());
    state.ca_key = Some(ca_key.clone());
    state.tls_cert = Some(tls_cert.clone());
    state.tls_key = Some(tls_key.clone());
    state.save(&db).await?;

    let tls = build_tls(&state);

    let pin = config.peering_pin.unwrap_or_else(generate_pin);
    let node_id = node_id_from_key(mesh.public_key());
    let raft_addr = format!("[{}]:4001", mesh.address().address);
    let own_pk = mesh.public_key().to_string();

    println!("nauka daemon");
    println!("  interface:  {}", mesh.interface_name());
    println!("  mesh:       {}", mesh.mesh_id());
    println!("  address:    {}", mesh.address());
    println!("  public key: {}", mesh.public_key());
    println!("  port:       {}", config.listen_port);
    println!("  raft:       {raft_addr}");
    println!("  tls:        {}", if tls.is_some() { "enabled" } else { "disabled" });
    println!("  join pin:   {pin}");

    let raft_node = RaftNode::new(node_id, db.clone(), tls)
        .await
        .map_err(|e| MeshError::State(e.to_string()))?;
    raft_node
        .init_cluster(&raft_addr)
        .await
        .map_err(|e| MeshError::State(e.to_string()))?;
    let _raft_server = raft_node.start_server(raft_addr.clone()).await;
    let raft = Arc::new(raft_node);

    // Register self as hypervisor via Raft consensus
    let surql = format!(
        "CREATE hypervisor SET \
         public_key = '{}', node_id = {}, address = '{}', \
         endpoint = NONE, allowed_ips = ['{}'], keepalive = 25, \
         raft_addr = '{raft_addr}'",
        own_pk,
        node_id as i64,
        mesh.address(),
        mesh.address(),
    );
    if let Err(e) = raft.write(surql).await {
        eprintln!("  ! raft write (register self): {e}");
    }

    let iface = config.interface_name.clone();
    let db2 = db.clone();
    let own_pk2 = own_pk.clone();

    let listener_handle = tokio::spawn(mesh_listener(
        Some(raft),
        mesh.mesh_id().clone(),
        mesh.keypair().clone(),
        mesh.address().to_string(),
        config.interface_name.clone(),
        config.listen_port,
        Some(pin),
        config.join_port,
        Some(ca_cert),
        Some(ca_key),
    ));

    let reconciler_handle = tokio::spawn(async move {
        reconciler::run(&db2, &iface, &own_pk2).await;
    });

    println!("\n  ctrl+c to stop\n");
    signal::ctrl_c().await.map_err(|e| MeshError::State(e.to_string()))?;

    listener_handle.abort();
    reconciler_handle.abort();
    mesh.down()?;
    println!("\ndaemon stopped (state preserved — use 'mesh start' to restart, 'mesh down' to teardown)");
    Ok(())
}

pub async fn run_daemon_join(
    db: Arc<Database>,
    host: &str,
    pin: &str,
    interface_name: String,
    listen_port: u16,
    join_port: u16,
) -> Result<(), MeshError> {
    let (mesh, bootstrap_peers, tls_certs) =
        join_mesh(host, pin, interface_name.clone(), listen_port, join_port)?;

    let mut state = mesh.to_state();
    if let Some(ref certs) = tls_certs {
        state.ca_cert = Some(certs.ca_cert.clone());
        state.tls_cert = Some(certs.tls_cert.clone());
        state.tls_key = Some(certs.tls_key.clone());
    }
    state.save(&db).await?;

    let tls = build_tls(&state);

    // Save bootstrap hypervisors to local DB so reconciler picks them up.
    // Validate every field before interpolating — the bootstrap server is
    // trusted by the PIN, but we still refuse to pass un-parseable strings
    // into SurQL.
    for p in &bootstrap_peers {
        let canonical_pk = match defguard_wireguard_rs::key::Key::from_str(&p.public_key) {
            Ok(k) => k.to_string(),
            Err(_) => {
                eprintln!("  ! bootstrap peer: invalid public_key, skipping");
                continue;
            }
        };
        let endpoint: std::net::SocketAddr = match p.endpoint.parse() {
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

    let node_id = node_id_from_key(mesh.public_key());
    let raft_addr = format!("[{}]:4001", mesh.address().address);
    let own_pk = mesh.public_key().to_string();

    println!("nauka daemon (joined)");
    println!("  interface:  {}", mesh.interface_name());
    println!("  mesh:       {}", mesh.mesh_id());
    println!("  address:    {}", mesh.address());
    println!("  public key: {}", mesh.public_key());
    println!("  port:       {listen_port}");
    println!("  raft:       {raft_addr}");
    println!("  tls:        {}", if tls.is_some() { "enabled" } else { "disabled" });

    let raft_node = RaftNode::new(node_id, db.clone(), tls)
        .await
        .map_err(|e| MeshError::State(e.to_string()))?;
    let _raft_server = raft_node.start_server(raft_addr).await;
    let raft = Arc::new(raft_node);

    let db2 = db.clone();
    let iface = interface_name.clone();
    let own_pk2 = own_pk.clone();

    let listener_handle = tokio::spawn(mesh_listener(
        Some(raft),
        mesh.mesh_id().clone(),
        mesh.keypair().clone(),
        mesh.address().to_string(),
        interface_name.clone(),
        listen_port,
        None,
        join_port,
        None,
        None,
    ));

    let reconciler_handle = tokio::spawn(async move {
        reconciler::run(&db2, &iface, &own_pk2).await;
    });

    println!("\n  ctrl+c to stop\n");
    signal::ctrl_c().await.map_err(|e| MeshError::State(e.to_string()))?;

    listener_handle.abort();
    reconciler_handle.abort();
    Mesh::down_interface(&interface_name)?;
    println!("\ndaemon stopped (state preserved)");
    Ok(())
}

pub async fn run_daemon_restart(db: Arc<Database>) -> Result<(), MeshError> {
    let state = MeshState::load(&db).await?;
    let mut mesh = Mesh::from_state(&state)?;
    mesh.up()?;

    let tls = build_tls(&state);

    let own_pk = mesh.public_key().to_string();
    let node_id = node_id_from_key(&own_pk);
    let raft_addr = format!("[{}]:4001", mesh.address().address);

    println!("nauka daemon (restart)");
    println!("  interface:  {}", mesh.interface_name());
    println!("  mesh:       {}", mesh.mesh_id());
    println!("  address:    {}", mesh.address());
    println!("  public key: {}", mesh.public_key());
    println!("  port:       {}", mesh.listen_port());
    println!("  raft:       {raft_addr}");
    println!("  tls:        {}", if tls.is_some() { "enabled" } else { "disabled" });

    let raft_node = RaftNode::new(node_id, db.clone(), tls)
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
        mesh.mesh_id().clone(),
        mesh.keypair().clone(),
        mesh.address().to_string(),
        state.interface_name.clone(),
        state.listen_port,
        None,
        DEFAULT_JOIN_PORT,
        state.ca_cert,
        state.ca_key,
    ));

    let refresh_handle = tokio::spawn(refresh_own_endpoint(
        db.clone(),
        raft.clone(),
        own_pk.clone(),
        state.listen_port,
    ));

    println!("\n  ctrl+c to stop\n");
    signal::ctrl_c().await.map_err(|e| MeshError::State(e.to_string()))?;

    listener_handle.abort();
    reconciler_handle.abort();
    refresh_handle.abort();
    mesh.down()?;
    println!("\ndaemon stopped (state preserved)");
    Ok(())
}

/// Discover our public endpoint via `whoami` to a peer, then, if it differs
/// from what's currently in the hypervisor table, UPDATE via Raft so every
/// node's reconciler can refresh its WG peer endpoint for us.
///
/// Runs in a background task after `run_daemon_restart` brings Raft up,
/// because restart is the only flow where our IP might have changed while
/// peers still hold the old one.
async fn refresh_own_endpoint(
    db: Arc<Database>,
    raft: Arc<RaftNode>,
    own_pk: String,
    listen_port: u16,
) {
    // Let Raft elect a leader before we try to write.
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Find a peer with a known public endpoint.
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

    // Ask the peer what IP it sees us on.
    let observed_ip = match whoami(&peer_ip, DEFAULT_JOIN_PORT).await {
        Ok(ip) => ip,
        Err(e) => {
            eprintln!("  endpoint refresh: whoami {peer_ip} failed: {e}");
            return;
        }
    };
    let new_endpoint = SocketAddr::new(observed_ip, listen_port).to_string();

    // Compare with the value currently in the table.
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

async fn read_own_endpoint(db: &Database, own_pk: &str) -> Result<Option<String>, MeshError> {
    let rows: Vec<EndpointRow> = db
        .query_take(&format!(
            "SELECT endpoint FROM hypervisor WHERE public_key = '{own_pk}'"
        ))
        .await
        .map_err(|e| MeshError::State(e.to_string()))?;
    Ok(rows.into_iter().next().and_then(|r| r.endpoint))
}

/// Explicit teardown — removes state from DB
pub async fn run_mesh_down(db: Arc<Database>, interface_name: &str) -> Result<(), MeshError> {
    Mesh::down_interface(interface_name).ok();
    MeshState::delete(&db).await?;
    println!("mesh down — state deleted");
    Ok(())
}
