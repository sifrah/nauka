use std::str::FromStr;
use std::sync::Arc;

use nauka_state::{node_id_from_key, Database, RaftNode, TlsConfig};
use tokio::signal;

use crate::mesh::{
    certs, generate_pin, join_mesh, mesh_listener, KeyPair, Mesh, MeshError, MeshId, MeshState,
    DEFAULT_JOIN_PORT,
};

use crate::mesh::reconciler;

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
        Some(raft),
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

    println!("\n  ctrl+c to stop\n");
    signal::ctrl_c().await.map_err(|e| MeshError::State(e.to_string()))?;

    listener_handle.abort();
    reconciler_handle.abort();
    mesh.down()?;
    println!("\ndaemon stopped (state preserved)");
    Ok(())
}

/// Explicit teardown — removes state from DB
pub async fn run_mesh_down(db: Arc<Database>, interface_name: &str) -> Result<(), MeshError> {
    Mesh::down_interface(interface_name).ok();
    MeshState::delete(&db).await?;
    println!("mesh down — state deleted");
    Ok(())
}
