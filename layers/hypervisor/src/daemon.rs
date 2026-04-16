use std::sync::Arc;

use nauka_state::{node_id_from_key, Database, RaftNode, DEFAULT_RAFT_DIR};
use tokio::signal;

use crate::mesh::{
    generate_pin, join_mesh, mesh_listener, KeyPair, Mesh, MeshError, MeshId, MeshState,
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

pub async fn run_daemon(db: Arc<Database>, config: DaemonConfig) -> Result<(), MeshError> {
    let mut mesh = Mesh::new(
        config.interface_name.clone(),
        config.listen_port,
        config.mesh_id,
        config.keypair,
        None,
    )?;
    mesh.up()?;
    mesh.to_state().save(&db).await?;

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
    println!("  join pin:   {pin}");

    let raft_node = RaftNode::new(node_id, db.clone(), DEFAULT_RAFT_DIR)
        .await
        .map_err(|e| MeshError::State(e.to_string()))?;
    raft_node
        .init_cluster(&raft_addr)
        .await
        .map_err(|e| MeshError::State(e.to_string()))?;
    let _raft_server = raft_node.start_server(raft_addr).await;
    let raft = Arc::new(raft_node);

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
    let (mesh, bootstrap_peers) =
        join_mesh(host, pin, interface_name.clone(), listen_port, join_port)?;
    mesh.to_state().save(&db).await?;

    // Save bootstrap peers to local DB so reconciler doesn't remove them
    for p in &bootstrap_peers {
        let surql = format!(
            "CREATE peer SET public_key = '{}', endpoint = '{}', allowed_ips = ['{}'], keepalive = 25",
            p.public_key, p.endpoint, p.mesh_address
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

    let raft_node = RaftNode::new(node_id, db.clone(), DEFAULT_RAFT_DIR)
        .await
        .map_err(|e| MeshError::State(e.to_string()))?;
    let _raft_server = raft_node.start_server(raft_addr).await;

    let db2 = db.clone();
    let iface = interface_name.clone();
    let own_pk2 = own_pk.clone();

    let listener_handle = tokio::spawn(mesh_listener(
        None,
        mesh.mesh_id().clone(),
        mesh.keypair().clone(),
        mesh.address().to_string(),
        interface_name.clone(),
        listen_port,
        None,
        join_port,
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

    let raft_node = RaftNode::new(node_id, db.clone(), DEFAULT_RAFT_DIR)
        .await
        .map_err(|e| MeshError::State(e.to_string()))?;
    let _raft_server = raft_node.start_server(raft_addr).await;

    let db2 = db.clone();
    let iface = state.interface_name.clone();
    let own_pk2 = own_pk.clone();

    let reconciler_handle = tokio::spawn(async move {
        reconciler::run(&db2, &iface, &own_pk2).await;
    });

    let listener_handle = tokio::spawn(mesh_listener(
        None,
        mesh.mesh_id().clone(),
        mesh.keypair().clone(),
        mesh.address().to_string(),
        state.interface_name.clone(),
        state.listen_port,
        None,
        DEFAULT_JOIN_PORT,
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
