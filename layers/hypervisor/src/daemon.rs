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

use nauka_core::resource::{Datetime, ResourceOps};
use nauka_core::{LogErr, LogNaukaErr};
use nauka_state::{node_id_from_key, Database, RaftNode, TlsConfig, Writer};
use serde::Deserialize;
use surrealdb::types::SurrealValue;
use tokio::signal::unix::{signal, SignalKind};

use crate::definition::Hypervisor;
use crate::mesh::reconciler;
use crate::mesh::{
    certs, generate_pin, join_mesh, mesh_listener, whoami, Mesh, MeshError, MeshId, MeshState,
    PeerInfo, DEFAULT_JOIN_PORT,
};

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
    TlsConfig::new(ca, cert, key).ok_warn()
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
    nauka_core::instrument_op("mesh.init", async move {
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
        let raft =
            RaftNode::new_with_snapshot_threshold(node_id, db.clone(), tls, snapshot_threshold())
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

        // Timestamps are set here on the leader so every node's state
        // machine applies the same byte-identical value — a schema-level
        // `DEFAULT time::now()` would diverge by clock drift and break
        // Raft determinism (see ADR 0006 rule #7).
        let now = Datetime::now();
        let address_str = mesh.address().to_string();
        let hv = Hypervisor {
            public_key: own_pk.clone(),
            node_id,
            raft_addr: raft_addr.clone(),
            address: address_str.clone(),
            endpoint: None,
            allowed_ips: vec![address_str],
            keepalive: Some(25),
            created_at: now,
            updated_at: now,
            version: 0,
        };
        Writer::new(&db)
            .with_raft(&raft)
            .create(&hv)
            .await
            .map_err(|e| MeshError::State(format!("register self: {e}")))?;

        // Seed the IAM permission catalog + primitive roles now
        // that Raft is live on the bootstrap node. Every future
        // joiner replays the writes from the Raft log, so only the
        // founder has to run this. Idempotent on retry.
        nauka_iam::bootstrap(&db, &raft)
            .await
            .map_err(|e| MeshError::State(format!("iam bootstrap: {e}")))?;

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
    })
    .await
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
                tracing::warn!(
                    event = "bootstrap.peer.invalid_public_key",
                    public_key = %p.public_key,
                    "bootstrap peer: invalid public_key, skipping"
                );
                continue;
            }
        };
        let endpoint: SocketAddr = match p.endpoint.parse() {
            Ok(a) => a,
            Err(_) => {
                tracing::warn!(
                    event = "bootstrap.peer.invalid_endpoint",
                    public_key = %canonical_pk,
                    endpoint = %p.endpoint,
                    "bootstrap peer: invalid endpoint, skipping"
                );
                continue;
            }
        };
        let mesh_addr_mask: defguard_wireguard_rs::net::IpAddrMask = match p.mesh_address.parse() {
            Ok(m) => m,
            Err(_) => {
                tracing::warn!(
                    event = "bootstrap.peer.invalid_mesh_address",
                    public_key = %canonical_pk,
                    mesh_address = %p.mesh_address,
                    "bootstrap peer: invalid mesh_address, skipping"
                );
                continue;
            }
        };
        // Local-only bootstrap write — the joiner isn't in the Raft
        // cluster yet, so we cache the peer record locally so the WG
        // reconciler can dial it immediately. Once the joiner becomes
        // a Raft voter, the leader's replicated CREATE takes
        // precedence. `Writer::create` is not used here because it
        // would route to Raft (Hypervisor is `scope = "cluster"`),
        // which is exactly what we cannot do pre-membership.
        let now = Datetime::now();
        let mesh_addr_str = mesh_addr_mask.to_string();
        let hv = Hypervisor {
            public_key: canonical_pk.clone(),
            node_id: node_id_from_key(&canonical_pk),
            raft_addr: format!("[{}]:4001", mesh_addr_mask.address),
            address: mesh_addr_str.clone(),
            endpoint: Some(endpoint.to_string()),
            allowed_ips: vec![mesh_addr_str],
            keepalive: Some(25),
            created_at: now,
            updated_at: now,
            version: 0,
        };
        let _ = db
            .query(&hv.create_query())
            .await
            .warn_if_err("bootstrap.peer.write");
    }
}

/// Teardown helper called after the daemon is already stopped. Removes
/// the WG interface, nukes the whole SurrealKV directory, removes the
/// systemd unit file. The CLI is responsible for the IPC leave
/// notification and the `systemctl stop` that precede this call.
pub async fn leave_hypervisor(interface_name: &str) -> Result<(), MeshError> {
    let _ = Mesh::down_interface(interface_name);
    // Remove the whole DB directory rather than DELETE records — a later
    // `init`/`join` should start from a truly blank slate, with no stale
    // SurrealKV LSM files hanging around.
    let _ = std::fs::remove_dir_all("/var/lib/nauka/db");
    crate::systemd::remove_unit_file()?;
    tracing::info!(
        event = "hypervisor.leave.done",
        "hypervisor left mesh — systemd unit removed, local state wiped"
    );
    Ok(())
}

/// Long-running service entrypoint — executed by systemd.
///
/// The `on_ready` callback fires once Raft is up, with the shared
/// `db` / `raft` handles the daemon owns. Callers use this to spawn
/// sibling servers that need access to the same handles without
/// having to re-plumb them — the API layer (`nauka-api`) is the
/// motivating case. The callback must return every `JoinHandle` it
/// spawns; the daemon aborts them alongside its own tasks on
/// shutdown, so no extra handles leak past SIGTERM.
///
/// Passing an empty callback (`|_, _| async { Vec::new() }`) keeps
/// the historical behavior where `run_daemon` owned every task
/// itself.
pub async fn run_daemon<F, Fut>(db: Arc<Database>, on_ready: F) -> Result<(), MeshError>
where
    F: FnOnce(Arc<Database>, Arc<RaftNode>) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Vec<tokio::task::JoinHandle<()>>> + Send,
{
    let state = MeshState::load(&db).await?;
    let mut mesh = Mesh::from_state(&state)?;
    mesh.up()?; // idempotent — if the interface already exists, configure it

    let tls = build_tls(&state);
    let own_pk = mesh.public_key().to_string();
    let node_id = node_id_from_key(&own_pk);
    nauka_core::set_node_id(node_id);
    let raft_addr = format!("[{}]:4001", mesh.address().address);

    tracing::info!(
        event = "daemon.start",
        interface = %mesh.interface_name(),
        mesh = %mesh.mesh_id(),
        address = %mesh.address(),
        public_key = %mesh.public_key(),
        listen_port = mesh.listen_port(),
        raft_addr = %raft_addr,
        tls = tls.is_some(),
        peering_open = state.peering_pin.is_some(),
        "nauka hypervisor daemon starting"
    );

    let raft_node =
        RaftNode::new_with_snapshot_threshold(node_id, db.clone(), tls, snapshot_threshold())
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

    // Let the caller spawn anything else that needs db + raft —
    // typically the axum API server (342-B2). Any returned handles
    // are aborted alongside the daemon's own on shutdown.
    let extra_handles = on_ready(db.clone(), raft.clone()).await;

    // systemd sends SIGTERM on `systemctl stop`; Ctrl+C is SIGINT in
    // foreground mode. Handle either.
    let mut sigint =
        signal(SignalKind::interrupt()).map_err(|e| MeshError::State(e.to_string()))?;
    let mut sigterm =
        signal(SignalKind::terminate()).map_err(|e| MeshError::State(e.to_string()))?;
    tokio::select! {
        _ = sigint.recv() => tracing::info!(event = "daemon.signal", signal = "SIGINT", "received SIGINT"),
        _ = sigterm.recv() => tracing::info!(event = "daemon.signal", signal = "SIGTERM", "received SIGTERM"),
    }

    listener_handle.abort();
    reconciler_handle.abort();
    refresh_handle.abort();
    for h in extra_handles {
        h.abort();
    }
    mesh.down()?;
    tracing::info!(event = "daemon.stop", "daemon stopped (state preserved)");
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
            tracing::debug!(
                event = "endpoint.refresh.no_peer",
                "endpoint refresh: no peer with endpoint yet — skipping"
            );
            return;
        }
        Err(e) => {
            tracing::warn!(
                event = "endpoint.refresh.pick_peer_failed",
                error = %e,
                "endpoint refresh: pick peer failed"
            );
            return;
        }
    };

    let observed_ip = match whoami(&peer_ip, DEFAULT_JOIN_PORT).await {
        Ok(ip) => ip,
        Err(e) => {
            tracing::warn!(
                event = "endpoint.refresh.whoami_failed",
                peer_ip = %peer_ip,
                error = %e,
                "endpoint refresh: whoami failed"
            );
            return;
        }
    };
    let new_endpoint = SocketAddr::new(observed_ip, listen_port).to_string();

    let mut hv = match fetch_hypervisor(&db, &own_pk).await {
        Ok(Some(h)) => h,
        Ok(None) => {
            tracing::warn!(
                event = "endpoint.refresh.own_record_missing",
                public_key = %own_pk,
                "endpoint refresh: own hypervisor record not found"
            );
            return;
        }
        Err(e) => {
            tracing::warn!(
                event = "endpoint.refresh.read_own_failed",
                error = %e,
                "endpoint refresh: read own endpoint failed"
            );
            return;
        }
    };
    if hv.endpoint.as_deref() == Some(new_endpoint.as_str()) {
        tracing::debug!(
            event = "endpoint.refresh.unchanged",
            endpoint = %new_endpoint,
            "endpoint refresh: unchanged"
        );
        return;
    }

    tracing::info!(
        event = "endpoint.refresh.change",
        from = %hv.endpoint.as_deref().unwrap_or("NONE"),
        to = %new_endpoint,
        "endpoint refresh: changed"
    );

    hv.endpoint = Some(new_endpoint.clone());
    hv.updated_at = Datetime::now();
    hv.version += 1;

    match Writer::new(&db).with_raft(&raft).update(&hv).await {
        Ok(_) => tracing::info!(
            event = "endpoint.refresh.propagated",
            "endpoint refresh: propagated via Raft"
        ),
        Err(e) => tracing::warn!(
            event = "endpoint.refresh.raft_write_failed",
            error = %e,
            "endpoint refresh: raft write failed"
        ),
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
    db.query_take("SELECT public_key, address, endpoint FROM hypervisor ORDER BY public_key")
        .await
        .map_err(|e| MeshError::State(e.to_string()))
}

async fn fetch_hypervisor(
    db: &Database,
    public_key: &str,
) -> Result<Option<Hypervisor>, MeshError> {
    let query = Hypervisor::get_query(&public_key.to_string());
    let rows: Vec<Hypervisor> = db
        .query_take(&query)
        .await
        .map_err(|e| MeshError::State(e.to_string()))?;
    Ok(rows.into_iter().next())
}
