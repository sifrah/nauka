//! Nauka's Raft consensus (openraft over QUIC).
//!
//! The Raft log replicates METADATA only (manifest registry, membership) —
//! never shard bytes, which travel directly over nauka-transport. The cluster
//! elects a leader; writes go through it, while state reads are served
//! locally on every node.

pub mod network;
pub mod store;
pub mod types;

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::Result;
use openraft::error::{ClientWriteError, RaftError};
use openraft::{BasicNode, Config, Raft};
use tracing::info;

use network::QuicRaftNetworkFactory;
use store::{LogStore, StateMachineStore};
use types::{
    AdminRequest, AdminResponse, AppCommand, AppState, NodeId, TypeConfig,
};

pub use types::AppResponse;
pub use openraft;

/// A node's Raft instance, with access to the materialized state.
pub struct RaftApp {
    pub id: NodeId,
    pub raft: Raft<TypeConfig>,
    state_machine: StateMachineStore,
}

impl RaftApp {
    /// Starts this node's Raft engine, with durable state under `dir`
    /// (log + vote in redb, snapshots on file). A node restarting with the
    /// same dir picks up where it left off; a whole cluster powered off
    /// comes back without loss. The node stays passive until the cluster is
    /// initialized (`AdminRequest::Init`) or until an existing member adds
    /// it.
    pub async fn start(id: NodeId, dir: &std::path::Path) -> Result<Arc<Self>> {
        let config = Arc::new(
            Config {
                heartbeat_interval: 500,
                election_timeout_min: 1500,
                election_timeout_max: 3000,
                // Snapshot regularly to bound the redb log; keep a margin of
                // entries so slightly lagging followers catch up from the log
                // rather than from a full snapshot.
                snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(256),
                max_in_snapshot_log_to_keep: 64,
                ..Default::default()
            }
            .validate()?,
        );
        let log_store = LogStore::open(dir)?;
        let state_machine = StateMachineStore::open(dir)?;
        let raft = Raft::new(
            id,
            config,
            QuicRaftNetworkFactory,
            log_store,
            state_machine.clone(),
        )
        .await?;
        info!("raft started, node_id={id}");
        Ok(Arc::new(Self { id, raft, state_machine }))
    }

    /// Current replicated state (local read, possibly lagging behind the
    /// leader — good enough for the healer and for display).
    pub fn app_state(&self) -> AppState {
        self.state_machine.read_state()
    }

    /// Writes a command to the registry: locally if this node is the leader,
    /// otherwise by forwarding it to the leader over the transport.
    pub async fn write(&self, cmd: AppCommand) -> Result<AppResponse> {
        match self.raft.client_write(cmd.clone()).await {
            Ok(resp) => Ok(resp.data),
            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(f))) => {
                let addr: std::net::SocketAddr = f
                    .leader_node
                    .ok_or_else(|| anyhow::anyhow!("no known leader"))?
                    .addr
                    .parse()?;
                let client = nauka_transport::PeerClient::connect(addr).await?;
                match admin_call(&client, &AdminRequest::Write(cmd)).await? {
                    AdminResponse::Ok(resp) => Ok(resp),
                    other => anyhow::bail!("write via leader: {other:?}"),
                }
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Current members (id → address), from the Raft metrics.
    pub fn members(&self) -> BTreeMap<NodeId, String> {
        let metrics = self.raft.metrics().borrow().clone();
        metrics
            .membership_config
            .nodes()
            .map(|(id, node)| (*id, node.addr.clone()))
            .collect()
    }

    /// Network coordinates known to the cluster (address → position).
    pub fn coords(&self) -> BTreeMap<String, nauka_cluster::vivaldi::Coord> {
        self.app_state().node_coords
    }

    /// Weighted view of the cluster for placement: membership members with
    /// their declared capacity (default if not declared yet), sorted.
    pub fn weighted_view(&self, default_capacity: u64) -> Vec<(String, u64)> {
        let capacities = self.app_state().node_capacities;
        let mut view: Vec<(String, u64)> = self
            .members()
            .into_values()
            .map(|addr| {
                let w = capacities.get(&addr).copied().unwrap_or(default_capacity);
                (addr, w)
            })
            .collect();
        view.sort();
        view
    }

    async fn handle_admin(&self, req: AdminRequest) -> AdminResponse {
        match req {
            AdminRequest::Init(nodes) => {
                let members: BTreeMap<NodeId, BasicNode> = nodes
                    .into_iter()
                    .map(|(id, addr)| (id, BasicNode { addr }))
                    .collect();
                match self.raft.initialize(members).await {
                    Ok(()) => AdminResponse::Ok(AppResponse { ok: true, info: None }),
                    Err(e) => AdminResponse::Err(e.to_string()),
                }
            }
            AdminRequest::AddLearner { id, addr } => {
                match self.raft.add_learner(id, BasicNode { addr }, true).await {
                    Ok(_) => AdminResponse::Ok(AppResponse { ok: true, info: None }),
                    Err(e) => self.forward_or_err(e),
                }
            }
            AdminRequest::ChangeMembership(ids) => {
                let set: std::collections::BTreeSet<NodeId> = ids.into_iter().collect();
                match self.raft.change_membership(set, false).await {
                    Ok(_) => AdminResponse::Ok(AppResponse { ok: true, info: None }),
                    Err(e) => self.forward_or_err(e),
                }
            }
            AdminRequest::Write(cmd) => match self.raft.client_write(cmd).await {
                Ok(resp) => AdminResponse::Ok(resp.data),
                Err(RaftError::APIError(ClientWriteError::ForwardToLeader(f))) => {
                    AdminResponse::ForwardTo {
                        leader: f
                            .leader_id
                            .zip(f.leader_node)
                            .map(|(id, node)| (id, node.addr)),
                    }
                }
                Err(e) => AdminResponse::Err(e.to_string()),
            },
            AdminRequest::Metrics => {
                let metrics = self.raft.metrics().borrow().clone();
                AdminResponse::Metrics {
                    id: self.id,
                    leader: metrics.current_leader,
                    members: self.members(),
                    last_applied: metrics.last_applied.map(|l| l.index),
                    capacities: self.app_state().node_capacities,
                }
            }
            AdminRequest::ListManifests => {
                AdminResponse::Manifests(self.app_state().manifests.keys().cloned().collect())
            }
        }
    }

    fn forward_or_err(
        &self,
        e: RaftError<NodeId, ClientWriteError<NodeId, BasicNode>>,
    ) -> AdminResponse {
        match e {
            RaftError::APIError(ClientWriteError::ForwardToLeader(f)) => AdminResponse::ForwardTo {
                leader: f.leader_id.zip(f.leader_node).map(|(id, node)| (id, node.addr)),
            },
            other => AdminResponse::Err(other.to_string()),
        }
    }
}

/// Adapter: takes the Raft RPCs arriving over the QUIC transport and hands
/// them to the local openraft engine.
#[async_trait::async_trait]
impl nauka_transport::server::RaftHandler for RaftApp {
    async fn handle(
        &self,
        rpc: nauka_transport::protocol::RaftRpc,
    ) -> Result<Vec<u8>, String> {
        use nauka_transport::protocol::RaftRpc;
        let err = |e: &dyn std::fmt::Display| e.to_string();
        match rpc {
            RaftRpc::AppendEntries(p) => {
                let req = bincode::deserialize(&p).map_err(|e| err(&e))?;
                let resp = self.raft.append_entries(req).await.map_err(|e| err(&e))?;
                bincode::serialize(&resp).map_err(|e| err(&e))
            }
            RaftRpc::Vote(p) => {
                let req = bincode::deserialize(&p).map_err(|e| err(&e))?;
                let resp = self.raft.vote(req).await.map_err(|e| err(&e))?;
                bincode::serialize(&resp).map_err(|e| err(&e))
            }
            RaftRpc::InstallSnapshot(p) => {
                let req = bincode::deserialize(&p).map_err(|e| err(&e))?;
                let resp = self.raft.install_snapshot(req).await.map_err(|e| err(&e))?;
                bincode::serialize(&resp).map_err(|e| err(&e))
            }
            RaftRpc::Admin(p) => {
                let req: AdminRequest = bincode::deserialize(&p).map_err(|e| err(&e))?;
                let resp = self.handle_admin(req).await;
                bincode::serialize(&resp).map_err(|e| err(&e))
            }
        }
    }
}

/// Client helper: sends an AdminRequest to a node and decodes the response.
pub async fn admin_call(
    client: &nauka_transport::PeerClient,
    req: &AdminRequest,
) -> Result<AdminResponse> {
    let payload = bincode::serialize(req)?;
    let resp = client
        .raft(nauka_transport::protocol::RaftRpc::Admin(payload))
        .await?;
    Ok(bincode::deserialize(&resp)?)
}

/// Runs an AdminRequest following the redirect to the leader: tries each
/// peer, follows `ForwardTo`, retries across leader changes.
pub async fn admin_via_leader(
    peers: &[std::net::SocketAddr],
    req: &AdminRequest,
) -> Result<AdminResponse> {
    let mut targets: Vec<std::net::SocketAddr> = peers.to_vec();
    let mut last_err = String::from("no reachable peer");
    for _ in 0..4 {
        for addr in targets.clone() {
            let Ok(client) = nauka_transport::PeerClient::connect(addr).await else {
                continue;
            };
            match admin_call(&client, req).await {
                Ok(AdminResponse::ForwardTo { leader: Some((_, leader_addr)) }) => {
                    if let Ok(a) = leader_addr.parse() {
                        targets = vec![a];
                    }
                }
                Ok(AdminResponse::ForwardTo { leader: None }) => {
                    last_err = "no leader elected yet".into();
                }
                Ok(AdminResponse::Err(e)) => last_err = e,
                Ok(resp) => return Ok(resp),
                Err(e) => last_err = e.to_string(),
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    anyhow::bail!("failed via leader: {last_err}")
}

/// Writes a command to the registry, following the redirect to the leader
/// if needed.
pub async fn write_via_leader(
    peers: &[std::net::SocketAddr],
    cmd: AppCommand,
) -> Result<AppResponse> {
    match admin_via_leader(peers, &AdminRequest::Write(cmd)).await? {
        AdminResponse::Ok(resp) => Ok(resp),
        other => anyhow::bail!("unexpected response: {other:?}"),
    }
}
