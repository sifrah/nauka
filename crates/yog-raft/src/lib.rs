//! Consensus Raft de yogfile (openraft sur QUIC).
//!
//! Le log Raft ne réplique que des MÉTADONNÉES (registre des manifests,
//! membership) — jamais les octets des shards, qui transitent en direct
//! par yog-transport. Le cluster élit un leader ; les écritures passent
//! par lui, les lectures d'état se font localement sur chaque nœud.

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

/// Instance Raft d'un nœud + accès à l'état matérialisé.
pub struct RaftApp {
    pub id: NodeId,
    pub raft: Raft<TypeConfig>,
    state_machine: StateMachineStore,
}

impl RaftApp {
    /// Démarre le moteur Raft de ce nœud, avec état durable dans `dir`
    /// (log + vote en redb, snapshots sur fichier). Un nœud qui redémarre
    /// avec le même dir reprend là où il s'était arrêté ; un cluster entier
    /// éteint redémarre sans perte. Le nœud reste passif tant que le cluster
    /// n'est pas initialisé (`AdminRequest::Init`) ou qu'il n'est pas ajouté
    /// par un membre existant.
    pub async fn start(id: NodeId, dir: &std::path::Path) -> Result<Arc<Self>> {
        let config = Arc::new(
            Config {
                heartbeat_interval: 500,
                election_timeout_min: 1500,
                election_timeout_max: 3000,
                // Snapshot régulier pour borner le log redb ; on garde une
                // marge d'entrées pour que les followers un peu en retard
                // rattrapent par le log plutôt que par snapshot complet.
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
        info!("raft démarré, node_id={id}");
        Ok(Arc::new(Self { id, raft, state_machine }))
    }

    /// État répliqué courant (lecture locale, éventuellement en retard sur
    /// le leader — suffisant pour le healer et l'affichage).
    pub fn app_state(&self) -> AppState {
        self.state_machine.read_state()
    }

    /// Écrit une commande dans le registre : localement si ce nœud est
    /// leader, sinon en la transmettant au leader via le transport.
    pub async fn write(&self, cmd: AppCommand) -> Result<AppResponse> {
        match self.raft.client_write(cmd.clone()).await {
            Ok(resp) => Ok(resp.data),
            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(f))) => {
                let addr: std::net::SocketAddr = f
                    .leader_node
                    .ok_or_else(|| anyhow::anyhow!("pas de leader connu"))?
                    .addr
                    .parse()?;
                let client = yog_transport::PeerClient::connect(addr).await?;
                match admin_call(&client, &AdminRequest::Write(cmd)).await? {
                    AdminResponse::Ok(resp) => Ok(resp),
                    other => anyhow::bail!("écriture via le leader: {other:?}"),
                }
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Membres actuels (id → adresse), d'après les métriques Raft.
    pub fn members(&self) -> BTreeMap<NodeId, String> {
        let metrics = self.raft.metrics().borrow().clone();
        metrics
            .membership_config
            .nodes()
            .map(|(id, node)| (*id, node.addr.clone()))
            .collect()
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

/// Adaptateur : reçoit les RPCs Raft arrivées par le transport QUIC et les
/// remet au moteur openraft local.
#[async_trait::async_trait]
impl yog_transport::server::RaftHandler for RaftApp {
    async fn handle(
        &self,
        rpc: yog_transport::protocol::RaftRpc,
    ) -> Result<Vec<u8>, String> {
        use yog_transport::protocol::RaftRpc;
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

/// Helper client : envoie une AdminRequest à un nœud et décode la réponse.
pub async fn admin_call(
    client: &yog_transport::PeerClient,
    req: &AdminRequest,
) -> Result<AdminResponse> {
    let payload = bincode::serialize(req)?;
    let resp = client
        .raft(yog_transport::protocol::RaftRpc::Admin(payload))
        .await?;
    Ok(bincode::deserialize(&resp)?)
}

/// Exécute une AdminRequest en suivant la redirection vers le leader :
/// essaie chaque peer, suit les `ForwardTo`, retente pendant les bascules.
pub async fn admin_via_leader(
    peers: &[std::net::SocketAddr],
    req: &AdminRequest,
) -> Result<AdminResponse> {
    let mut targets: Vec<std::net::SocketAddr> = peers.to_vec();
    let mut last_err = String::from("aucun peer joignable");
    for _ in 0..4 {
        for addr in targets.clone() {
            let Ok(client) = yog_transport::PeerClient::connect(addr).await else {
                continue;
            };
            match admin_call(&client, req).await {
                Ok(AdminResponse::ForwardTo { leader: Some((_, leader_addr)) }) => {
                    if let Ok(a) = leader_addr.parse() {
                        targets = vec![a];
                    }
                }
                Ok(AdminResponse::ForwardTo { leader: None }) => {
                    last_err = "pas de leader élu pour l'instant".into();
                }
                Ok(AdminResponse::Err(e)) => last_err = e,
                Ok(resp) => return Ok(resp),
                Err(e) => last_err = e.to_string(),
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    anyhow::bail!("échec via le leader: {last_err}")
}

/// Écrit une commande dans le registre en suivant la redirection vers le
/// leader si nécessaire.
pub async fn write_via_leader(
    peers: &[std::net::SocketAddr],
    cmd: AppCommand,
) -> Result<AppResponse> {
    match admin_via_leader(peers, &AdminRequest::Write(cmd)).await? {
        AdminResponse::Ok(resp) => Ok(resp),
        other => anyhow::bail!("réponse inattendue: {other:?}"),
    }
}
