pub mod log_store;
pub mod network;
pub mod server;
pub mod state_machine;
pub mod tls;
pub mod types;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use openraft::BasicNode;
use openraft::ChangeMembers;
use openraft::Config;

use crate::db::Database;
use crate::StateError;

use self::log_store::LogStore;
use self::network::NetworkFactory;
use self::state_machine::StateMachineStore;
use self::tls::TlsConfig;
use self::types::{SurqlCommand, SurqlResponse, TypeConfig};

pub type Raft = openraft::Raft<TypeConfig, StateMachineStore<TypeConfig>>;

pub fn node_id_from_key(public_key: &str) -> u64 {
    let bytes = public_key.as_bytes();
    let mut buf = [0u8; 8];
    for (i, b) in bytes.iter().take(8).enumerate() {
        buf[i] = *b;
    }
    u64::from_be_bytes(buf)
}

pub struct RaftNode {
    pub raft: Raft,
    pub node_id: u64,
    tls: Option<TlsConfig>,
}

impl RaftNode {
    pub async fn new(
        node_id: u64,
        db: Arc<Database>,
        tls: Option<TlsConfig>,
    ) -> Result<Self, StateError> {
        let config = Config {
            heartbeat_interval: 500,
            election_timeout_min: 1500,
            election_timeout_max: 3000,
            ..Default::default()
        };
        let config = Arc::new(
            config
                .validate()
                .map_err(|e| StateError::Raft(e.to_string()))?,
        );

        let log_store = LogStore::<TypeConfig>::open(db.clone(), node_id)
            .await
            .map_err(|e| StateError::Raft(format!("open raft log: {e}")))?;
        let state_machine = StateMachineStore::<TypeConfig>::new(db, node_id)
            .await
            .map_err(|e| StateError::Raft(format!("open state machine: {e}")))?;
        let network = NetworkFactory { tls: tls.clone() };

        let raft = openraft::Raft::new(node_id, config, network, log_store, state_machine)
            .await
            .map_err(|e| StateError::Raft(e.to_string()))?;

        Ok(Self { raft, node_id, tls })
    }

    pub async fn init_cluster(&self, addr: &str) -> Result<(), StateError> {
        let mut members = BTreeMap::new();
        members.insert(self.node_id, BasicNode::new(addr));
        self.raft
            .initialize(members)
            .await
            .map_err(|e| StateError::Raft(e.to_string()))?;
        Ok(())
    }

    /// Add a node as a Raft learner. Learners receive all replicated log entries.
    pub async fn add_learner(&self, node_id: u64, addr: &str) -> Result<(), StateError> {
        self.raft
            .add_learner(node_id, BasicNode::new(addr), true)
            .await
            .map_err(|e| StateError::Raft(format!("add_learner: {e}")))?;
        Ok(())
    }

    /// Promote a learner to voter so it participates in leader elections.
    pub async fn promote_voter(&self, node_id: u64) -> Result<(), StateError> {
        let mut ids = BTreeSet::new();
        ids.insert(node_id);
        self.raft
            .change_membership(ChangeMembers::AddVoterIds(ids), true)
            .await
            .map_err(|e| StateError::Raft(format!("promote_voter: {e}")))?;
        Ok(())
    }

    /// Write a SurrealQL command through Raft consensus.
    /// Replicates to all nodes before returning.
    pub async fn write(&self, query: String) -> Result<SurqlResponse, StateError> {
        let resp = self
            .raft
            .client_write(SurqlCommand { query })
            .await
            .map_err(|e| StateError::Raft(format!("client_write: {e}")))?;
        let response = resp.response().clone();
        if let Some(ref err) = response.error {
            return Err(StateError::Raft(format!("state machine: {err}")));
        }
        Ok(response)
    }

    pub async fn start_server(&self, bind_addr: String) -> tokio::task::JoinHandle<()> {
        let raft = self.raft.clone();
        let tls = self.tls.clone();
        tokio::spawn(async move {
            if let Err(e) = server::start_raft_server(raft, &bind_addr, tls).await {
                tracing::error!(error = %e, "raft server stopped");
            }
        })
    }
}
