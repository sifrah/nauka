//! Nauka's Raft consensus (openraft over QUIC).
//!
//! The Raft log replicates METADATA only (manifest registry, membership) —
//! never shard bytes, which travel directly over nauka-transport. The cluster
//! elects a leader; writes go through it, while state reads are served
//! locally on every node.

pub mod network;
pub mod store;
pub mod telemetry;
pub mod types;

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::Result;
use openraft::error::{ClientWriteError, RaftError};
use openraft::{BasicNode, Config, Raft};
use tracing::info;

use network::QuicRaftNetworkFactory;
use store::{LogStore, StateMachineStore};
use types::{AdminRequest, AdminResponse, AppCommand, AppState, NodeId, TypeConfig};

pub use openraft;
pub use types::AppResponse;

/// Outcome of a pre-read freshness catch-up (`catch_up_with_leader`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Freshness {
    /// Local state has applied at least up to the leader's applied index
    /// from a query started after the call — a negative lookup is real.
    Fresh,
    /// The leader is confirmed AHEAD and this node could not catch up in
    /// time (typically a node healing after a fault). A negative lookup
    /// here would lie; the reader should answer "retry later" instead.
    ConfirmedStale,
    /// No leader known or reachable (an election in flight, a partition).
    /// Freshness is unknowable — and an unprovable negative is still not
    /// a trustworthy one: readers treat this like `ConfirmedStale` for
    /// negative answers, while positive lookups are served normally
    /// (objects are immutable-addressed; a stale HIT is still correct).
    Unknown,
}

/// A node's Raft instance, with access to the materialized state.
pub struct RaftApp {
    pub id: NodeId,
    pub raft: Raft<TypeConfig>,
    state_machine: StateMachineStore,
    fresh: Arc<FreshGate>,
}

/// Batches concurrent leader-freshness queries. A single miss pays one
/// leader round-trip; a WAVE of them (every LIST, plus every miss during
/// a churn-induced lag spike) must not open one QUIC connection to the
/// leader each. Queries are generational: an answer only satisfies a
/// caller if the query STARTED after the caller arrived — an in-flight
/// query may predate the write whose ack the caller is entitled to see —
/// so a wave costs at most two round-trips, and the fetch itself runs in
/// a detached task (a caller cancelled mid-flight can't wedge the gate).
struct FreshGate {
    state: tokio::sync::Mutex<FreshState>,
    /// (generation, leader-applied-index) of the last finished query.
    done: tokio::sync::watch::Sender<(u64, Option<u64>)>,
}

#[derive(Default)]
struct FreshState {
    started: u64,
    fetching: bool,
}

impl Default for FreshGate {
    fn default() -> Self {
        Self {
            state: tokio::sync::Mutex::new(FreshState::default()),
            done: tokio::sync::watch::channel((0, None)).0,
        }
    }
}

impl RaftApp {
    /// Whether this node has ever been part of a cluster. Distinguishes
    /// "never initialized" (a blank data dir, safe to found or join) from
    /// "restarting with state" (must NOT found: openraft refuses and the
    /// node crashes). Reads the Raft core directly via `is_initialized`,
    /// NOT the metrics watch channel — that channel reports empty for the
    /// instant between engine start and its first tick, which is exactly
    /// the window a restart passes through and `members()` got wrong.
    pub async fn has_cluster_state(&self) -> bool {
        self.raft.is_initialized().await.unwrap_or(true)
    }

    /// Found a single-node cluster with this node as its only voter.
    /// Called once, on a blank data dir, when no discovery layer exists to
    /// negotiate membership — the birth of a cluster is now a deliberate
    /// local act, not the outcome of a race.
    ///
    /// `addr` is this node's advertised address — its membership identity,
    /// the same string `members()` returns and placement keys on.
    pub async fn found_alone(&self, addr: String) -> Result<()> {
        let members = std::collections::BTreeMap::from([(self.id, openraft::BasicNode { addr })]);
        self.raft
            .initialize(members)
            .await
            .map_err(|e| anyhow::anyhow!("initialize: {e}"))
    }

    /// Starts this node's Raft engine, with durable state under `dir`
    /// (log + vote in redb, snapshots on file). A node restarting with the
    /// same dir picks up where it left off; a whole cluster powered off
    /// comes back without loss. The node stays passive until it founds a
    /// cluster ([`Self::found_alone`]) or an existing member adds it.
    pub async fn start(id: NodeId, dir: &std::path::Path) -> Result<Arc<Self>> {
        let config = Arc::new(
            Config {
                heartbeat_interval: 500,
                election_timeout_min: 1500,
                election_timeout_max: 3000,
                // openraft's default is 200ms — enough to ship a snapshot
                // across a rack, not across an ocean. A fresh or lagging
                // member on a far continent receives the whole snapshot in
                // one RPC; under the default it times out on every attempt,
                // the follower never catches up, its election timer fires,
                // and the stale candidate storms the cluster with term
                // inflation forever. Give the transfer a real budget.
                install_snapshot_timeout: 60_000,
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
        let app = Arc::new(Self {
            id,
            raft,
            state_machine,
            fresh: Arc::new(FreshGate::default()),
        });
        // Consensus telemetry: describe the metrics, then follow openraft's
        // watch channel. Both are no-ops when no recorder is installed, so
        // this costs an embedder that does not want metrics one idle task.
        telemetry::describe();
        telemetry::spawn(Arc::downgrade(&app));
        Ok(app)
    }

    /// Current replicated state (local read, possibly lagging behind the
    /// leader — good enough for the healer and for display).
    pub fn app_state(&self) -> AppState {
        self.state_machine.read_state()
    }

    /// Upper bound on a registry write. Without quorum, `client_write`
    /// waits for a commit that will never come — the HTTP client that
    /// triggered the upload hangs with no status, forever (observed with
    /// 2 nodes alive out of 5: the request sat at "100 Continue" until the
    /// client's own timeout). A cluster that cannot commit must say so.
    ///
    /// A healthy commit is metadata-only and takes milliseconds, so this is
    /// slack, not a target. It is kept just above the election timeout
    /// (`election_timeout_max` = 3s): a partitioned leader that still
    /// believes it leads sheds leadership within that window, after which
    /// `leader_known()` fails the write instantly. So the worst case a
    /// caller sees is one ~4s timeout, then immediate 503s — not a 10s
    /// hang on every attempt.
    pub const WRITE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(4);

    /// Writes a command to the registry: locally if this node is the leader,
    /// otherwise by forwarding it to the leader over the transport. Bounded
    /// by [`Self::WRITE_TIMEOUT`]: a write that cannot reach quorum fails
    /// instead of hanging.
    pub async fn write(&self, cmd: AppCommand) -> Result<AppResponse> {
        tokio::time::timeout(Self::WRITE_TIMEOUT, self.write_inner(cmd))
            .await
            .map_err(|_| {
                anyhow::anyhow!(
                    "the registry did not commit within {:?} — no quorum?",
                    Self::WRITE_TIMEOUT
                )
            })?
    }

    async fn write_inner(&self, cmd: AppCommand) -> Result<AppResponse> {
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

    /// Upper bound on a freshness catch-up before a negative read.
    pub const FRESH_READ_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(1200);

    /// Closes the read-after-write window before a negative answer.
    ///
    /// State reads are local and the log is applied asynchronously, so a
    /// key can be committed cluster-wide (its PUT acked by the leader)
    /// yet invisible on this node for a moment. On a lookup MISS the
    /// reader calls this: fetch the leader's applied index — the leader
    /// acks a write only after applying it, so that index is exactly the
    /// freshness an acked write is entitled to — wait (bounded) until
    /// this node has applied at least as far, then look again.
    ///
    /// Never blocks past its bounds, and reports what it achieved: `Fresh`
    /// when the local state provably covers every acked write, `Unknown`
    /// when no leader answered (serve the local view best-effort — the
    /// pre-existing behaviour), `ConfirmedStale` when the leader is known
    /// to be ahead and the wait timed out — a healing node mid-catch-up,
    /// whose negative answers must not be trusted. On the leader itself
    /// this is a cheap local no-op.
    pub async fn catch_up_with_leader(&self) -> Freshness {
        let Some(target) = self.leader_applied_fresh().await else {
            // No leader, or unreachable: freshness is unknowable. The
            // caller serves its local view — same as before this existed.
            return Freshness::Unknown;
        };
        match self
            .raft
            .wait(Some(Self::FRESH_READ_TIMEOUT))
            .metrics(
                |m| m.last_applied.is_some_and(|l| l.index >= target),
                "catch up to the leader's applied index",
            )
            .await
        {
            Ok(_) => Freshness::Fresh,
            Err(_) => {
                // We KNOW the leader is ahead and we could not catch up in
                // time (a node healing after a fault, mid-snapshot). A
                // negative answer from here would be a lie.
                tracing::debug!(target, "read freshness: confirmed behind the leader");
                Freshness::ConfirmedStale
            }
        }
    }

    /// The leader's applied index, from a query started after this call —
    /// batched through [`FreshGate`]. `None` when the leader is unknown or
    /// unreachable: the caller degrades to serving its local view.
    async fn leader_applied_fresh(&self) -> Option<u64> {
        let mut rx = self.fresh.done.subscribe();
        let need = {
            let mut st = self.fresh.state.lock().await;
            if st.fetching {
                st.started + 1
            } else {
                self.spawn_fetch(&mut st);
                st.started
            }
        };
        loop {
            {
                let mut st = self.fresh.state.lock().await;
                let (finished, result) = *self.fresh.done.borrow();
                if finished >= need {
                    return result;
                }
                if !st.fetching {
                    self.spawn_fetch(&mut st);
                }
            }
            if rx.changed().await.is_err() {
                return None;
            }
        }
    }

    /// Starts one leader query in a detached task (caller may be cancelled;
    /// the gate must still settle). Caller holds the state lock.
    fn spawn_fetch(&self, st: &mut FreshState) {
        st.fetching = true;
        st.started += 1;
        let generation = st.started;
        let raft = self.raft.clone();
        let self_id = self.id;
        let fresh = self.fresh.clone();
        tokio::spawn(async move {
            let result = tokio::time::timeout(
                std::time::Duration::from_millis(900),
                query_leader_applied(&raft, self_id),
            )
            .await
            .ok()
            .flatten();
            let mut st = fresh.state.lock().await;
            st.fetching = false;
            drop(st);
            let _ = fresh.done.send((generation, result));
        });
    }

    /// Whether the state machine we serve reads from is still behind the
    /// log this node has accepted — true only while a freshly-received
    /// snapshot or a burst of entries is being applied. openraft bumps the
    /// applied-index METRIC a hair before the state machine reflects an
    /// installed snapshot, so a reader that just got a `Fresh` verdict yet
    /// finds a key absent uses this to tell "genuinely absent" (not behind:
    /// trust the 404) from "not visible yet" (behind: poll a moment).
    pub fn state_lagging(&self) -> bool {
        let metric = self
            .raft
            .metrics()
            .borrow()
            .last_applied
            .map(|l| l.index)
            .unwrap_or(0);
        metric > self.state_machine.applied_index()
    }

    /// Whether a leader is currently known to this node. A write with no
    /// leader cannot commit — it would sit until [`Self::WRITE_TIMEOUT`]
    /// and then fail. Checking first lets the caller fail fast and
    /// retryably (503) instead of hanging (an isolated node, a partition
    /// with no quorum, or an election in flight).
    pub fn leader_known(&self) -> bool {
        self.raft.metrics().borrow().current_leader.is_some()
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
        let state = self.app_state();
        let capacities = state.node_capacities;
        // A DISABLED (draining) node leaves the placement view while
        // remaining a full member: every shard it holds gains an owner
        // elsewhere, the scrubbers migrate them, its own GC releases them
        // against proofs — and its store drains to zero without the
        // cluster ever dipping below full redundancy. Replicated state,
        // so every node computes the same filtered view.
        let mut view: Vec<(String, u64)> = self
            .members()
            .into_values()
            .filter(|addr| !state.disabled.contains(addr))
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
                    Ok(()) => AdminResponse::Ok(AppResponse {
                        ok: true,
                        info: None,
                    }),
                    Err(e) => AdminResponse::Err(e.to_string()),
                }
            }
            AdminRequest::AddLearner { id, addr } => {
                match self.raft.add_learner(id, BasicNode { addr }, true).await {
                    Ok(_) => AdminResponse::Ok(AppResponse {
                        ok: true,
                        info: None,
                    }),
                    Err(e) => self.forward_or_err(e),
                }
            }
            AdminRequest::ChangeMembership(ids) => {
                let set: std::collections::BTreeSet<NodeId> = ids.into_iter().collect();
                match self.raft.change_membership(set, false).await {
                    Ok(_) => AdminResponse::Ok(AppResponse {
                        ok: true,
                        info: None,
                    }),
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
            AdminRequest::S3State => AdminResponse::S3State(Box::new(self.app_state().s3)),
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
                leader: f
                    .leader_id
                    .zip(f.leader_node)
                    .map(|(id, node)| (id, node.addr)),
            },
            other => AdminResponse::Err(other.to_string()),
        }
    }
}

/// One leader-freshness query: the leader's applied index right now. On
/// the leader itself that's the local metric; on a follower it's one
/// admin round-trip. The leader acks a write only after applying it, so
/// this index is exactly the freshness an acked write is entitled to.
async fn query_leader_applied(raft: &Raft<TypeConfig>, self_id: NodeId) -> Option<u64> {
    let metrics = raft.metrics().borrow().clone();
    let leader = metrics.current_leader?;
    if leader == self_id {
        // We believe we are the leader — but a leader that was paused (a GC
        // stall, a SIGSTOP, a partition) still reports `state == Leader` with
        // its OLD applied index right after it resumes, before it learns it
        // was deposed. Trusting that stale index would declare a stale read
        // "fresh". So confirm leadership the linearizable way: a heartbeat to
        // a quorum. A genuine leader gets the acks and the confirmed applied
        // index back; a deposed one cannot, and we return None so the caller
        // answers SlowDown instead of a false negative. Only runs on a local
        // read MISS, so a served (present) key never pays for it.
        return match raft.ensure_linearizable().await {
            Ok(read_log_id) => read_log_id.map(|l| l.index),
            Err(_) => None,
        };
    }
    let leader_addr = metrics
        .membership_config
        .nodes()
        .find(|(id, _)| **id == leader)
        .map(|(_, node)| node.addr.clone())?;
    let addr: std::net::SocketAddr = leader_addr.parse().ok()?;
    let client = nauka_transport::PeerClient::connect(addr).await.ok()?;
    match admin_call(&client, &AdminRequest::Metrics).await {
        Ok(AdminResponse::Metrics { last_applied, .. }) => last_applied,
        _ => None,
    }
}

/// Adapter: takes the Raft RPCs arriving over the QUIC transport and hands
/// them to the local openraft engine.
#[async_trait::async_trait]
impl nauka_transport::server::RaftHandler for RaftApp {
    async fn handle(&self, rpc: nauka_transport::protocol::RaftRpc) -> Result<Vec<u8>, String> {
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
                // An election in progress, observed from the side that is
                // being asked. A term that moved between two scrapes only
                // suggests one; this proves it, and says whether we backed
                // the candidate.
                telemetry::record_vote_received(resp.vote_granted);
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
                Ok(AdminResponse::ForwardTo {
                    leader: Some((_, leader_addr)),
                }) => {
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
/// Reads the replicated S3 view from a node (any node: the state is
/// replicated, so this needs no leader).
pub async fn fetch_s3_state(client: &nauka_transport::PeerClient) -> Result<nauka_s3::S3State> {
    match admin_call(client, &AdminRequest::S3State).await? {
        AdminResponse::S3State(state) => Ok(*state),
        other => anyhow::bail!("unexpected response: {other:?}"),
    }
}

pub async fn write_via_leader(
    peers: &[std::net::SocketAddr],
    cmd: AppCommand,
) -> Result<AppResponse> {
    match admin_via_leader(peers, &AdminRequest::Write(cmd)).await? {
        AdminResponse::Ok(resp) => Ok(resp),
        other => anyhow::bail!("unexpected response: {other:?}"),
    }
}
