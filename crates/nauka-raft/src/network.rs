//! openraft networking carried over nauka-transport's QUIC.
//!
//! Every Raft RPC is serialized with bincode and sent as
//! `Request::Raft(...)`; the remote node hands it to its Raft instance
//! through the [`RaftHandler`] registered on its server.

use std::net::SocketAddr;

use nauka_transport::protocol::RaftRpc;
use nauka_transport::PeerClient;
use openraft::error::{InstallSnapshotError, RPCError, RaftError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::BasicNode;

use crate::types::{NodeId, TypeConfig};

/// Builds one network connection per target node.
#[derive(Clone, Default)]
pub struct QuicRaftNetworkFactory;

pub struct QuicRaftClient {
    addr: SocketAddr,
    /// The peer's advertised (data-plane) address, used as the `peer` label
    /// on this node's RPC metrics. Deliberately the advertised address and
    /// not the consensus one: it is the identity the rest of the cluster
    /// knows the node by, so the label joins against `nauka_build_info` and
    /// against the membership.
    peer: String,
    client: Option<PeerClient>,
}

impl RaftNetworkFactory<TypeConfig> for QuicRaftNetworkFactory {
    type Network = QuicRaftClient;

    async fn new_client(&mut self, _target: NodeId, node: &BasicNode) -> Self::Network {
        // Membership stores the data address; Raft RPCs go over the dedicated
        // consensus plane (port+1) — never queued behind shards.
        let data: std::net::SocketAddr = node
            .addr
            .parse()
            .expect("invalid node address in membership");
        QuicRaftClient {
            addr: nauka_transport::consensus_addr(data),
            peer: node.addr.clone(),
            client: None,
        }
    }
}

impl QuicRaftClient {
    /// The single choke point for every outbound Raft RPC — and therefore
    /// the only place per-peer RPC health has to be measured. `rpc` names
    /// the caller for the metric label; it is one of the three constants in
    /// [`crate::telemetry`], never a formatted value.
    async fn call<Req, Resp>(
        &mut self,
        wrap: fn(Vec<u8>) -> RaftRpc,
        rpc: &'static str,
        req: &Req,
        option: RPCOption,
    ) -> Result<Resp, Unreachable>
    where
        Req: serde::Serialize,
        Resp: serde::de::DeserializeOwned,
    {
        // Enforce openraft's deadline OURSELVES, a hair early. openraft
        // also wraps this call in a timeout, but that one cancels the
        // future from the outside: our error path never runs, so a
        // connection that went bad stays cached and every later RPC to
        // that peer fails the same way. Owning the deadline lets us drop
        // the dead client and reconnect on the next attempt.
        let budget = option.hard_ttl().mul_f32(0.9);
        let started = std::time::Instant::now();
        match tokio::time::timeout(budget, self.call_inner(wrap, req)).await {
            Ok(Ok(resp)) => {
                crate::telemetry::record_rpc(&self.peer, rpc, started.elapsed());
                Ok(resp)
            }
            // Answered nothing, but not by running out of time: no
            // connection, a connection that died mid-call, or a payload we
            // could not put on the wire.
            Ok(Err(e)) => {
                crate::telemetry::record_rpc_failure(
                    &self.peer,
                    rpc,
                    crate::telemetry::FAIL_UNREACHABLE,
                );
                Err(e)
            }
            // Silence for the whole budget. Distinct from unreachable: the
            // peer may be alive and merely wedged, which is the difference
            // between "restart the node" and "look at its disk".
            Err(_) => {
                self.client = None;
                crate::telemetry::record_rpc_failure(
                    &self.peer,
                    rpc,
                    crate::telemetry::FAIL_TIMEOUT,
                );
                Err(Unreachable::new(&IoErr(format!(
                    "{} did not answer within {budget:?}",
                    self.addr
                ))))
            }
        }
    }

    /// A peer that answered, and refused. Counted apart from a peer that did
    /// not answer at all: a cluster full of `rejected` is a consensus
    /// problem, a cluster full of `unreachable` is a network one.
    fn note_rejected(&self, rpc: &'static str) {
        crate::telemetry::record_rpc_failure(&self.peer, rpc, crate::telemetry::FAIL_REJECTED);
    }

    async fn call_inner<Req, Resp>(
        &mut self,
        wrap: fn(Vec<u8>) -> RaftRpc,
        req: &Req,
    ) -> Result<Resp, Unreachable>
    where
        Req: serde::Serialize,
        Resp: serde::de::DeserializeOwned,
    {
        if self.client.is_none() {
            self.client = Some(
                PeerClient::connect_consensus(self.addr)
                    .await
                    .map_err(|e| Unreachable::new(&IoErr(e.to_string())))?,
            );
        }
        let payload = bincode::serialize(req).map_err(|e| Unreachable::new(&e))?;
        let client = self.client.as_ref().unwrap();
        match client.raft(wrap(payload)).await {
            Ok(bytes) => bincode::deserialize(&bytes).map_err(|e| Unreachable::new(&e)),
            Err(e) => {
                // Dead connection: drop the client so the next attempt
                // reconnects.
                self.client = None;
                Err(Unreachable::new(&IoErr(e.to_string())))
            }
        }
    }
}

impl RaftNetwork<TypeConfig> for QuicRaftClient {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let resp: AppendEntriesResponse<NodeId> = self
            .call(
                RaftRpc::AppendEntries,
                crate::telemetry::RPC_APPEND_ENTRIES,
                &rpc,
                option,
            )
            .await
            .map_err(RPCError::Unreachable)?;
        // `Conflict` is the follower saying our previous log id does not
        // match — normal once per follower while a fresh leader backtracks,
        // pathological if it keeps happening. `HigherVote` is the follower
        // telling us we are not the leader any more.
        if matches!(
            resp,
            AppendEntriesResponse::Conflict | AppendEntriesResponse::HigherVote(_)
        ) {
            self.note_rejected(crate::telemetry::RPC_APPEND_ENTRIES);
        }
        Ok(resp)
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        self.call(
            RaftRpc::InstallSnapshot,
            crate::telemetry::RPC_INSTALL_SNAPSHOT,
            &rpc,
            option,
        )
        .await
        .map_err(RPCError::Unreachable)
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let resp: VoteResponse<NodeId> = self
            .call(RaftRpc::Vote, crate::telemetry::RPC_VOTE, &rpc, option)
            .await
            .map_err(RPCError::Unreachable)?;
        // A denied vote is the other half of a contested election, seen
        // from the candidate's side.
        if !resp.vote_granted {
            self.note_rejected(crate::telemetry::RPC_VOTE);
        }
        Ok(resp)
    }
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
struct IoErr(String);
