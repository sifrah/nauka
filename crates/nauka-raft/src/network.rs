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
            client: None,
        }
    }
}

impl QuicRaftClient {
    async fn call<Req, Resp>(
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
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        self.call(RaftRpc::AppendEntries, &rpc)
            .await
            .map_err(RPCError::Unreachable)
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        self.call(RaftRpc::InstallSnapshot, &rpc)
            .await
            .map_err(RPCError::Unreachable)
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        self.call(RaftRpc::Vote, &rpc)
            .await
            .map_err(RPCError::Unreachable)
    }
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
struct IoErr(String);
