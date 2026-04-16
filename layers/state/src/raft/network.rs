use std::future::Future;
use std::io;

use openraft::alias::{SnapshotOf, VoteOf};
use openraft::error::{NetworkError, RPCError, ReplicationClosed, StreamingError, Unreachable};
use openraft::network::v2::RaftNetworkV2;
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::{BasicNode, OptionalSend};
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

use super::types::TypeConfig;

pub const DEFAULT_RAFT_PORT: u16 = 4001;

pub struct NetworkFactory;

impl openraft::network::RaftNetworkFactory<TypeConfig> for NetworkFactory {
    type Network = NetworkClient;

    async fn new_client(&mut self, target: u64, node: &BasicNode) -> Self::Network {
        eprintln!("  raft network: new_client target={target} addr={}", node.addr);
        NetworkClient {
            addr: node.addr.clone(),
        }
    }
}

pub struct NetworkClient {
    addr: String,
}

impl NetworkClient {
    async fn rpc<Req: Serialize, Resp: DeserializeOwned>(
        &self,
        uri: &str,
        req: &Req,
    ) -> Result<Resp, RPCError<TypeConfig>> {
        let mut stream = TcpStream::connect(&self.addr)
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;

        let payload = serde_json::json!({ "rpc": uri, "body": req });
        let mut line = serde_json::to_string(&payload)
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        line.push('\n');

        stream
            .write_all(line.as_bytes())
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        let reader = BufReader::new(stream);
        let mut lines = reader.lines();
        let resp_line = lines
            .next_line()
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?
            .ok_or_else(|| {
                RPCError::Network(NetworkError::new(&io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "no response",
                )))
            })?;

        serde_json::from_str(&resp_line).map_err(|e| RPCError::Network(NetworkError::new(&e)))
    }
}

impl RaftNetworkV2<TypeConfig> for NetworkClient {
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<TypeConfig>, RPCError<TypeConfig>> {
        eprintln!("  raft: sending append_entries to {}", self.addr);
        let result = self.rpc("append", &req).await;
        if let Err(ref e) = result {
            eprintln!("  raft: append_entries failed: {e}");
        }
        result
    }

    async fn vote(
        &mut self,
        req: VoteRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        eprintln!("  raft: sending vote to {}", self.addr);
        self.rpc("vote", &req).await
    }

    async fn full_snapshot(
        &mut self,
        vote: VoteOf<TypeConfig>,
        snapshot: SnapshotOf<TypeConfig>,
        cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
        let data = snapshot.snapshot.into_inner();

        let payload = serde_json::json!({
            "rpc": "snapshot",
            "body": {
                "vote": vote,
                "meta": snapshot.meta,
                "data": data,
            }
        });

        let send_fut = async {
            let mut stream = TcpStream::connect(&self.addr)
                .await
                .map_err(|e| StreamingError::Unreachable(Unreachable::new(&e)))?;

            let mut line = serde_json::to_string(&payload)
                .map_err(|e| StreamingError::Network(NetworkError::new(&e)))?;
            line.push('\n');

            stream
                .write_all(line.as_bytes())
                .await
                .map_err(|e| StreamingError::Network(NetworkError::new(&e)))?;

            let reader = BufReader::new(stream);
            let mut lines = reader.lines();
            let resp_line = lines
                .next_line()
                .await
                .map_err(|e| StreamingError::Network(NetworkError::new(&e)))?
                .ok_or_else(|| {
                    StreamingError::Network(NetworkError::new(&io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "no snapshot response",
                    )))
                })?;

            let resp: SnapshotResponse<TypeConfig> = serde_json::from_str(&resp_line)
                .map_err(|e| StreamingError::Network(NetworkError::new(&e)))?;

            Ok(resp)
        };

        tokio::select! {
            result = send_fut => result,
            closed = cancel => Err(StreamingError::Closed(closed)),
        }
    }
}
