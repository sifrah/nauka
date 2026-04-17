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
use rustls::pki_types::ServerName;
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

use super::tls::{TlsConfig, RAFT_TLS_SAN};
use super::types::TypeConfig;

pub const DEFAULT_RAFT_PORT: u16 = 4001;

pub struct NetworkFactory {
    pub tls: Option<TlsConfig>,
}

impl openraft::network::RaftNetworkFactory<TypeConfig> for NetworkFactory {
    type Network = NetworkClient;

    async fn new_client(&mut self, target: u64, node: &BasicNode) -> Self::Network {
        eprintln!(
            "  raft network: new_client target={target} addr={}",
            node.addr
        );
        NetworkClient {
            addr: node.addr.clone(),
            tls: self.tls.clone(),
        }
    }
}

pub struct NetworkClient {
    addr: String,
    tls: Option<TlsConfig>,
}

pub(super) async fn rpc_over<S, Req, Resp>(
    stream: S,
    uri: &str,
    req: &Req,
) -> Result<Resp, io::Error>
where
    S: AsyncRead + AsyncWrite + Unpin,
    Req: Serialize,
    Resp: DeserializeOwned,
{
    let (reader, mut writer) = tokio::io::split(stream);

    let payload = serde_json::json!({ "rpc": uri, "body": req });
    let mut line = serde_json::to_string(&payload)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    line.push('\n');
    writer.write_all(line.as_bytes()).await?;

    let mut lines = BufReader::new(reader).lines();
    let resp_line = lines
        .next_line()
        .await?
        .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "no response"))?;

    serde_json::from_str(&resp_line).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

impl NetworkClient {
    async fn rpc<Req: Serialize, Resp: DeserializeOwned>(
        &self,
        uri: &str,
        req: &Req,
    ) -> Result<Resp, RPCError<TypeConfig>> {
        let tcp = TcpStream::connect(&self.addr)
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;

        if let Some(ref tls) = self.tls {
            let server_name = ServerName::try_from(RAFT_TLS_SAN)
                .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
            let connector = tokio_rustls::TlsConnector::from(tls.client.clone());
            let stream = connector
                .connect(server_name, tcp)
                .await
                .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
            rpc_over(stream, uri, req)
                .await
                .map_err(|e| RPCError::Network(NetworkError::new(&e)))
        } else {
            rpc_over(tcp, uri, req)
                .await
                .map_err(|e| RPCError::Network(NetworkError::new(&e)))
        }
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

        let tls = self.tls.clone();
        let addr = self.addr.clone();

        let send_fut = async move {
            let tcp = TcpStream::connect(&addr)
                .await
                .map_err(|e| StreamingError::Unreachable(Unreachable::new(&e)))?;

            let mut line = serde_json::to_string(&payload)
                .map_err(|e| StreamingError::Network(NetworkError::new(&e)))?;
            line.push('\n');

            if let Some(ref tls) = tls {
                let server_name = ServerName::try_from(RAFT_TLS_SAN)
                    .map_err(|e| StreamingError::Network(NetworkError::new(&e)))?;
                let connector = tokio_rustls::TlsConnector::from(tls.client.clone());
                let mut stream = connector
                    .connect(server_name, tcp)
                    .await
                    .map_err(|e| StreamingError::Network(NetworkError::new(&e)))?;

                stream
                    .write_all(line.as_bytes())
                    .await
                    .map_err(|e| StreamingError::Network(NetworkError::new(&e)))?;

                let mut lines = BufReader::new(stream).lines();
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
                serde_json::from_str(&resp_line)
                    .map_err(|e| StreamingError::Network(NetworkError::new(&e)))
            } else {
                let mut stream = tcp;
                stream
                    .write_all(line.as_bytes())
                    .await
                    .map_err(|e| StreamingError::Network(NetworkError::new(&e)))?;

                let mut lines = BufReader::new(stream).lines();
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
                serde_json::from_str(&resp_line)
                    .map_err(|e| StreamingError::Network(NetworkError::new(&e)))
            }
        };

        tokio::select! {
            result = send_fut => result,
            closed = cancel => Err(StreamingError::Closed(closed)),
        }
    }
}
