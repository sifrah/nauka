use std::io::Cursor;

use openraft::alias::{SnapshotMetaOf, SnapshotOf, VoteOf};
use openraft::raft::{AppendEntriesRequest, VoteRequest};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

use super::tls::TlsConfig;
use super::types::TypeConfig;
use super::Raft;
use crate::StateError;

pub async fn start_raft_server(
    raft: Raft,
    bind_addr: &str,
    tls: Option<TlsConfig>,
) -> Result<(), StateError> {
    let listener = TcpListener::bind(bind_addr)
        .await
        .map_err(|e| StateError::Network(format!("raft bind {bind_addr}: {e}")))?;

    let acceptor = tls.map(|t| tokio_rustls::TlsAcceptor::from(t.server));

    eprintln!("  raft server listening on {bind_addr} (tls={})", acceptor.is_some());

    loop {
        let (stream, peer) = listener
            .accept()
            .await
            .map_err(|e| StateError::Network(e.to_string()))?;

        let raft = raft.clone();
        let acceptor = acceptor.clone();

        tokio::spawn(async move {
            let result = if let Some(ref acc) = acceptor {
                match acc.accept(stream).await {
                    Ok(tls_stream) => handle_rpc(tls_stream, raft).await,
                    Err(e) => {
                        eprintln!("  raft tls handshake failed from {peer}: {e}");
                        return;
                    }
                }
            } else {
                handle_rpc(stream, raft).await
            };

            if let Err(e) = result {
                eprintln!("  raft rpc error from {peer}: {e}");
            }
        });
    }
}

async fn handle_rpc<S: AsyncRead + AsyncWrite + Unpin + Send>(
    stream: S,
    raft: Raft,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (reader, mut writer) = tokio::io::split(stream);
    let mut lines = BufReader::new(reader).lines();

    let line = lines.next_line().await?.ok_or("empty request")?;

    let msg: serde_json::Value = serde_json::from_str(&line)?;
    let rpc = msg["rpc"].as_str().unwrap_or("");
    let body = &msg["body"];

    let response = match rpc {
        "append" => {
            let req: AppendEntriesRequest<TypeConfig> = serde_json::from_value(body.clone())?;
            let resp = raft.append_entries(req).await?;
            serde_json::to_string(&resp)?
        }
        "vote" => {
            let req: VoteRequest<TypeConfig> = serde_json::from_value(body.clone())?;
            let resp = raft.vote(req).await?;
            serde_json::to_string(&resp)?
        }
        "snapshot" => {
            let vote: VoteOf<TypeConfig> = serde_json::from_value(body["vote"].clone())?;
            let meta: SnapshotMetaOf<TypeConfig> = serde_json::from_value(body["meta"].clone())?;
            let data: Vec<u8> = serde_json::from_value(body["data"].clone())?;

            let snapshot = SnapshotOf::<TypeConfig> {
                meta,
                snapshot: Cursor::new(data),
            };

            let resp = raft
                .install_full_snapshot(vote, snapshot)
                .await
                .map_err(|e| format!("install_full_snapshot: {e}"))?;
            serde_json::to_string(&resp)?
        }
        other => return Err(format!("unknown rpc: {other}").into()),
    };

    writer.write_all(response.as_bytes()).await?;
    writer.write_all(b"\n").await?;
    Ok(())
}
