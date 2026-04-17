use std::io::Cursor;

use std::collections::BTreeSet;

use openraft::alias::{SnapshotMetaOf, SnapshotOf, VoteOf};
use openraft::raft::{AppendEntriesRequest, VoteRequest};
use openraft::{BasicNode, ChangeMembers};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

use super::tls::TlsConfig;
use super::types::{SurqlCommand, TypeConfig};
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

    tracing::info!(
        event = "raft.server.listen",
        bind_addr,
        tls = acceptor.is_some(),
        "raft server listening"
    );

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
                        tracing::warn!(
                            event = "raft.server.tls.handshake_failed",
                            peer = %peer,
                            error = %e,
                            "raft tls handshake failed"
                        );
                        return;
                    }
                }
            } else {
                handle_rpc(stream, raft).await
            };

            if let Err(e) = result {
                tracing::warn!(
                    event = "raft.server.rpc.error",
                    peer = %peer,
                    error = %e,
                    "raft rpc error"
                );
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
        "app_write" => {
            // Followers forward application writes to the leader via this RPC.
            // On the leader, client_write actually commits the entry. On a
            // non-leader receiver, client_write still returns an error — we
            // surface it so the caller can retry somewhere else.
            let cmd: SurqlCommand = serde_json::from_value(body.clone())?;
            let resp = raft
                .client_write(cmd)
                .await
                .map_err(|e| format!("client_write: {e}"))?;
            serde_json::to_string(resp.response())?
        }
        "membership" => {
            // Followers forward membership changes (add_learner,
            // promote_voter) to the leader the same way app_write works.
            let op = body["op"].as_str().unwrap_or("");
            let node_id = body["node_id"].as_u64().ok_or("missing node_id")?;
            match op {
                "add_learner" => {
                    let addr = body["addr"].as_str().ok_or("missing addr")?;
                    match raft.add_learner(node_id, BasicNode::new(addr), true).await {
                        Ok(_) => serde_json::json!({ "ok": true }).to_string(),
                        Err(e) => serde_json::json!({ "error": e.to_string() }).to_string(),
                    }
                }
                "promote_voter" => {
                    let mut ids = BTreeSet::new();
                    ids.insert(node_id);
                    match raft
                        .change_membership(ChangeMembers::AddVoterIds(ids), true)
                        .await
                    {
                        Ok(_) => serde_json::json!({ "ok": true }).to_string(),
                        Err(e) => serde_json::json!({ "error": e.to_string() }).to_string(),
                    }
                }
                other => serde_json::json!({ "error": format!("unknown membership op: {other}") })
                    .to_string(),
            }
        }
        other => return Err(format!("unknown rpc: {other}").into()),
    };

    writer.write_all(response.as_bytes()).await?;
    writer.write_all(b"\n").await?;
    Ok(())
}
