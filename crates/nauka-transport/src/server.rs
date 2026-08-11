//! Server side: a node that listens over QUIC and serves its [`ShardStore`].

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, Result};
use nauka_store::ShardStore;
use quinn::crypto::rustls::QuicServerConfig;
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use tracing::{debug, info, warn};

use crate::protocol::{read_message, write_message, RaftRpc, Request, Response, ALPN};
use crate::telemetry;

/// Extension point: the consensus layer (nauka-raft) registers here to receive
/// the Raft RPCs coming in over the transport.
#[async_trait::async_trait]
pub trait RaftHandler: Send + Sync {
    async fn handle(&self, rpc: RaftRpc) -> Result<Vec<u8>, String>;
}

/// Starts the QUIC server and serves requests until the process stops.
/// With consensus enabled, also opens the dedicated plane on port+1.
pub async fn serve(
    store: Arc<ShardStore>,
    listen: SocketAddr,
    raft: Option<Arc<dyn RaftHandler>>,
) -> Result<()> {
    match raft {
        Some(handler) => {
            let (data, consensus) = make_endpoint_pair(listen)?;
            info!(
                "node listening on {} (consensus on {})",
                data.local_addr()?,
                consensus.local_addr()?
            );
            tokio::spawn(serve_consensus_endpoint(consensus, handler.clone()));
            serve_endpoint(store, data, Some(handler)).await
        }
        None => {
            let endpoint = make_endpoint(listen)?;
            info!("node listening on {}", endpoint.local_addr()?);
            serve_endpoint(store, endpoint, None).await
        }
    }
}

/// Accept loop over an already-built endpoint (lets tests learn the effective
/// address before blocking).
pub async fn serve_endpoint(
    store: Arc<ShardStore>,
    endpoint: quinn::Endpoint,
    raft: Option<Arc<dyn RaftHandler>>,
) -> Result<()> {
    while let Some(incoming) = endpoint.accept().await {
        let store = store.clone();
        let raft = raft.clone();
        tokio::spawn(async move {
            match incoming.await {
                Ok(conn) => {
                    telemetry::record_connection(telemetry::IN, telemetry::conn::ACCEPTED);
                    handle_connection(Some(store), raft, conn).await
                }
                Err(e) => {
                    telemetry::record_connection(telemetry::IN, telemetry::conn::REJECTED);
                    warn!("connection rejected: {e}")
                }
            }
        });
    }
    Ok(())
}

/// Accept loop for the consensus plane: serves ONLY Raft RPCs. No access to
/// the store — a port collision cannot turn this plane into a rogue data
/// plane.
pub async fn serve_consensus_endpoint(
    endpoint: quinn::Endpoint,
    handler: Arc<dyn RaftHandler>,
) -> Result<()> {
    while let Some(incoming) = endpoint.accept().await {
        let handler = handler.clone();
        tokio::spawn(async move {
            match incoming.await {
                Ok(conn) => {
                    telemetry::record_connection(telemetry::IN, telemetry::conn::ACCEPTED);
                    handle_connection(None, Some(handler), conn).await
                }
                Err(e) => {
                    telemetry::record_connection(telemetry::IN, telemetry::conn::REJECTED);
                    warn!("connection rejected: {e}")
                }
            }
        });
    }
    Ok(())
}

/// Builds the data-plane server endpoint (exposed for tests, which need the
/// effective address before blocking on accept).
pub fn make_endpoint(listen: SocketAddr) -> Result<quinn::Endpoint> {
    make_endpoint_buf(listen, crate::DATA_SOCKET_BUF)
}

/// Builds a node's endpoint pair: data plane on `listen`, consensus plane on
/// port+1 (separate UDP socket — shard traffic can no longer queue up in front
/// of the Raft heartbeats). With port 0, looks for a free pair of adjacent
/// ports.
pub fn make_endpoint_pair(listen: SocketAddr) -> Result<(quinn::Endpoint, quinn::Endpoint)> {
    if listen.port() != 0 {
        let data = make_endpoint_buf(listen, crate::DATA_SOCKET_BUF)?;
        let consensus =
            make_endpoint_buf(crate::consensus_addr(listen), crate::CONSENSUS_SOCKET_BUF)?;
        return Ok((data, consensus));
    }
    for _ in 0..32 {
        let data = make_endpoint_buf(listen, crate::DATA_SOCKET_BUF)?;
        let bound = data.local_addr()?;
        match make_endpoint_buf(crate::consensus_addr(bound), crate::CONSENSUS_SOCKET_BUF) {
            Ok(consensus) => return Ok((data, consensus)),
            Err(_) => continue, // port+1 taken: draw another pair
        }
    }
    anyhow::bail!("could not find two free adjacent ports")
}

fn make_endpoint_buf(listen: SocketAddr, buf: usize) -> Result<quinn::Endpoint> {
    let mut crypto = match crate::tls::cluster_tls() {
        Some(tls) => {
            // mTLS: only holders of a certificate signed by the cluster key
            // can connect.
            let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
                Arc::new(tls.roots.clone()),
                crate::crypto_provider(),
            )
            .build()
            .context("building the client certificate verifier")?;
            rustls::ServerConfig::builder_with_provider(crate::crypto_provider())
                .with_safe_default_protocol_versions()?
                .with_client_cert_verifier(verifier)
                .with_single_cert(tls.cert_chain.clone(), tls.key.clone_key())?
        }
        None => {
            warn!(
                "INSECURE mode: no cluster key loaded — link is encrypted \
                 but peers are not authenticated"
            );
            let cert = rcgen::generate_simple_self_signed(vec!["nauka".into()])
                .context("generating the self-signed certificate")?;
            let cert_der = CertificateDer::from(cert.cert);
            let key = PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der());
            rustls::ServerConfig::builder_with_provider(crate::crypto_provider())
                .with_safe_default_protocol_versions()?
                .with_no_client_auth()
                .with_single_cert(vec![cert_der], key.into())?
        }
    };
    crypto.alpn_protocols = vec![ALPN.to_vec()];

    let mut config =
        quinn::ServerConfig::with_crypto(Arc::new(QuicServerConfig::try_from(crypto)?));
    config.transport_config(crate::transport_config());
    let socket = crate::make_socket(listen, buf)?;
    Ok(quinn::Endpoint::new(
        crate::endpoint_config(),
        Some(config),
        socket,
        Arc::new(quinn::TokioRuntime),
    )?)
}

/// Serves every request of an incoming connection, one stream per task
/// (the streams of a single connection run in parallel).
pub async fn handle_connection(
    store: Option<Arc<ShardStore>>,
    raft: Option<Arc<dyn RaftHandler>>,
    conn: quinn::Connection,
) {
    debug!("connection from {}", conn.remote_address());
    loop {
        let (mut send, mut recv) = match conn.accept_bi().await {
            Ok(s) => s,
            // The classification was already here, only unlabelled: the
            // three expected endings are how a connection normally goes
            // away, everything else is a fault. Counting them apart turns
            // "peers reconnect constantly" into a question with an answer —
            // clean churn from short-lived clients, or idle timeouts from
            // peers that keep dying.
            Err(quinn::ConnectionError::ApplicationClosed(_)) => {
                telemetry::record_close(telemetry::close::APPLICATION);
                return;
            }
            Err(quinn::ConnectionError::ConnectionClosed(_)) => {
                telemetry::record_close(telemetry::close::CONNECTION);
                return;
            }
            Err(quinn::ConnectionError::TimedOut) => {
                telemetry::record_close(telemetry::close::TIMED_OUT);
                return;
            }
            Err(e) => {
                telemetry::record_close(telemetry::close::ERROR);
                warn!("accept_bi: {e}");
                return;
            }
        };
        let store = store.clone();
        let raft = raft.clone();
        tokio::spawn(async move {
            let response = match read_message::<Request>(&mut recv).await {
                Ok(Request::Raft(rpc)) => match &raft {
                    Some(h) => match h.handle(rpc).await {
                        Ok(payload) => Response::Raft(payload),
                        Err(e) => Response::Error(e),
                    },
                    None => Response::Error("consensus is not enabled on this node".into()),
                },
                Ok(req) => match &store {
                    Some(s) => handle_request(s, req),
                    None => {
                        Response::Error("consensus plane: only Raft RPCs are accepted here".into())
                    }
                },
                Err(e) => Response::Error(format!("unreadable request: {e}")),
            };
            if let Err(e) = write_message(&mut send, &response).await {
                warn!("response not sent: {e}");
            }
            let _ = send.finish();
        });
    }
}

/// The inbound counterpart of `PeerClient::call`: every non-Raft peer RPC
/// this node serves passes through here exactly once.
///
/// Inbound Raft is deliberately not counted here. It is dispatched upstream
/// in [`handle_connection`], and the consensus plane carries its own
/// instrumentation — counting it twice under two different names would only
/// make the two disagree.
fn handle_request(store: &ShardStore, req: Request) -> Response {
    let op = telemetry::op(&req);
    let response = dispatch(store, req);
    telemetry::record_request(
        telemetry::IN,
        op,
        match &response {
            Response::Error(_) => telemetry::result::ERROR,
            _ => telemetry::result::OK,
        },
    );
    response
}

fn dispatch(store: &ShardStore, req: Request) -> Response {
    match req {
        Request::Raft(_) => unreachable!("handled upstream"),
        Request::Ping => Response::Pong,
        Request::PutShard(data) => match store.put_shard(&data) {
            Ok(hash) => Response::PutShardOk(hash),
            Err(e) => Response::Error(e.to_string()),
        },
        // Missing or corrupt → None: it makes no difference to the client, the
        // shard will be rebuilt by Reed-Solomon from the other nodes.
        Request::GetShard(hash) => Response::Shard(store.get_shard(&hash).ok()),
        Request::HasShard(hash) => Response::Has(store.has_shard(&hash)),
        // get_shard re-verifies integrity: a corrupt shard yields no proof, so
        // it is treated as missing.
        Request::ProveShard { hash, nonce } => {
            Response::Proof(store.get_shard(&hash).ok().map(|data| {
                let mut hasher = blake3::Hasher::new();
                hasher.update(&nonce);
                hasher.update(&data);
                *hasher.finalize().as_bytes()
            }))
        }
        Request::PutManifest(manifest) => match store.put_manifest(&manifest) {
            Ok(()) => Response::PutManifestOk,
            Err(e) => Response::Error(e.to_string()),
        },
        Request::GetManifest(hash) => Response::Manifest(store.get_manifest(&hash).ok()),
    }
}
