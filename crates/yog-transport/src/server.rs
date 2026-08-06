//! Côté serveur : un nœud qui écoute en QUIC et sert son [`ShardStore`].

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, Result};
use quinn::crypto::rustls::QuicServerConfig;
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use tracing::{debug, info, warn};
use yog_store::ShardStore;

use crate::protocol::{read_message, write_message, RaftRpc, Request, Response, ALPN};

/// Point d'extension : la couche consensus (yog-raft) s'enregistre ici pour
/// recevoir les RPCs Raft qui arrivent par le transport.
#[async_trait::async_trait]
pub trait RaftHandler: Send + Sync {
    async fn handle(&self, rpc: RaftRpc) -> Result<Vec<u8>, String>;
}

/// Démarre le serveur QUIC et sert les requêtes jusqu'à l'arrêt du process.
pub async fn serve(
    store: Arc<ShardStore>,
    listen: SocketAddr,
    raft: Option<Arc<dyn RaftHandler>>,
) -> Result<()> {
    let endpoint = make_endpoint(listen)?;
    info!("nœud à l'écoute sur {}", endpoint.local_addr()?);
    serve_endpoint(store, endpoint, raft).await
}

/// Boucle d'accept sur un endpoint déjà construit (permet aux tests de
/// connaître l'adresse effective avant de bloquer).
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
                Ok(conn) => handle_connection(store, raft, conn).await,
                Err(e) => warn!("connexion refusée: {e}"),
            }
        });
    }
    Ok(())
}

/// Construit l'endpoint serveur (exposé pour les tests, qui ont besoin de
/// l'adresse effective avant de bloquer sur accept).
pub fn make_endpoint(listen: SocketAddr) -> Result<quinn::Endpoint> {
    let cert = rcgen::generate_simple_self_signed(vec!["yogfile".into()])
        .context("génération du certificat auto-signé")?;
    let cert_der = CertificateDer::from(cert.cert);
    let key = PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der());

    let mut crypto = rustls::ServerConfig::builder_with_provider(crate::crypto_provider())
        .with_safe_default_protocol_versions()?
        .with_no_client_auth()
        .with_single_cert(vec![cert_der], key.into())?;
    crypto.alpn_protocols = vec![ALPN.to_vec()];

    let config = quinn::ServerConfig::with_crypto(Arc::new(QuicServerConfig::try_from(crypto)?));
    Ok(quinn::Endpoint::server(config, listen)?)
}

/// Sert toutes les requêtes d'une connexion entrante, un stream à la fois
/// par tâche (les streams d'une même connexion tournent en parallèle).
pub async fn handle_connection(
    store: Arc<ShardStore>,
    raft: Option<Arc<dyn RaftHandler>>,
    conn: quinn::Connection,
) {
    debug!("connexion de {}", conn.remote_address());
    loop {
        let (mut send, mut recv) = match conn.accept_bi().await {
            Ok(s) => s,
            Err(quinn::ConnectionError::ApplicationClosed(_))
            | Err(quinn::ConnectionError::ConnectionClosed(_))
            | Err(quinn::ConnectionError::TimedOut) => return,
            Err(e) => {
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
                    None => Response::Error("consensus inactif sur ce nœud".into()),
                },
                Ok(req) => handle_request(&store, req),
                Err(e) => Response::Error(format!("requête illisible: {e}")),
            };
            if let Err(e) = write_message(&mut send, &response).await {
                warn!("réponse non envoyée: {e}");
            }
            let _ = send.finish();
        });
    }
}

fn handle_request(store: &ShardStore, req: Request) -> Response {
    match req {
        Request::Raft(_) => unreachable!("traité en amont"),
        Request::Ping => Response::Pong,
        Request::PutShard(data) => match store.put_shard(&data) {
            Ok(hash) => Response::PutShardOk(hash),
            Err(e) => Response::Error(e.to_string()),
        },
        // Absent ou corrompu → None : côté client c'est pareil, le shard
        // sera reconstruit par Reed-Solomon depuis les autres nœuds.
        Request::GetShard(hash) => Response::Shard(store.get_shard(&hash).ok()),
        Request::HasShard(hash) => Response::Has(store.has_shard(&hash)),
        Request::PutManifest(manifest) => match store.put_manifest(&manifest) {
            Ok(()) => Response::PutManifestOk,
            Err(e) => Response::Error(e.to_string()),
        },
        Request::GetManifest(hash) => Response::Manifest(store.get_manifest(&hash).ok()),
    }
}
