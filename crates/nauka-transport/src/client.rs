//! Client side: connection to a peer and typed per-request helpers.

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{anyhow, bail, Result};
use nauka_erasure::FileManifest;
use quinn::crypto::rustls::QuicClientConfig;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};

use crate::protocol::{read_message, write_message, Request, Response, ALPN};
use crate::telemetry;

/// Upper bound on a single request/response exchange. Generous enough for a
/// 1 MiB shard over a slow WAN link, short enough that a wedged peer cannot
/// stall a scrub, an audit or a download.
const REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

/// Upper bound on establishing a connection. A dead peer cannot refuse a
/// UDP packet the way TCP sends a RST, so quinn keeps retransmitting the
/// handshake until its idle timeout — 30 s per attempt. The maintenance
/// loop connects to every owner of every shard, so a single dead node was
/// enough to freeze scrub, GC and audit indefinitely (observed: a node's
/// loop stuck for an hour after two peers died). Three seconds is far more
/// than a healthy WAN handshake needs (~2 RTT, i.e. ~70 ms across Europe).
const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);

/// Client connection to a cluster node.
#[derive(Clone)]
pub struct PeerClient {
    conn: quinn::Connection,
    pub addr: SocketAddr,
}

impl PeerClient {
    pub async fn connect(addr: SocketAddr) -> Result<Self> {
        Self::connect_buf(addr, crate::DATA_SOCKET_BUF).await
    }

    /// Connects to a node's consensus plane (small buffers: bounded latency
    /// rather than throughput). `addr` is already the consensus address.
    pub async fn connect_consensus(addr: SocketAddr) -> Result<Self> {
        Self::connect_buf(addr, crate::CONSENSUS_SOCKET_BUF).await
    }

    async fn connect_buf(addr: SocketAddr, buf: usize) -> Result<Self> {
        // The single establishment point, so the only place outbound
        // connections can be counted. `connect_inner` builds a fresh
        // endpoint every time — there is no pool to read a live count
        // off, which is exactly why the attempts have to be counted here.
        let outcome = tokio::time::timeout(CONNECT_TIMEOUT, Self::connect_inner(addr, buf)).await;
        telemetry::record_connection(
            telemetry::OUT,
            match &outcome {
                Err(_) => telemetry::conn::TIMEOUT,
                Ok(Err(_)) => telemetry::conn::ERROR,
                Ok(Ok(_)) => telemetry::conn::OK,
            },
        );
        outcome.map_err(|_| anyhow!("peer {addr} unreachable after {CONNECT_TIMEOUT:?}"))?
    }

    async fn connect_inner(addr: SocketAddr, buf: usize) -> Result<Self> {
        let socket = crate::make_socket("0.0.0.0:0".parse().unwrap(), buf)?;
        let mut endpoint = quinn::Endpoint::new(
            crate::endpoint_config(),
            None,
            socket,
            std::sync::Arc::new(quinn::TokioRuntime),
        )?;
        endpoint.set_default_client_config(client_config()?);
        // mTLS: the SNI must match the SAN of the node certificates.
        // Insecure: SNI is required by rustls but never verified.
        let server_name = if crate::tls::cluster_tls().is_some() {
            crate::tls::NODE_SAN
        } else {
            "nauka"
        };
        let conn = endpoint.connect(addr, server_name)?.await?;
        Ok(Self { conn, addr })
    }

    /// Access to the underlying quinn connection (benches, advanced uses).
    pub fn connection(&self) -> &quinn::Connection {
        &self.conn
    }

    async fn call(&self, req: Request) -> Result<Response> {
        // Every typed helper below funnels through here, so this is the one
        // place that sees every outbound RPC — and the one place where the
        // three failure modes are still distinguishable. Once the error has
        // been flattened into `anyhow`, a wedged peer and a peer that
        // politely refused look identical.
        let op = telemetry::op(&req);
        let started = std::time::Instant::now();
        let outcome = self.call_inner(req).await;
        telemetry::record_request_duration(op, started.elapsed());
        telemetry::record_request(telemetry::OUT, op, outcome.label());
        outcome.into_result(self.addr)
    }

    async fn call_inner(&self, req: Request) -> Outcome {
        // Every exchange is bounded. A peer that accepts the stream and then
        // goes silent — a stalled path, a wedged process — must surface as an
        // error the caller can retry or route around, never as an indefinite
        // hang. Callers layer their own, shorter deadlines on top.
        let exchange = async {
            let (mut send, mut recv) = self.conn.open_bi().await?;
            write_message(&mut send, &req).await?;
            send.finish()?;
            read_message::<Response>(&mut recv)
                .await
                .map_err(anyhow::Error::from)
        };
        match tokio::time::timeout(REQUEST_TIMEOUT, exchange).await {
            Err(_) => Outcome::Timeout,
            Ok(Err(e)) => Outcome::Transport(e),
            Ok(Ok(Response::Error(e))) => Outcome::PeerError(e),
            Ok(Ok(resp)) => Outcome::Ok(resp),
        }
    }

    pub async fn ping(&self) -> Result<()> {
        match self.call(Request::Ping).await? {
            Response::Pong => Ok(()),
            other => Err(unexpected(other)),
        }
    }

    pub async fn put_shard(&self, data: Vec<u8>) -> Result<String> {
        match self.call(Request::PutShard(data)).await? {
            Response::PutShardOk(hash) => Ok(hash),
            other => Err(unexpected(other)),
        }
    }

    pub async fn get_shard(&self, hash: &str) -> Result<Option<Vec<u8>>> {
        match self.call(Request::GetShard(hash.to_string())).await? {
            Response::Shard(data) => Ok(data),
            other => Err(unexpected(other)),
        }
    }

    pub async fn has_shard(&self, hash: &str) -> Result<bool> {
        match self.call(Request::HasShard(hash.to_string())).await? {
            Response::Has(b) => Ok(b),
            other => Err(unexpected(other)),
        }
    }

    /// Asks for a proof of possession of a shard: the peer must return
    /// `blake3(nonce ‖ bytes)`, which it can only do by re-reading them.
    pub async fn prove_shard(&self, hash: &str, nonce: [u8; 32]) -> Result<Option<[u8; 32]>> {
        match self
            .call(Request::ProveShard {
                hash: hash.to_string(),
                nonce,
            })
            .await?
        {
            Response::Proof(p) => Ok(p),
            other => Err(unexpected(other)),
        }
    }

    /// Proof of possession plus ownership claim (see
    /// [`Request::ProveShardOwned`]). `None` means "no usable proof": shard
    /// missing or corrupt on the peer, or the peer predates the request
    /// (a mixed-version cluster mid-deploy answers `Error`) — in every
    /// case the caller must keep its copy.
    pub async fn prove_shard_owned(
        &self,
        hash: &str,
        nonce: [u8; 32],
    ) -> Result<Option<([u8; 32], bool)>> {
        match self
            .call(Request::ProveShardOwned {
                hash: hash.to_string(),
                nonce,
            })
            .await?
        {
            Response::ProofOwned { proof, owner } => Ok(proof.map(|p| (p, owner))),
            Response::Error(_) => Ok(None),
            other => Err(unexpected(other)),
        }
    }

    pub async fn put_manifest(&self, manifest: &FileManifest) -> Result<()> {
        match self.call(Request::PutManifest(manifest.clone())).await? {
            Response::PutManifestOk => Ok(()),
            other => Err(unexpected(other)),
        }
    }

    pub async fn get_manifest(&self, file_hash: &str) -> Result<Option<FileManifest>> {
        match self
            .call(Request::GetManifest(file_hash.to_string()))
            .await?
        {
            Response::Manifest(m) => Ok(m),
            other => Err(unexpected(other)),
        }
    }

    /// Sends a Raft RPC and returns the opaque response payload.
    pub async fn raft(&self, rpc: crate::protocol::RaftRpc) -> Result<Vec<u8>> {
        match self.call(Request::Raft(rpc)).await? {
            Response::Raft(payload) => Ok(payload),
            other => Err(unexpected(other)),
        }
    }
}

/// How one exchange ended, before it is flattened into `anyhow`.
///
/// The three failure arms are the distinction the metric is built on and
/// the reason this type exists at all: a timeout means the peer is slow or
/// wedged, a transport error means the connection broke, and a peer error
/// means a healthy peer answered with a refusal — an application fault
/// that says nothing about the network. Collapsing them loses the only
/// signal that tells an operator which of the three to go and fix.
enum Outcome {
    Ok(Response),
    Timeout,
    Transport(anyhow::Error),
    PeerError(String),
}

impl Outcome {
    fn label(&self) -> &'static str {
        match self {
            Outcome::Ok(_) => telemetry::result::OK,
            Outcome::Timeout => telemetry::result::TIMEOUT,
            Outcome::Transport(_) => telemetry::result::TRANSPORT,
            Outcome::PeerError(_) => telemetry::result::PEER_ERROR,
        }
    }

    /// The error text callers already match on and log; unchanged.
    fn into_result(self, addr: SocketAddr) -> Result<Response> {
        match self {
            Outcome::Ok(resp) => Ok(resp),
            Outcome::Timeout => bail!("peer {addr} timed out after {REQUEST_TIMEOUT:?}"),
            Outcome::Transport(e) => Err(e),
            Outcome::PeerError(e) => bail!("error from peer {addr}: {e}"),
        }
    }
}

fn unexpected(resp: Response) -> anyhow::Error {
    anyhow!("unexpected response from peer: {resp:?}")
}

fn client_config() -> Result<quinn::ClientConfig> {
    let mut crypto = match crate::tls::cluster_tls() {
        Some(tls) => {
            // mTLS: verify the server against the cluster CA AND present our
            // own signed certificate.
            rustls::ClientConfig::builder_with_provider(crate::crypto_provider())
                .with_safe_default_protocol_versions()?
                .with_root_certificates(tls.roots.clone())
                .with_client_auth_cert(tls.cert_chain.clone(), tls.key.clone_key())?
        }
        None => rustls::ClientConfig::builder_with_provider(crate::crypto_provider())
            .with_safe_default_protocol_versions()?
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(SkipServerVerification(
                crate::crypto_provider(),
            )))
            .with_no_client_auth(),
    };
    crypto.alpn_protocols = vec![ALPN.to_vec()];
    let mut config = quinn::ClientConfig::new(Arc::new(QuicClientConfig::try_from(crypto)?));
    config.transport_config(crate::transport_config());
    Ok(config)
}

/// v0: accepts the peers' self-signed certificate. QUIC encryption stays on;
/// only the server identity goes unverified. To be replaced by a cluster PKI
/// (shared key / mTLS) with the membership layer.
#[derive(Debug)]
struct SkipServerVerification(Arc<rustls::crypto::CryptoProvider>);

impl ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}
