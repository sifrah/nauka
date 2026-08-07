//! Côté client : connexion à un peer et helpers typés par requête.

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{anyhow, bail, Result};
use quinn::crypto::rustls::QuicClientConfig;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use yog_erasure::FileManifest;

use crate::protocol::{read_message, write_message, Request, Response, ALPN};

/// Connexion client vers un nœud du cluster.
#[derive(Clone)]
pub struct PeerClient {
    conn: quinn::Connection,
    pub addr: SocketAddr,
}

impl PeerClient {
    pub async fn connect(addr: SocketAddr) -> Result<Self> {
        Self::connect_buf(addr, crate::DATA_SOCKET_BUF).await
    }

    /// Connexion au plan consensus d'un nœud (petits buffers : latence
    /// bornée plutôt que débit). `addr` est déjà l'adresse consensus.
    pub async fn connect_consensus(addr: SocketAddr) -> Result<Self> {
        Self::connect_buf(addr, crate::CONSENSUS_SOCKET_BUF).await
    }

    async fn connect_buf(addr: SocketAddr, buf: usize) -> Result<Self> {
        let socket = crate::make_socket("0.0.0.0:0".parse().unwrap(), buf)?;
        let mut endpoint = quinn::Endpoint::new(
            crate::endpoint_config(),
            None,
            socket,
            std::sync::Arc::new(quinn::TokioRuntime),
        )?;
        endpoint.set_default_client_config(client_config()?);
        // mTLS: le SNI doit correspondre au SAN des certificats de nœud.
        // Insecure: SNI requis par rustls mais non vérifié.
        let server_name = if crate::tls::cluster_tls().is_some() {
            crate::tls::NODE_SAN
        } else {
            "yogfile"
        };
        let conn = endpoint.connect(addr, server_name)?.await?;
        Ok(Self { conn, addr })
    }

    /// Accès à la connexion quinn sous-jacente (benchs, usages avancés).
    pub fn connection(&self) -> &quinn::Connection {
        &self.conn
    }

    async fn call(&self, req: Request) -> Result<Response> {
        let (mut send, mut recv) = self.conn.open_bi().await?;
        write_message(&mut send, &req).await?;
        send.finish()?;
        let resp = read_message::<Response>(&mut recv).await?;
        if let Response::Error(e) = resp {
            bail!("erreur du peer {}: {e}", self.addr);
        }
        Ok(resp)
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

    /// Demande une preuve de détention d'un shard : le pair doit renvoyer
    /// `blake3(nonce ‖ octets)`, ce qu'il ne peut faire qu'en les relisant.
    pub async fn prove_shard(&self, hash: &str, nonce: [u8; 32]) -> Result<Option<[u8; 32]>> {
        match self.call(Request::ProveShard { hash: hash.to_string(), nonce }).await? {
            Response::Proof(p) => Ok(p),
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
        match self.call(Request::GetManifest(file_hash.to_string())).await? {
            Response::Manifest(m) => Ok(m),
            other => Err(unexpected(other)),
        }
    }

    /// Envoie une RPC Raft et retourne le payload de réponse opaque.
    pub async fn raft(&self, rpc: crate::protocol::RaftRpc) -> Result<Vec<u8>> {
        match self.call(Request::Raft(rpc)).await? {
            Response::Raft(payload) => Ok(payload),
            other => Err(unexpected(other)),
        }
    }
}

fn unexpected(resp: Response) -> anyhow::Error {
    anyhow!("réponse inattendue du peer: {resp:?}")
}

fn client_config() -> Result<quinn::ClientConfig> {
    let mut crypto = match crate::tls::cluster_tls() {
        Some(tls) => {
            // mTLS : vérifie le serveur contre la CA du cluster ET présente
            // notre certificat signé.
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

/// v0 : accepte le certificat auto-signé des peers. Le chiffrement QUIC reste
/// actif ; seule l'identité du serveur n'est pas vérifiée. À remplacer par une
/// PKI de cluster (clé partagée / mTLS) avec la couche membership.
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
