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
        let mut endpoint = quinn::Endpoint::client("0.0.0.0:0".parse().unwrap())?;
        endpoint.set_default_client_config(client_config()?);
        // Le SNI est requis par rustls mais non vérifié (cluster fermé, v0).
        let conn = endpoint.connect(addr, "yogfile")?.await?;
        Ok(Self { conn, addr })
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
}

fn unexpected(resp: Response) -> anyhow::Error {
    anyhow!("réponse inattendue du peer: {resp:?}")
}

fn client_config() -> Result<quinn::ClientConfig> {
    let mut crypto = rustls::ClientConfig::builder_with_provider(crate::crypto_provider())
        .with_safe_default_protocol_versions()?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(SkipServerVerification(
            crate::crypto_provider(),
        )))
        .with_no_client_auth();
    crypto.alpn_protocols = vec![ALPN.to_vec()];
    Ok(quinn::ClientConfig::new(Arc::new(QuicClientConfig::try_from(
        crypto,
    )?)))
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
