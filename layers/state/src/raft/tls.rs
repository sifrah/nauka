use std::io::BufReader;
use std::sync::Arc;

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::{ClientConfig, RootCertStore, ServerConfig};

use crate::StateError;

pub const RAFT_TLS_SAN: &str = "raft.nauka.local";

#[derive(Clone)]
pub struct TlsConfig {
    pub client: Arc<ClientConfig>,
    pub server: Arc<ServerConfig>,
}

impl TlsConfig {
    pub fn new(
        ca_cert_pem: &str,
        node_cert_pem: &str,
        node_key_pem: &str,
    ) -> Result<Self, StateError> {
        let ca_certs = rustls_pemfile::certs(&mut BufReader::new(ca_cert_pem.as_bytes()))
            .collect::<Result<Vec<CertificateDer>, _>>()
            .map_err(|e| StateError::Raft(format!("parse CA cert: {e}")))?;

        let node_certs = rustls_pemfile::certs(&mut BufReader::new(node_cert_pem.as_bytes()))
            .collect::<Result<Vec<CertificateDer>, _>>()
            .map_err(|e| StateError::Raft(format!("parse node cert: {e}")))?;

        let node_key: PrivateKeyDer =
            rustls_pemfile::private_key(&mut BufReader::new(node_key_pem.as_bytes()))
                .map_err(|e| StateError::Raft(format!("parse node key: {e}")))?
                .ok_or_else(|| StateError::Raft("no private key in PEM".into()))?;

        let mut root_store = RootCertStore::empty();
        for cert in &ca_certs {
            root_store
                .add(cert.clone())
                .map_err(|e| StateError::Raft(format!("add CA cert: {e}")))?;
        }

        let client = ClientConfig::builder()
            .with_root_certificates(root_store.clone())
            .with_client_auth_cert(node_certs.clone(), node_key.clone_key())
            .map_err(|e| StateError::Raft(format!("client config: {e}")))?;

        let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(root_store))
            .build()
            .map_err(|e| StateError::Raft(format!("client verifier: {e}")))?;

        let server = ServerConfig::builder()
            .with_client_cert_verifier(verifier)
            .with_single_cert(node_certs, node_key)
            .map_err(|e| StateError::Raft(format!("server config: {e}")))?;

        Ok(Self {
            client: Arc::new(client),
            server: Arc::new(server),
        })
    }
}
