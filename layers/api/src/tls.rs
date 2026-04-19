//! TLS config helpers for the axum daemon server.
//!
//! The node's mesh identity stores a TLS cert + key (see
//! `MeshState::tls_cert` / `tls_key`) that gets provisioned during
//! `nauka hypervisor init` and encrypted at rest. 342-D2 reuses
//! those same bytes to terminate HTTPS on `:4000`: one PKI chain
//! for Raft + HTTP API + (future) mesh-to-mesh calls, instead of a
//! parallel cert provisioning surface.
//!
//! Unlike the Raft `TlsConfig` which requires client certs
//! (mTLS between nodes), the HTTP server is one-way TLS: the CLI
//! verifies the daemon's cert, the daemon does not check the
//! CLI's. Bearer-token auth on the request body is what gates
//! callers, not cert presence.

use std::io::BufReader;
use std::sync::Arc;

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::ServerConfig;

#[derive(Debug, thiserror::Error)]
pub enum TlsError {
    #[error("parse cert PEM: {0}")]
    ParseCert(String),
    #[error("parse key PEM: {0}")]
    ParseKey(String),
    #[error("rustls config: {0}")]
    Config(String),
}

/// Build a `rustls::ServerConfig` from the cert + key PEM strings
/// the daemon already decrypts on `MeshState::load`. Installs
/// nothing global — the caller is expected to have initialised a
/// `rustls::crypto` provider once at process startup.
pub fn server_config(cert_pem: &str, key_pem: &str) -> Result<Arc<ServerConfig>, TlsError> {
    let certs: Vec<CertificateDer> = rustls_pemfile::certs(&mut BufReader::new(cert_pem.as_bytes()))
        .collect::<Result<_, _>>()
        .map_err(|e| TlsError::ParseCert(e.to_string()))?;

    let key: PrivateKeyDer = rustls_pemfile::private_key(&mut BufReader::new(key_pem.as_bytes()))
        .map_err(|e| TlsError::ParseKey(e.to_string()))?
        .ok_or_else(|| TlsError::ParseKey("no private key in PEM".into()))?;

    let cfg = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| TlsError::Config(e.to_string()))?;

    Ok(Arc::new(cfg))
}
