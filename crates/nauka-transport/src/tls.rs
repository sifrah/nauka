//! Cluster cryptographic identity.
//!
//! - A **cluster key** (Ed25519 CA) generated once (`keygen`) and handed out
//!   to the nodes: holding the file = belonging to the cluster.
//! - An **Ed25519 keypair per node** (node.key, auto-generated), with a
//!   certificate signed by the CA. The Raft node-id is derived from the public
//!   key: identity is not declared, it is proven.
//! - **mTLS**: the server requires a client certificate signed by the CA, and
//!   the client verifies the server against the CA. With no keys supplied,
//!   falls back to the legacy insecure mode (encrypted but not authenticated)
//!   with a warning.
//!
//! Accepted v1 limitation: the CA is handed out to every node (any holder can
//! issue certificates). Same blast radius as a shared secret, but the link is
//! genuinely authenticated and encrypted. Per-node offline issuance comes next.

use std::path::Path;
use std::sync::{Arc, OnceLock};

use anyhow::{Context, Result};
use rcgen::{
    BasicConstraints, CertificateParams, DnType, IsCa, KeyPair, PKCS_ED25519,
};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::RootCertStore;

/// SAN shared by every node certificate: the identity being verified is
/// cluster membership (CA signature), not the address.
pub const NODE_SAN: &str = "node.nauka";

const CA_KEY_FILE: &str = "cluster-ca.key";
const CA_CERT_FILE: &str = "cluster-ca.pem";

fn ca_params() -> Result<CertificateParams> {
    let mut params = CertificateParams::new(Vec::<String>::new())?;
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.distinguished_name.push(DnType::CommonName, "yogfile-cluster-ca");
    Ok(params)
}

/// TLS material of a participant (node or CLI client): its chain, its key,
/// and the cluster root.
#[derive(Debug)]
pub struct ClusterTls {
    pub roots: RootCertStore,
    pub cert_chain: Vec<CertificateDer<'static>>,
    pub key: PrivateKeyDer<'static>,
    /// blake3 fingerprint (hex) of THIS participant's public key.
    pub fingerprint: String,
    /// Raft node-id derived from the fingerprint (first 8 bytes, little-endian).
    pub node_id: u64,
}

impl Clone for ClusterTls {
    fn clone(&self) -> Self {
        Self {
            roots: self.roots.clone(),
            cert_chain: self.cert_chain.clone(),
            key: self.key.clone_key(),
            fingerprint: self.fingerprint.clone(),
            node_id: self.node_id,
        }
    }
}

/// Generates the cluster key in `dir` (refuses to overwrite).
pub fn generate_cluster_ca(dir: &Path) -> Result<()> {
    std::fs::create_dir_all(dir)?;
    let key_path = dir.join(CA_KEY_FILE);
    let cert_path = dir.join(CA_CERT_FILE);
    if key_path.exists() || cert_path.exists() {
        anyhow::bail!("{} already exists — manual removal required", key_path.display());
    }
    let key = KeyPair::generate_for(&PKCS_ED25519)?;
    let cert = ca_params()?.self_signed(&key)?;
    std::fs::write(&key_path, key.serialize_pem())?;
    std::fs::write(&cert_path, cert.pem())?;
    // The cluster key is a secret: owner-only read permission.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

/// Loads the cluster key and builds this participant's identity.
///
/// `identity_key_path`: the participant's private key, created if missing
/// (a node's node.key, or an ephemeral CLI client key when `None`).
pub fn load_cluster_tls(keys_dir: &Path, identity_key_path: Option<&Path>) -> Result<ClusterTls> {
    let ca_key_pem = std::fs::read_to_string(keys_dir.join(CA_KEY_FILE))
        .with_context(|| format!("reading {}", keys_dir.join(CA_KEY_FILE).display()))?;
    let ca_cert_pem = std::fs::read_to_string(keys_dir.join(CA_CERT_FILE))
        .with_context(|| format!("reading {}", keys_dir.join(CA_CERT_FILE).display()))?;
    let ca_key = KeyPair::from_pem(&ca_key_pem)?;

    // Identity key: persisted for a node, ephemeral for a client.
    let identity_key = match identity_key_path {
        Some(path) if path.exists() => KeyPair::from_pem(&std::fs::read_to_string(path)?)?,
        Some(path) => {
            let key = KeyPair::generate_for(&PKCS_ED25519)?;
            std::fs::create_dir_all(path.parent().unwrap_or(Path::new(".")))?;
            std::fs::write(path, key.serialize_pem())?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
            }
            key
        }
        None => KeyPair::generate_for(&PKCS_ED25519)?,
    };

    let fingerprint = blake3::hash(&identity_key.public_key_der()).to_hex().to_string();
    let node_id = u64::from_le_bytes(
        blake3::hash(&identity_key.public_key_der()).as_bytes()[..8].try_into().unwrap(),
    );

    // Participant certificate, signed by the cluster CA. The CA object is
    // rebuilt with the SAME params as in `generate_cluster_ca`: only the
    // (issuer DN, signature) pair matters — the trust root shipped to peers
    // is still the stored ca.pem.
    let mut params = CertificateParams::new(vec![NODE_SAN.to_string()])?;
    params.distinguished_name.push(DnType::CommonName, &fingerprint[..16]);
    let ca_cert = ca_params()?.self_signed(&ca_key)?;
    let cert = params.signed_by(&identity_key, &ca_cert, &ca_key)?;

    let mut roots = RootCertStore::empty();
    for der in rustls_pemfile::certs(&mut ca_cert_pem.as_bytes()) {
        roots.add(der?)?;
    }

    Ok(ClusterTls {
        roots,
        cert_chain: vec![cert.der().clone()],
        key: PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(identity_key.serialize_der())),
        fingerprint,
        node_id,
    })
}

static CLUSTER_TLS: OnceLock<Option<Arc<ClusterTls>>> = OnceLock::new();

/// Installs the process-wide cluster identity (must happen BEFORE any endpoint
/// or connection). Without this call, everything stays in the legacy insecure
/// mode.
pub fn set_cluster_tls(tls: ClusterTls) {
    if CLUSTER_TLS.set(Some(Arc::new(tls))).is_err() {
        panic!("set_cluster_tls must be called exactly once, before any network use");
    }
}

pub(crate) fn cluster_tls() -> Option<Arc<ClusterTls>> {
    CLUSTER_TLS.get_or_init(|| None).clone()
}
