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
use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, KeyPair, PKCS_ED25519};
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
    params
        .distinguished_name
        .push(DnType::CommonName, "nauka-cluster-ca");
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

/// Prefix of a cluster token. The `1` is the derivation scheme version:
/// bumping it changes every derived key, so it must never move silently.
const TOKEN_PREFIX: &str = "nauka1_";

/// Generates a cluster token: 32 random bytes, base64url. The token IS the
/// cluster key — the Ed25519 CA (and everything downstream: certificates,
/// the DHT rendezvous) is derived from it deterministically. Same entropy
/// as the key file it replaces; what changes is ergonomics, one string
/// instead of a directory to copy around.
pub fn generate_token() -> String {
    use rand::RngCore;
    let mut secret = [0u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut secret);
    format!(
        "{TOKEN_PREFIX}{}",
        data_encoding::BASE64URL_NOPAD.encode(&secret)
    )
}

/// The CA keypair derived from a token. Deterministic: every holder of the
/// token computes the exact same key, so nodes need to share nothing else.
fn ca_keypair_from_token(token: &str) -> Result<KeyPair> {
    let payload = token
        .trim()
        .strip_prefix(TOKEN_PREFIX)
        .with_context(|| format!("a cluster token starts with {TOKEN_PREFIX}"))?;
    let secret = data_encoding::BASE64URL_NOPAD
        .decode(payload.as_bytes())
        .context("token payload is not valid base64url")?;
    let secret: [u8; 32] = secret
        .try_into()
        .map_err(|_| anyhow::anyhow!("a cluster token carries exactly 32 bytes"))?;
    let seed = blake3::derive_key("nauka cluster-ca v1", &secret);
    // PKCS#8 v1 document for an Ed25519 seed: fixed 16-byte header + seed.
    let mut der = Vec::with_capacity(48);
    der.extend_from_slice(&[
        0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x04, 0x22, 0x04,
        0x20,
    ]);
    der.extend_from_slice(&seed);
    KeyPair::from_pkcs8_der_and_sign_algo(&PrivatePkcs8KeyDer::from(der), &PKCS_ED25519)
        .context("deriving the CA key from the token")
}

/// Materializes the token's key material in `dir` (0600), overwriting: the
/// token is the source of truth and the files are a deterministic cache,
/// kept only so that every existing consumer (mTLS load, DHT derivation)
/// reads the same two files whether the cluster uses a token or key files.
pub fn materialize_token_keys(token: &str, dir: &Path) -> Result<()> {
    let key = ca_keypair_from_token(token)?;
    let cert = ca_params()?.self_signed(&key)?;
    std::fs::create_dir_all(dir)?;
    let key_path = dir.join(CA_KEY_FILE);
    std::fs::write(&key_path, key.serialize_pem())?;
    std::fs::write(dir.join(CA_CERT_FILE), cert.pem())?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

/// Generates the cluster key in `dir` (refuses to overwrite).
pub fn generate_cluster_ca(dir: &Path) -> Result<()> {
    std::fs::create_dir_all(dir)?;
    let key_path = dir.join(CA_KEY_FILE);
    let cert_path = dir.join(CA_CERT_FILE);
    if key_path.exists() || cert_path.exists() {
        anyhow::bail!(
            "{} already exists — manual removal required",
            key_path.display()
        );
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

    let fingerprint = blake3::hash(&identity_key.public_key_der())
        .to_hex()
        .to_string();
    let node_id = u64::from_le_bytes(
        blake3::hash(&identity_key.public_key_der()).as_bytes()[..8]
            .try_into()
            .unwrap(),
    );

    // Participant certificate, signed by the cluster CA. The CA object is
    // rebuilt with the SAME params as in `generate_cluster_ca`: only the
    // (issuer DN, signature) pair matters — the trust root shipped to peers
    // is still the stored ca.pem.
    let mut params = CertificateParams::new(vec![NODE_SAN.to_string()])?;
    params
        .distinguished_name
        .push(DnType::CommonName, &fingerprint[..16]);
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

#[cfg(test)]
mod token_tests {
    use super::*;

    #[test]
    fn tokens_derive_deterministically() {
        let t = generate_token();
        let a = ca_keypair_from_token(&t).unwrap();
        let b = ca_keypair_from_token(&t).unwrap();
        assert_eq!(
            a.serialize_der(),
            b.serialize_der(),
            "same token must derive the same CA on every machine"
        );
        // Whitespace from copy-paste must not change the cluster.
        let c = ca_keypair_from_token(&format!("  {t}\n")).unwrap();
        assert_eq!(a.serialize_der(), c.serialize_der());
    }

    #[test]
    fn different_tokens_are_different_clusters() {
        let a = ca_keypair_from_token(&generate_token()).unwrap();
        let b = ca_keypair_from_token(&generate_token()).unwrap();
        assert_ne!(a.serialize_der(), b.serialize_der());
    }

    #[test]
    fn malformed_tokens_are_refused() {
        assert!(ca_keypair_from_token("nope").is_err(), "missing prefix");
        assert!(
            ca_keypair_from_token("nauka1_c2hvcnQ").is_err(),
            "payload shorter than 32 bytes"
        );
        assert!(
            ca_keypair_from_token("nauka1_!!!invalid!!!").is_err(),
            "payload that is not base64url"
        );
    }

    #[test]
    fn materialized_keys_load_and_agree() {
        let t = generate_token();
        let d1 = tempfile::tempdir().unwrap();
        let d2 = tempfile::tempdir().unwrap();
        materialize_token_keys(&t, d1.path()).unwrap();
        materialize_token_keys(&t, d2.path()).unwrap();
        // Two machines materializing independently hold the same CA key…
        let k1 = std::fs::read_to_string(d1.path().join("cluster-ca.key")).unwrap();
        let k2 = std::fs::read_to_string(d2.path().join("cluster-ca.key")).unwrap();
        assert_eq!(k1, k2);
        // …and the standard loader accepts the directory as-is.
        let tls = load_cluster_tls(d1.path(), None).unwrap();
        assert!(!tls.cert_chain.is_empty());
    }
}
