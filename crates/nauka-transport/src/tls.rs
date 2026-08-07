//! Identité cryptographique du cluster.
//!
//! - Une **clé de cluster** (CA Ed25519) générée une fois (`keygen`) et
//!   distribuée aux nœuds : posséder le fichier = appartenir au cluster.
//! - Une **keypair Ed25519 par nœud** (node.key, auto-générée), certificat
//!   signé par la CA. Le node-id Raft est dérivé de la clé publique :
//!   l'identité ne se décrète pas, elle se prouve.
//! - **mTLS** : le serveur exige un certificat client signé par la CA, le
//!   client vérifie le serveur contre la CA. Sans clés fournies, mode
//!   insecure historique (chiffré mais non authentifié) avec warning.
//!
//! Limite v1 assumée : la CA est distribuée à tous les nœuds (n'importe
//! quel détenteur peut émettre des certificats). Blast radius identique à
//! un secret partagé, mais le lien est réellement authentifié et chiffré.
//! L'émission hors-ligne par nœud viendra ensuite.

use std::path::Path;
use std::sync::{Arc, OnceLock};

use anyhow::{Context, Result};
use rcgen::{
    BasicConstraints, CertificateParams, DnType, IsCa, KeyPair, PKCS_ED25519,
};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::RootCertStore;

/// SAN commun à tous les certificats de nœud : l'identité vérifiée est
/// l'appartenance au cluster (signature CA), pas l'adresse.
pub const NODE_SAN: &str = "node.nauka";

const CA_KEY_FILE: &str = "cluster-ca.key";
const CA_CERT_FILE: &str = "cluster-ca.pem";

fn ca_params() -> Result<CertificateParams> {
    let mut params = CertificateParams::new(Vec::<String>::new())?;
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.distinguished_name.push(DnType::CommonName, "yogfile-cluster-ca");
    Ok(params)
}

/// Matériel TLS d'un participant (nœud ou client CLI) : sa chaîne, sa clé,
/// et la racine du cluster.
#[derive(Debug)]
pub struct ClusterTls {
    pub roots: RootCertStore,
    pub cert_chain: Vec<CertificateDer<'static>>,
    pub key: PrivateKeyDer<'static>,
    /// Empreinte blake3 (hex) de la clé publique de CE participant.
    pub fingerprint: String,
    /// Node-id Raft dérivé de l'empreinte (8 premiers octets, little-endian).
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

/// Génère la clé de cluster dans `dir` (refuse d'écraser).
pub fn generate_cluster_ca(dir: &Path) -> Result<()> {
    std::fs::create_dir_all(dir)?;
    let key_path = dir.join(CA_KEY_FILE);
    let cert_path = dir.join(CA_CERT_FILE);
    if key_path.exists() || cert_path.exists() {
        anyhow::bail!("{} existe déjà — suppression manuelle requise", key_path.display());
    }
    let key = KeyPair::generate_for(&PKCS_ED25519)?;
    let cert = ca_params()?.self_signed(&key)?;
    std::fs::write(&key_path, key.serialize_pem())?;
    std::fs::write(&cert_path, cert.pem())?;
    // La clé de cluster est un secret : lecture propriétaire uniquement.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

/// Charge la clé de cluster et construit l'identité de ce participant.
///
/// `identity_key_path` : la clé privée du participant, créée si absente
/// (node.key d'un nœud, ou clé éphémère d'un client CLI si `None`).
pub fn load_cluster_tls(keys_dir: &Path, identity_key_path: Option<&Path>) -> Result<ClusterTls> {
    let ca_key_pem = std::fs::read_to_string(keys_dir.join(CA_KEY_FILE))
        .with_context(|| format!("lecture de {}", keys_dir.join(CA_KEY_FILE).display()))?;
    let ca_cert_pem = std::fs::read_to_string(keys_dir.join(CA_CERT_FILE))
        .with_context(|| format!("lecture de {}", keys_dir.join(CA_CERT_FILE).display()))?;
    let ca_key = KeyPair::from_pem(&ca_key_pem)?;

    // Clé d'identité : persistée pour un nœud, éphémère pour un client.
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

    // Certificat du participant, signé par la CA du cluster. L'objet CA est
    // reconstruit avec les MÊMES params que ceux de `generate_cluster_ca` :
    // seul compte le couple (DN émetteur, signature) — la racine de
    // confiance envoyée aux pairs reste le ca.pem stocké.
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

/// Installe l'identité cluster du process (à faire AVANT tout endpoint ou
/// connexion). Sans appel, tout reste en mode insecure historique.
pub fn set_cluster_tls(tls: ClusterTls) {
    if CLUSTER_TLS.set(Some(Arc::new(tls))).is_err() {
        panic!("set_cluster_tls doit être appelé une seule fois, avant tout usage réseau");
    }
}

pub(crate) fn cluster_tls() -> Option<Arc<ClusterTls>> {
    CLUSTER_TLS.get_or_init(|| None).clone()
}
