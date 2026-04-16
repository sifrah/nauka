use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, KeyPair, KeyUsagePurpose, SanType};

use super::MeshError;

pub struct TlsCerts {
    pub ca_cert: String,
    pub tls_cert: String,
    pub tls_key: String,
}

const SAN_DNS: &str = "raft.nauka.local";

fn ca_params() -> CertificateParams {
    let mut params = CertificateParams::default();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params
        .distinguished_name
        .push(DnType::CommonName, "nauka mesh CA");
    params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    params
}

pub fn generate_ca() -> Result<(String, String), MeshError> {
    let ca_key =
        KeyPair::generate().map_err(|e| MeshError::State(format!("generate CA key: {e}")))?;
    let ca_cert = ca_params()
        .self_signed(&ca_key)
        .map_err(|e| MeshError::State(format!("self-sign CA: {e}")))?;
    Ok((ca_cert.pem(), ca_key.serialize_pem()))
}

pub fn sign_node_cert(
    _ca_cert_pem: &str,
    ca_key_pem: &str,
) -> Result<(String, String), MeshError> {
    // Reconstruct the CA from its key — same public key, so verification works
    let ca_key = KeyPair::from_pem(ca_key_pem)
        .map_err(|e| MeshError::State(format!("parse CA key: {e}")))?;
    let ca_cert = ca_params()
        .self_signed(&ca_key)
        .map_err(|e| MeshError::State(format!("reconstruct CA: {e}")))?;

    let node_key =
        KeyPair::generate().map_err(|e| MeshError::State(format!("generate node key: {e}")))?;

    let mut params = CertificateParams::default();
    params
        .distinguished_name
        .push(DnType::CommonName, "nauka node");
    params.subject_alt_names = vec![SanType::DnsName(
        SAN_DNS
            .try_into()
            .map_err(|e| MeshError::State(format!("SAN: {e}")))?,
    )];
    params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    params.extended_key_usages = vec![
        rcgen::ExtendedKeyUsagePurpose::ServerAuth,
        rcgen::ExtendedKeyUsagePurpose::ClientAuth,
    ];

    let node_cert = params
        .signed_by(&node_key, &ca_cert, &ca_key)
        .map_err(|e| MeshError::State(format!("sign node cert: {e}")))?;

    Ok((node_cert.pem(), node_key.serialize_pem()))
}
