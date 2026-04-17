use base64::prelude::*;
use ring::aead;
use ring::hkdf;
use ring::rand::{SecureRandom, SystemRandom};

use super::MeshError;

const SALT: &[u8] = b"nauka-mesh-v1";
const INFO_PRIVATE_KEY: &[u8] = b"private-key-encryption";
const ENC_PREFIX: &str = "enc:";

fn machine_id() -> Result<String, MeshError> {
    std::fs::read_to_string("/etc/machine-id")
        .map(|s| s.trim().to_string())
        .map_err(|e| MeshError::State(format!("read /etc/machine-id: {e}")))
}

fn derive_key() -> Result<aead::LessSafeKey, MeshError> {
    let mid = machine_id()?;
    let salt = hkdf::Salt::new(hkdf::HKDF_SHA256, SALT);
    let prk = salt.extract(mid.as_bytes());

    let mut key_bytes = [0u8; 32];
    prk.expand(&[INFO_PRIVATE_KEY], hkdf::HKDF_SHA256)
        .map_err(|_| MeshError::State("hkdf expand failed".into()))?
        .fill(&mut key_bytes)
        .map_err(|_| MeshError::State("hkdf fill failed".into()))?;

    let unbound = aead::UnboundKey::new(&aead::CHACHA20_POLY1305, &key_bytes)
        .map_err(|_| MeshError::State("create aead key failed".into()))?;
    Ok(aead::LessSafeKey::new(unbound))
}

pub fn encrypt_secret(plaintext: &str) -> Result<String, MeshError> {
    let key = derive_key()?;
    let rng = SystemRandom::new();

    let mut nonce_bytes = [0u8; 12];
    rng.fill(&mut nonce_bytes)
        .map_err(|_| MeshError::State("random nonce failed".into()))?;

    let nonce = aead::Nonce::assume_unique_for_key(nonce_bytes);
    let mut data = plaintext.as_bytes().to_vec();
    key.seal_in_place_append_tag(nonce, aead::Aad::empty(), &mut data)
        .map_err(|_| MeshError::State("encrypt failed".into()))?;

    let mut combined = Vec::with_capacity(12 + data.len());
    combined.extend_from_slice(&nonce_bytes);
    combined.extend_from_slice(&data);

    Ok(format!("{ENC_PREFIX}{}", BASE64_STANDARD.encode(&combined)))
}

pub fn decrypt_secret(stored: &str) -> Result<String, MeshError> {
    if !stored.starts_with(ENC_PREFIX) {
        return Ok(stored.to_string());
    }

    let encoded = &stored[ENC_PREFIX.len()..];
    let combined = BASE64_STANDARD
        .decode(encoded)
        .map_err(|e| MeshError::State(format!("base64 decode: {e}")))?;

    if combined.len() < 12 {
        return Err(MeshError::State("encrypted data too short".into()));
    }

    let key = derive_key()?;
    let nonce_bytes: [u8; 12] = combined[..12]
        .try_into()
        .map_err(|_| MeshError::State("nonce extract failed".into()))?;
    let nonce = aead::Nonce::assume_unique_for_key(nonce_bytes);

    let mut ciphertext = combined[12..].to_vec();
    let plaintext = key
        .open_in_place(nonce, aead::Aad::empty(), &mut ciphertext)
        .map_err(|_| MeshError::State("decrypt failed".into()))?;

    String::from_utf8(plaintext.to_vec()).map_err(|e| MeshError::State(format!("utf8 decode: {e}")))
}
