//! Space authentication: Ed25519 request signatures for writes.
//!
//! A space's client signs its HTTP writes with a private key it generated
//! itself and never transmitted; every node verifies with the public keys
//! replicated in the Raft state. No shared secrets anywhere: a compromised
//! node can check signatures, not mint them.
//!
//! The canonical string a write signature covers:
//!
//! ```text
//! {method}\n{path}\n{space}\n{timestamp}\n{content_hash or "-"}
//! ```
//!
//! `timestamp` is unix seconds and must be within [`MAX_CLOCK_SKEW`] of
//! the serving node's clock — a captured signature dies in minutes. When
//! the client pre-computes the BLAKE3 of its upload it includes it, and
//! the signature then binds the exact bytes; "-" leaves the body unbound
//! within the window (documented trade-off for plain-curl clients — the
//! signed-link format of the read path is separate and always fully
//! bound).

use anyhow::{bail, Context, Result};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};

/// Accepted clock difference between signer and verifier, seconds.
pub const MAX_CLOCK_SKEW: u64 = 300;

/// Prefix of the textual private-key form (`nsk_<hex seed>`): printed
/// once at generation, recognized by `nauka space sign`.
pub const SECRET_PREFIX: &str = "nsk_";

/// The canonical string both sides sign/verify.
pub fn canonical_write(
    method: &str,
    path: &str,
    space: &str,
    timestamp: u64,
    content_hash: Option<&str>,
) -> String {
    format!(
        "{method}\n{path}\n{space}\n{timestamp}\n{}",
        content_hash.unwrap_or("-")
    )
}

/// Parses an `nsk_…` private key into a signing key.
pub fn parse_secret(secret: &str) -> Result<SigningKey> {
    let hex_part = secret
        .strip_prefix(SECRET_PREFIX)
        .context("a private key starts with nsk_")?;
    let bytes: [u8; 32] = hex::decode(hex_part)
        .ok()
        .and_then(|v| v.try_into().ok())
        .context("a private key is nsk_ followed by 64 hex chars")?;
    Ok(SigningKey::from_bytes(&bytes))
}

/// Generates a fresh keypair; returns `(nsk_… secret, public key bytes)`.
pub fn generate() -> (String, [u8; 32]) {
    let signing = SigningKey::generate(&mut rand::rngs::OsRng);
    let secret = format!("{SECRET_PREFIX}{}", hex::encode(signing.to_bytes()));
    (secret, signing.verifying_key().to_bytes())
}

/// Signs a canonical string; returns the signature as hex (128 chars).
pub fn sign(secret: &SigningKey, canonical: &str) -> String {
    hex::encode(secret.sign(canonical.as_bytes()).to_bytes())
}

/// Verifies a hex signature over a canonical string against a raw public
/// key. Any malformed input is simply "not verified".
pub fn verify(public_key: &[u8; 32], canonical: &str, signature_hex: &str) -> bool {
    let Ok(vk) = VerifyingKey::from_bytes(public_key) else {
        return false;
    };
    let Ok(sig_bytes) = hex::decode(signature_hex) else {
        return false;
    };
    let Ok(sig_arr) = <[u8; 64]>::try_from(sig_bytes) else {
        return false;
    };
    vk.verify(canonical.as_bytes(), &Signature::from_bytes(&sig_arr))
        .is_ok()
}

/// `now` for the signing side.
pub fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Is `timestamp` within the accepted window of `now`?
pub fn timestamp_fresh(timestamp: u64, now: u64) -> bool {
    now.abs_diff(timestamp) <= MAX_CLOCK_SKEW
}

/// Parses a hex public key (64 chars) into raw bytes.
pub fn parse_public_hex(s: &str) -> Result<[u8; 32]> {
    let bytes: [u8; 32] = hex::decode(s)
        .ok()
        .and_then(|v| v.try_into().ok())
        .context("a public key is 64 hex chars")?;
    // Reject values that are not valid curve points outright: they could
    // never verify anything, so registering one is always a mistake.
    if VerifyingKey::from_bytes(&bytes).is_err() {
        bail!("not a valid Ed25519 public key");
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sign_verify_round_trip() {
        let (secret, public) = generate();
        let sk = parse_secret(&secret).unwrap();
        let c = canonical_write("PUT", "/api/upload", "yogfile/uploads", 1_755_000_000, None);
        let sig = sign(&sk, &c);
        assert!(verify(&public, &c, &sig));
        // Any field change breaks the signature.
        let c2 = canonical_write("PUT", "/api/upload", "yogfile/other", 1_755_000_000, None);
        assert!(!verify(&public, &c2, &sig));
        // Binding the content hash changes the canonical string.
        let c3 = canonical_write(
            "PUT",
            "/api/upload",
            "yogfile/uploads",
            1_755_000_000,
            Some("abcd"),
        );
        assert!(!verify(&public, &c3, &sig));
    }

    #[test]
    fn foreign_key_never_verifies() {
        let (secret, _) = generate();
        let (_, other_public) = generate();
        let sk = parse_secret(&secret).unwrap();
        let c = canonical_write("PUT", "/api/upload", "yogfile/uploads", 1, None);
        assert!(!verify(&other_public, &c, &sign(&sk, &c)));
    }

    #[test]
    fn timestamp_window() {
        assert!(timestamp_fresh(1000, 1000 + MAX_CLOCK_SKEW));
        assert!(timestamp_fresh(1000 + MAX_CLOCK_SKEW, 1000));
        assert!(!timestamp_fresh(1000, 1001 + MAX_CLOCK_SKEW));
    }

    #[test]
    fn secret_format_round_trips() {
        let (secret, public) = generate();
        assert!(secret.starts_with(SECRET_PREFIX));
        assert_eq!(
            parse_secret(&secret).unwrap().verifying_key().to_bytes(),
            public
        );
        assert!(parse_secret("nsk_zz").is_err());
        assert!(parse_secret("deadbeef").is_err());
    }
}
