//! Authentification de la face opérateur de l'API HTTP.
//!
//! Les routes de données restent publiques et portent leur propre modèle
//! d'autorisation (grants d'upload, liens de lecture). Les inventaires,
//! registres et détails de topologie ne doivent en revanche jamais répondre
//! à Internet. Une requête locale est admise directement ; entre membres ou
//! depuis un CLI qui possède l'identité du cluster, une preuve HMAC courte
//! lie la méthode, le chemin exact et l'heure sans envoyer la clé du cluster.

use std::path::Path;
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use axum::http::{HeaderMap, Method, Uri};
use hmac::{Hmac, Mac};
use sha2::Sha256;

const TIMESTAMP_HEADER: &str = "x-nauka-operator-timestamp";
const SIGNATURE_HEADER: &str = "x-nauka-operator-signature";
const MAX_CLOCK_SKEW_SECS: u64 = 60;
static OPERATOR_KEY: OnceLock<[u8; 32]> = OnceLock::new();

pub fn key_from_dir(keys_dir: &Path) -> Result<[u8; 32]> {
    let path = keys_dir.join("cluster-ca.key");
    let material = std::fs::read(&path).with_context(|| format!("reading {}", path.display()))?;
    Ok(blake3::derive_key(
        "nauka operator http authentication v1",
        &material,
    ))
}

pub fn install(key: [u8; 32]) -> Result<()> {
    if let Some(current) = OPERATOR_KEY.get() {
        anyhow::ensure!(
            current == &key,
            "another cluster operator key is already installed"
        );
        return Ok(());
    }
    OPERATOR_KEY
        .set(key)
        .map_err(|_| anyhow::anyhow!("installing the cluster operator key"))
}

pub fn installed() -> Option<[u8; 32]> {
    OPERATOR_KEY.get().copied()
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn path_and_query(uri: &Uri) -> &str {
    uri.path_and_query()
        .map_or(uri.path(), |value| value.as_str())
}

fn canonical(method: &Method, path_and_query: &str, timestamp: u64) -> String {
    format!("nauka-operator-v1\n{method}\n{path_and_query}\n{timestamp}")
}

fn signature(key: &[u8; 32], method: &Method, path_and_query: &str, timestamp: u64) -> [u8; 32] {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("a 32-byte HMAC key is valid");
    mac.update(canonical(method, path_and_query, timestamp).as_bytes());
    mac.finalize().into_bytes().into()
}

pub fn signed_get(client: &reqwest::Client, url: &str) -> Result<reqwest::RequestBuilder> {
    let key = installed().context("cluster operator authentication is not installed")?;
    let url_parsed = reqwest::Url::parse(url).with_context(|| format!("invalid URL {url}"))?;
    let uri: Uri = url_parsed
        .as_str()
        .parse()
        .with_context(|| format!("invalid request URI {url}"))?;
    let timestamp = now();
    let proof = signature(&key, &Method::GET, path_and_query(&uri), timestamp);
    Ok(client
        .get(url)
        .header(TIMESTAMP_HEADER, timestamp.to_string())
        .header(SIGNATURE_HEADER, hex::encode(proof)))
}

pub fn verify(key: &[u8; 32], headers: &HeaderMap, method: &Method, uri: &Uri) -> bool {
    let Some(timestamp) = headers
        .get(TIMESTAMP_HEADER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
    else {
        return false;
    };
    if now().abs_diff(timestamp) > MAX_CLOCK_SKEW_SECS {
        return false;
    }
    let Some(proof) = headers
        .get(SIGNATURE_HEADER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| hex::decode(value).ok())
    else {
        return false;
    };
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("a 32-byte HMAC key is valid");
    mac.update(canonical(method, path_and_query(uri), timestamp).as_bytes());
    mac.verify_slice(&proof).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    fn headers(key: &[u8; 32], method: &Method, uri: &Uri, timestamp: u64) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(TIMESTAMP_HEADER, HeaderValue::from(timestamp));
        headers.insert(
            SIGNATURE_HEADER,
            HeaderValue::from_str(&hex::encode(signature(
                key,
                method,
                path_and_query(uri),
                timestamp,
            )))
            .unwrap(),
        );
        headers
    }

    #[test]
    fn proof_binds_method_path_query_and_time() {
        let key = [7; 32];
        let uri: Uri = "/api/removal-check?target=10.0.0.1%3A7311".parse().unwrap();
        let valid = headers(&key, &Method::GET, &uri, now());
        assert!(verify(&key, &valid, &Method::GET, &uri));
        assert!(!verify(&key, &valid, &Method::POST, &uri));
        assert!(!verify(
            &key,
            &valid,
            &Method::GET,
            &"/api/files".parse().unwrap()
        ));
    }

    #[test]
    fn stale_and_malformed_proofs_fail_closed() {
        let key = [9; 32];
        let uri: Uri = "/api/status".parse().unwrap();
        assert!(!verify(
            &key,
            &headers(&key, &Method::GET, &uri, now() - MAX_CLOCK_SKEW_SECS - 1),
            &Method::GET,
            &uri
        ));
        let mut malformed = HeaderMap::new();
        malformed.insert(TIMESTAMP_HEADER, HeaderValue::from(now()));
        malformed.insert(SIGNATURE_HEADER, HeaderValue::from_static("not-hex"));
        assert!(!verify(&key, &malformed, &Method::GET, &uri));
    }
}
