//! Cluster mTLS: only holders of a certificate signed by the cluster key get
//! through the handshake — in both directions.
//! (Separate test binary: a process's TLS identity is a singleton.)

use std::sync::Arc;

use quinn::crypto::rustls::QuicClientConfig;
use nauka_store::ShardStore;
use nauka_transport::server::{make_endpoint, serve_endpoint};
use nauka_transport::{load_cluster_tls, set_cluster_tls, PeerClient};

fn crypto_provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

/// Raw quinn client with an arbitrary rustls config (to simulate attackers
/// without going through PeerClient, which uses the global identity).
async fn raw_connect(
    addr: std::net::SocketAddr,
    crypto: rustls::ClientConfig,
    server_name: &str,
) -> anyhow::Result<quinn::Connection> {
    let mut crypto = crypto;
    crypto.alpn_protocols = vec![b"yog/0".to_vec()];
    let mut endpoint = quinn::Endpoint::client("0.0.0.0:0".parse()?)?;
    endpoint.set_default_client_config(quinn::ClientConfig::new(Arc::new(
        QuicClientConfig::try_from(crypto)?,
    )));
    Ok(endpoint.connect(addr, server_name)?.await?)
}

#[tokio::test]
async fn cluster_mtls_accepts_members_rejects_strangers() {
    // Cluster key + process identity (both the node AND the test client).
    let keys_dir = tempfile::tempdir().unwrap();
    nauka_transport::generate_cluster_ca(keys_dir.path()).unwrap();
    let tls =
        load_cluster_tls(keys_dir.path(), Some(&keys_dir.path().join("node.key"))).unwrap();
    let fingerprint = tls.fingerprint.clone();
    let node_id = tls.node_id;
    set_cluster_tls(tls);

    // The identity is stable and key-derived: reloading yields the same id.
    let again =
        load_cluster_tls(keys_dir.path(), Some(&keys_dir.path().join("node.key"))).unwrap();
    assert_eq!(again.fingerprint, fingerprint);
    assert_eq!(again.node_id, node_id);

    // mTLS node.
    let store_dir = tempfile::tempdir().unwrap();
    let store = Arc::new(ShardStore::open(store_dir.path()).unwrap());
    let endpoint = make_endpoint("127.0.0.1:0".parse().unwrap()).unwrap();
    let addr = endpoint.local_addr().unwrap();
    tokio::spawn(serve_endpoint(store, endpoint, None));

    // 1. Legitimate member (our global identity): everything works.
    let client = PeerClient::connect(addr).await.unwrap();
    client.ping().await.unwrap();
    let hash = client.put_shard(b"authenticated".to_vec()).await.unwrap();
    assert_eq!(client.get_shard(&hash).await.unwrap().unwrap(), b"authenticated");

    // 2. Client WITHOUT a certificate (still verifies the CA): rejected at
    //    handshake time by the server.
    let ca_only = rustls::ClientConfig::builder_with_provider(crypto_provider())
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(
            load_cluster_tls(keys_dir.path(), None).unwrap().roots.clone(),
        )
        .with_no_client_auth();
    assert_rejected(raw_connect(addr, ca_only, nauka_transport::tls::NODE_SAN).await).await;

    // 3. Client from ANOTHER cluster (foreign CA): rejected.
    let other_dir = tempfile::tempdir().unwrap();
    nauka_transport::generate_cluster_ca(other_dir.path()).unwrap();
    let other = load_cluster_tls(other_dir.path(), None).unwrap();
    let foreign = rustls::ClientConfig::builder_with_provider(crypto_provider())
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(other.roots.clone())
        .with_client_auth_cert(other.cert_chain.clone(), other.key.clone_key())
        .unwrap();
    assert_rejected(raw_connect(addr, foreign, nauka_transport::tls::NODE_SAN).await).await;
}

/// An mTLS rejection can surface either at `connect` (alert received during
/// the handshake) or just after it (immediate close): either way the
/// connection must be unusable.
async fn assert_rejected(result: anyhow::Result<quinn::Connection>) {
    let conn = match result {
        Err(_) => return,
        Ok(conn) => conn,
    };
    match tokio::time::timeout(std::time::Duration::from_secs(3), conn.closed()).await {
        Ok(quinn::ConnectionError::ApplicationClosed(_)) => {
            panic!("connection closed gracefully instead of being rejected")
        }
        Ok(_) => {} // closed by a TLS/transport error: rejected
        Err(_) => panic!("the unauthenticated connection stayed open"),
    }
}
