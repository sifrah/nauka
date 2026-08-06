//! mTLS de cluster : seuls les porteurs d'un certificat signé par la clé
//! de cluster passent la poignée de main — dans les deux sens.
//! (Binaire de test séparé : l'identité TLS d'un process est un singleton.)

use std::sync::Arc;

use quinn::crypto::rustls::QuicClientConfig;
use yog_store::ShardStore;
use yog_transport::server::{make_endpoint, serve_endpoint};
use yog_transport::{load_cluster_tls, set_cluster_tls, PeerClient};

fn crypto_provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

/// Client quinn brut avec une config rustls arbitraire (pour simuler des
/// attaquants sans passer par PeerClient, qui utilise l'identité globale).
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
    // Clé de cluster + identité du process (nœud ET client de test).
    let keys_dir = tempfile::tempdir().unwrap();
    yog_transport::generate_cluster_ca(keys_dir.path()).unwrap();
    let tls =
        load_cluster_tls(keys_dir.path(), Some(&keys_dir.path().join("node.key"))).unwrap();
    let fingerprint = tls.fingerprint.clone();
    let node_id = tls.node_id;
    set_cluster_tls(tls);

    // L'identité est stable et dérivée de la clé : recharger donne le même id.
    let again =
        load_cluster_tls(keys_dir.path(), Some(&keys_dir.path().join("node.key"))).unwrap();
    assert_eq!(again.fingerprint, fingerprint);
    assert_eq!(again.node_id, node_id);

    // Nœud mTLS.
    let store_dir = tempfile::tempdir().unwrap();
    let store = Arc::new(ShardStore::open(store_dir.path()).unwrap());
    let endpoint = make_endpoint("127.0.0.1:0".parse().unwrap()).unwrap();
    let addr = endpoint.local_addr().unwrap();
    tokio::spawn(serve_endpoint(store, endpoint, None));

    // 1. Membre légitime (notre identité globale) : tout fonctionne.
    let client = PeerClient::connect(addr).await.unwrap();
    client.ping().await.unwrap();
    let hash = client.put_shard(b"authentifie".to_vec()).await.unwrap();
    assert_eq!(client.get_shard(&hash).await.unwrap().unwrap(), b"authentifie");

    // 2. Client SANS certificat (vérifie quand même la CA) : rejeté au
    //    handshake par le serveur.
    let ca_only = rustls::ClientConfig::builder_with_provider(crypto_provider())
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(
            load_cluster_tls(keys_dir.path(), None).unwrap().roots.clone(),
        )
        .with_no_client_auth();
    assert_rejected(raw_connect(addr, ca_only, yog_transport::tls::NODE_SAN).await).await;

    // 3. Client d'un AUTRE cluster (CA étrangère) : rejeté.
    let other_dir = tempfile::tempdir().unwrap();
    yog_transport::generate_cluster_ca(other_dir.path()).unwrap();
    let other = load_cluster_tls(other_dir.path(), None).unwrap();
    let foreign = rustls::ClientConfig::builder_with_provider(crypto_provider())
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(other.roots.clone())
        .with_client_auth_cert(other.cert_chain.clone(), other.key.clone_key())
        .unwrap();
    assert_rejected(raw_connect(addr, foreign, yog_transport::tls::NODE_SAN).await).await;
}

/// Un rejet mTLS peut apparaître soit à `connect` (alerte reçue pendant le
/// handshake), soit juste après (fermeture immédiate) : dans les deux cas
/// la connexion doit être inutilisable.
async fn assert_rejected(result: anyhow::Result<quinn::Connection>) {
    let conn = match result {
        Err(_) => return,
        Ok(conn) => conn,
    };
    match tokio::time::timeout(std::time::Duration::from_secs(3), conn.closed()).await {
        Ok(quinn::ConnectionError::ApplicationClosed(_)) => {
            panic!("connexion fermée proprement au lieu d'être rejetée")
        }
        Ok(_) => {} // fermée par erreur TLS/transport : rejetée
        Err(_) => panic!("la connexion non authentifiée est restée ouverte"),
    }
}
