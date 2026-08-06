//! Rendez-vous DHT sur une Mainline locale (Testnet in-process, sans
//! internet) : dérivation déterministe, publication, résolution.

use yog_discovery::{derive_dht_keypair, make_client, publish_seeds, resolve_seeds};

#[tokio::test]
async fn derive_publish_resolve_roundtrip() {
    // Clé de cluster réelle (générée par yog-transport).
    let keys = tempfile::tempdir().unwrap();
    yog_transport::generate_cluster_ca(keys.path()).unwrap();

    // Dérivation déterministe : deux détenteurs des clés obtiennent la
    // même identité DHT.
    let kp1 = derive_dht_keypair(keys.path()).unwrap();
    let kp2 = derive_dht_keypair(keys.path()).unwrap();
    assert_eq!(kp1.public_key(), kp2.public_key());

    // Un autre cluster dérive une identité différente.
    let other_keys = tempfile::tempdir().unwrap();
    yog_transport::generate_cluster_ca(other_keys.path()).unwrap();
    let kp_other = derive_dht_keypair(other_keys.path()).unwrap();
    assert_ne!(kp1.public_key(), kp_other.public_key());

    // DHT locale : 8 nœuds mainline in-process.
    let testnet = mainline::Testnet::new(8).unwrap();
    let publisher = make_client(Some(&testnet.bootstrap)).unwrap();
    let resolver = make_client(Some(&testnet.bootstrap)).unwrap();

    // Le "leader" publie les seeds ; un nouveau venu (qui ne connaît que
    // les clés du cluster) les résout.
    let seeds = vec!["10.0.0.1:7311".parse().unwrap(), "10.0.0.2:7311".parse().unwrap()];
    publish_seeds(&publisher, &kp1, &seeds).await.unwrap();

    let resolved = resolve_seeds(&resolver, &kp2.public_key()).await.unwrap();
    assert_eq!(resolved, seeds);

    // Republication avec une nouvelle vue (membership qui a changé) : le
    // résolveur voit la version la plus récente.
    let seeds2 = vec![
        "10.0.0.1:7311".parse().unwrap(),
        "10.0.0.2:7311".parse().unwrap(),
        "10.0.0.3:7311".parse().unwrap(),
    ];
    publish_seeds(&publisher, &kp1, &seeds2).await.unwrap();
    let resolved2 = resolve_seeds(&resolver, &kp1.public_key()).await.unwrap();
    assert_eq!(resolved2, seeds2);

    // Un cluster étranger ne trouve rien sous SA clé.
    let nothing = resolve_seeds(&resolver, &kp_other.public_key()).await.unwrap();
    assert!(nothing.is_empty());

    // Auto-détection d'IP via la DHT (BEP42) : sur le testnet local, les
    // nœuds nous voient depuis la loopback.
    let detected = yog_discovery::detect_public_ip(Some(&testnet.bootstrap)).await.unwrap();
    assert_eq!(detected, Some("127.0.0.1".parse().unwrap()));
}
