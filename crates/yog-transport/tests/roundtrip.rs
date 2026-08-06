//! Test d'intégration : deux nœuds réels en QUIC sur localhost.

use std::sync::Arc;

use yog_erasure::{encode_file, ErasureConfig};
use yog_store::ShardStore;
use yog_transport::server::{make_endpoint, serve_endpoint};
use yog_transport::PeerClient;

async fn spawn_node() -> (std::net::SocketAddr, Arc<ShardStore>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(ShardStore::open(dir.path()).unwrap());
    let endpoint = make_endpoint("127.0.0.1:0".parse().unwrap()).unwrap();
    let addr = endpoint.local_addr().unwrap();
    tokio::spawn(serve_endpoint(store.clone(), endpoint, None));
    (addr, store, dir)
}

#[tokio::test]
async fn shard_and_manifest_roundtrip_over_quic() {
    let (addr, server_store, _dir) = spawn_node().await;
    let client = PeerClient::connect(addr).await.unwrap();

    client.ping().await.unwrap();

    // Push d'un shard, présence, récupération.
    let hash = client.put_shard(b"shard over quic".to_vec()).await.unwrap();
    assert!(client.has_shard(&hash).await.unwrap());
    assert!(server_store.has_shard(&hash));
    assert_eq!(
        client.get_shard(&hash).await.unwrap().unwrap(),
        b"shard over quic"
    );

    // Shard inconnu → None, pas d'erreur.
    let missing = yog_erasure::hash_bytes(b"nope");
    assert!(client.get_shard(&missing).await.unwrap().is_none());
    assert!(!client.has_shard(&missing).await.unwrap());

    // Manifests.
    let cfg = ErasureConfig { data_shards: 2, parity_shards: 1, shard_size: 128 };
    let (manifest, _) = encode_file(b"contenu du fichier", &cfg).unwrap();
    client.put_manifest(&manifest).await.unwrap();
    let loaded = client.get_manifest(&manifest.file_hash).await.unwrap().unwrap();
    assert_eq!(loaded.file_hash, manifest.file_hash);
    assert!(client.get_manifest(&missing).await.unwrap().is_none());
}

#[tokio::test]
async fn full_file_dispatch_across_three_nodes() {
    // 3 nœuds, config 4+2 : les 6 shards de chaque stripe sont répartis
    // en round-robin, puis le fichier est reconstruit en lisant les nœuds —
    // même avec un nœud entièrement mort.
    let mut nodes = Vec::new();
    for _ in 0..3 {
        nodes.push(spawn_node().await);
    }
    let mut clients = Vec::new();
    for (addr, _, _) in &nodes {
        clients.push(PeerClient::connect(*addr).await.unwrap());
    }

    let data: Vec<u8> = (0..1_000_000u32).map(|i| (i % 251) as u8).collect();
    let cfg = ErasureConfig { data_shards: 4, parity_shards: 2, shard_size: 64 * 1024 };
    let (manifest, stripes) = encode_file(&data, &cfg).unwrap();

    // Dispatch round-robin + manifest répliqué partout.
    for stripe in &stripes {
        for shard in stripe {
            let client = &clients[shard.index % clients.len()];
            client.put_shard(shard.data.clone()).await.unwrap();
        }
    }
    for client in &clients {
        client.put_manifest(&manifest).await.unwrap();
    }

    // Nœud 2 meurt : on ne lit que les nœuds 0 et 1.
    let survivors = &clients[..2];
    let mut stripes_slots = Vec::new();
    for stripe in &manifest.stripes {
        let mut slots = Vec::new();
        for hash in &stripe.shard_hashes {
            let mut found = None;
            for client in survivors {
                if let Some(data) = client.get_shard(hash).await.unwrap() {
                    found = Some(data);
                    break;
                }
            }
            slots.push(found);
        }
        stripes_slots.push(slots);
    }

    let restored = yog_erasure::decode_file(&manifest, stripes_slots).unwrap();
    assert_eq!(restored, data);
}
