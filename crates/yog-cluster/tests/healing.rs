//! Intégration : un cluster de 3 nœuds QUIC, un nœud perd/corrompt ses
//! shards, son scrubber les régénère depuis les autres.

use std::sync::Arc;
use std::time::Duration;

use yog_cluster::healer::scrub_once;
use yog_cluster::placement::shards_owned_by;
use yog_erasure::{encode_file, ErasureConfig};
use yog_store::ShardStore;
use yog_transport::server::{make_endpoint, serve_endpoint};

struct Node {
    id: String,
    store: Arc<ShardStore>,
    _dir: tempfile::TempDir,
}

async fn spawn_node() -> Node {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(ShardStore::open(dir.path()).unwrap());
    let endpoint = make_endpoint("127.0.0.1:0".parse().unwrap()).unwrap();
    let id = endpoint.local_addr().unwrap().to_string();
    tokio::spawn(serve_endpoint(store.clone(), endpoint));
    Node { id, store, _dir: dir }
}

#[tokio::test]
async fn node_heals_lost_and_corrupted_shards() {
    let nodes = [spawn_node().await, spawn_node().await, spawn_node().await];
    let ids: Vec<String> = nodes.iter().map(|n| n.id.clone()).collect();
    let id_refs: Vec<&str> = ids.iter().map(String::as_str).collect();

    // Un fichier 4+2 de 3 stripes, placé selon le placement officiel.
    let cfg = ErasureConfig { data_shards: 4, parity_shards: 2, shard_size: 32 * 1024 };
    let data: Vec<u8> = (0..cfg.stripe_data_len() * 3).map(|i| (i % 253) as u8).collect();
    let (manifest, stripes) = encode_file(&data, &cfg).unwrap();

    for node in &nodes {
        node.store.put_manifest(&manifest).unwrap();
        for (si, i, _) in shards_owned_by(&manifest, &node.id, &id_refs) {
            node.store.put_shard(&stripes[si][i].data).unwrap();
        }
    }

    // Sanité : rien à réparer au départ.
    let r = scrub_once(&nodes[0].store, &nodes[0].id, &ids).await.unwrap();
    assert_eq!(r.shards_healed, 0);
    assert!(r.shards_checked > 0);

    // Désastre sur le nœud 0 : il perd TOUS ses shards.
    let victim = &nodes[0];
    let owned = shards_owned_by(&manifest, &victim.id, &id_refs);
    assert!(!owned.is_empty());
    for (_, _, hash) in &owned {
        victim.store.delete_shard(hash).unwrap();
    }

    // Son scrubber régénère tout depuis les deux autres nœuds.
    let r = scrub_once(&victim.store, &victim.id, &ids).await.unwrap();
    assert_eq!(r.shards_healed, owned.len());
    assert_eq!(r.shards_unrecoverable, 0);
    for (_, _, hash) in &owned {
        assert!(victim.store.get_shard(hash).is_ok(), "shard {hash} non régénéré");
    }

    // Deuxième passe : plus rien à faire.
    let r = scrub_once(&victim.store, &victim.id, &ids).await.unwrap();
    assert_eq!(r.shards_healed, 0);
    assert_eq!(r.shards_unrecoverable, 0);

    tokio::time::timeout(Duration::from_secs(1), async {}).await.unwrap();
}
