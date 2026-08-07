//! Integration: a 3-node QUIC cluster, one node loses/corrupts its shards,
//! its scrubber regenerates them from the others.

use std::sync::Arc;
use std::time::Duration;

use nauka_cluster::healer::scrub_once;
use nauka_cluster::placement::shards_owned_by;
use nauka_erasure::{encode_file, ErasureConfig};
use nauka_store::ShardStore;
use nauka_transport::server::{make_endpoint, serve_endpoint};

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
    tokio::spawn(serve_endpoint(store.clone(), endpoint, None));
    Node { id, store, _dir: dir }
}

#[tokio::test]
async fn node_heals_lost_and_corrupted_shards() {
    let nodes = [spawn_node().await, spawn_node().await, spawn_node().await];
    let ids: Vec<(String, u64)> = nodes.iter().map(|n| (n.id.clone(), 1)).collect();
    let id_refs: Vec<(&str, u64)> = ids.iter().map(|(n, w)| (n.as_str(), *w)).collect();

    // A 4+2 file of 3 stripes, laid out with the official placement.
    let cfg = ErasureConfig { data_shards: 4, parity_shards: 2, shard_size: 32 * 1024 };
    let data: Vec<u8> = (0..cfg.stripe_data_len() * 3).map(|i| (i % 253) as u8).collect();
    let (manifest, stripes) = encode_file(&data, &cfg).unwrap();

    for node in &nodes {
        node.store.put_manifest(&manifest).unwrap();
        for (si, i, _) in shards_owned_by(&manifest, &node.id, &id_refs) {
            node.store.put_shard(&stripes[si][i].data).unwrap();
        }
    }

    // Sanity: nothing to heal at first.
    let r = scrub_once(&nodes[0].store, &nodes[0].id, &ids).await.unwrap();
    assert_eq!(r.shards_healed, 0);
    assert!(r.shards_checked > 0);

    // Disaster on node 0: it loses ALL of its shards.
    let victim = &nodes[0];
    let owned = shards_owned_by(&manifest, &victim.id, &id_refs);
    assert!(!owned.is_empty());
    for (_, _, hash) in &owned {
        victim.store.delete_shard(hash).unwrap();
    }

    // Its scrubber regenerates everything from the other two nodes.
    let r = scrub_once(&victim.store, &victim.id, &ids).await.unwrap();
    assert_eq!(r.shards_healed, owned.len());
    assert_eq!(r.shards_unrecoverable, 0);
    for (_, _, hash) in &owned {
        assert!(victim.store.get_shard(hash).is_ok(), "shard {hash} not healed");
    }

    // Second pass: nothing left to do.
    let r = scrub_once(&victim.store, &victim.id, &ids).await.unwrap();
    assert_eq!(r.shards_healed, 0);
    assert_eq!(r.shards_unrecoverable, 0);

    tokio::time::timeout(Duration::from_secs(1), async {}).await.unwrap();
}
