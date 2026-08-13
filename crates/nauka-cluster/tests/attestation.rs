//! Storage attestation:
//! - the audit unmasks a peer that lost (or corrupted) what it must hold;
//! - the GC demands a PROOF of possession before releasing its copy;
//! - the nonce proof is neither replayable nor forgeable.

use std::collections::BTreeSet;
use std::sync::{Arc, RwLock};

use nauka_cluster::audit::{audit_once, expected_proof};
use nauka_cluster::healer::gc_once;
use nauka_cluster::placement::shards_owned_by;
use nauka_erasure::{encode_file, ErasureConfig};
use nauka_store::ShardStore;
use nauka_transport::server::{make_endpoint, serve_endpoint_owned, OwnershipView};
use nauka_transport::PeerClient;

struct Node {
    id: String,
    store: Arc<ShardStore>,
    /// What this node advertises as its own to peers — the ownership half
    /// of `ProveShardOwned` answers from here.
    claims: Arc<RwLock<BTreeSet<String>>>,
    dir: tempfile::TempDir,
}

async fn spawn_node() -> Node {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(ShardStore::open(dir.path()).unwrap());
    let endpoint = make_endpoint("127.0.0.1:0".parse().unwrap()).unwrap();
    let id = endpoint.local_addr().unwrap().to_string();
    let claims: Arc<RwLock<BTreeSet<String>>> = Arc::new(Default::default());
    tokio::spawn(serve_endpoint_owned(
        store.clone(),
        endpoint,
        None,
        Some(claims.clone() as Arc<dyn OwnershipView>),
        None,
    ));
    Node {
        id,
        store,
        claims,
        dir,
    }
}

fn view(nodes: &[&Node]) -> Vec<(String, u64)> {
    let mut v: Vec<(String, u64)> = nodes.iter().map(|n| (n.id.clone(), 1u64)).collect();
    v.sort();
    v
}

/// Places a file on the cluster following the official placement.
fn seed_cluster(nodes: &[&Node], seed: u8) -> nauka_erasure::FileManifest {
    let cfg = ErasureConfig {
        data_shards: 4,
        parity_shards: 2,
        shard_size: 4096,
    };
    let data: Vec<u8> = (0..cfg.stripe_data_len() * 3)
        .map(|i| (i as u8) ^ seed)
        .collect();
    let (manifest, stripes) = encode_file(&data, &cfg).unwrap();
    let v = view(nodes);
    let refs: Vec<(&str, u64)> = v.iter().map(|(n, w)| (n.as_str(), *w)).collect();
    for node in nodes {
        node.store.put_manifest(&manifest).unwrap();
        for (si, sj, _) in shards_owned_by(&manifest, &node.id, &refs) {
            node.store.put_shard(&stripes[si][sj].data).unwrap();
        }
    }
    manifest
}

#[tokio::test]
async fn audit_detects_a_peer_that_lost_its_data() {
    let auditor = spawn_node().await;
    let honest = spawn_node().await;
    let all = [&auditor, &honest];
    let manifest = seed_cluster(&all, 0x5a);
    let v = view(&all);

    // ── Honest peer: every possession is proved.
    let r = audit_once(&auditor.store, &auditor.id, &v).await.unwrap();
    assert!(r.challenged > 0, "the auditor must challenge its peer");
    assert_eq!(r.proved, r.challenged, "an honest peer proves everything");
    assert_eq!(r.failed, 0);
    assert_eq!(r.missing, 0);

    // ── The peer loses its disk but stays online (implicit lie).
    let refs: Vec<(&str, u64)> = v.iter().map(|(n, w)| (n.as_str(), *w)).collect();
    for (_, _, h) in shards_owned_by(&manifest, &honest.id, &refs) {
        honest.store.delete_shard(h).unwrap();
    }
    let mut detected = false;
    for _ in 0..6 {
        let r = audit_once(&auditor.store, &auditor.id, &v).await.unwrap();
        assert_eq!(r.proved, 0, "an emptied node can prove nothing");
        if r.missing > 0 {
            detected = true;
        }
    }
    assert!(detected, "data loss must be detected");
}

#[tokio::test]
async fn audit_detects_silent_corruption() {
    let auditor = spawn_node().await;
    let rotten = spawn_node().await;
    let all = [&auditor, &rotten];
    let manifest = seed_cluster(&all, 0x11);
    let v = view(&all);
    let refs: Vec<(&str, u64)> = v.iter().map(|(n, w)| (n.as_str(), *w)).collect();

    // Bit rot: same sizes, altered content, on all of its shards.
    for (_, _, h) in shards_owned_by(&manifest, &rotten.id, &refs) {
        let path = std::fs::read_dir(rotten.dir.path().join("shards").join(&h[..2]))
            .unwrap()
            .filter_map(Result::ok)
            .map(|e| e.path())
            .find(|p| p.file_name().unwrap().to_string_lossy() == h[2..])
            .expect("shard on disk");
        let len = std::fs::metadata(&path).unwrap().len() as usize;
        std::fs::write(&path, vec![0xAAu8; len]).unwrap();
    }

    let mut detected = false;
    for _ in 0..6 {
        let r = audit_once(&auditor.store, &auditor.id, &v).await.unwrap();
        // The transport refuses to serve a corrupted shard: "missing".
        // Never a false proof.
        assert_eq!(r.failed, 0);
        assert_eq!(r.proved, 0, "corrupted bytes prove nothing");
        if r.missing > 0 {
            detected = true;
        }
    }
    assert!(detected, "silent corruption must be detected");
}

#[tokio::test]
async fn gc_requires_proof_before_releasing() {
    // Two nodes, then the GC runs against a view REDUCED to the peer alone:
    // the auditor is no longer an owner and must release — but only if the
    // peer proves it holds the bytes.
    let holder = spawn_node().await;
    let peer = spawn_node().await;
    let both = [&holder, &peer];
    let manifest = seed_cluster(&both, 0x7f);

    // Single-node view (the peer): everything belongs to the peer.
    let solo = vec![(peer.id.clone(), 1u64)];
    let refs: Vec<(&str, u64)> = vec![(peer.id.as_str(), 1)];

    // The peer really holds everything the solo view assigns to it.
    let cfg = manifest.config;
    let data: Vec<u8> = (0..cfg.stripe_data_len() * 3)
        .map(|i| (i as u8) ^ 0x7f)
        .collect();
    let (_, stripes) = encode_file(&data, &cfg).unwrap();
    for (si, sj, _) in shards_owned_by(&manifest, &peer.id, &refs) {
        peer.store.put_shard(&stripes[si][sj].data).unwrap();
    }

    // The peer advertises everything it holds as its own — proof AND claim.
    *peer.claims.write().unwrap() = peer.store.list_shards().unwrap().into_iter().collect();

    let before = holder.store.list_shards().unwrap().len();
    assert!(before > 0);
    let g = gc_once(&holder.store, &holder.id, &solo, &Default::default())
        .await
        .unwrap();
    assert_eq!(g.shards_released, before, "proof + claim → release");
    assert_eq!(holder.store.list_shards().unwrap().len(), 0);

    // ── Opposite case: the peer does NOT hold the bytes → no proof, so the
    // holder keeps everything (we never reduce redundancy).
    let holder2 = spawn_node().await;
    let empty_peer = spawn_node().await;
    let pair2 = [&holder2, &empty_peer];
    seed_cluster(&pair2, 0x22);
    // The peer is emptied: it can prove nothing (even while claiming).
    *empty_peer.claims.write().unwrap() = empty_peer
        .store
        .list_shards()
        .unwrap()
        .into_iter()
        .collect();
    for h in empty_peer.store.list_shards().unwrap() {
        empty_peer.store.delete_shard(&h).unwrap();
    }
    let solo2 = vec![(empty_peer.id.clone(), 1u64)];
    let kept_before = holder2.store.list_shards().unwrap().len();
    let g = gc_once(&holder2.store, &holder2.id, &solo2, &Default::default())
        .await
        .unwrap();
    assert_eq!(g.shards_released, 0, "without a proof, nothing is released");
    assert_eq!(g.shards_kept, kept_before);
    assert_eq!(holder2.store.list_shards().unwrap().len(), kept_before);
}

#[tokio::test]
async fn gc_survives_crossed_ownership_views() {
    // The 2026-08-12 data-loss race, replayed deterministically. Two nodes
    // both hold every shard; their placement views CROSSED mid-rebalance:
    // each sees the OTHER as the owner, and neither claims the shards as
    // its own. Under proof-only GC both sides get a valid proof and both
    // delete — the cluster ends with ZERO copies. The ownership claim must
    // make both keep everything instead.
    let a = spawn_node().await;
    let b = spawn_node().await;
    let both = [&a, &b];
    let manifest = seed_cluster(&both, 0x3c);

    // Both hold the full set (mid-rebalance duplication).
    let cfg = manifest.config;
    let data: Vec<u8> = (0..cfg.stripe_data_len() * 3)
        .map(|i| (i as u8) ^ 0x3c)
        .collect();
    let (_, stripes) = encode_file(&data, &cfg).unwrap();
    for node in &both {
        for stripe in &stripes {
            for shard in stripe {
                node.store.put_shard(&shard.data).unwrap();
            }
        }
    }
    let total = a.store.list_shards().unwrap().len();
    assert!(total > 0);

    // Crossed views: A believes everything belongs to B, B believes
    // everything belongs to A. Nobody claims anything (each one's claim
    // set reflects its own view, which assigns it nothing). Sequential
    // gc runs are enough: the second run still sees the first holder's
    // bytes gone only if the first released — the race needs no timing.
    let view_b_owns = vec![(b.id.clone(), 1u64)];
    let view_a_owns = vec![(a.id.clone(), 1u64)];
    let ga = gc_once(&a.store, &a.id, &view_b_owns, &Default::default())
        .await
        .unwrap();
    let gb = gc_once(&b.store, &b.id, &view_a_owns, &Default::default())
        .await
        .unwrap();
    assert_eq!(
        (ga.shards_released, gb.shards_released),
        (0, 0),
        "no ownership claim anywhere → nobody may release"
    );
    assert_eq!(a.store.list_shards().unwrap().len(), total);
    assert_eq!(b.store.list_shards().unwrap().len(), total);

    // Convergence: B now claims the shards (its view agrees it owns them).
    // A may release; B, claiming, still keeps — at least one copy of every
    // shard survives at every instant, by construction.
    *b.claims.write().unwrap() = b.store.list_shards().unwrap().into_iter().collect();
    let b_claims: BTreeSet<String> = b.claims.read().unwrap().clone();
    let ga = gc_once(&a.store, &a.id, &view_b_owns, &Default::default())
        .await
        .unwrap();
    assert_eq!(ga.shards_released, total, "claimed elsewhere → A releases");
    assert_eq!(a.store.list_shards().unwrap().len(), 0);
    let gb = gc_once(&b.store, &b.id, &view_a_owns, &b_claims)
        .await
        .unwrap();
    assert_eq!(
        gb.shards_released, 0,
        "B advertises the shards as its own: it must not release them, \
         whatever its fresh view says"
    );
    assert_eq!(b.store.list_shards().unwrap().len(), total);
}

#[tokio::test]
async fn proof_is_bound_to_nonce_and_bytes() {
    let node = spawn_node().await;
    let data = vec![42u8; 8192];
    let hash = node.store.put_shard(&data).unwrap();
    let client = PeerClient::connect(node.id.parse().unwrap()).await.unwrap();

    let p1 = client.prove_shard(&hash, [7u8; 32]).await.unwrap().unwrap();
    let p2 = client.prove_shard(&hash, [8u8; 32]).await.unwrap().unwrap();
    assert_ne!(p1, p2, "a different nonce must yield a different proof");
    assert_eq!(
        p1,
        expected_proof(&[7u8; 32], &data),
        "proof verifiable locally"
    );

    // Unknown shard: no proof possible.
    let unknown = nauka_erasure::hash_bytes(b"never stored");
    assert!(client
        .prove_shard(&unknown, [1u8; 32])
        .await
        .unwrap()
        .is_none());
}
