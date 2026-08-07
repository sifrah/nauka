//! Attestation de stockage :
//! - l'audit démasque un pair qui a perdu (ou corrompu) ce qu'il doit détenir ;
//! - le GC exige une PREUVE de détention avant de libérer sa copie ;
//! - la preuve par nonce n'est ni rejouable ni falsifiable.

use std::sync::Arc;

use yog_cluster::audit::{audit_once, expected_proof};
use yog_cluster::healer::gc_once;
use yog_cluster::placement::shards_owned_by;
use yog_erasure::{encode_file, ErasureConfig};
use yog_store::ShardStore;
use yog_transport::server::{make_endpoint, serve_endpoint};
use yog_transport::PeerClient;

struct Node {
    id: String,
    store: Arc<ShardStore>,
    dir: tempfile::TempDir,
}

async fn spawn_node() -> Node {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(ShardStore::open(dir.path()).unwrap());
    let endpoint = make_endpoint("127.0.0.1:0".parse().unwrap()).unwrap();
    let id = endpoint.local_addr().unwrap().to_string();
    tokio::spawn(serve_endpoint(store.clone(), endpoint, None));
    Node { id, store, dir }
}

fn view(nodes: &[&Node]) -> Vec<(String, u64)> {
    let mut v: Vec<(String, u64)> = nodes.iter().map(|n| (n.id.clone(), 1u64)).collect();
    v.sort();
    v
}

/// Place un fichier sur le cluster selon le placement officiel.
fn seed_cluster(nodes: &[&Node], seed: u8) -> yog_erasure::FileManifest {
    let cfg = ErasureConfig { data_shards: 4, parity_shards: 2, shard_size: 4096 };
    let data: Vec<u8> = (0..cfg.stripe_data_len() * 3).map(|i| (i as u8) ^ seed).collect();
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

    // ── Pair honnête : toutes les détentions sont prouvées.
    let r = audit_once(&auditor.store, &auditor.id, &v).await.unwrap();
    assert!(r.challenged > 0, "l'auditeur doit challenger son pair");
    assert_eq!(r.proved, r.challenged, "un pair honnête prouve tout");
    assert_eq!(r.failed, 0);
    assert_eq!(r.missing, 0);

    // ── Le pair perd son disque mais reste en ligne (mensonge implicite).
    let refs: Vec<(&str, u64)> = v.iter().map(|(n, w)| (n.as_str(), *w)).collect();
    for (_, _, h) in shards_owned_by(&manifest, &honest.id, &refs) {
        honest.store.delete_shard(h).unwrap();
    }
    let mut detected = false;
    for _ in 0..6 {
        let r = audit_once(&auditor.store, &auditor.id, &v).await.unwrap();
        assert_eq!(r.proved, 0, "un nœud vidé ne peut rien prouver");
        if r.missing > 0 {
            detected = true;
        }
    }
    assert!(detected, "la perte de données doit être détectée");
}

#[tokio::test]
async fn audit_detects_silent_corruption() {
    let auditor = spawn_node().await;
    let rotten = spawn_node().await;
    let all = [&auditor, &rotten];
    let manifest = seed_cluster(&all, 0x11);
    let v = view(&all);
    let refs: Vec<(&str, u64)> = v.iter().map(|(n, w)| (n.as_str(), *w)).collect();

    // Bit rot : mêmes tailles, contenu altéré, sur tous ses shards.
    for (_, _, h) in shards_owned_by(&manifest, &rotten.id, &refs) {
        let path = std::fs::read_dir(rotten.dir.path().join("shards").join(&h[..2]))
            .unwrap()
            .filter_map(Result::ok)
            .map(|e| e.path())
            .find(|p| p.file_name().unwrap().to_string_lossy() == h[2..])
            .expect("shard sur disque");
        let len = std::fs::metadata(&path).unwrap().len() as usize;
        std::fs::write(&path, vec![0xAAu8; len]).unwrap();
    }

    let mut detected = false;
    for _ in 0..6 {
        let r = audit_once(&auditor.store, &auditor.id, &v).await.unwrap();
        // Le transport refuse de servir un shard corrompu : « absent ».
        // Jamais de fausse preuve.
        assert_eq!(r.failed, 0);
        assert_eq!(r.proved, 0, "des octets corrompus ne prouvent rien");
        if r.missing > 0 {
            detected = true;
        }
    }
    assert!(detected, "la corruption silencieuse doit être détectée");
}

#[tokio::test]
async fn gc_requires_proof_before_releasing() {
    // Deux nœuds, puis on calcule le GC avec une vue RÉDUITE au seul pair :
    // l'auditeur n'est plus propriétaire et doit libérer — mais seulement
    // si le pair prouve qu'il détient les octets.
    let holder = spawn_node().await;
    let peer = spawn_node().await;
    let both = [&holder, &peer];
    let manifest = seed_cluster(&both, 0x7f);

    // Vue à un seul nœud (le pair) : tout appartient au pair.
    let solo = vec![(peer.id.clone(), 1u64)];
    let refs: Vec<(&str, u64)> = vec![(peer.id.as_str(), 1)];

    // Le pair détient réellement tout ce que la vue solo lui attribue.
    let cfg = manifest.config;
    let data: Vec<u8> = (0..cfg.stripe_data_len() * 3).map(|i| (i as u8) ^ 0x7f).collect();
    let (_, stripes) = encode_file(&data, &cfg).unwrap();
    for (si, sj, _) in shards_owned_by(&manifest, &peer.id, &refs) {
        peer.store.put_shard(&stripes[si][sj].data).unwrap();
    }

    let before = holder.store.list_shards().unwrap().len();
    assert!(before > 0);
    let g = gc_once(&holder.store, &holder.id, &solo).await.unwrap();
    assert_eq!(g.shards_released, before, "preuve fournie → libération");
    assert_eq!(holder.store.list_shards().unwrap().len(), 0);

    // ── Cas inverse : le pair NE détient PAS les octets → pas de preuve,
    // donc le détenteur garde tout (on ne réduit jamais la redondance).
    let holder2 = spawn_node().await;
    let empty_peer = spawn_node().await;
    let pair2 = [&holder2, &empty_peer];
    seed_cluster(&pair2, 0x22);
    // Le pair est vidé : il ne peut rien prouver.
    for h in empty_peer.store.list_shards().unwrap() {
        empty_peer.store.delete_shard(&h).unwrap();
    }
    let solo2 = vec![(empty_peer.id.clone(), 1u64)];
    let kept_before = holder2.store.list_shards().unwrap().len();
    let g = gc_once(&holder2.store, &holder2.id, &solo2).await.unwrap();
    assert_eq!(g.shards_released, 0, "sans preuve, rien n'est libéré");
    assert_eq!(g.shards_kept, kept_before);
    assert_eq!(holder2.store.list_shards().unwrap().len(), kept_before);
}

#[tokio::test]
async fn proof_is_bound_to_nonce_and_bytes() {
    let node = spawn_node().await;
    let data = vec![42u8; 8192];
    let hash = node.store.put_shard(&data).unwrap();
    let client = PeerClient::connect(node.id.parse().unwrap()).await.unwrap();

    let p1 = client.prove_shard(&hash, [7u8; 32]).await.unwrap().unwrap();
    let p2 = client.prove_shard(&hash, [8u8; 32]).await.unwrap().unwrap();
    assert_ne!(p1, p2, "un nonce différent doit donner une preuve différente");
    assert_eq!(p1, expected_proof(&[7u8; 32], &data), "preuve vérifiable localement");

    // Shard inconnu : pas de preuve possible.
    let unknown = yog_erasure::hash_bytes(b"jamais stocke");
    assert!(client.prove_shard(&unknown, [1u8; 32]).await.unwrap().is_none());
}
