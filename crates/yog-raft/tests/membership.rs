//! Membership à chaud : un cluster de 3 nœuds grandit à 4, les shards se
//! rebalancent (le nouveau acquiert, les anciens libèrent), puis un nœud
//! est retiré et le cluster re-réplique sans lui.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use yog_cluster::healer::{gc_once, scrub_once};
use yog_cluster::placement::shards_owned_by;
use yog_erasure::{encode_file, ErasureConfig};
use yog_raft::types::{AdminRequest, AdminResponse, AppCommand};
use yog_raft::{admin_via_leader, write_via_leader, RaftApp};
use yog_store::ShardStore;
use yog_transport::server::{make_endpoint_pair, serve_consensus_endpoint, serve_endpoint};

struct Node {
    addr: SocketAddr,
    app: Arc<RaftApp>,
    store: Arc<ShardStore>,
    _dir: tempfile::TempDir,
}

async fn spawn(id: u64) -> Node {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(ShardStore::open(dir.path().join("store")).unwrap());
    let (data, consensus) = make_endpoint_pair("127.0.0.1:0".parse().unwrap()).unwrap();
    let addr = data.local_addr().unwrap();
    let app = RaftApp::start(id, &dir.path().join("raft")).await.unwrap();
    let handler: Arc<dyn yog_transport::server::RaftHandler> = app.clone();
    tokio::spawn(serve_endpoint(store.clone(), data, Some(handler.clone())));
    tokio::spawn(serve_consensus_endpoint(consensus, handler));
    Node { addr, app, store, _dir: dir }
}

/// Vue triée des adresses membres, comme la boucle de fond des nœuds.
fn view_of(members: &BTreeMap<u64, String>) -> Vec<(String, u64)> {
    let mut v: Vec<(String, u64)> = members.values().map(|a| (a.clone(), 1)).collect();
    v.sort();
    v
}

/// Synchronise manifests + scrub + gc sur chaque nœud listé (une passe de
/// la boucle de fond, en synchrone pour le déterminisme du test).
async fn converge(nodes: &[&Node], view: &[(String, u64)]) {
    for n in nodes {
        for manifest in n.app.app_state().manifests.values() {
            if n.store.get_manifest(&manifest.file_hash).is_err() {
                n.store.put_manifest(manifest).unwrap();
            }
        }
        let id = n.addr.to_string();
        scrub_once(&n.store, &id, view).await.unwrap();
    }
    for n in nodes {
        let id = n.addr.to_string();
        gc_once(&n.store, &id, view).await.unwrap();
    }
}

/// Vérifie que chaque shard de chaque manifest est présent chez son
/// propriétaire selon `view`, et que les non-propriétaires ne gardent rien.
fn assert_placement_clean(nodes: &[&Node], view: &[(String, u64)]) {
    let refs: Vec<(&str, u64)> = view.iter().map(|(n, w)| (n.as_str(), *w)).collect();
    for n in nodes {
        let id = n.addr.to_string();
        let mut owned: std::collections::BTreeSet<String> = Default::default();
        for fh in n.store.list_manifests().unwrap() {
            let m = n.store.get_manifest(&fh).unwrap();
            for (_, _, h) in shards_owned_by(&m, &id, &refs) {
                owned.insert(h.to_string());
            }
        }
        for h in &owned {
            assert!(n.store.get_shard(h).is_ok(), "{id} devrait avoir {h}");
        }
        for h in n.store.list_shards().unwrap() {
            assert!(owned.contains(&h), "{id} garde un shard étranger {h}");
        }
    }
}

#[tokio::test]
async fn grow_to_four_then_remove_one_rebalances() {
    // ── Cluster initial : 3 nœuds + 5 fichiers.
    let n1 = spawn(1).await;
    let n2 = spawn(2).await;
    let n3 = spawn(3).await;
    let members: BTreeMap<u64, String> = [&n1, &n2, &n3]
        .iter()
        .map(|n| (n.app.id, n.addr.to_string()))
        .collect();
    let peers: Vec<SocketAddr> = [&n1, &n2, &n3].iter().map(|n| n.addr).collect();
    let c = yog_transport::PeerClient::connect(n1.addr).await.unwrap();
    assert!(matches!(
        yog_raft::admin_call(&c, &AdminRequest::Init(members)).await.unwrap(),
        AdminResponse::Ok(_)
    ));

    let cfg = ErasureConfig { data_shards: 4, parity_shards: 2, shard_size: 8 * 1024 };
    let mut manifests = Vec::new();
    for i in 0..5 {
        let data: Vec<u8> = (0..cfg.stripe_data_len() * 2)
            .map(|b| ((b + i * 31) % 251) as u8)
            .collect();
        let (manifest, stripes) = encode_file(&data, &cfg).unwrap();
        // Dépose les shards chez leurs propriétaires (vue à 3 nœuds).
        let view3: Vec<(String, u64)> = {
            let mut v: Vec<(String, u64)> = peers.iter().map(|a| (a.to_string(), 1)).collect();
            v.sort();
            v
        };
        let refs: Vec<(&str, u64)> = view3.iter().map(|(n, w)| (n.as_str(), *w)).collect();
        for node in [&n1, &n2, &n3] {
            node.store.put_manifest(&manifest).unwrap();
            for (si, sj, _) in shards_owned_by(&manifest, &node.addr.to_string(), &refs) {
                node.store.put_shard(&stripes[si][sj].data).unwrap();
            }
        }
        write_via_leader(&peers, AppCommand::RegisterManifest(manifest.clone()))
            .await
            .unwrap();
        manifests.push(manifest);
    }

    // ── Croissance : nœud 4 rejoint (learner → votant).
    let n4 = spawn(4).await;
    match admin_via_leader(
        &peers,
        &AdminRequest::AddLearner { id: 4, addr: n4.addr.to_string() },
    )
    .await
    .unwrap()
    {
        AdminResponse::Ok(_) => {}
        other => panic!("add-learner: {other:?}"),
    }
    match admin_via_leader(&peers, &AdminRequest::ChangeMembership(vec![1, 2, 3, 4]))
        .await
        .unwrap()
    {
        AdminResponse::Ok(_) => {}
        other => panic!("promotion: {other:?}"),
    }

    // Le membership à 4 se propage partout (y compris au nouveau).
    for _ in 0..50 {
        if n4.app.members().len() == 4 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert_eq!(n4.app.members().len(), 4, "membership pas propagé au nœud 4");

    // Rebalancement : scrub (n4 acquiert) puis gc (les anciens libèrent).
    let all4 = [&n1, &n2, &n3, &n4];
    let view4 = view_of(&n4.app.members());
    converge(&all4, &view4).await;
    converge(&all4, &view4).await; // 2e passe : gc après que tous ont scrubé

    let n4_shards = n4.store.list_shards().unwrap().len();
    assert!(n4_shards > 0, "le nouveau nœud n'a acquis aucun shard");
    assert_placement_clean(&all4, &view4);

    // ── Retrait : le nœud 3 quitte le cluster.
    match admin_via_leader(&peers, &AdminRequest::ChangeMembership(vec![1, 2, 4]))
        .await
        .unwrap()
    {
        AdminResponse::Ok(_) => {}
        other => panic!("retrait: {other:?}"),
    }
    for _ in 0..50 {
        if n1.app.members().len() == 3 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let view3b = view_of(&n1.app.members());
    assert!(!view3b.iter().any(|(n, _)| *n == n3.addr.to_string()));

    // Les 3 restants convergent (n3 encore allumé mais plus dans la vue —
    // il sert encore les lectures pendant le drain).
    let rest = [&n1, &n2, &n4];
    converge(&rest, &view3b).await;
    converge(&rest, &view3b).await;
    assert_placement_clean(&rest, &view3b);

    // Chaque fichier reste entièrement reconstructible SANS le nœud 3.
    for manifest in &manifests {
        for (si, stripe) in manifest.stripes.iter().enumerate() {
            let mut slots = Vec::new();
            for hash in &stripe.shard_hashes {
                let mut found = None;
                for n in &rest {
                    if let Ok(d) = n.store.get_shard(hash) {
                        found = Some(d);
                        break;
                    }
                }
                slots.push(found);
            }
            yog_erasure::decode_stripe(slots, stripe, &manifest.config)
                .unwrap_or_else(|e| panic!("stripe {si} irrécupérable sans n3: {e}"));
        }
    }
}
