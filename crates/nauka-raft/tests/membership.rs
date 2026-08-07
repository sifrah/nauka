//! Live membership changes: a 3-node cluster grows to 4, shards rebalance
//! (the newcomer acquires, the old ones release), then a node is removed and
//! the cluster re-replicates without it.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use nauka_cluster::healer::{gc_once, scrub_once};
use nauka_cluster::placement::shards_owned_by;
use nauka_erasure::{encode_file, ErasureConfig};
use nauka_raft::types::{AdminRequest, AdminResponse, AppCommand};
use nauka_raft::{admin_via_leader, write_via_leader, RaftApp};
use nauka_store::ShardStore;
use nauka_transport::server::{make_endpoint_pair, serve_consensus_endpoint, serve_endpoint};

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
    let handler: Arc<dyn nauka_transport::server::RaftHandler> = app.clone();
    tokio::spawn(serve_endpoint(store.clone(), data, Some(handler.clone())));
    tokio::spawn(serve_consensus_endpoint(consensus, handler));
    Node {
        addr,
        app,
        store,
        _dir: dir,
    }
}

/// Sorted view of the member addresses, like the nodes' background loop.
fn view_of(members: &BTreeMap<u64, String>) -> Vec<(String, u64)> {
    let mut v: Vec<(String, u64)> = members.values().map(|a| (a.clone(), 1)).collect();
    v.sort();
    v
}

/// Syncs manifests + scrub + gc on each listed node (one pass of the
/// background loop, run synchronously so the test stays deterministic).
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

/// Checks that every shard of every manifest is present on its owner
/// according to `view`, and that non-owners keep nothing.
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
            assert!(n.store.get_shard(h).is_ok(), "{id} should hold {h}");
        }
        for h in n.store.list_shards().unwrap() {
            assert!(owned.contains(&h), "{id} keeps a foreign shard {h}");
        }
    }
}

#[tokio::test]
async fn grow_to_four_then_remove_one_rebalances() {
    // ── Initial cluster: 3 nodes + 5 files.
    let n1 = spawn(1).await;
    let n2 = spawn(2).await;
    let n3 = spawn(3).await;
    let members: BTreeMap<u64, String> = [&n1, &n2, &n3]
        .iter()
        .map(|n| (n.app.id, n.addr.to_string()))
        .collect();
    let peers: Vec<SocketAddr> = [&n1, &n2, &n3].iter().map(|n| n.addr).collect();
    let c = nauka_transport::PeerClient::connect(n1.addr).await.unwrap();
    assert!(matches!(
        nauka_raft::admin_call(&c, &AdminRequest::Init(members))
            .await
            .unwrap(),
        AdminResponse::Ok(_)
    ));

    let cfg = ErasureConfig {
        data_shards: 4,
        parity_shards: 2,
        shard_size: 8 * 1024,
    };
    let mut manifests = Vec::new();
    for i in 0..5 {
        let data: Vec<u8> = (0..cfg.stripe_data_len() * 2)
            .map(|b| ((b + i * 31) % 251) as u8)
            .collect();
        let (manifest, stripes) = encode_file(&data, &cfg).unwrap();
        // Store the shards on their owners (3-node view).
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

    // ── Growth: node 4 joins (learner → voter).
    let n4 = spawn(4).await;
    match admin_via_leader(
        &peers,
        &AdminRequest::AddLearner {
            id: 4,
            addr: n4.addr.to_string(),
        },
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

    // The 4-member membership propagates everywhere (including the newcomer).
    for _ in 0..50 {
        if n4.app.members().len() == 4 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert_eq!(
        n4.app.members().len(),
        4,
        "membership not propagated to node 4"
    );

    // Rebalance: scrub (n4 acquires) then gc (the old ones release).
    let all4 = [&n1, &n2, &n3, &n4];
    let view4 = view_of(&n4.app.members());
    converge(&all4, &view4).await;
    converge(&all4, &view4).await; // 2nd pass: gc once everyone has scrubbed

    let n4_shards = n4.store.list_shards().unwrap().len();
    assert!(n4_shards > 0, "the new node acquired no shard");
    assert_placement_clean(&all4, &view4);

    // ── Removal: node 3 leaves the cluster.
    match admin_via_leader(&peers, &AdminRequest::ChangeMembership(vec![1, 2, 4]))
        .await
        .unwrap()
    {
        AdminResponse::Ok(_) => {}
        other => panic!("removal: {other:?}"),
    }
    for _ in 0..50 {
        if n1.app.members().len() == 3 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let view3b = view_of(&n1.app.members());
    assert!(!view3b.iter().any(|(n, _)| *n == n3.addr.to_string()));

    // The remaining 3 converge (n3 is still up but out of the view — it keeps
    // serving reads while draining).
    let rest = [&n1, &n2, &n4];
    converge(&rest, &view3b).await;
    converge(&rest, &view3b).await;
    assert_placement_clean(&rest, &view3b);

    // Every file stays fully reconstructible WITHOUT node 3.
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
            nauka_erasure::decode_stripe(slots, stripe, &manifest.config)
                .unwrap_or_else(|e| panic!("stripe {si} unrecoverable without n3: {e}"));
        }
    }
}
