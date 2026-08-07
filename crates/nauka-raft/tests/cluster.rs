//! Integration: 3-node Raft cluster over QUIC — init, election, replicated
//! write to the manifest registry, redirect to the leader.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use nauka_erasure::{encode_file, ErasureConfig};
use nauka_raft::types::{AdminRequest, AdminResponse, AppCommand};
use nauka_raft::{admin_call, write_via_leader, RaftApp};
use nauka_store::ShardStore;
use nauka_transport::server::{make_endpoint_pair, serve_consensus_endpoint, serve_endpoint};
use nauka_transport::PeerClient;

struct Node {
    addr: std::net::SocketAddr,
    app: Arc<RaftApp>,
    _dir: tempfile::TempDir,
}

async fn spawn_raft_node(id: u64) -> Node {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(ShardStore::open(dir.path()).unwrap());
    let (data, consensus) = make_endpoint_pair("127.0.0.1:0".parse().unwrap()).unwrap();
    let addr = data.local_addr().unwrap();
    let app = RaftApp::start(id, &dir.path().join("raft")).await.unwrap();
    let handler: Arc<dyn nauka_transport::server::RaftHandler> = app.clone();
    tokio::spawn(serve_endpoint(store.clone(), data, Some(handler.clone())));
    tokio::spawn(serve_consensus_endpoint(consensus, handler));
    Node {
        addr,
        app,
        _dir: dir,
    }
}

#[tokio::test]
async fn three_node_raft_replicates_manifest_registry() {
    let nodes = [
        spawn_raft_node(1).await,
        spawn_raft_node(2).await,
        spawn_raft_node(3).await,
    ];

    // Init the cluster from node 1 with all 3 members.
    let members: BTreeMap<u64, String> = nodes
        .iter()
        .enumerate()
        .map(|(i, n)| ((i + 1) as u64, n.addr.to_string()))
        .collect();
    let c1 = PeerClient::connect(nodes[0].addr).await.unwrap();
    match admin_call(&c1, &AdminRequest::Init(members)).await.unwrap() {
        AdminResponse::Ok(_) => {}
        other => panic!("init: {other:?}"),
    }

    // Wait for a leader to be elected.
    let mut leader = None;
    for _ in 0..50 {
        if let AdminResponse::Metrics {
            leader: Some(l), ..
        } = admin_call(&c1, &AdminRequest::Metrics).await.unwrap()
        {
            leader = Some(l);
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let leader = leader.expect("no leader elected");
    assert!((1..=3).contains(&leader));

    // Write a manifest through write_via_leader (entry point does not matter).
    let cfg = ErasureConfig {
        data_shards: 2,
        parity_shards: 1,
        shard_size: 64,
    };
    let (manifest, _) = encode_file(b"file replicated by raft", &cfg).unwrap();
    let resp = write_via_leader(
        &nodes.iter().map(|n| n.addr).collect::<Vec<_>>(),
        AppCommand::RegisterManifest(manifest.clone()),
    )
    .await
    .unwrap();
    assert!(resp.ok);

    // The registry is visible on all 3 nodes (replication).
    for node in &nodes {
        let mut found = false;
        for _ in 0..25 {
            if node
                .app
                .app_state()
                .manifests
                .contains_key(&manifest.file_hash)
            {
                found = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        assert!(found, "manifest missing from node {}", node.app.id);
    }

    // Membership is consistent everywhere.
    for node in &nodes {
        assert_eq!(node.app.members().len(), 3);
    }
}
