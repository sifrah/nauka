//! Consensus stress tests: concurrent write volume, leader crash in the
//! middle of traffic, revival of a node with empty state.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use nauka_erasure::{encode_file, ErasureConfig};
use nauka_raft::types::{AdminRequest, AdminResponse, AppCommand};
use nauka_raft::{admin_call, RaftApp};
use nauka_store::ShardStore;
use nauka_transport::server::{make_endpoint_pair, serve_consensus_endpoint, serve_endpoint};
use nauka_transport::PeerClient;

struct Node {
    addr: SocketAddr,
    app: Arc<RaftApp>,
    endpoint: quinn::Endpoint,
    consensus_endpoint: quinn::Endpoint,
    _dir: tempfile::TempDir,
}

async fn spawn_raft_node(id: u64, bind: &str) -> Node {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(ShardStore::open(dir.path()).unwrap());
    let (endpoint, consensus_endpoint) = make_endpoint_pair(bind.parse().unwrap()).unwrap();
    let addr = endpoint.local_addr().unwrap();
    let app = RaftApp::start(id, &dir.path().join("raft")).await.unwrap();
    let handler: Arc<dyn nauka_transport::server::RaftHandler> = app.clone();
    tokio::spawn(serve_endpoint(store.clone(), endpoint.clone(), Some(handler.clone())));
    tokio::spawn(serve_consensus_endpoint(consensus_endpoint.clone(), handler));
    Node { addr, app, endpoint, consensus_endpoint, _dir: dir }
}

fn test_manifest(i: usize) -> nauka_erasure::FileManifest {
    let cfg = ErasureConfig { data_shards: 2, parity_shards: 1, shard_size: 64 };
    let data = format!("stress manifest number {i}");
    encode_file(data.as_bytes(), &cfg).unwrap().0
}

async fn init_cluster(nodes: &[Node]) {
    let members: BTreeMap<u64, String> =
        nodes.iter().map(|n| (n.app.id, n.addr.to_string())).collect();
    let c = PeerClient::connect(nodes[0].addr).await.unwrap();
    match admin_call(&c, &AdminRequest::Init(members)).await.unwrap() {
        AdminResponse::Ok(_) => {}
        other => panic!("init: {other:?}"),
    }
}

async fn wait_leader(nodes: &[Node], exclude: Option<u64>) -> (u64, SocketAddr) {
    for _ in 0..100 {
        for n in nodes {
            if Some(n.app.id) == exclude {
                continue;
            }
            let metrics = n.app.raft.metrics().borrow().clone();
            if let Some(l) = metrics.current_leader {
                if Some(l) != exclude {
                    if let Some(ln) = nodes.iter().find(|n| n.app.id == l) {
                        return (l, ln.addr);
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    panic!("no leader elected");
}

async fn wait_registry_size(app: &Arc<RaftApp>, expected: usize, timeout_s: u64) {
    let deadline = Instant::now() + Duration::from_secs(timeout_s);
    loop {
        let n = app.app_state().manifests.len();
        if n >= expected {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "node {} stuck at {n}/{expected} manifests",
            app.id
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// 1. Volume: 500 writes from 32 concurrent workers against the leader,
///    convergence checked on all 3 nodes.
#[tokio::test]
async fn concurrent_write_volume_converges() {
    const WRITES: usize = 500;
    const WORKERS: usize = 32;

    let nodes = [
        spawn_raft_node(1, "127.0.0.1:0").await,
        spawn_raft_node(2, "127.0.0.1:0").await,
        spawn_raft_node(3, "127.0.0.1:0").await,
    ];
    init_cluster(&nodes).await;
    let (_, leader_addr) = wait_leader(&nodes, None).await;

    let client = PeerClient::connect(leader_addr).await.unwrap();
    let start = Instant::now();
    let mut handles = Vec::new();
    for w in 0..WORKERS {
        let client = client.clone();
        handles.push(tokio::spawn(async move {
            let mut ok = 0usize;
            for i in (w..WRITES).step_by(WORKERS) {
                let cmd = AppCommand::RegisterManifest(test_manifest(i));
                match admin_call(&client, &AdminRequest::Write(cmd)).await {
                    Ok(AdminResponse::Ok(r)) if r.ok => ok += 1,
                    other => panic!("write {i}: {other:?}"),
                }
            }
            ok
        }));
    }
    let mut total = 0;
    for h in handles {
        total += h.await.unwrap();
    }
    let elapsed = start.elapsed();
    assert_eq!(total, WRITES);
    println!(
        "{WRITES} writes in {elapsed:?} ({:.0} writes/s)",
        WRITES as f64 / elapsed.as_secs_f64()
    );

    for n in &nodes {
        wait_registry_size(&n.app, WRITES, 30).await;
    }
}

/// 2. Leader crash in the middle of traffic: the survivors re-elect and
///    writes resume without losing anything that was committed.
/// 3. Revival: the crashed node comes back with EMPTY state (in-memory log)
///    and catches up on the whole registry from the new leader.
#[tokio::test]
async fn leader_crash_failover_and_catchup() {
    const BEFORE: usize = 100;
    const AFTER: usize = 100;

    let mut nodes = vec![
        spawn_raft_node(1, "127.0.0.1:0").await,
        spawn_raft_node(2, "127.0.0.1:0").await,
        spawn_raft_node(3, "127.0.0.1:0").await,
    ];
    init_cluster(&nodes).await;
    let (leader_id, leader_addr) = wait_leader(&nodes, None).await;

    // Initial traffic.
    let client = PeerClient::connect(leader_addr).await.unwrap();
    for i in 0..BEFORE {
        let cmd = AppCommand::RegisterManifest(test_manifest(i));
        match admin_call(&client, &AdminRequest::Write(cmd)).await.unwrap() {
            AdminResponse::Ok(r) if r.ok => {}
            other => panic!("write {i}: {other:?}"),
        }
    }

    // Hard leader crash: Raft engine stopped, endpoint closed, and all of its
    // state dropped (data lost, socket released).
    let idx = nodes.iter().position(|n| n.app.id == leader_id).unwrap();
    let crashed = nodes.remove(idx);
    let crashed_addr = crashed.addr;
    crashed.app.raft.shutdown().await.unwrap();
    crashed.endpoint.close(0u32.into(), b"crash");
    crashed.consensus_endpoint.close(0u32.into(), b"crash");
    drop(crashed);
    println!("leader {leader_id} crashed");

    // Re-election among the survivors.
    let (new_leader, new_leader_addr) = wait_leader(&nodes, Some(leader_id)).await;
    assert_ne!(new_leader, leader_id);
    println!("new leader: {new_leader}");

    // Traffic resumes. The first writes may fail during the switchover, so we
    // retry.
    let client = PeerClient::connect(new_leader_addr).await.unwrap();
    for i in BEFORE..BEFORE + AFTER {
        let cmd = AppCommand::RegisterManifest(test_manifest(i));
        let mut done = false;
        for _ in 0..20 {
            match admin_call(&client, &AdminRequest::Write(cmd.clone())).await {
                Ok(AdminResponse::Ok(r)) if r.ok => {
                    done = true;
                    break;
                }
                _ => tokio::time::sleep(Duration::from_millis(250)).await,
            }
        }
        assert!(done, "write {i} impossible after failover");
    }

    // The 2 survivors converge at BEFORE+AFTER.
    for n in &nodes {
        wait_registry_size(&n.app, BEFORE + AFTER, 30).await;
    }

    // Revival: same id, same address, completely empty state (fresh data-dir
    // — the worst case, disk lost).
    // The socket can take a moment to be released after the drop.
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(ShardStore::open(dir.path()).unwrap());
    let mut pair = None;
    for _ in 0..50 {
        match make_endpoint_pair(crashed_addr) {
            Ok(p) => {
                pair = Some(p);
                break;
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(200)).await,
        }
    }
    let (endpoint, consensus_endpoint) = pair.expect("sockets never released");
    let revived = RaftApp::start(leader_id, &dir.path().join("raft")).await.unwrap();
    let handler: Arc<dyn nauka_transport::server::RaftHandler> = revived.clone();
    tokio::spawn(serve_endpoint(store.clone(), endpoint, Some(handler.clone())));
    tokio::spawn(serve_consensus_endpoint(consensus_endpoint, handler));

    // It must catch up on the whole registry (snapshot or log replay).
    wait_registry_size(&revived, BEFORE + AFTER, 60).await;
    println!(
        "node {leader_id} revived and up to date: {} manifests",
        revived.app_state().manifests.len()
    );
}
