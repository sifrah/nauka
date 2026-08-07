//! THE durability test: the ENTIRE cluster goes down (total crash, power cut)
//! and restarts from the data-dirs. The registry must come back intact, with
//! no healthy node left to help.
//!
//! Phase 1: few writes (< snapshot threshold) → recovery by pure redb log
//! replay. Phase 2: enough writes to trigger a snapshot + log purge →
//! recovery by snapshot + replay of the remainder.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
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
}

async fn spawn(id: u64, dir: &PathBuf, addr: SocketAddr) -> Node {
    let store = Arc::new(ShardStore::open(dir.join("store")).unwrap());
    // After a shutdown, the sockets can take a moment to be released.
    let mut pair = None;
    for _ in 0..50 {
        match make_endpoint_pair(addr) {
            Ok(p) => {
                pair = Some(p);
                break;
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(200)).await,
        }
    }
    let (endpoint, consensus_endpoint) = pair.expect("sockets never released");
    let addr = endpoint.local_addr().unwrap();
    let app = RaftApp::start(id, &dir.join("raft")).await.unwrap();
    let handler: Arc<dyn nauka_transport::server::RaftHandler> = app.clone();
    tokio::spawn(serve_endpoint(store.clone(), endpoint.clone(), Some(handler.clone())));
    tokio::spawn(serve_consensus_endpoint(consensus_endpoint.clone(), handler));
    Node { addr, app, endpoint, consensus_endpoint }
}

async fn full_shutdown(nodes: Vec<Node>) {
    for n in &nodes {
        n.app.raft.shutdown().await.unwrap();
        n.endpoint.close(0u32.into(), b"power-cut");
        n.consensus_endpoint.close(0u32.into(), b"power-cut");
    }
    drop(nodes);
    tokio::time::sleep(Duration::from_millis(300)).await;
}

async fn wait_any_leader(nodes: &[Node]) -> u64 {
    for _ in 0..150 {
        for n in nodes {
            let m = n.app.raft.metrics().borrow().clone();
            if let Some(l) = m.current_leader {
                return l;
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    panic!("no leader after restart");
}

async fn wait_registry(nodes: &[Node], expected: usize) {
    let deadline = Instant::now() + Duration::from_secs(60);
    for n in nodes {
        loop {
            let count = n.app.app_state().manifests.len();
            if count == expected {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "node {} stuck at {count}/{expected}",
                n.app.id
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
}

fn manifest(i: usize) -> nauka_erasure::FileManifest {
    let cfg = ErasureConfig { data_shards: 2, parity_shards: 1, shard_size: 64 };
    encode_file(format!("persistence {i}").as_bytes(), &cfg).unwrap().0
}

async fn write_batch(nodes: &[Node], range: std::ops::Range<usize>) {
    // Write through the current leader, retrying across leader changes.
    for i in range {
        let cmd = AppCommand::RegisterManifest(manifest(i));
        let mut done = false;
        'attempts: for _ in 0..30 {
            for n in nodes {
                let Ok(c) = PeerClient::connect(n.addr).await else { continue };
                if let Ok(AdminResponse::Ok(r)) =
                    admin_call(&c, &AdminRequest::Write(cmd.clone())).await
                {
                    if r.ok {
                        done = true;
                        break 'attempts;
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
        assert!(done, "write {i} impossible");
    }
}

#[tokio::test]
async fn full_cluster_power_cut_and_restart() {
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let dir_paths: Vec<PathBuf> = dirs.iter().map(|d| d.path().to_path_buf()).collect();

    // Initial startup on ephemeral ports, addresses remembered.
    let mut nodes = Vec::new();
    for (i, dir) in dir_paths.iter().enumerate() {
        nodes.push(spawn((i + 1) as u64, dir, "127.0.0.1:0".parse().unwrap()).await);
    }
    let addrs: Vec<SocketAddr> = nodes.iter().map(|n| n.addr).collect();
    let members: BTreeMap<u64, String> = nodes
        .iter()
        .map(|n| (n.app.id, n.addr.to_string()))
        .collect();
    let c = PeerClient::connect(addrs[0]).await.unwrap();
    assert!(matches!(
        admin_call(&c, &AdminRequest::Init(members)).await.unwrap(),
        AdminResponse::Ok(_)
    ));
    wait_any_leader(&nodes).await;

    // ── Phase 1: 40 writes (< 256, no snapshot) then a full power cut.
    write_batch(&nodes, 0..40).await;
    wait_registry(&nodes, 40).await;
    full_shutdown(nodes).await;

    // Full restart from the data-dirs: redb log replay.
    let mut nodes = Vec::new();
    for (i, dir) in dir_paths.iter().enumerate() {
        nodes.push(spawn((i + 1) as u64, dir, addrs[i]).await);
    }
    let leader = wait_any_leader(&nodes).await;
    println!("restart 1: leader {leader}");
    wait_registry(&nodes, 40).await;

    // ── Phase 2: 300 more writes → snapshots + log purge, then another full
    //    power cut.
    write_batch(&nodes, 40..340).await;
    wait_registry(&nodes, 340).await;
    full_shutdown(nodes).await;

    // Restart: recovery by snapshot + leftover log.
    let mut nodes = Vec::new();
    for (i, dir) in dir_paths.iter().enumerate() {
        nodes.push(spawn((i + 1) as u64, dir, addrs[i]).await);
    }
    let leader = wait_any_leader(&nodes).await;
    println!("restart 2: leader {leader}");
    wait_registry(&nodes, 340).await;

    // And the cluster stays functional: one more write goes through.
    write_batch(&nodes, 340..341).await;
    wait_registry(&nodes, 341).await;
}
