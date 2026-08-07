//! Anti-starvation regression: while the data plane is saturated with shards,
//! consensus (its own plane on port+1) must stay stable — no re-election,
//! registry writes still going through.
//!
//! This is the scenario that flipped the leader during the 15 GB stress test
//! back when everything shared the same UDP socket.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
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
    _dir: tempfile::TempDir,
}

async fn spawn(id: u64) -> Node {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(ShardStore::open(dir.path()).unwrap());
    let (data, consensus) = make_endpoint_pair("127.0.0.1:0".parse().unwrap()).unwrap();
    let addr = data.local_addr().unwrap();
    let app = RaftApp::start(id, &dir.path().join("raft")).await.unwrap();
    let handler: Arc<dyn nauka_transport::server::RaftHandler> = app.clone();
    tokio::spawn(serve_endpoint(store.clone(), data, Some(handler.clone())));
    tokio::spawn(serve_consensus_endpoint(consensus, handler));
    Node { addr, app, _dir: dir }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn leader_stable_under_data_flood() {
    const FLOOD_SECS: u64 = 12;
    const FLOODERS_PER_NODE: usize = 4;

    let nodes = [spawn(1).await, spawn(2).await, spawn(3).await];
    let members: BTreeMap<u64, String> =
        nodes.iter().map(|n| (n.app.id, n.addr.to_string())).collect();
    let c = PeerClient::connect(nodes[0].addr).await.unwrap();
    assert!(matches!(
        admin_call(&c, &AdminRequest::Init(members)).await.unwrap(),
        AdminResponse::Ok(_)
    ));

    // Initial leader.
    let mut leader0 = None;
    for _ in 0..50 {
        let m = nodes[0].app.raft.metrics().borrow().clone();
        if let Some(l) = m.current_leader {
            leader0 = Some(l);
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let leader0 = leader0.expect("no leader");

    // Flood the data plane: 12 flooders × 1 MiB shards, continuously, at all
    // 3 nodes.
    let stop = Arc::new(AtomicBool::new(false));
    let mut flooders = Vec::new();
    for node in &nodes {
        for f in 0..FLOODERS_PER_NODE {
            let addr = node.addr;
            let stop = stop.clone();
            flooders.push(tokio::spawn(async move {
                let Ok(client) = PeerClient::connect(addr).await else { return 0usize };
                let mut base = vec![0u8; 1024 * 1024];
                base[..8].copy_from_slice(&(f as u64).to_le_bytes());
                let mut sent = 0usize;
                let mut i = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    i += 1;
                    base[8..16].copy_from_slice(&i.to_le_bytes());
                    if client.put_shard(base.clone()).await.is_ok() {
                        sent += 1;
                    }
                }
                sent
            }));
        }
    }

    // During the flood: watch the leader and write to the registry.
    let cfg = ErasureConfig { data_shards: 2, parity_shards: 1, shard_size: 64 };
    let deadline = Instant::now() + Duration::from_secs(FLOOD_SECS);
    let mut leader_changes = 0;
    let mut last_leader = leader0;
    let mut writes_ok = 0usize;
    let mut writes_failed = 0usize;
    let mut wi = 0usize;
    while Instant::now() < deadline {
        for n in &nodes {
            let m = n.app.raft.metrics().borrow().clone();
            if let Some(l) = m.current_leader {
                if l != last_leader {
                    leader_changes += 1;
                    last_leader = l;
                }
            }
        }
        wi += 1;
        let (manifest, _) =
            encode_file(format!("flood-write-{wi}").as_bytes(), &cfg).unwrap();
        let cmd = AppCommand::RegisterManifest(manifest);
        let mut ok = false;
        for n in &nodes {
            if let Ok(client) = PeerClient::connect(n.addr).await {
                if let Ok(AdminResponse::Ok(r)) =
                    admin_call(&client, &AdminRequest::Write(cmd.clone())).await
                {
                    if r.ok {
                        ok = true;
                        break;
                    }
                }
            }
        }
        if ok {
            writes_ok += 1;
        } else {
            writes_failed += 1;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    stop.store(true, Ordering::Relaxed);
    let mut total_shards = 0usize;
    for f in flooders {
        total_shards += f.await.unwrap();
    }

    println!(
        "flood: {total_shards} shards ({} MB) in {FLOOD_SECS}s, \
         {writes_ok} writes ok / {writes_failed} failed, \
         {leader_changes} leader change(s)",
        total_shards
    );
    assert!(total_shards > 100, "the flood did not actually saturate ({total_shards} shards)");
    assert_eq!(leader_changes, 0, "the leader flipped under data load");
    assert_eq!(writes_failed, 0, "registry writes failed under load");
}
