//! Stress tests du consensus : volume d'écritures concurrentes, crash du
//! leader en plein trafic, résurrection d'un nœud à état vide.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use yog_erasure::{encode_file, ErasureConfig};
use yog_raft::types::{AdminRequest, AdminResponse, AppCommand};
use yog_raft::{admin_call, RaftApp};
use yog_store::ShardStore;
use yog_transport::server::{make_endpoint, serve_endpoint};
use yog_transport::PeerClient;

struct Node {
    addr: SocketAddr,
    app: Arc<RaftApp>,
    endpoint: quinn::Endpoint,
    _dir: tempfile::TempDir,
}

async fn spawn_raft_node(id: u64, bind: &str) -> Node {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(ShardStore::open(dir.path()).unwrap());
    let endpoint = make_endpoint(bind.parse().unwrap()).unwrap();
    let addr = endpoint.local_addr().unwrap();
    let app = RaftApp::start(id).await.unwrap();
    let handler: Arc<dyn yog_transport::server::RaftHandler> = app.clone();
    tokio::spawn(serve_endpoint(store, endpoint.clone(), Some(handler)));
    Node { addr, app, endpoint, _dir: dir }
}

fn test_manifest(i: usize) -> yog_erasure::FileManifest {
    let cfg = ErasureConfig { data_shards: 2, parity_shards: 1, shard_size: 64 };
    let data = format!("manifest de stress numero {i}");
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
    panic!("pas de leader élu");
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
            "nœud {} bloqué à {n}/{expected} manifests",
            app.id
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// 1. Volume : 500 écritures par 32 workers concurrents sur le leader,
///    convergence vérifiée sur les 3 nœuds.
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
                    other => panic!("écriture {i}: {other:?}"),
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
        "{WRITES} écritures en {elapsed:?} ({:.0} writes/s)",
        WRITES as f64 / elapsed.as_secs_f64()
    );

    for n in &nodes {
        wait_registry_size(&n.app, WRITES, 30).await;
    }
}

/// 2. Crash du leader en plein trafic : les survivants ré-élisent et les
///    écritures reprennent sans perte de ce qui a été committé.
/// 3. Résurrection : le nœud crashé revient avec un état VIDE (log mémoire)
///    et rattrape tout le registre depuis le nouveau leader.
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

    // Trafic initial.
    let client = PeerClient::connect(leader_addr).await.unwrap();
    for i in 0..BEFORE {
        let cmd = AppCommand::RegisterManifest(test_manifest(i));
        match admin_call(&client, &AdminRequest::Write(cmd)).await.unwrap() {
            AdminResponse::Ok(r) if r.ok => {}
            other => panic!("écriture {i}: {other:?}"),
        }
    }

    // Crash brutal du leader : moteur Raft arrêté, endpoint fermé, et tout
    // son état droppé (données perdues, socket libéré).
    let idx = nodes.iter().position(|n| n.app.id == leader_id).unwrap();
    let crashed = nodes.remove(idx);
    let crashed_addr = crashed.addr;
    crashed.app.raft.shutdown().await.unwrap();
    crashed.endpoint.close(0u32.into(), b"crash");
    drop(crashed);
    println!("leader {leader_id} crashé");

    // Ré-élection parmi les survivants.
    let (new_leader, new_leader_addr) = wait_leader(&nodes, Some(leader_id)).await;
    assert_ne!(new_leader, leader_id);
    println!("nouveau leader: {new_leader}");

    // Le trafic reprend. Les premières écritures peuvent échouer pendant la
    // bascule : on retente.
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
        assert!(done, "écriture {i} impossible après failover");
    }

    // Les 2 survivants convergent à BEFORE+AFTER.
    for n in &nodes {
        wait_registry_size(&n.app, BEFORE + AFTER, 30).await;
    }

    // Résurrection : même id, même adresse, état totalement vide.
    // Le socket peut mettre un instant à se libérer après le drop.
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(ShardStore::open(dir.path()).unwrap());
    let mut endpoint = None;
    for _ in 0..50 {
        match make_endpoint(crashed_addr) {
            Ok(e) => {
                endpoint = Some(e);
                break;
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(200)).await,
        }
    }
    let endpoint = endpoint.expect("socket jamais libéré");
    let revived = RaftApp::start(leader_id).await.unwrap();
    let handler: Arc<dyn yog_transport::server::RaftHandler> = revived.clone();
    tokio::spawn(serve_endpoint(store, endpoint, Some(handler)));

    // Il doit rattraper tout le registre (snapshot ou replay du log).
    wait_registry_size(&revived, BEFORE + AFTER, 60).await;
    println!(
        "nœud {leader_id} ressuscité et à jour: {} manifests",
        revived.app_state().manifests.len()
    );
}
