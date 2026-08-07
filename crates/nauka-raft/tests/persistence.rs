//! LE test de durabilité : le cluster ENTIER s'éteint (crash total, coupure
//! électrique) et redémarre depuis les data-dirs. Le registre doit revenir
//! intact, sans aucun nœud sain pour aider.
//!
//! Phase 1 : peu d'écritures (< seuil de snapshot) → recovery par replay du
//! log redb pur. Phase 2 : assez d'écritures pour déclencher snapshot +
//! purge du log → recovery par snapshot + replay du reliquat.

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
    // Après un arrêt, les sockets peuvent mettre un instant à se libérer.
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
    let (endpoint, consensus_endpoint) = pair.expect("sockets jamais libérés");
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
    panic!("pas de leader après redémarrage");
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
                "nœud {} bloqué à {count}/{expected}",
                n.app.id
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
}

fn manifest(i: usize) -> nauka_erasure::FileManifest {
    let cfg = ErasureConfig { data_shards: 2, parity_shards: 1, shard_size: 64 };
    encode_file(format!("persistance {i}").as_bytes(), &cfg).unwrap().0
}

async fn write_batch(nodes: &[Node], range: std::ops::Range<usize>) {
    // Écrit via le leader courant, avec retry pendant les bascules.
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
        assert!(done, "écriture {i} impossible");
    }
}

#[tokio::test]
async fn full_cluster_power_cut_and_restart() {
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let dir_paths: Vec<PathBuf> = dirs.iter().map(|d| d.path().to_path_buf()).collect();

    // Démarrage initial sur ports éphémères, adresses mémorisées.
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

    // ── Phase 1 : 40 écritures (< 256, aucun snapshot) puis coupure totale.
    write_batch(&nodes, 0..40).await;
    wait_registry(&nodes, 40).await;
    full_shutdown(nodes).await;

    // Redémarrage complet depuis les data-dirs : replay du log redb.
    let mut nodes = Vec::new();
    for (i, dir) in dir_paths.iter().enumerate() {
        nodes.push(spawn((i + 1) as u64, dir, addrs[i]).await);
    }
    let leader = wait_any_leader(&nodes).await;
    println!("redémarrage 1 : leader {leader}");
    wait_registry(&nodes, 40).await;

    // ── Phase 2 : 300 écritures de plus → snapshots + purge du log,
    //    puis nouvelle coupure totale.
    write_batch(&nodes, 40..340).await;
    wait_registry(&nodes, 340).await;
    full_shutdown(nodes).await;

    // Redémarrage : recovery par snapshot + reliquat de log.
    let mut nodes = Vec::new();
    for (i, dir) in dir_paths.iter().enumerate() {
        nodes.push(spawn((i + 1) as u64, dir, addrs[i]).await);
    }
    let leader = wait_any_leader(&nodes).await;
    println!("redémarrage 2 : leader {leader}");
    wait_registry(&nodes, 340).await;

    // Et le cluster reste fonctionnel : une écriture de plus passe.
    write_batch(&nodes, 340..341).await;
    wait_registry(&nodes, 341).await;
}
