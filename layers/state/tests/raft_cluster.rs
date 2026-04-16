//! End-to-end integration tests for the Raft consensus layer.
//!
//! Spins up in-process 3-node clusters over loopback TCP and exercises
//! `RaftNode::write` — the same path used by the daemon.

use std::sync::Arc;
use std::time::{Duration, Instant};

use nauka_state::{Database, RaftNode};
use serde::Deserialize;
use surrealdb::types::SurrealValue;

const TEST_SCHEMA: &str = "\
DEFINE TABLE IF NOT EXISTS kv SCHEMAFULL;\
DEFINE FIELD IF NOT EXISTS key ON kv TYPE string;\
DEFINE FIELD IF NOT EXISTS value ON kv TYPE string;\
DEFINE INDEX IF NOT EXISTS kv_key ON kv FIELDS key UNIQUE;\
";

#[derive(Deserialize, SurrealValue, Debug)]
struct Kv {
    #[allow(dead_code)]
    key: String,
    value: String,
}

struct TestNode {
    raft: Arc<RaftNode>,
    db: Arc<Database>,
    addr: String,
    _dir: tempfile::TempDir,
}

fn pick_free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    l.local_addr().expect("local_addr").port()
}

async fn spawn_node(node_id: u64) -> TestNode {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("raft.db");
    let db = Arc::new(
        Database::open(Some(path.to_str().unwrap()))
            .await
            .expect("open db"),
    );
    db.query(nauka_state::SCHEMA).await.expect("raft schema");
    db.query(TEST_SCHEMA).await.expect("test schema");

    let port = pick_free_port();
    let addr = format!("127.0.0.1:{port}");

    let raft = Arc::new(
        RaftNode::new(node_id, db.clone(), None)
            .await
            .expect("raft node"),
    );
    raft.start_server(addr.clone()).await;

    TestNode {
        raft,
        db,
        addr,
        _dir: dir,
    }
}

async fn add_and_promote(leader: &RaftNode, node_id: u64, addr: &str) {
    for attempt in 1..=10 {
        match leader.add_learner(node_id, addr).await {
            Ok(_) => break,
            Err(e) if attempt < 10 => {
                tokio::time::sleep(Duration::from_millis(300)).await;
                eprintln!("  test: add_learner retry {attempt}: {e}");
            }
            Err(e) => panic!("add_learner failed after retries: {e}"),
        }
    }
    for attempt in 1..=10 {
        match leader.promote_voter(node_id).await {
            Ok(_) => return,
            Err(e) if attempt < 10 => {
                tokio::time::sleep(Duration::from_millis(300)).await;
                eprintln!("  test: promote_voter retry {attempt}: {e}");
            }
            Err(e) => panic!("promote_voter failed after retries: {e}"),
        }
    }
}

async fn poll_kv(db: &Database, key: &str, timeout: Duration) -> Option<Kv> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let rows: Vec<Kv> = db
            .query_take(&format!("SELECT key, value FROM kv WHERE key = '{key}'"))
            .await
            .unwrap_or_default();
        if let Some(row) = rows.into_iter().next() {
            return Some(row);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    None
}

#[tokio::test]
async fn write_replicates_to_all_nodes() {
    let n1 = spawn_node(1).await;
    let n2 = spawn_node(2).await;
    let n3 = spawn_node(3).await;

    n1.raft.init_cluster(&n1.addr).await.expect("init_cluster");
    tokio::time::sleep(Duration::from_millis(500)).await;

    add_and_promote(&n1.raft, 2, &n2.addr).await;
    add_and_promote(&n1.raft, 3, &n3.addr).await;

    n1.raft
        .write("CREATE kv SET key = 'hello', value = 'world'".into())
        .await
        .expect("write");

    for (i, node) in [&n1, &n2, &n3].iter().enumerate() {
        let row = poll_kv(&node.db, "hello", Duration::from_secs(5))
            .await
            .unwrap_or_else(|| panic!("record not replicated to node {}", i + 1));
        assert_eq!(row.value, "world");
    }
}

#[tokio::test]
async fn invalid_surql_surfaces_error_to_caller() {
    let n1 = spawn_node(10).await;
    n1.raft.init_cluster(&n1.addr).await.expect("init_cluster");
    tokio::time::sleep(Duration::from_millis(500)).await;

    n1.raft
        .write("CREATE kv SET key = 'dup', value = '1'".into())
        .await
        .expect("first write");

    let result = n1
        .raft
        .write("CREATE kv SET key = 'dup', value = '2'".into())
        .await;

    assert!(
        result.is_err(),
        "expected unique-constraint error to surface, got: {result:?}"
    );
}

#[tokio::test]
async fn followers_see_writes_committed_before_they_joined() {
    let n1 = spawn_node(100).await;
    n1.raft.init_cluster(&n1.addr).await.expect("init_cluster");
    tokio::time::sleep(Duration::from_millis(500)).await;

    n1.raft
        .write("CREATE kv SET key = 'early', value = '1'".into())
        .await
        .expect("early write");

    let n2 = spawn_node(200).await;
    add_and_promote(&n1.raft, 200, &n2.addr).await;

    let row = poll_kv(&n2.db, "early", Duration::from_secs(5))
        .await
        .expect("late joiner missed the pre-join write");
    assert_eq!(row.value, "1");
}
