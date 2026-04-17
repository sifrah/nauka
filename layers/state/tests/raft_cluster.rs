//! End-to-end integration tests for the Raft consensus layer.
//!
//! Spins up in-process 3-node clusters over loopback TCP and exercises
//! `RaftNode::write` — the same path used by the daemon.

use std::sync::Arc;
use std::time::{Duration, Instant};

use nauka_state::{Database, RaftNode};
use serde::Deserialize;
use surrealdb::types::{Datetime, SurrealValue};

const TEST_SCHEMA: &str = "\
DEFINE TABLE IF NOT EXISTS kv SCHEMAFULL;\
DEFINE FIELD IF NOT EXISTS key ON kv TYPE string;\
DEFINE FIELD IF NOT EXISTS value ON kv TYPE string;\
DEFINE INDEX IF NOT EXISTS kv_key ON kv FIELDS key UNIQUE;\
";

// Mirror of the Raft-replicated portion of `layers/hypervisor/definition.surql`.
// Kept inline to avoid the state crate reaching across layer boundaries; if
// the hypervisor schema changes, update here too — the point of this test is
// to catch non-deterministic defaults being reintroduced.
const HYPERVISOR_SCHEMA: &str = "\
DEFINE TABLE IF NOT EXISTS hypervisor SCHEMAFULL;\
DEFINE FIELD IF NOT EXISTS public_key  ON hypervisor TYPE string;\
DEFINE FIELD IF NOT EXISTS node_id     ON hypervisor TYPE int;\
DEFINE FIELD IF NOT EXISTS address     ON hypervisor TYPE string;\
DEFINE FIELD IF NOT EXISTS endpoint    ON hypervisor TYPE option<string>;\
DEFINE FIELD IF NOT EXISTS allowed_ips ON hypervisor TYPE array<string>;\
DEFINE FIELD IF NOT EXISTS keepalive   ON hypervisor TYPE option<int>;\
DEFINE FIELD IF NOT EXISTS raft_addr   ON hypervisor TYPE string;\
DEFINE FIELD IF NOT EXISTS joined_at   ON hypervisor TYPE datetime;\
DEFINE INDEX IF NOT EXISTS hypervisor_pubkey ON hypervisor FIELDS public_key UNIQUE;\
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
    spawn_node_with_threshold(node_id, nauka_state::raft::SNAPSHOT_THRESHOLD).await
}

async fn spawn_node_with_threshold(node_id: u64, threshold: u64) -> TestNode {
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
        RaftNode::new_with_snapshot_threshold(node_id, db.clone(), None, threshold)
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
async fn follower_write_is_forwarded_to_leader() {
    let n1 = spawn_node(1000).await;
    let n2 = spawn_node(2000).await;
    let n3 = spawn_node(3000).await;

    n1.raft.init_cluster(&n1.addr).await.expect("init_cluster");
    tokio::time::sleep(Duration::from_millis(500)).await;

    add_and_promote(&n1.raft, 2000, &n2.addr).await;
    add_and_promote(&n1.raft, 3000, &n3.addr).await;

    // Give followers a moment to sync their view of the leader.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // n1 is the leader; n2 is a follower. Writing on n2 must still land.
    n2.raft
        .write("CREATE kv SET key = 'from-follower', value = 'ok'".into())
        .await
        .expect("follower write via forwarding");

    for (i, node) in [&n1, &n2, &n3].iter().enumerate() {
        let row = poll_kv(&node.db, "from-follower", Duration::from_secs(5))
            .await
            .unwrap_or_else(|| panic!("forwarded write not replicated to node {}", i + 1));
        assert_eq!(row.value, "ok");
    }
}

#[tokio::test]
async fn writes_past_threshold_trigger_snapshot_and_log_purge() {
    #[derive(serde::Deserialize, SurrealValue)]
    struct Count {
        count: i64,
    }

    const THRESHOLD: u64 = 5;
    let n1 = spawn_node_with_threshold(5000, THRESHOLD).await;
    n1.raft.init_cluster(&n1.addr).await.expect("init_cluster");
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Write well past the threshold — openraft builds a snapshot and the
    // log_store purges committed entries in the background.
    for i in 0..(THRESHOLD as usize * 4) {
        n1.raft
            .write(format!("CREATE kv SET key = 'snap-{i}', value = '{i}'"))
            .await
            .expect("write");
    }

    // Give the snapshot + purge tasks a moment to run.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // A snapshot row should exist in _raft_snapshot for our node_id.
    let snaps: Vec<Count> = n1
        .db
        .query_take("SELECT count() AS count FROM _raft_snapshot GROUP ALL")
        .await
        .expect("count snapshots");
    let snap_count = snaps.first().map(|c| c.count).unwrap_or(0);
    assert!(
        snap_count >= 1,
        "expected at least one snapshot row, got {snap_count}"
    );

    // And the log should have been purged: fewer live _raft_log rows than
    // total writes (we wrote THRESHOLD*4 entries plus membership ones).
    let logs: Vec<Count> = n1
        .db
        .query_take("SELECT count() AS count FROM _raft_log GROUP ALL")
        .await
        .expect("count logs");
    let log_count = logs.first().map(|c| c.count).unwrap_or(0);
    assert!(
        log_count < (THRESHOLD as i64 * 4),
        "expected log to be purged below write count, got {log_count}"
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

#[derive(Deserialize, SurrealValue, Debug, PartialEq, Eq)]
struct HypervisorRow {
    public_key: String,
    node_id: i64,
    address: String,
    endpoint: Option<String>,
    allowed_ips: Vec<String>,
    keepalive: Option<i64>,
    raft_addr: String,
    joined_at: Datetime,
}

async fn poll_hypervisor(
    db: &Database,
    public_key: &str,
    timeout: Duration,
) -> Option<HypervisorRow> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let rows: Vec<HypervisorRow> = db
            .query_take(&format!(
                "SELECT public_key, node_id, address, endpoint, allowed_ips, \
                 keepalive, raft_addr, joined_at \
                 FROM hypervisor WHERE public_key = '{public_key}'"
            ))
            .await
            .unwrap_or_default();
        if let Some(row) = rows.into_iter().next() {
            return Some(row);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    None
}

/// Regression test for #329: Raft state-machine determinism for the
/// `hypervisor` table. Before the fix, `DEFINE FIELD joined_at ... DEFAULT
/// time::now()` caused each node to evaluate `time::now()` at apply time,
/// so the same logical record ended up with different `joined_at` values
/// on each node. The daemon now formats `joined_at` into the SurQL literal
/// on the leader, so every node's state machine writes byte-identical data.
#[tokio::test]
async fn hypervisor_write_is_byte_identical_across_nodes() {
    let n1 = spawn_node(7001).await;
    let n2 = spawn_node(7002).await;
    let n3 = spawn_node(7003).await;

    for node in [&n1, &n2, &n3] {
        node.db
            .query(HYPERVISOR_SCHEMA)
            .await
            .expect("hypervisor schema");
    }

    n1.raft.init_cluster(&n1.addr).await.expect("init_cluster");
    tokio::time::sleep(Duration::from_millis(500)).await;
    add_and_promote(&n1.raft, 7002, &n2.addr).await;
    add_and_promote(&n1.raft, 7003, &n3.addr).await;

    // Caller formats joined_at up front so every node's apply writes the
    // same literal — the point of the fix.
    let joined_at = "2026-04-17T12:34:56.123456789Z";
    let surql = format!(
        "CREATE hypervisor SET \
         public_key = 'pk-deterministic', node_id = 42, address = 'fd00::1', \
         endpoint = '203.0.113.5:51820', allowed_ips = ['fd00::1', 'fd00::2'], \
         keepalive = 25, raft_addr = '[fd00::1]:4001', \
         joined_at = d'{joined_at}'"
    );
    n1.raft.write(surql).await.expect("write");

    let row_n1 = poll_hypervisor(&n1.db, "pk-deterministic", Duration::from_secs(5))
        .await
        .expect("record not present on n1");
    let row_n2 = poll_hypervisor(&n2.db, "pk-deterministic", Duration::from_secs(5))
        .await
        .expect("record not replicated to n2");
    let row_n3 = poll_hypervisor(&n3.db, "pk-deterministic", Duration::from_secs(5))
        .await
        .expect("record not replicated to n3");

    assert_eq!(row_n1, row_n2, "n1 and n2 diverge");
    assert_eq!(row_n1, row_n3, "n1 and n3 diverge");

    // Extra guard: if anyone re-adds `DEFAULT time::now()`, this equality
    // catches it because the Datetime would reflect apply-time wall clock,
    // not the caller-supplied literal.
    let expected: Datetime = joined_at.parse().expect("parse expected joined_at");
    assert_eq!(row_n1.joined_at, expected, "joined_at changed on apply");
}

/// Regression test for #330: when a late joiner receives a snapshot, the
/// snapshot replay used to hit the `hypervisor_pubkey` UNIQUE index and log
/// `"snapshot replay failed"` while silently continuing — leaving the new
/// node's state machine divergent from the leader's. `install_snapshot`
/// now wipes Raft-replicated tables first, and replay errors propagate.
#[tokio::test]
async fn late_joiner_snapshot_install_matches_leader() {
    const THRESHOLD: u64 = 5;
    let n1 = spawn_node_with_threshold(8001, THRESHOLD).await;
    let n2 = spawn_node_with_threshold(8002, THRESHOLD).await;
    let n3 = spawn_node_with_threshold(8003, THRESHOLD).await;

    for node in [&n1, &n2, &n3] {
        node.db
            .query(HYPERVISOR_SCHEMA)
            .await
            .expect("hypervisor schema");
    }

    n1.raft.init_cluster(&n1.addr).await.expect("init_cluster");
    tokio::time::sleep(Duration::from_millis(500)).await;
    add_and_promote(&n1.raft, 8002, &n2.addr).await;
    add_and_promote(&n1.raft, 8003, &n3.addr).await;

    // Write past the threshold so openraft builds a snapshot and purges the
    // log. A later joiner then *has* to replay via install_snapshot.
    let write_count = (THRESHOLD as usize) * 3;
    for i in 0..write_count {
        let ts = format!("2026-04-17T10:00:{:02}.000000000Z", i);
        let surql = format!(
            "CREATE hypervisor SET \
             public_key = 'pk-snap-{i}', node_id = {i}, address = 'fd00::{i:x}', \
             endpoint = NONE, allowed_ips = ['fd00::{i:x}'], keepalive = 25, \
             raft_addr = '[fd00::{i:x}]:4001', joined_at = d'{ts}'"
        );
        n1.raft.write(surql).await.expect("write");
    }

    // Give snapshot + log-purge tasks time to run.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Now bring up the late joiner. It has no log, so openraft has to ship
    // it a snapshot rather than a log-replay catchup.
    let n4 = spawn_node_with_threshold(8004, THRESHOLD).await;
    n4.db
        .query(HYPERVISOR_SCHEMA)
        .await
        .expect("hypervisor schema");

    // Pre-populate n4's hypervisor table with a row whose public_key matches
    // one the snapshot will replay. This simulates the real-world case the
    // issue describes: a node whose state machine has applied rows from log
    // entries before receiving a snapshot that re-contains those same rows.
    // Without the install_snapshot fix, the UNIQUE index on `public_key`
    // fires during replay and the node silently diverges.
    n4.db
        .query(
            "CREATE hypervisor SET \
             public_key = 'pk-snap-0', node_id = 999, address = 'fd00::pre', \
             endpoint = NONE, allowed_ips = ['fd00::pre'], keepalive = 25, \
             raft_addr = '[fd00::pre]:4001', \
             joined_at = d'2020-01-01T00:00:00.000000000Z'",
        )
        .await
        .expect("pre-populate n4");

    add_and_promote(&n1.raft, 8004, &n4.addr).await;

    // Wait until the late joiner has caught up.
    let deadline = Instant::now() + Duration::from_secs(10);
    let want_count = write_count as i64;
    loop {
        #[derive(Deserialize, SurrealValue)]
        struct Count {
            count: i64,
        }
        let rows: Vec<Count> = n4
            .db
            .query_take("SELECT count() AS count FROM hypervisor GROUP ALL")
            .await
            .unwrap_or_default();
        let got = rows.first().map(|c| c.count).unwrap_or(0);
        if got == want_count {
            break;
        }
        if Instant::now() >= deadline {
            panic!("n4 never converged: got {got}, want {want_count}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Compare n1 and n4 row-by-row — every column must match.
    let select_all = "SELECT public_key, node_id, address, endpoint, allowed_ips, \
                      keepalive, raft_addr, joined_at \
                      FROM hypervisor ORDER BY public_key";
    let rows_n1: Vec<HypervisorRow> = n1.db.query_take(select_all).await.expect("n1 select");
    let rows_n4: Vec<HypervisorRow> = n4.db.query_take(select_all).await.expect("n4 select");

    assert_eq!(rows_n1.len(), write_count, "n1 missing rows");
    assert_eq!(
        rows_n1, rows_n4,
        "n4 (post-snapshot-install) diverges from leader n1"
    );
}
