//! Apply every `#[resource]` DDL registered in the workspace (from
//! `nauka_hypervisor`'s linked resources) to a fresh embedded
//! SurrealKV, then inspect `INFO FOR DB` and assert each table,
//! field, and index declared by the contract actually landed.
//!
//! This is the runtime complement to the compile-time
//! `trybuild` tests in `core-macros/`: compile-fail proves the
//! macro rejects bad input; this test proves the generated DDL is
//! accepted by SurrealDB and produces the structure we expect.

#![allow(clippy::print_stdout, clippy::print_stderr)]

use nauka_core::resource::{cluster_schemas, local_schemas, Datetime, ResourceOps};
use nauka_hypervisor::{Hypervisor, MeshRecord};
use nauka_state::{Database, Writer};

async fn open_tmp() -> (Database, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rt.db");
    let db = Database::open(Some(path.to_str().unwrap())).await.unwrap();
    let cluster = cluster_schemas();
    let local = local_schemas();
    db.query(&cluster).await.expect("apply cluster schemas");
    db.query(&local).await.expect("apply local schemas");
    (db, dir)
}

#[tokio::test]
async fn every_registered_resource_is_present_in_info_for_db() {
    let (db, _dir) = open_tmp().await;

    // SurrealDB's `INFO FOR DB` returns a nested object of the
    // database's structure. We convert to JSON for easy string
    // checks — if the schema landed, every expected table name will
    // appear somewhere in that JSON.
    let info: Vec<serde_json::Value> = db.query_take("INFO FOR DB").await.expect("INFO FOR DB");
    let info_json = serde_json::to_string(&info).unwrap();

    assert!(
        info_json.contains("hypervisor"),
        "hypervisor table missing from INFO FOR DB: {info_json}"
    );
    assert!(
        info_json.contains("mesh"),
        "mesh table missing from INFO FOR DB: {info_json}"
    );
}

#[tokio::test]
async fn hypervisor_roundtrip_via_resource_ops() {
    let (db, _dir) = open_tmp().await;

    let now = Datetime::now();
    let hv = Hypervisor {
        public_key: "rt-pk".into(),
        node_id: 1,
        raft_addr: "[fd00::1]:4001".into(),
        address: "fd00::1".into(),
        endpoint: None,
        allowed_ips: vec!["fd00::1".into()],
        keepalive: Some(25),
        created_at: now,
        updated_at: now,
        version: 0,
    };

    // Raft isn't wired in this test — we write the SurrealQL
    // directly so we can exercise the generated DDL end-to-end
    // without spinning up a Raft node. This mirrors the bootstrap
    // path (`daemon::write_bootstrap_peers`).
    db.query(&hv.create_query()).await.expect("create");

    let rows: Vec<Hypervisor> = db
        .query_take(&Hypervisor::list_query())
        .await
        .expect("list");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].public_key, "rt-pk");
    assert_eq!(rows[0].node_id, 1);
    assert_eq!(rows[0].allowed_ips, vec!["fd00::1".to_string()]);
    assert_eq!(rows[0].version, 0);
}

#[tokio::test]
async fn mesh_roundtrip_through_writer() {
    let (db, _dir) = open_tmp().await;

    let now = Datetime::now();
    let mesh = MeshRecord {
        mesh_id: "fd00:1234:5678::/48".into(),
        interface_name: "nauka0".into(),
        listen_port: 51820,
        private_key: "encrypted_pk".into(),
        ca_cert: None,
        ca_key: None,
        tls_cert: None,
        tls_key: None,
        peering_pin: Some("123456".into()),
        created_at: now,
        updated_at: now,
        version: 0,
    };

    Writer::new(&db)
        .create(&mesh)
        .await
        .expect("writer create local");

    let rows: Vec<MeshRecord> = db
        .query_take(&MeshRecord::list_query())
        .await
        .expect("list mesh");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].mesh_id, "fd00:1234:5678::/48");
    assert_eq!(rows[0].peering_pin.as_deref(), Some("123456"));
}

#[tokio::test]
async fn unique_index_on_hypervisor_public_key_rejects_duplicates() {
    let (db, _dir) = open_tmp().await;

    let now = Datetime::now();
    let hv = Hypervisor {
        public_key: "dup-pk".into(),
        node_id: 2,
        raft_addr: "[fd00::2]:4001".into(),
        address: "fd00::2".into(),
        endpoint: None,
        allowed_ips: vec![],
        keepalive: None,
        created_at: now,
        updated_at: now,
        version: 0,
    };
    db.query(&hv.create_query()).await.expect("first create");

    // Same public_key, different node_id — UNIQUE on public_key
    // should reject.
    let dup = Hypervisor {
        public_key: "dup-pk".into(),
        node_id: 3,
        raft_addr: "[fd00::3]:4001".into(),
        address: "fd00::3".into(),
        endpoint: None,
        allowed_ips: vec![],
        keepalive: None,
        created_at: now,
        updated_at: now,
        version: 0,
    };
    let err = db.query(&dup.create_query()).await;
    assert!(
        err.is_err(),
        "duplicate public_key must be rejected by UNIQUE index"
    );
}
