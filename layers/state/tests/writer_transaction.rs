//! Integration tests for `Writer::transaction`.
//!
//! Runs against a real embedded SurrealKV database. Verifies that
//! the generated `BEGIN TRANSACTION; … COMMIT TRANSACTION;` block
//! behaves atomically — every statement commits together, or none
//! does.

#![allow(clippy::print_stdout, clippy::print_stderr)]

use nauka_core::resource::{Datetime, Resource, ResourceOps, SurrealValue};
use nauka_core_macros::resource;
use nauka_state::{Database, TxBuilder, Writer};
use serde::{Deserialize, Serialize};

// Local-scope resources — transactions route to the embedded
// SurrealKV directly, no Raft indirection.
#[resource(table = "txn_a", scope = "local")]
#[derive(Serialize, Deserialize, SurrealValue, Debug)]
pub struct TxnA {
    #[id]
    pub name: String,
    pub val: i64,
}

#[resource(table = "txn_b", scope = "local")]
#[derive(Serialize, Deserialize, SurrealValue, Debug)]
pub struct TxnB {
    #[id]
    pub name: String,
    pub note: String,
}

// A cluster-scope resource — used only to verify scope-mismatch
// rejection, never written here.
#[resource(table = "txn_cluster", scope = "cluster")]
#[derive(Serialize, Deserialize, SurrealValue, Debug)]
pub struct TxnCluster {
    #[id]
    pub name: String,
}

async fn open_tmp() -> (Database, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tx.db");
    let db = Database::open(Some(path.to_str().unwrap())).await.unwrap();
    (db, dir)
}

/// Apply the DDL of every resource defined in this test file.
/// Hand-picked rather than using `nauka_core::local_schemas()` so
/// the fixture doesn't depend on crates outside this one.
fn test_ddl() -> String {
    format!("{}\n{}\n", TxnA::DDL, TxnB::DDL)
}

#[tokio::test]
async fn transaction_commits_all_statements_atomically() {
    let (db, _dir) = open_tmp().await;
    db.query(&test_ddl()).await.unwrap();

    let writer = Writer::new(&db);

    let now = Datetime::now();
    let a = TxnA {
        name: "a1".into(),
        val: 42,
        created_at: now,
        updated_at: now,
        version: 0,
    };
    let b = TxnB {
        name: "b1".into(),
        note: "hi".into(),
        created_at: now,
        updated_at: now,
        version: 0,
    };

    writer
        .transaction(|tx: &mut TxBuilder| {
            tx.create(&a)?;
            tx.create(&b)?;
            Ok(())
        })
        .await
        .expect("transaction should commit");

    let rows_a: Vec<TxnA> = db.query_take(TxnA::list_query().as_str()).await.unwrap();
    let rows_b: Vec<TxnB> = db.query_take(TxnB::list_query().as_str()).await.unwrap();
    assert_eq!(rows_a.len(), 1);
    assert_eq!(rows_b.len(), 1);
}

#[tokio::test]
async fn transaction_rolls_back_all_statements_when_any_fails() {
    let (db, _dir) = open_tmp().await;
    db.query(&test_ddl()).await.unwrap();

    // Seed: a1 already exists, so the second CREATE in the
    // transaction will hit a record-id conflict (SurrealDB rejects
    // CREATE on an existing record id).
    let now = Datetime::now();
    let existing = TxnA {
        name: "conflict".into(),
        val: 1,
        created_at: now,
        updated_at: now,
        version: 0,
    };
    Writer::new(&db).create(&existing).await.unwrap();

    let writer = Writer::new(&db);

    let new_a = TxnA {
        name: "conflict".into(),
        val: 999,
        created_at: now,
        updated_at: now,
        version: 0,
    };
    let new_b = TxnB {
        name: "should_not_persist".into(),
        note: "if you see me the rollback broke".into(),
        created_at: now,
        updated_at: now,
        version: 0,
    };

    let result = writer
        .transaction(|tx| {
            tx.create(&new_b)?; // succeeds in isolation
            tx.create(&new_a)?; // duplicate id — will make the whole tx fail
            Ok(())
        })
        .await;

    assert!(result.is_err(), "duplicate CREATE should fail the tx");

    let rows_b: Vec<TxnB> = db.query_take(TxnB::list_query().as_str()).await.unwrap();
    assert!(
        rows_b.is_empty(),
        "TxnB inserted before the failing statement must have rolled back"
    );
    let rows_a: Vec<TxnA> = db.query_take(TxnA::list_query().as_str()).await.unwrap();
    assert_eq!(rows_a.len(), 1, "the pre-existing seed row must survive");
    assert_eq!(
        rows_a[0].val, 1,
        "the pre-existing seed row must not have been overwritten"
    );
}

#[tokio::test]
async fn transaction_rejects_mixing_scopes() {
    let (db, _dir) = open_tmp().await;

    let writer = Writer::new(&db);
    let now = Datetime::now();
    let a = TxnA {
        name: "a".into(),
        val: 0,
        created_at: now,
        updated_at: now,
        version: 0,
    };
    let c = TxnCluster {
        name: "c".into(),
        created_at: now,
        updated_at: now,
        version: 0,
    };

    let result = writer
        .transaction(|tx| {
            tx.create(&a)?; // locks scope to Local
            tx.create(&c)?; // should error: scope mismatch
            Ok(())
        })
        .await;

    let Err(err) = result else {
        panic!("mixed-scope transaction should have errored");
    };
    let msg = err.to_string();
    assert!(
        msg.contains("cannot mix"),
        "unexpected error message: {msg}"
    );
}

#[tokio::test]
async fn closure_error_means_nothing_sent() {
    let (db, _dir) = open_tmp().await;
    db.query(&test_ddl()).await.unwrap();

    let writer = Writer::new(&db);
    let now = Datetime::now();
    let a = TxnA {
        name: "never_created".into(),
        val: 0,
        created_at: now,
        updated_at: now,
        version: 0,
    };

    let result: Result<(), _> = writer
        .transaction(|tx| {
            tx.create(&a)?;
            Err(nauka_state::StateError::Schema("caller aborted".into()))
        })
        .await;

    assert!(result.is_err());
    let rows: Vec<TxnA> = db.query_take(TxnA::list_query().as_str()).await.unwrap();
    assert!(
        rows.is_empty(),
        "closure returning Err must prevent any statement from landing in the DB"
    );
}
