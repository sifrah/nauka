//! Integration test for IAM-1: signup → signin → JWT round-trip.
//!
//! Spins up a throw-away SurrealKV database, loads the full schema
//! (including the `DEFINE ACCESS user` emitted by `#[access]` on
//! `User`), and exercises both code paths that can create an
//! authenticated session:
//!
//! 1. `db.signup(...)` — the SurrealDB built-in flow defined by the
//!    DEFINE ACCESS SIGNUP clause. Valid for single-node / dev /
//!    tests only (writes bypass Raft).
//! 2. `crate::signup(...)` — the daemon-side Rust flow we use in
//!    production: hash with Argon2id locally, write via Raft, mint
//!    JWT via `db.signin`.
//!
//! The test runs without Raft (single-node, `Writer::new(&db)` with
//! no Raft handle). To exercise that path against `User` (which is
//! `scope = "cluster"`) we bypass the Writer and write directly
//! through `db.query` — the production daemon always has a Raft
//! handle. The hash/signin boundary is what this test really cares
//! about: that a hash produced by the Rust `argon2` crate is
//! verifiable by SurrealDB's `crypto::argon2::compare`.

use nauka_core::resource::{Datetime, ResourceOps};
use nauka_iam::{decode_claims, hash_password, signin, User};
use nauka_state::{Database, RaftNode, TlsConfig};
use std::sync::Arc;
use surrealdb::opt::auth::Record;
use surrealdb::types::SurrealValue;

async fn single_node_raft(db: Arc<Database>) -> RaftNode {
    let raft = RaftNode::new(1, db, None::<TlsConfig>).await.unwrap();
    raft.init_cluster("[::1]:0").await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    raft
}

/// Tear up a fresh database with the full Nauka schema applied —
/// this is what `bin/nauka` does at startup, condensed.
async fn fresh_db() -> (Arc<Database>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("iam-test.db");
    let db = Arc::new(Database::open(Some(path.to_str().unwrap())).await.unwrap());

    let functions = nauka_core::function_definitions();
    let cluster = nauka_core::cluster_schemas();
    let local = nauka_core::local_schemas();
    let access = nauka_core::access_definitions();
    nauka_state::load_schemas(
        &db,
        &[nauka_state::SCHEMA, &functions, &cluster, &local, &access],
    )
    .await
    .unwrap();

    (db, dir)
}

#[tokio::test]
async fn access_definitions_includes_user() {
    let defs = nauka_core::access_definitions();
    assert!(
        defs.contains("DEFINE ACCESS IF NOT EXISTS user ON DATABASE TYPE RECORD"),
        "missing DEFINE ACCESS user: {defs}"
    );
    assert!(
        defs.contains("DURATION FOR TOKEN 1h"),
        "jwt duration: {defs}"
    );
    assert!(defs.contains("FOR SESSION 24h"), "session duration: {defs}");
}

#[tokio::test]
async fn user_ddl_registered_as_cluster_resource() {
    let cluster = nauka_core::cluster_schemas();
    assert!(
        cluster.contains("DEFINE TABLE IF NOT EXISTS user SCHEMAFULL"),
        "user table missing from cluster_schemas"
    );
    assert!(
        cluster.contains("DEFINE FIELD IF NOT EXISTS email ON user TYPE string"),
        "email field missing"
    );
    assert!(
        cluster.contains("DEFINE FIELD IF NOT EXISTS password_hash ON user TYPE string"),
        "password_hash field missing"
    );
}

#[tokio::test]
async fn builtin_signup_then_signin_round_trip() {
    // End-to-end through SurrealDB's built-in SIGNUP clause — hashes
    // inside the engine. Validates that the DEFINE ACCESS DDL emitted
    // by `#[access]` is syntactically valid and semantically wired.
    let (db, _dir) = fresh_db().await;

    #[derive(SurrealValue)]
    struct SignupParams {
        email: String,
        password: String,
        display_name: String,
    }
    #[derive(SurrealValue)]
    struct SigninParams {
        email: String,
        password: String,
    }

    let signup_tok = db
        .inner()
        .signup(Record {
            namespace: nauka_state::DEFAULT_NAMESPACE.to_string(),
            database: nauka_state::DEFAULT_DATABASE.to_string(),
            access: "user".to_string(),
            params: SignupParams {
                email: "alice@example.com".into(),
                password: "hunter2".into(),
                display_name: "Alice".into(),
            },
        })
        .await
        .expect("signup");
    let signup_jwt = signup_tok.access.into_insecure_token();
    assert!(
        signup_jwt.contains('.'),
        "signup jwt looks wrong: {signup_jwt}"
    );

    let signin_tok = db
        .inner()
        .signin(Record {
            namespace: nauka_state::DEFAULT_NAMESPACE.to_string(),
            database: nauka_state::DEFAULT_DATABASE.to_string(),
            access: "user".to_string(),
            params: SigninParams {
                email: "alice@example.com".into(),
                password: "hunter2".into(),
            },
        })
        .await
        .expect("signin");
    let signin_jwt = signin_tok.access.into_insecure_token();

    // Both tokens are independently-minted JWTs — claims should name
    // the same record id.
    let signup_claims = decode_claims(&signup_jwt).unwrap();
    let signin_claims = decode_claims(&signin_jwt).unwrap();
    assert_eq!(signup_claims.access.as_deref(), Some("user"));
    assert_eq!(signin_claims.access.as_deref(), Some("user"));
    assert_eq!(signup_claims.email().as_deref(), Some("alice@example.com"));
    assert_eq!(signin_claims.email().as_deref(), Some("alice@example.com"));
}

#[tokio::test]
async fn wrong_password_is_invalid_credentials() {
    let (db, _dir) = fresh_db().await;

    #[derive(SurrealValue)]
    struct SignupParams {
        email: String,
        password: String,
        display_name: String,
    }
    db.inner()
        .signup(Record {
            namespace: nauka_state::DEFAULT_NAMESPACE.to_string(),
            database: nauka_state::DEFAULT_DATABASE.to_string(),
            access: "user".to_string(),
            params: SignupParams {
                email: "bob@example.com".into(),
                password: "correct-horse".into(),
                display_name: "Bob".into(),
            },
        })
        .await
        .expect("signup");

    let raft = single_node_raft(db.clone()).await;
    let err = signin(&db, &raft, "bob@example.com", "wrong-password", "127.0.0.1")
        .await
        .expect_err("wrong password must not sign in");
    assert!(
        matches!(err, nauka_iam::IamError::InvalidCredentials),
        "expected InvalidCredentials, got: {err:?}"
    );
}

#[tokio::test]
async fn rust_hashed_record_verifies_with_surreal_compare() {
    // The production daemon path: hash in Rust (so the hash is
    // byte-identical across replicas), insert the record through the
    // database, then call `signin` — SurrealDB's
    // `crypto::argon2::compare` must accept hashes produced by the
    // `argon2` crate. If this fails, the production signup flow is
    // broken even if SIGNUP-via-DEFINE-ACCESS works.
    let (db, _dir) = fresh_db().await;

    let hash = hash_password("correct-horse").unwrap();
    let user = User {
        email: "carol@example.com".into(),
        password_hash: hash,
        display_name: "Carol".into(),
        email_verified_at: None,
        active: true,
        created_at: Datetime::default(),
        updated_at: Datetime::default(),
        version: 0,
    };
    // Write directly through the DB (not the Writer) — this test
    // runs without a Raft handle, and `User` is `scope = "cluster"`,
    // so `Writer::create` would refuse. In production, `Writer` is
    // always Raft-backed.
    db.query(&user.create_query())
        .await
        .expect("db create user");

    // Sanity-check the record actually landed.
    let rows: Vec<User> = db
        .query_take(&<User as ResourceOps>::get_query(
            &"carol@example.com".to_string(),
        ))
        .await
        .expect("read back user");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].email, "carol@example.com");

    let raft = single_node_raft(db.clone()).await;
    let jwt = signin(
        &db,
        &raft,
        "carol@example.com",
        "correct-horse",
        "127.0.0.1",
    )
    .await
    .expect("signin with rust-hashed password");
    let claims = decode_claims(jwt.as_str()).unwrap();
    assert_eq!(claims.email().as_deref(), Some("carol@example.com"));
}
