//! Integration test for IAM-7 (#351) — password-reset flow +
//! complexity gate.
//!
//! Happy path: request → consume → old password rejected / new
//! password works. Reuse: consuming the same token twice fails the
//! second time. Unknown email: request returns `None` silently.
//! Complexity: weak passwords rejected at reset time.

use nauka_core::resource::{Datetime, ResourceOps};
use nauka_iam::{
    consume_password_reset, hash_password, request_password_reset, validate_password_complexity,
    User,
};
use nauka_state::{Database, RaftNode, TlsConfig};
use std::sync::Arc;
use surrealdb::opt::auth::Record;
use surrealdb::types::SurrealValue;

async fn fresh_db() -> (Arc<Database>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("iam7-test.db");
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

async fn single_node_raft(db: Arc<Database>) -> RaftNode {
    // Reset flow routes writes through `Writer::create` which needs
    // a Raft handle for cluster-scoped resources. Spin up a solo
    // in-process node bound to a sacrificial address — the actual
    // TCP server isn't needed for single-node writes to commit.
    let node_id = 1;
    let raft = RaftNode::new(node_id, db, None::<TlsConfig>).await.unwrap();
    raft.init_cluster("[::1]:0").await.unwrap();
    // Let the initial vote + blank entry apply before we push user writes.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    raft
}

async fn seed_user(db: &Database, email: &str, password: &str) {
    let user = User {
        email: email.into(),
        password_hash: hash_password(password).unwrap(),
        display_name: "User".into(),
        email_verified_at: None,
        created_at: Datetime::default(),
        updated_at: Datetime::default(),
        version: 0,
    };
    db.query(&user.create_query()).await.unwrap();
}

async fn can_signin(db: &Database, email: &str, password: &str) -> bool {
    #[derive(SurrealValue)]
    struct P {
        email: String,
        password: String,
    }
    // The signin path needs a clean session — other tests in this
    // suite might leave $auth set from an earlier call. Invalidate
    // defensively so "works?" doesn't answer "yes" because the
    // previous session was still valid.
    let _ = db.inner().invalidate().await;
    let res = db
        .inner()
        .signin(Record {
            namespace: nauka_state::DEFAULT_NAMESPACE.to_string(),
            database: nauka_state::DEFAULT_DATABASE.to_string(),
            access: "user".to_string(),
            params: P {
                email: email.to_string(),
                password: password.to_string(),
            },
        })
        .await;
    let _ = db.inner().invalidate().await;
    res.is_ok()
}

#[test]
fn complexity_accepts_reasonable_passwords() {
    assert!(validate_password_complexity("hunter2-stronger").is_ok());
    assert!(validate_password_complexity("correct-horse-1").is_ok());
}

#[test]
fn complexity_rejects_weak_passwords() {
    // too short
    assert!(validate_password_complexity("short1").is_err());
    // letters only
    assert!(validate_password_complexity("alllowercasepw").is_err());
    // digits only
    assert!(validate_password_complexity("1234567890").is_err());
    // empty
    assert!(validate_password_complexity("").is_err());
}

#[tokio::test]
async fn reset_request_for_unknown_email_returns_none_silently() {
    let (db, _dir) = fresh_db().await;
    let raft = single_node_raft(db.clone()).await;
    let result = request_password_reset(&db, &raft, "ghost@example.com")
        .await
        .expect("request never errors on unknown email");
    assert!(result.is_none(), "unknown email must not mint a token");
}

#[tokio::test]
async fn reset_request_for_known_email_mints_token_and_consume_rotates_password() {
    let (db, _dir) = fresh_db().await;
    let raft = single_node_raft(db.clone()).await;
    seed_user(&db, "alice@example.com", "old-password-111").await;

    // Request a token for the known email.
    let token = request_password_reset(&db, &raft, "alice@example.com")
        .await
        .expect("request succeeds")
        .expect("token minted for known email");
    assert_eq!(token.len(), 32, "token should be 32 URL-safe chars");

    // Old password still works before we consume.
    assert!(can_signin(&db, "alice@example.com", "old-password-111").await);

    // Consume the token — password rotates.
    consume_password_reset(&db, &raft, &token, "new-password-222")
        .await
        .expect("consume accepts the valid token");

    // New password now works, old one doesn't.
    assert!(
        can_signin(&db, "alice@example.com", "new-password-222").await,
        "new password must allow signin"
    );
    assert!(
        !can_signin(&db, "alice@example.com", "old-password-111").await,
        "old password must no longer work"
    );
}

#[tokio::test]
async fn consume_rejects_reuse() {
    let (db, _dir) = fresh_db().await;
    let raft = single_node_raft(db.clone()).await;
    seed_user(&db, "bob@example.com", "start-password-333").await;

    let token = request_password_reset(&db, &raft, "bob@example.com")
        .await
        .unwrap()
        .unwrap();

    consume_password_reset(&db, &raft, &token, "fresh-password-444")
        .await
        .expect("first consume succeeds");

    // Second consume of the same token: the row is marked consumed
    // and the check rejects it. `try_again-555` is a complexity-OK
    // password so the failure reason must be the token state, not
    // the complexity gate.
    let err = consume_password_reset(&db, &raft, &token, "try-again-555")
        .await
        .expect_err("replay must fail");
    assert!(
        matches!(err, nauka_iam::IamError::InvalidCredentials),
        "expected InvalidCredentials on replay, got {err:?}"
    );
}

#[tokio::test]
async fn consume_rejects_weak_new_password() {
    let (db, _dir) = fresh_db().await;
    let raft = single_node_raft(db.clone()).await;
    seed_user(&db, "carol@example.com", "old-password-666").await;

    let token = request_password_reset(&db, &raft, "carol@example.com")
        .await
        .unwrap()
        .unwrap();

    let err = consume_password_reset(&db, &raft, &token, "short")
        .await
        .expect_err("complexity rejects short password");
    assert!(
        matches!(err, nauka_iam::IamError::Password(_)),
        "expected Password error, got {err:?}"
    );

    // Token was not consumed — a subsequent valid attempt must still succeed.
    consume_password_reset(&db, &raft, &token, "proper-pass-777")
        .await
        .expect("token still redeemable after a complexity-reject attempt");
}
