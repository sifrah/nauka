//! Integration test for IAM-9 (#353) — governance primitives.
//!
//! Covers: RoleBinding requires a non-empty reason, deactivated
//! users cannot signin, reactivation restores signin.

use nauka_core::resource::{Datetime, ResourceOps};
use nauka_iam::{bind_role, hash_password, request_password_reset, set_user_active, signin, User};
use nauka_state::{Database, RaftNode, TlsConfig};
use std::sync::Arc;

async fn fresh_db() -> (Arc<Database>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("iam9-test.db");
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
    let raft = RaftNode::new(1, db, None::<TlsConfig>).await.unwrap();
    raft.init_cluster("[::1]:0").await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    raft
}

async fn seed_user(db: &Database, email: &str, password: &str, active: bool) {
    let user = User {
        email: email.into(),
        password_hash: hash_password(password).unwrap(),
        display_name: "User".into(),
        email_verified_at: None,
        active,
        created_at: Datetime::default(),
        updated_at: Datetime::default(),
        version: 0,
    };
    db.query(&user.create_query()).await.unwrap();
}

/// Build a JWT for the admin actor by hitting the `user` DEFINE
/// ACCESS flow. Every IAM-9 op takes a JWT the same way the CLI
/// does; re-running signin is the easiest way to mint one inside a
/// test harness.
async fn admin_jwt(db: &Database, raft: &RaftNode, email: &str, password: &str) -> String {
    signin(db, raft, email, password, "127.0.0.1")
        .await
        .expect("admin signin")
        .into_string()
}

#[tokio::test]
async fn schema_marks_role_binding_reason_required() {
    let cluster = nauka_core::cluster_schemas();
    assert!(
        cluster.contains(
            "DEFINE FIELD IF NOT EXISTS reason ON role_binding TYPE string \
             ASSERT string::len($value) > 0"
        ),
        "role_binding.reason missing non-empty ASSERT: {cluster}"
    );
}

#[tokio::test]
async fn schema_has_user_active_field() {
    let cluster = nauka_core::cluster_schemas();
    assert!(
        cluster.contains("DEFINE FIELD IF NOT EXISTS active ON user TYPE bool"),
        "user.active missing: {cluster}"
    );
}

#[tokio::test]
async fn bind_role_rejects_empty_reason() {
    let (db, _dir) = fresh_db().await;
    let raft = single_node_raft(db.clone()).await;
    seed_user(&db, "alice@example.com", "password-abc-123", true).await;
    let jwt = admin_jwt(&db, &raft, "alice@example.com", "password-abc-123").await;

    // `role bind` also needs an Org, an owner, a Role and the
    // principal to exist — we'd need to stage a full RBAC tree to
    // reach the reason check naturally. Instead we exercise the
    // reason gate in isolation: it's a trim-and-check *before* any
    // DB work.
    let err = bind_role(
        &db,
        &raft,
        &jwt,
        "alice@example.com",
        "viewer",
        "acme",
        "   ",
    )
    .await
    .expect_err("whitespace-only reason must be rejected");
    match err {
        nauka_iam::IamError::InvalidSlug(msg) => {
            assert!(msg.contains("reason"), "wrong error message: {msg}");
        }
        other => panic!("expected InvalidSlug, got {other:?}"),
    }
}

#[tokio::test]
async fn deactivated_user_cannot_signin() {
    let (db, _dir) = fresh_db().await;
    let raft = single_node_raft(db.clone()).await;
    seed_user(&db, "alice@example.com", "alice-strong-1", true).await;

    // Baseline: active user can sign in.
    signin(
        &db,
        &raft,
        "alice@example.com",
        "alice-strong-1",
        "127.0.0.1",
    )
    .await
    .expect("active signin");

    // Deactivate via admin (alice herself acts here — the CLI
    // scaffolding for admin-only gating lands with the full
    // RoleBinding enforcement issue). The reason is audited.
    let admin_jwt = admin_jwt(&db, &raft, "alice@example.com", "alice-strong-1").await;
    set_user_active(
        &db,
        &raft,
        &admin_jwt,
        "alice@example.com",
        false,
        "offboarding",
    )
    .await
    .expect("deactivate");

    // After deactivation the SIGNIN clause filters the row out, so
    // the password no longer matches. Same error shape as "wrong
    // password" / "unknown user".
    let err = signin(
        &db,
        &raft,
        "alice@example.com",
        "alice-strong-1",
        "127.0.0.1",
    )
    .await
    .expect_err("deactivated signin must fail");
    assert!(
        matches!(err, nauka_iam::IamError::InvalidCredentials),
        "expected InvalidCredentials, got {err:?}"
    );

    // Reactivate — password works again.
    set_user_active(
        &db,
        &raft,
        &admin_jwt,
        "alice@example.com",
        true,
        "back from leave",
    )
    .await
    .expect("reactivate");
    signin(
        &db,
        &raft,
        "alice@example.com",
        "alice-strong-1",
        "127.0.0.1",
    )
    .await
    .expect("reactivated signin");
}

#[tokio::test]
async fn set_user_active_rejects_empty_reason() {
    let (db, _dir) = fresh_db().await;
    let raft = single_node_raft(db.clone()).await;
    seed_user(&db, "alice@example.com", "alice-strong-1", true).await;
    let jwt = admin_jwt(&db, &raft, "alice@example.com", "alice-strong-1").await;

    let err = set_user_active(&db, &raft, &jwt, "alice@example.com", false, "")
        .await
        .expect_err("empty reason must be rejected");
    match err {
        nauka_iam::IamError::InvalidSlug(msg) => {
            assert!(msg.contains("reason"));
        }
        other => panic!("expected InvalidSlug, got {other:?}"),
    }
}

/// Sanity check: password reset still works alongside IAM-9's
/// schema changes. Regression guard for the `User` struct gaining
/// a new required field and a test silently mis-initializing it.
#[tokio::test]
async fn password_reset_still_works_after_schema_changes() {
    let (db, _dir) = fresh_db().await;
    let raft = single_node_raft(db.clone()).await;
    seed_user(&db, "carol@example.com", "carol-password-1", true).await;
    let _ = request_password_reset(&db, &raft, "carol@example.com")
        .await
        .expect("request");
}
