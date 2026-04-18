//! Integration test for IAM-4 (#348) — `nk_live_…` tokens + the
//! `service_account` DEFINE ACCESS.
//!
//! Mint a token against a hand-seeded `service_account`, then call
//! `db.signin` on the `service_account` access with the `token_id`
//! and `secret` params the SIGNIN clause expects. A successful
//! signin returns a JWT whose `$auth.ID` names the SA record —
//! that's the round trip we assert.

use nauka_core::resource::{Datetime, Ref, ResourceOps};
use nauka_iam::{decode_claims, hash_password, ApiToken, Org, ServiceAccount, User};
use nauka_state::Database;
use surrealdb::opt::auth::Record;
use surrealdb::types::SurrealValue;

async fn fresh_db() -> (Database, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("iam4-test.db");
    let db = Database::open(Some(path.to_str().unwrap())).await.unwrap();

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

async fn seed_owner_and_sa(db: &Database) {
    // Minimal chain: a user owns an org, an SA belongs to that org.
    let user = User {
        email: "alice@example.com".into(),
        password_hash: hash_password("alice-pw").unwrap(),
        display_name: "Alice".into(),
        created_at: Datetime::default(),
        updated_at: Datetime::default(),
        version: 0,
    };
    db.query(&user.create_query()).await.unwrap();

    let org = Org {
        slug: "acme".into(),
        display_name: "Acme".into(),
        owner: Ref::new("alice@example.com"),
        created_at: Datetime::default(),
        updated_at: Datetime::default(),
        version: 0,
    };
    db.query(&org.create_query()).await.unwrap();

    let sa = ServiceAccount {
        slug: "acme-ci".into(),
        display_name: "CI bot".into(),
        org: Ref::new("acme"),
        created_at: Datetime::default(),
        updated_at: Datetime::default(),
        version: 0,
    };
    db.query(&sa.create_query()).await.unwrap();
}

fn sign_in_params(token_id: &str, secret: &str) -> impl SurrealValue {
    #[derive(SurrealValue)]
    struct Params {
        token_id: String,
        secret: String,
    }
    Params {
        token_id: token_id.to_string(),
        secret: secret.to_string(),
    }
}

#[tokio::test]
async fn service_account_access_definition_is_registered() {
    let defs = nauka_core::access_definitions();
    assert!(
        defs.contains("DEFINE ACCESS IF NOT EXISTS service_account ON DATABASE TYPE RECORD"),
        "service_account access missing: {defs}"
    );
    assert!(defs.contains("DURATION FOR TOKEN 15m"));
}

#[tokio::test]
async fn api_token_resource_is_cluster_scoped() {
    let cluster = nauka_core::cluster_schemas();
    assert!(cluster.contains("DEFINE TABLE IF NOT EXISTS api_token SCHEMAFULL"));
    assert!(cluster.contains("DEFINE FIELD IF NOT EXISTS token_id ON api_token TYPE string"));
    assert!(cluster.contains("DEFINE FIELD IF NOT EXISTS hash ON api_token TYPE string"));
    assert!(cluster.contains(
        "DEFINE FIELD IF NOT EXISTS service_account ON api_token TYPE record<service_account>"
    ));
}

#[tokio::test]
async fn signin_with_valid_token_returns_service_account_jwt() {
    let (db, _dir) = fresh_db().await;
    seed_owner_and_sa(&db).await;

    let secret = "the-quick-brown-fox-jumps-over-the-lazy-dog";
    let token = ApiToken {
        token_id: "public-id-1".into(),
        name: "deploy-bot".into(),
        service_account: Ref::new("acme-ci"),
        hash: hash_password(secret).unwrap(),
        created_at: Datetime::default(),
        updated_at: Datetime::default(),
        version: 0,
    };
    db.query(&token.create_query()).await.unwrap();

    let jwt = db
        .inner()
        .signin(Record {
            namespace: nauka_state::DEFAULT_NAMESPACE.to_string(),
            database: nauka_state::DEFAULT_DATABASE.to_string(),
            access: "service_account".to_string(),
            params: sign_in_params("public-id-1", secret),
        })
        .await
        .expect("service_account signin");
    let jwt_str = jwt.access.into_insecure_token();

    let claims = decode_claims(&jwt_str).unwrap();
    assert_eq!(claims.access.as_deref(), Some("service_account"));
    // The ID claim names the service account record — that's how
    // `fn::iam::can` knows to treat `$auth` as an SA when evaluating
    // scope queries.
    let id = claims.id.as_deref().expect("ID claim present");
    assert!(
        id.starts_with("service_account:"),
        "expected service_account record-id, got {id:?}"
    );
    assert!(
        id.contains("acme-ci"),
        "expected acme-ci in record-id, got {id:?}"
    );
}

#[tokio::test]
async fn signin_with_wrong_secret_fails() {
    let (db, _dir) = fresh_db().await;
    seed_owner_and_sa(&db).await;

    let token = ApiToken {
        token_id: "public-id-2".into(),
        name: "bot".into(),
        service_account: Ref::new("acme-ci"),
        hash: hash_password("real-secret").unwrap(),
        created_at: Datetime::default(),
        updated_at: Datetime::default(),
        version: 0,
    };
    db.query(&token.create_query()).await.unwrap();

    let err = db
        .inner()
        .signin(Record {
            namespace: nauka_state::DEFAULT_NAMESPACE.to_string(),
            database: nauka_state::DEFAULT_DATABASE.to_string(),
            access: "service_account".to_string(),
            params: sign_in_params("public-id-2", "wrong-secret"),
        })
        .await;
    assert!(err.is_err(), "wrong secret must be rejected");
}

#[tokio::test]
async fn signin_with_unknown_token_id_fails() {
    let (db, _dir) = fresh_db().await;
    seed_owner_and_sa(&db).await;

    let err = db
        .inner()
        .signin(Record {
            namespace: nauka_state::DEFAULT_NAMESPACE.to_string(),
            database: nauka_state::DEFAULT_DATABASE.to_string(),
            access: "service_account".to_string(),
            params: sign_in_params("does-not-exist", "anything"),
        })
        .await;
    assert!(err.is_err(), "unknown token id must be rejected");
}
