//! Integration test for IAM-6 (#350) — `#[hidden]` field
//! permissions.
//!
//! Verifies that:
//! 1. The schema emits `PERMISSIONS FOR select WHERE $auth = NONE`
//!    on the hidden fields (`user.password_hash`, `api_token.hash`).
//! 2. DEFINE ACCESS SIGNIN still works — SurrealDB runs that query
//!    with `$auth = NONE`, so it can still read `password_hash`.
//! 3. A record-level session that authenticated as the record's
//!    owner still sees `password_hash = NONE` when it SELECTs the
//!    row back — the owner does not get read access to their own
//!    stored secret.

use nauka_core::resource::{Datetime, Ref, ResourceOps};
use nauka_iam::{hash_password, ApiToken, Org, ServiceAccount, User};
use nauka_state::Database;
use serde::Deserialize;
use surrealdb::opt::auth::Record;
use surrealdb::types::SurrealValue;

async fn fresh_db() -> (Database, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("iam6-test.db");
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

#[tokio::test]
async fn schema_marks_password_hash_with_auth_none_select() {
    let cluster = nauka_core::cluster_schemas();
    assert!(
        cluster.contains(
            "DEFINE FIELD IF NOT EXISTS password_hash ON user TYPE string \
             PERMISSIONS FOR select WHERE $auth = NONE"
        ),
        "user.password_hash permissions clause missing: {cluster}"
    );
}

#[tokio::test]
async fn schema_marks_api_token_hash_with_auth_none_select() {
    let cluster = nauka_core::cluster_schemas();
    assert!(
        cluster.contains(
            "DEFINE FIELD IF NOT EXISTS hash ON api_token TYPE string \
             PERMISSIONS FOR select WHERE $auth = NONE"
        ),
        "api_token.hash permissions clause missing: {cluster}"
    );
}

/// Partial view of a `User` row used to inspect what a record-level
/// session actually sees in `password_hash`. `Option<String>`
/// because SurrealDB returns `NONE` when the field's SELECT
/// permission denies the reader.
#[derive(Deserialize, SurrealValue)]
struct UserHashView {
    email: String,
    #[serde(default)]
    password_hash: Option<String>,
}

#[tokio::test]
async fn owner_session_sees_password_hash_as_none() {
    let (db, _dir) = fresh_db().await;

    // Hash + insert a user directly (state-machine path — $auth = NONE).
    let real_hash = hash_password("hunter2").unwrap();
    let user = User {
        email: "alice@example.com".into(),
        password_hash: real_hash.clone(),
        display_name: "Alice".into(),
        created_at: Datetime::default(),
        updated_at: Datetime::default(),
        version: 0,
    };
    db.query(&user.create_query()).await.unwrap();

    // Sanity: the hash IS stored — a root-level read (no auth)
    // should still surface it. Otherwise our DEFINE ACCESS SIGNIN
    // would break.
    let rows: Vec<UserHashView> = db
        .query_take("SELECT email, password_hash FROM user")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].password_hash.as_deref(),
        Some(real_hash.as_str()),
        "root-level SELECT must still read password_hash — \
         otherwise SIGNIN can't verify passwords"
    );

    // Sign in as alice (via the DEFINE ACCESS SIGNIN path — proves
    // that SIGNIN can still read the hash despite the field
    // permission).
    #[derive(SurrealValue)]
    struct P {
        email: String,
        password: String,
    }
    db.inner()
        .signin(Record {
            namespace: nauka_state::DEFAULT_NAMESPACE.to_string(),
            database: nauka_state::DEFAULT_DATABASE.to_string(),
            access: "user".to_string(),
            params: P {
                email: "alice@example.com".into(),
                password: "hunter2".into(),
            },
        })
        .await
        .expect("signin with vetted argon2 params");

    // Alice SELECTs her own row. She can see the user record
    // (record-level permissions let her), but the password_hash
    // field is hidden.
    let rows: Vec<UserHashView> = db
        .query_take("SELECT email, password_hash FROM user")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].email, "alice@example.com");
    assert!(
        rows[0].password_hash.is_none(),
        "alice's session must not see her own password_hash, got {:?}",
        rows[0].password_hash
    );
}

#[derive(Deserialize, SurrealValue)]
struct TokenHashView {
    token_id: String,
    #[serde(default)]
    hash: Option<String>,
}

#[tokio::test]
async fn service_account_session_sees_api_token_hash_as_none() {
    let (db, _dir) = fresh_db().await;

    // Seed: alice, org, service account, api token.
    db.query(
        &User {
            email: "alice@example.com".into(),
            password_hash: hash_password("x").unwrap(),
            display_name: "A".into(),
            created_at: Datetime::default(),
            updated_at: Datetime::default(),
            version: 0,
        }
        .create_query(),
    )
    .await
    .unwrap();
    db.query(
        &Org {
            slug: "acme".into(),
            display_name: "Acme".into(),
            owner: Ref::new("alice@example.com"),
            created_at: Datetime::default(),
            updated_at: Datetime::default(),
            version: 0,
        }
        .create_query(),
    )
    .await
    .unwrap();
    db.query(
        &ServiceAccount {
            slug: "acme-ci".into(),
            display_name: "CI".into(),
            org: Ref::new("acme"),
            created_at: Datetime::default(),
            updated_at: Datetime::default(),
            version: 0,
        }
        .create_query(),
    )
    .await
    .unwrap();

    let secret = "secret-secret-secret";
    let real_hash = hash_password(secret).unwrap();
    db.query(
        &ApiToken {
            token_id: "token-id-1".into(),
            name: "deploy-bot".into(),
            service_account: Ref::new("acme-ci"),
            hash: real_hash.clone(),
            created_at: Datetime::default(),
            updated_at: Datetime::default(),
            version: 0,
        }
        .create_query(),
    )
    .await
    .unwrap();

    // Root can still see `hash` — required so `service_account`
    // DEFINE ACCESS SIGNIN works.
    let rows: Vec<TokenHashView> = db
        .query_take("SELECT token_id, hash FROM api_token")
        .await
        .unwrap();
    assert_eq!(rows[0].hash.as_deref(), Some(real_hash.as_str()));

    // Sign in as the SA via the token; the resulting session is
    // record-level and the hash must be invisible even to the SA
    // itself.
    #[derive(SurrealValue)]
    struct P {
        token_id: String,
        secret: String,
    }
    db.inner()
        .signin(Record {
            namespace: nauka_state::DEFAULT_NAMESPACE.to_string(),
            database: nauka_state::DEFAULT_DATABASE.to_string(),
            access: "service_account".to_string(),
            params: P {
                token_id: "token-id-1".into(),
                secret: secret.into(),
            },
        })
        .await
        .expect("SA signin");

    let rows: Vec<TokenHashView> = db
        .query_take("SELECT token_id, hash FROM api_token")
        .await
        .unwrap();
    assert!(
        !rows.is_empty(),
        "SA session should still see the token row (scope_by matches its org)"
    );
    assert!(
        rows[0].hash.is_none(),
        "SA session must not see the hash, got {:?}",
        rows[0].hash
    );
}
