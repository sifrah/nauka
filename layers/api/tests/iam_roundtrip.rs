//! Integration test for #356 (342-C2): the IAM resources land
//! through the generic `mount_crud` handler. One test per shape
//! that's distinct from Org (already covered in
//! `org_roundtrip.rs`):
//!
//! - **Project** → the simplest cluster-scoped child of Org;
//!   validates the generic `mount_crud` wiring stays green when
//!   applied to a fresh struct shape.
//! - **User** → has a `#[serde(skip)]` field (`password_hash`);
//!   the REST GET response must never carry the digest even when
//!   the daemon reads the row under `$auth = NONE`.
//! - **ApiToken** → same masking check on `hash`, plus the
//!   `api_verbs = "get, list, delete"` policy (no POST should
//!   reach the create handler — the server returns 405).
//!
//! Permission / RoleBinding / etc. share the same generic path and
//! don't need separate coverage; adding them would be boilerplate.

use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::http::{header, Method, Request, StatusCode};
use nauka_api::{router, Deps};
use nauka_core::resource::{Datetime, Ref};
use nauka_iam::{ApiToken, Project, ServiceAccount, User};
use nauka_state::{Database, RaftNode, TlsConfig, Writer};
use serde_json::Value;
use tower::ServiceExt;

async fn fresh_stack() -> (Deps, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("api-iam-test.db");
    let db = Arc::new(
        Database::open(Some(path.to_str().unwrap()))
            .await
            .unwrap(),
    );

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

    let raft = RaftNode::new(1, db.clone(), None::<TlsConfig>)
        .await
        .unwrap();
    raft.init_cluster("[::1]:0").await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    (Deps::new(db, Some(Arc::new(raft))), dir)
}

async fn seed_user(deps: &Deps, email: &str, password_hash: &str) {
    let now = Datetime::now();
    let user = User {
        email: email.to_string(),
        password_hash: password_hash.to_string(),
        display_name: "Test".to_string(),
        email_verified_at: None,
        active: true,
        created_at: now,
        updated_at: now,
        version: 0,
    };
    Writer::new(&deps.db)
        .with_raft(deps.raft.as_deref().unwrap())
        .create(&user)
        .await
        .unwrap();
}

async fn seed_org(deps: &Deps, slug: &str, owner: &str) {
    let now = Datetime::now();
    Writer::new(&deps.db)
        .with_raft(deps.raft.as_deref().unwrap())
        .create(&nauka_iam::Org {
            slug: slug.to_string(),
            display_name: format!("{slug} Corp"),
            owner: Ref::<User>::new(owner.to_string()),
            created_at: now,
            updated_at: now,
            version: 0,
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn rest_project_crud_roundtrip() {
    let (deps, _dir) = fresh_stack().await;
    seed_user(&deps, "alice@example.com", "$argon2id$v=19$m=65536,t=3,p=1$x$y").await;
    seed_org(&deps, "acme", "alice@example.com").await;
    let app = router(deps);

    let now = Datetime::now();
    let body = serde_json::to_vec(&Project {
        uid: "acme-web".to_string(),
        slug: "web".to_string(),
        org: Ref::<nauka_iam::Org>::new("acme".to_string()),
        display_name: "Web app".to_string(),
        created_at: now,
        updated_at: now,
        version: 0,
    })
    .unwrap();

    let req = Request::builder()
        .method(Method::POST)
        .uri("/v1/projects")
        .header(header::AUTHORIZATION, "Bearer test-jwt")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "create project");

    let req = Request::builder()
        .method(Method::GET)
        .uri("/v1/projects/acme-web")
        .header(header::AUTHORIZATION, "Bearer test-jwt")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "get project");
    let bytes = to_bytes(resp.into_body(), 64 * 1024).await.unwrap();
    let got: Project = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(got.uid, "acme-web");
    assert_eq!(got.slug, "web");
}

const PASSWORD_HASH_MARKER: &str = "PHC_PASSWORD_HASH_XYZ";

#[tokio::test]
async fn user_get_never_leaks_password_hash() {
    let (deps, _dir) = fresh_stack().await;
    seed_user(&deps, "bob@example.com", PASSWORD_HASH_MARKER).await;
    let app = router(deps);

    let req = Request::builder()
        .method(Method::GET)
        .uri("/v1/users")
        .header(header::AUTHORIZATION, "Bearer test-jwt")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = to_bytes(resp.into_body(), 64 * 1024).await.unwrap();
    let raw = String::from_utf8(bytes.to_vec()).unwrap();
    assert!(
        !raw.contains(PASSWORD_HASH_MARKER),
        "password_hash leaked in user list: {raw}"
    );

    // Also verify the get-by-id path.
    let req = Request::builder()
        .method(Method::GET)
        .uri("/v1/users/bob%40example.com")
        .header(header::AUTHORIZATION, "Bearer test-jwt")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = to_bytes(resp.into_body(), 64 * 1024).await.unwrap();
    let raw = String::from_utf8(bytes.to_vec()).unwrap();
    let v: Value = serde_json::from_str(&raw).unwrap();
    assert_eq!(v["email"].as_str(), Some("bob@example.com"));
    assert!(
        !raw.contains(PASSWORD_HASH_MARKER),
        "password_hash leaked in user get: {raw}"
    );
}

const TOKEN_HASH_MARKER: &str = "API_TOKEN_HASH_CIPHERTEXT_XYZ";

#[tokio::test]
async fn api_token_create_is_405_and_hash_never_leaks() {
    let (deps, _dir) = fresh_stack().await;
    seed_user(&deps, "carol@example.com", "$argon2id$v=19$m=65536,t=3,p=1$x$y").await;
    seed_org(&deps, "acme", "carol@example.com").await;

    // Seed a service_account + api_token through the Writer so
    // the GET / LIST responses have something to render.
    let now = Datetime::now();
    let sa = ServiceAccount {
        slug: "acme-ci".to_string(),
        display_name: "CI".to_string(),
        org: Ref::<nauka_iam::Org>::new("acme".to_string()),
        created_at: now,
        updated_at: now,
        version: 0,
    };
    Writer::new(&deps.db)
        .with_raft(deps.raft.as_deref().unwrap())
        .create(&sa)
        .await
        .unwrap();

    let tok = ApiToken {
        token_id: "tok_0123456789ab".to_string(),
        name: "build-bot".to_string(),
        service_account: Ref::<ServiceAccount>::new("acme-ci".to_string()),
        hash: TOKEN_HASH_MARKER.to_string(),
        created_at: now,
        updated_at: now,
        version: 0,
    };
    Writer::new(&deps.db)
        .with_raft(deps.raft.as_deref().unwrap())
        .create(&tok)
        .await
        .unwrap();

    let app = router(deps);

    // 1. POST /v1/api-tokens → 405 because the resource opted out
    //    of `create` (api_verbs = "get, list, delete").
    let req = Request::builder()
        .method(Method::POST)
        .uri("/v1/api-tokens")
        .header(header::AUTHORIZATION, "Bearer test-jwt")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from("{}"))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::METHOD_NOT_ALLOWED,
        "POST /v1/api-tokens should be 405 for a no-create resource"
    );

    // 2. LIST must hide the hash.
    let req = Request::builder()
        .method(Method::GET)
        .uri("/v1/api-tokens")
        .header(header::AUTHORIZATION, "Bearer test-jwt")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = to_bytes(resp.into_body(), 64 * 1024).await.unwrap();
    let raw = String::from_utf8(bytes.to_vec()).unwrap();
    assert!(
        !raw.contains(TOKEN_HASH_MARKER),
        "api_token hash leaked: {raw}"
    );
    // But the public-looking fields must be present.
    assert!(raw.contains("tok_0123456789ab"), "token_id missing: {raw}");
    assert!(raw.contains("build-bot"), "token name missing: {raw}");
}
