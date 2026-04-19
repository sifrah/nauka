//! Integration test for #356 (342-C1): `Org` round-trips over
//! REST + GraphQL + SDK through the shared generic CRUD handler.
//!
//! Seeds a `User` first (Org's `owner` field is a `Ref<User>` so the
//! referential-integrity constraint would reject the insert
//! otherwise), then exercises the generated CRUD surface end-to-
//! end. The Hypervisor test at `hypervisor_roundtrip.rs` covers the
//! same handler code against a non-IAM resource; this adds the IAM
//! scope-tree variant so both shapes are validated.

use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::http::{header, Method, Request, StatusCode};
use nauka_api::{router, Deps};
use nauka_api_client::{encode_path_segment, Client};
use nauka_core::resource::{Datetime, Ref};
use nauka_iam::{Org, User};
use nauka_state::{Database, RaftNode, TlsConfig, Writer};
use serde_json::{json, Value};
use tower::ServiceExt;

async fn fresh_stack() -> (Deps, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("api-org-test.db");
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

async fn seed_user(deps: &Deps, email: &str) {
    let now = Datetime::now();
    let user = User {
        email: email.to_string(),
        password_hash: "$argon2id$v=19$m=65536,t=3,p=1$nope$nope".to_string(),
        display_name: "Test User".to_string(),
        email_verified_at: None,
        active: true,
        created_at: now,
        updated_at: now,
        version: 0,
    };
    let raft = deps.raft.as_deref().unwrap();
    Writer::new(&deps.db)
        .with_raft(raft)
        .create(&user)
        .await
        .unwrap();
}

fn sample_org(slug: &str, owner_email: &str) -> Org {
    let now = Datetime::now();
    Org {
        slug: slug.to_string(),
        display_name: format!("{slug} display"),
        owner: Ref::<User>::new(owner_email.to_string()),
        created_at: now,
        updated_at: now,
        version: 0,
    }
}

#[tokio::test]
async fn rest_create_then_get_list_update() {
    let (deps, _dir) = fresh_stack().await;
    seed_user(&deps, "alice@example.com").await;
    let app = router(deps);

    // CREATE
    let body = serde_json::to_vec(&sample_org("acme", "alice@example.com")).unwrap();
    let req = Request::builder()
        .method(Method::POST)
        .uri("/v1/orgs")
        .header(header::AUTHORIZATION, "Bearer test-jwt")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "create");
    let body = to_bytes(resp.into_body(), 64 * 1024).await.unwrap();
    let created: Org = serde_json::from_slice(&body).unwrap();
    assert_eq!(created.slug, "acme");
    assert_eq!(created.version, 0);

    // GET
    let req = Request::builder()
        .method(Method::GET)
        .uri("/v1/orgs/acme")
        .header(header::AUTHORIZATION, "Bearer test-jwt")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "get");
    let body = to_bytes(resp.into_body(), 64 * 1024).await.unwrap();
    let got: Org = serde_json::from_slice(&body).unwrap();
    assert_eq!(got.slug, "acme");
    assert_eq!(got.display_name, "acme display");

    // LIST
    let req = Request::builder()
        .method(Method::GET)
        .uri("/v1/orgs")
        .header(header::AUTHORIZATION, "Bearer test-jwt")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "list");
    let body = to_bytes(resp.into_body(), 64 * 1024).await.unwrap();
    let rows: Vec<Org> = serde_json::from_slice(&body).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].slug, "acme");

    // UPDATE — change display_name, version must bump to 1
    let mut updated = got.clone();
    updated.display_name = "ACME Corp".to_string();
    let body = serde_json::to_vec(&updated).unwrap();
    let req = Request::builder()
        .method(Method::PATCH)
        .uri("/v1/orgs/acme")
        .header(header::AUTHORIZATION, "Bearer test-jwt")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "update");
    let body = to_bytes(resp.into_body(), 64 * 1024).await.unwrap();
    let patched: Org = serde_json::from_slice(&body).unwrap();
    assert_eq!(patched.display_name, "ACME Corp");
    assert_eq!(patched.version, 1);
}

#[tokio::test]
async fn rest_update_path_body_mismatch_is_422() {
    let (deps, _dir) = fresh_stack().await;
    seed_user(&deps, "alice@example.com").await;
    let raft = deps.raft.as_deref().unwrap();
    Writer::new(&deps.db)
        .with_raft(raft)
        .create(&sample_org("acme", "alice@example.com"))
        .await
        .unwrap();
    let app = router(deps);

    // PATCH /v1/orgs/nope with body.slug = "acme" → 422
    let body = serde_json::to_vec(&sample_org("acme", "alice@example.com")).unwrap();
    let req = Request::builder()
        .method(Method::PATCH)
        .uri("/v1/orgs/nope")
        .header(header::AUTHORIZATION, "Bearer test-jwt")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
async fn rest_delete_then_get_is_404() {
    let (deps, _dir) = fresh_stack().await;
    seed_user(&deps, "alice@example.com").await;
    let raft = deps.raft.as_deref().unwrap();
    Writer::new(&deps.db)
        .with_raft(raft)
        .create(&sample_org("acme", "alice@example.com"))
        .await
        .unwrap();
    let app = router(deps);

    let req = Request::builder()
        .method(Method::DELETE)
        .uri("/v1/orgs/acme")
        .header(header::AUTHORIZATION, "Bearer test-jwt")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    let req = Request::builder()
        .method(Method::GET)
        .uri("/v1/orgs/acme")
        .header(header::AUTHORIZATION, "Bearer test-jwt")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn graphql_org_query_reads_seeded_row() {
    let (deps, _dir) = fresh_stack().await;
    seed_user(&deps, "alice@example.com").await;
    let raft = deps.raft.as_deref().unwrap();
    Writer::new(&deps.db)
        .with_raft(raft)
        .create(&sample_org("acme", "alice@example.com"))
        .await
        .unwrap();
    let app = router(deps);

    let query = json!({
        "query": "query($id: String!) { org(id: $id) { slug displayName owner } }",
        "variables": { "id": "acme" },
    });
    let req = Request::builder()
        .method(Method::POST)
        .uri("/graphql")
        .header(header::AUTHORIZATION, "Bearer test-jwt")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&query).unwrap()))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = to_bytes(resp.into_body(), 64 * 1024).await.unwrap();
    let v: Value = serde_json::from_slice(&body).unwrap();
    let org = &v["data"]["org"];
    assert_eq!(org["slug"].as_str(), Some("acme"));
    assert_eq!(org["displayName"].as_str(), Some("acme display"));
    assert_eq!(org["owner"].as_str(), Some("alice@example.com"));
}

#[tokio::test]
async fn sdk_org_roundtrips_via_reqwest() {
    let (deps, _dir) = fresh_stack().await;
    seed_user(&deps, "alice@example.com").await;
    let app = router(deps);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let client = Client::new(format!("http://{addr}"), "test-jwt").unwrap();
    let created = client
        .org()
        .create(&sample_org("acme", "alice@example.com"))
        .await
        .expect("sdk create");
    assert_eq!(created.slug, "acme");

    let fetched = client.org().get("acme").await.expect("sdk get");
    assert_eq!(fetched.slug, "acme");
    assert_eq!(fetched.display_name, "acme display");

    let list = client.org().list().await.expect("sdk list");
    assert!(list.iter().any(|o| o.slug == "acme"));

    // encode_path_segment is exercised here indirectly — slugs are
    // simple today, but the helper stays in place for resources
    // with exotic ids (Mesh ULA CIDR is the motivating case).
    let _ = encode_path_segment("acme");

    client.org().delete("acme").await.expect("sdk delete");
    let after = client.org().get("acme").await;
    assert!(after.is_err(), "expected NotFound after delete, got {after:?}");

    server.abort();
}
