//! Integration test for #354 (342-A): round-trip CRUD on `Hypervisor`
//! over REST + GraphQL + SDK — three surfaces, one shared Deps,
//! same shape on the wire.
//!
//! Runs the full stack in-process: fresh SurrealKV, single-node
//! Raft, an axum router mounted via `tower::ServiceExt::oneshot` so
//! we never bind a socket.
//!
//! The SDK arm spins up a real `axum::serve` on an ephemeral port
//! because `nauka-api-client` uses `reqwest` and needs a real
//! socket. Tolerating one loopback listener is cheaper than
//! threading an ephemeral transport through the SDK for tests only.

use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::http::{header, Method, Request, StatusCode};
use nauka_api::{router, Deps};
use nauka_api_client::Client;
use nauka_core::resource::Datetime;
use nauka_hypervisor::Hypervisor;
use nauka_state::{Database, RaftNode, TlsConfig};
use serde_json::{json, Value};
use tower::ServiceExt;

async fn fresh_stack() -> (Deps, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("api-test.db");
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
    // Give Raft a beat to finish electing + applying the initial
    // membership entry before the handlers try to write.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    (Deps::new(db, Some(Arc::new(raft))), dir)
}

fn sample_hypervisor(public_key: &str) -> Hypervisor {
    let now = Datetime::now();
    Hypervisor {
        public_key: public_key.to_string(),
        node_id: 42,
        raft_addr: "[::1]:7010".to_string(),
        address: "fdaa::1".to_string(),
        endpoint: Some("203.0.113.5:51820".to_string()),
        allowed_ips: vec!["fdaa::/64".to_string()],
        keepalive: Some(25),
        created_at: now,
        updated_at: now,
        version: 0,
    }
}

#[tokio::test]
async fn rest_create_then_get_roundtrips() {
    let (deps, _dir) = fresh_stack().await;
    let app = router(deps);

    let body = serde_json::to_vec(&sample_hypervisor("pk-rest-1")).unwrap();
    let req = Request::builder()
        .method(Method::POST)
        .uri("/v1/hypervisors")
        .header(header::AUTHORIZATION, "Bearer test-jwt")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body))
        .unwrap();

    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "create response");
    let body = to_bytes(resp.into_body(), 64 * 1024).await.unwrap();
    let created: Hypervisor = serde_json::from_slice(&body).unwrap();
    assert_eq!(created.public_key, "pk-rest-1");
    assert_eq!(created.version, 0);

    let req = Request::builder()
        .method(Method::GET)
        .uri("/v1/hypervisors/pk-rest-1")
        .header(header::AUTHORIZATION, "Bearer test-jwt")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "get response");
    let body = to_bytes(resp.into_body(), 64 * 1024).await.unwrap();
    let fetched: Hypervisor = serde_json::from_slice(&body).unwrap();
    assert_eq!(fetched.public_key, "pk-rest-1");
    assert_eq!(fetched.node_id, 42);
}

#[tokio::test]
async fn unauth_is_401() {
    let (deps, _dir) = fresh_stack().await;
    let app = router(deps);

    let req = Request::builder()
        .method(Method::GET)
        .uri("/v1/hypervisors")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn graphql_query_reads_row_written_via_rest() {
    let (deps, _dir) = fresh_stack().await;
    let app = router(deps);

    // Seed via REST.
    let body = serde_json::to_vec(&sample_hypervisor("pk-gql-1")).unwrap();
    let req = Request::builder()
        .method(Method::POST)
        .uri("/v1/hypervisors")
        .header(header::AUTHORIZATION, "Bearer test-jwt")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Read via GraphQL.
    let query = json!({
        "query": "query($id: String!) { hypervisor(id: $id) { publicKey nodeId address } }",
        "variables": { "id": "pk-gql-1" },
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
    let hv = &v["data"]["hypervisor"];
    assert_eq!(hv["publicKey"].as_str(), Some("pk-gql-1"));
    assert_eq!(hv["nodeId"].as_str(), Some("42"));
    assert_eq!(hv["address"].as_str(), Some("fdaa::1"));
}

#[tokio::test]
async fn sdk_creates_then_lists_via_reqwest() {
    let (deps, _dir) = fresh_stack().await;
    let app = router(deps);

    // Real listener so reqwest can hit it.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let client = Client::new(format!("http://{addr}"), "test-jwt").unwrap();
    let created = client
        .hypervisor()
        .create(&sample_hypervisor("pk-sdk-1"))
        .await
        .expect("sdk create");
    assert_eq!(created.public_key, "pk-sdk-1");

    let fetched = client.hypervisor().get("pk-sdk-1").await.expect("sdk get");
    assert_eq!(fetched.public_key, "pk-sdk-1");
    assert_eq!(fetched.node_id, 42);

    let list = client.hypervisor().list().await.expect("sdk list");
    assert!(list.iter().any(|h| h.public_key == "pk-sdk-1"));

    server.abort();
}
