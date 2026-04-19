//! Integration test for #355 (342-B1): Mesh read surface over
//! REST + GraphQL + SDK, and verification that encrypted / secret
//! fields never appear in any response.
//!
//! Unlike Hypervisor (cluster-scoped, writes via Raft), Mesh is
//! local-scoped: its rows come from the regular SurrealKV layer so
//! the test can seed a row by writing straight through the daemon's
//! DB handle. No Raft needed. This keeps the fixture lean — roughly
//! 50ms to stand up vs. ~500ms for the Hypervisor test.

use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::http::{header, Method, Request, StatusCode};
use nauka_api::{router, Deps};
use nauka_api_client::{encode_path_segment, Client};
use nauka_core::resource::Datetime;
use nauka_hypervisor::MeshRecord;
use nauka_state::{Database, Writer};
use serde_json::{json, Value};
use tower::ServiceExt;

const SECRET_MARKER_PRIVATE: &str = "PRIVATE_KEY_CIPHERTEXT_XYZ";
const SECRET_MARKER_CA_KEY: &str = "CA_KEY_CIPHERTEXT_XYZ";
const SECRET_MARKER_TLS_KEY: &str = "TLS_KEY_CIPHERTEXT_XYZ";
const SECRET_MARKER_PIN: &str = "PEERING_PIN_XYZ";

async fn fresh_stack() -> (Deps, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("api-mesh-test.db");
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

    // Local resource — no Raft handle needed.
    (Deps::new(db, None), dir)
}

async fn seed_mesh(deps: &Deps) -> MeshRecord {
    let now = Datetime::now();
    let mesh = MeshRecord {
        mesh_id: "fdaa:bbbb:cccc::/48".to_string(),
        interface_name: "nauka0".to_string(),
        listen_port: 51820,
        private_key: SECRET_MARKER_PRIVATE.to_string(),
        ca_cert: Some(
            "-----BEGIN CERTIFICATE-----\nACME-CA\n-----END CERTIFICATE-----".to_string(),
        ),
        ca_key: Some(SECRET_MARKER_CA_KEY.to_string()),
        tls_cert: Some(
            "-----BEGIN CERTIFICATE-----\nACME-TLS\n-----END CERTIFICATE-----".to_string(),
        ),
        tls_key: Some(SECRET_MARKER_TLS_KEY.to_string()),
        peering_pin: Some(SECRET_MARKER_PIN.to_string()),
        created_at: now,
        updated_at: now,
        version: 0,
    };
    Writer::new(&deps.db).create(&mesh).await.unwrap();
    mesh
}

#[tokio::test]
async fn rest_list_returns_seeded_mesh_without_secrets() {
    let (deps, _dir) = fresh_stack().await;
    let seeded = seed_mesh(&deps).await;
    let app = router(deps);

    let req = Request::builder()
        .method(Method::GET)
        .uri("/v1/meshes")
        .header(header::AUTHORIZATION, "Bearer test-jwt")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = to_bytes(resp.into_body(), 64 * 1024).await.unwrap();
    let raw = String::from_utf8(body.to_vec()).unwrap();

    // The shape is there ...
    let rows: Value = serde_json::from_str(&raw).unwrap();
    let row = &rows[0];
    assert_eq!(row["mesh_id"].as_str(), Some(seeded.mesh_id.as_str()));
    assert_eq!(row["interface_name"].as_str(), Some("nauka0"));
    assert_eq!(row["listen_port"].as_u64(), Some(51820));

    // ... and the secrets are masked. Grep the raw JSON text so
    // we catch any leak even under renaming / nested field changes.
    assert!(
        !raw.contains(SECRET_MARKER_PRIVATE),
        "private_key leaked into REST response: {raw}"
    );
    assert!(
        !raw.contains(SECRET_MARKER_CA_KEY),
        "ca_key leaked into REST response: {raw}"
    );
    assert!(
        !raw.contains(SECRET_MARKER_TLS_KEY),
        "tls_key leaked into REST response: {raw}"
    );
    assert!(
        !raw.contains(SECRET_MARKER_PIN),
        "peering_pin leaked into REST response: {raw}"
    );
}

#[tokio::test]
async fn rest_get_by_id_works_and_masks_secrets() {
    let (deps, _dir) = fresh_stack().await;
    let seeded = seed_mesh(&deps).await;
    let app = router(deps);

    // mesh_id contains `/` and `:` (CIDR notation), so it must be
    // percent-encoded in the URL path. Same helper the SDK uses
    // internally — the server `Path` extractor decodes it.
    let req = Request::builder()
        .method(Method::GET)
        .uri(format!(
            "/v1/meshes/{}",
            encode_path_segment(&seeded.mesh_id)
        ))
        .header(header::AUTHORIZATION, "Bearer test-jwt")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = to_bytes(resp.into_body(), 64 * 1024).await.unwrap();
    let raw = String::from_utf8(body.to_vec()).unwrap();
    let v: Value = serde_json::from_str(&raw).unwrap();
    assert_eq!(v["mesh_id"].as_str(), Some(seeded.mesh_id.as_str()));
    assert!(
        !raw.contains(SECRET_MARKER_PRIVATE) && !raw.contains(SECRET_MARKER_PIN),
        "secrets leaked in GET response"
    );
}

#[tokio::test]
async fn rest_get_unknown_is_404() {
    let (deps, _dir) = fresh_stack().await;
    let app = router(deps);

    let req = Request::builder()
        .method(Method::GET)
        .uri("/v1/meshes/nope")
        .header(header::AUTHORIZATION, "Bearer test-jwt")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn graphql_mesh_query_reads_seeded_row() {
    let (deps, _dir) = fresh_stack().await;
    let seeded = seed_mesh(&deps).await;
    let app = router(deps);

    let query = json!({
        "query": "query($id: String!) { mesh(id: $id) { meshId interfaceName listenPort caCert } }",
        "variables": { "id": seeded.mesh_id },
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
    let raw = String::from_utf8(body.to_vec()).unwrap();
    let v: Value = serde_json::from_str(&raw).unwrap();
    let mesh = &v["data"]["mesh"];
    assert_eq!(mesh["meshId"].as_str(), Some(seeded.mesh_id.as_str()));
    assert_eq!(mesh["interfaceName"].as_str(), Some("nauka0"));
    assert_eq!(mesh["listenPort"].as_str(), Some("51820"));
    assert!(mesh["caCert"].as_str().unwrap().contains("ACME-CA"));
}

#[tokio::test]
async fn sdk_mesh_list_and_get_via_reqwest() {
    let (deps, _dir) = fresh_stack().await;
    let seeded = seed_mesh(&deps).await;
    let app = router(deps);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let client = Client::new(format!("http://{addr}"), "test-jwt").unwrap();
    let list = client.mesh().list().await.expect("sdk list");
    assert!(list.iter().any(|m| m.mesh_id == seeded.mesh_id));

    let fetched = client.mesh().get(&seeded.mesh_id).await.expect("sdk get");
    assert_eq!(fetched.mesh_id, seeded.mesh_id);
    // `#[serde(skip)]` means the SDK receives the default for each
    // hidden field (empty string / None) — not the original
    // ciphertext. This is the whole point of masking.
    assert_eq!(fetched.private_key, "");
    assert!(fetched.ca_key.is_none());
    assert!(fetched.tls_key.is_none());
    assert!(fetched.peering_pin.is_none());

    server.abort();
}
