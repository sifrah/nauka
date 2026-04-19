//! Integration test for #357 (342-D1): the public documentation
//! surfaces — `/openapi.json`, `/docs`, `/graphql` (GraphiQL),
//! `/graphql/schema` (SDL) — respond without a Bearer token and
//! advertise every resource the descriptor slices register.
//!
//! What this test deliberately does not assert: the exact SDL text
//! or the exact Scalar embed bytes. Both change with dep bumps, and
//! tying the test to either would turn a benign upgrade into a
//! red CI run. We check presence + shape instead.

use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::http::{Method, Request, StatusCode};
use nauka_api::{router, Deps};
use nauka_state::{Database, RaftNode, TlsConfig};
use serde_json::Value;
use tower::ServiceExt;

async fn fresh_router() -> (axum::Router, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("api-docs-test.db");
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

    // Docs surfaces don't need Raft — they just render descriptors.
    let raft = RaftNode::new(1, db.clone(), None::<TlsConfig>)
        .await
        .unwrap();
    raft.init_cluster("[::1]:0").await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    (router(Deps::new(db, Some(Arc::new(raft)))), dir)
}

fn no_auth_request(uri: &str) -> Request<Body> {
    Request::builder()
        .method(Method::GET)
        .uri(uri)
        .body(Body::empty())
        .unwrap()
}

#[tokio::test]
async fn openapi_json_serves_without_auth_and_lists_every_resource() {
    let (app, _dir) = fresh_router().await;

    let resp = app.oneshot(no_auth_request("/openapi.json")).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = to_bytes(resp.into_body(), 256 * 1024).await.unwrap();
    let doc: Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(doc["openapi"].as_str(), Some("3.1.0"));
    assert!(
        doc["info"]["title"].as_str().unwrap().contains("Nauka"),
        "info.title missing Nauka: {}",
        doc
    );
    assert!(doc["paths"].is_object(), "paths object missing");

    // Every resource we've wired so far has to appear in the spec.
    let paths = doc["paths"].as_object().unwrap();
    for expected in &[
        "/v1/hypervisors",
        "/v1/meshes",
        "/v1/orgs",
        "/v1/projects",
        "/v1/users",
        "/v1/api-tokens",
    ] {
        assert!(
            paths.contains_key(*expected),
            "missing path {expected} in OpenAPI — got: {:?}",
            paths.keys().collect::<Vec<_>>()
        );
    }

    // Resources that opted out (PasswordResetToken) must NOT appear.
    for forbidden in &["/v1/password-reset-tokens", "/v1/password_reset_tokens"] {
        assert!(
            !paths.contains_key(*forbidden),
            "internal-only resource leaked into OpenAPI: {forbidden}"
        );
    }

    // And the bearer security scheme must be declared.
    assert_eq!(
        doc["components"]["securitySchemes"]["bearerAuth"]["type"].as_str(),
        Some("http")
    );
}

#[tokio::test]
async fn docs_returns_scalar_html() {
    let (app, _dir) = fresh_router().await;
    let resp = app.oneshot(no_auth_request("/docs")).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = to_bytes(resp.into_body(), 64 * 1024).await.unwrap();
    let html = String::from_utf8(body.to_vec()).unwrap();
    assert!(
        html.contains("api-reference"),
        "scalar script tag missing in /docs: {html}"
    );
    assert!(
        html.contains("/openapi.json"),
        "scalar should point at /openapi.json: {html}"
    );
}

#[tokio::test]
async fn graphql_sdl_includes_known_types() {
    let (app, _dir) = fresh_router().await;
    let resp = app
        .oneshot(no_auth_request("/graphql/schema"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = to_bytes(resp.into_body(), 256 * 1024).await.unwrap();
    let sdl = String::from_utf8(body.to_vec()).unwrap();
    // Hypervisor + Mesh + Org all register GraphQL resolvers —
    // the dynamic schema's SDL must mention them.
    for expected in &["Hypervisor", "Mesh", "Org", "Query", "Mutation"] {
        assert!(sdl.contains(expected), "SDL missing `{expected}`: {sdl}");
    }
}

#[tokio::test]
async fn protected_routes_still_require_bearer() {
    // Sanity: the docs surfaces being public didn't accidentally
    // skip the auth layer on resource routes.
    let (app, _dir) = fresh_router().await;
    let resp = app
        .oneshot(no_auth_request("/v1/hypervisors"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}
