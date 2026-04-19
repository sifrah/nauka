//! Top-level router — REST + GraphQL + docs + auth middleware
//! composed into one `axum::Router` ready to hand to `axum::serve`.
//!
//! The router is assembled in two halves:
//!
//! 1. A **protected** tree that carries every resource surface — REST
//!    CRUD + `/graphql` POST. These go through `require_auth` so the
//!    axum handlers only run once the Bearer token is present.
//! 2. A **public** tree with the documentation surfaces —
//!    `/docs` (Scalar UI), `/openapi.json`, GET `/graphql`
//!    (GraphiQL), and `/graphql/schema` (SDL). Rendering an API
//!    reference page does not need credentials.
//!
//! Merging the two with `.merge(...)` at the top level keeps the
//! middleware stack non-overlapping: the protected-tree layer only
//! wraps what was mounted under it.

use async_graphql::dynamic::Schema;
use async_graphql::http::GraphiQLSource;
use async_graphql_axum::{GraphQLRequest, GraphQLResponse};
use axum::extract::Extension;
use axum::http::header;
use axum::response::{Html, IntoResponse};
use axum::routing::{get, post};
use axum::{middleware, Json, Router};

use crate::{graphql, hypervisor, iam, mesh, openapi, org, principal::require_auth, Deps};

/// Build the full API router.
pub fn router(deps: Deps) -> Router {
    let schema = graphql::build_schema(deps.clone());
    let sdl = schema.sdl();

    // Everything that touches resource state. One `require_auth`
    // layer wraps the whole subtree.
    let protected = Router::new()
        .merge(hypervisor::routes())
        .merge(mesh::routes())
        .merge(org::routes())
        .merge(iam::routes())
        .route("/graphql", post(graphql_handler))
        .layer(middleware::from_fn(require_auth))
        .with_state(deps);

    // Documentation surfaces — no auth. GraphiQL is deliberately
    // parked on `GET /graphql` so a browser URL lands on the
    // playground; the POST endpoint lives inside the protected
    // tree above.
    let public = Router::new()
        .route("/openapi.json", get(openapi_handler))
        .route("/docs", get(scalar_ui))
        .route("/graphql", get(graphiql))
        .route(
            "/graphql/schema",
            get({
                let sdl = sdl.clone();
                move || async move { ([(header::CONTENT_TYPE, "text/plain; charset=utf-8")], sdl) }
            }),
        );

    Router::new()
        .merge(protected)
        .merge(public)
        .layer(Extension(schema))
}

async fn graphql_handler(
    Extension(schema): Extension<Schema>,
    req: GraphQLRequest,
) -> GraphQLResponse {
    schema.execute(req.into_inner()).await.into()
}

async fn graphiql() -> impl IntoResponse {
    Html(GraphiQLSource::build().endpoint("/graphql").finish())
}

async fn openapi_handler() -> impl IntoResponse {
    Json(openapi::build_openapi())
}

async fn scalar_ui() -> impl IntoResponse {
    // Scalar embed snippet — static, zero JS dependencies beyond
    // the CDN script. Served over the same axum Router so the docs
    // URL follows the daemon everywhere without extra plumbing.
    Html(
        r#"<!DOCTYPE html>
<html>
<head>
  <title>Nauka API reference</title>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
</head>
<body>
  <script
    id="api-reference"
    data-url="/openapi.json"
    data-configuration='{"theme":"default","hideClientButton":false}'></script>
  <script src="https://cdn.jsdelivr.net/npm/@scalar/api-reference"></script>
</body>
</html>"#,
    )
}
