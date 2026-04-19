//! Top-level router — REST + GraphQL + auth middleware composed
//! into one `axum::Router` ready to hand to `axum::serve`.

use async_graphql::dynamic::Schema;
use async_graphql::http::GraphiQLSource;
use async_graphql_axum::{GraphQLRequest, GraphQLResponse};
use axum::extract::Extension;
use axum::response::{Html, IntoResponse};
use axum::routing::post;
use axum::{middleware, Router};

use crate::{graphql, hypervisor, mesh, org, principal::require_auth, Deps};

/// Build the full API router. Callers are expected to wrap the
/// returned router with `axum::serve` (or a test harness like
/// `tower::ServiceExt::oneshot`) — this fn takes care of composing
/// every sub-surface + the `require_auth` middleware once.
pub fn router(deps: Deps) -> Router {
    let schema = graphql::build_schema(deps.clone());

    Router::new()
        .merge(hypervisor::routes())
        .merge(mesh::routes())
        .merge(org::routes())
        .route("/graphql", post(graphql_handler).get(graphiql))
        // One middleware layer, applied to every route — `require_auth`
        // is the single enforcement point, handlers never parse the
        // Authorization header themselves.
        .layer(middleware::from_fn(require_auth))
        .with_state(deps)
        .layer(Extension(schema))
}

async fn graphql_handler(Extension(schema): Extension<Schema>, req: GraphQLRequest) -> GraphQLResponse {
    schema.execute(req.into_inner()).await.into()
}

async fn graphiql() -> impl IntoResponse {
    Html(
        GraphiQLSource::build()
            .endpoint("/graphql")
            .finish(),
    )
}
