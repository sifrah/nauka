//! Generic REST CRUD handlers — one monomorphization per resource,
//! zero boilerplate in the per-resource modules.
//!
//! The shape parameterises every verb by `R: ResourceOps` + whatever
//! extra bounds serde / FromStr pull in. Each resource's module
//! (`hypervisor.rs`, `org.rs`, …) just calls [`mount_crud`] with
//! its path prefix — the id-field name and timestamps are handled
//! through the `Resource` / `ResourceOps` trait methods the
//! `#[resource]` macro emits. Adding a new resource is one `mount_*`
//! call.
//!
//! GraphQL resolvers stay hand-written per resource (for now): the
//! field-shape varies, and async-graphql's dynamic API needs
//! concrete field names. 342-D can revisit that once more
//! resources reveal the pattern.

use std::str::FromStr;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json, Router,
};
use nauka_core::resource::{Datetime, Resource, ResourceOps, Scope};
use nauka_state::{RaftNode, Writer};
use serde::{de::DeserializeOwned, Serialize};

use crate::{Deps, NaukaApiError, Principal};

/// Verbs available to [`mount_crud`]. Mirrors
/// [`nauka_core::api::Verb`] — duplicating keeps `nauka-api` from
/// forcing re-exports on the macro crate.
#[derive(Debug, Clone, Copy)]
pub enum Verb {
    Create,
    Get,
    List,
    Update,
    Delete,
}

/// Mount the verbs in `verbs` onto `router`, using the path the
/// `#[resource]` macro registered for `R` in
/// [`nauka_core::api::ALL_API_RESOURCES`]. The descriptor is the
/// single source of truth — this keeps the OpenAPI spec and the
/// actual routes in lockstep by construction.
///
/// - `POST {path}` → create
/// - `GET {path}` → list
/// - `GET {path}/{{id}}` → get
/// - `PATCH {path}/{{id}}` → update
/// - `DELETE {path}/{{id}}` → delete (returns `204 No Content`)
///
/// Panics if the descriptor is missing — that would mean either the
/// resource's `#[resource]` attributes set `api_verbs = ""` (wrong
/// intent if we're calling `mount_crud`) or the link-time
/// distributed_slice is broken, and both are programming errors
/// worth surfacing loudly.
pub fn mount_crud<R>(mut router: Router<Deps>, verbs: &[Verb]) -> Router<Deps>
where
    R: ResourceOps + DeserializeOwned + Serialize + Send + Sync + 'static,
    <R as Resource>::Id: FromStr + Clone,
    <<R as Resource>::Id as FromStr>::Err: std::fmt::Display,
{
    let path_prefix: &'static str = nauka_core::api::api_resource(<R as Resource>::TABLE)
        .unwrap_or_else(|| {
            panic!(
                "mount_crud::<{}> called but no ApiResourceDescriptor \
                 for table `{}` in ALL_API_RESOURCES — either the \
                 resource opts out via `api_verbs = \"\"` or the \
                 distributed_slice is broken",
                std::any::type_name::<R>(),
                <R as Resource>::TABLE
            )
        })
        .path;
    // `axum::Router` allows the same path on multiple method-routers
    // only when merged through `.route(path, method_router)` — so we
    // build one method-router per path and attach the relevant verbs.
    // Empty method-routers default to responding 405, which is the
    // correct response for a resource that opts out of a verb.
    let mut root = axum::routing::MethodRouter::<Deps>::new();
    let mut item = axum::routing::MethodRouter::<Deps>::new();

    for v in verbs {
        match v {
            Verb::Create => root = root.post(create::<R>),
            Verb::List => root = root.get(list::<R>),
            Verb::Get => item = item.get(get_one::<R>),
            Verb::Update => item = item.patch(update::<R>),
            Verb::Delete => item = item.delete(delete_one::<R>),
        }
    }

    let item_path = format!("{path_prefix}/{{id}}");
    router = router.route(path_prefix, root);
    router = router.route(&item_path, item);
    router
}

// ---------- handlers ----------

async fn create<R>(
    State(deps): State<Deps>,
    _p: Principal,
    Json(mut body): Json<R>,
) -> Result<Json<R>, NaukaApiError>
where
    R: ResourceOps + DeserializeOwned + Serialize + Send + Sync + 'static,
{
    let now = Datetime::now();
    body.set_created_at(now);
    body.set_updated_at(now);
    body.set_version(0);
    write_with_scope::<R>(&deps, |w| Box::pin(async move { w.create(&body).await.map(|_| body) }))
        .await
        .map(Json)
}

async fn get_one<R>(
    State(deps): State<Deps>,
    _p: Principal,
    Path(id_str): Path<String>,
) -> Result<Json<R>, NaukaApiError>
where
    R: ResourceOps + DeserializeOwned + Serialize + Send + Sync + 'static,
    <R as Resource>::Id: FromStr,
    <<R as Resource>::Id as FromStr>::Err: std::fmt::Display,
{
    let id = parse_id::<R>(&id_str)?;
    match fetch_one::<R>(&deps, &id).await? {
        Some(row) => Ok(Json(row)),
        None => Err(NaukaApiError::NotFound(format!(
            "{}:{id_str}",
            <R as Resource>::TABLE
        ))),
    }
}

async fn list<R>(State(deps): State<Deps>, _p: Principal) -> Result<Json<Vec<R>>, NaukaApiError>
where
    R: ResourceOps + DeserializeOwned + Serialize + Send + Sync + 'static,
{
    let surql = <R as ResourceOps>::list_query();
    let rows: Vec<R> = deps.db.query_take(&surql).await?;
    Ok(Json(rows))
}

async fn update<R>(
    State(deps): State<Deps>,
    _p: Principal,
    Path(id_str): Path<String>,
    Json(mut body): Json<R>,
) -> Result<Json<R>, NaukaApiError>
where
    R: ResourceOps + DeserializeOwned + Serialize + Send + Sync + 'static,
    <R as Resource>::Id: FromStr + Clone,
    <<R as Resource>::Id as FromStr>::Err: std::fmt::Display,
{
    if body.id().to_string() != id_str {
        return Err(NaukaApiError::Validation(format!(
            "path id `{id_str}` does not match body id `{}`",
            body.id()
        )));
    }
    let id = parse_id::<R>(&id_str)?;
    let current = fetch_one::<R>(&deps, &id).await?.ok_or_else(|| {
        NaukaApiError::NotFound(format!("{}:{id_str}", <R as Resource>::TABLE))
    })?;
    body.set_created_at(*current.created_at());
    body.set_updated_at(Datetime::now());
    body.set_version(current.version() + 1);
    write_with_scope::<R>(&deps, |w| Box::pin(async move { w.update(&body).await.map(|_| body) }))
        .await
        .map(Json)
}

async fn delete_one<R>(
    State(deps): State<Deps>,
    _p: Principal,
    Path(id_str): Path<String>,
) -> Result<StatusCode, NaukaApiError>
where
    R: ResourceOps + DeserializeOwned + Serialize + Send + Sync + 'static,
    <R as Resource>::Id: FromStr,
    <<R as Resource>::Id as FromStr>::Err: std::fmt::Display,
{
    let id = parse_id::<R>(&id_str)?;
    match <R as Resource>::SCOPE {
        Scope::Cluster => {
            let raft = require_raft::<R>(&deps)?;
            Writer::new(&deps.db)
                .with_raft(raft)
                .delete::<R>(&id)
                .await?;
        }
        Scope::Local => {
            Writer::new(&deps.db).delete::<R>(&id).await?;
        }
    }
    Ok(StatusCode::NO_CONTENT)
}

// ---------- helpers ----------

fn parse_id<R>(id_str: &str) -> Result<<R as Resource>::Id, NaukaApiError>
where
    R: Resource,
    <R as Resource>::Id: FromStr,
    <<R as Resource>::Id as FromStr>::Err: std::fmt::Display,
{
    id_str.parse::<<R as Resource>::Id>().map_err(|e| {
        NaukaApiError::Validation(format!(
            "invalid id `{id_str}` for {}: {e}",
            <R as Resource>::TABLE
        ))
    })
}

/// Fetch a single resource by id — shared by `get_one` / `update`.
pub(crate) async fn fetch_one<R>(
    deps: &Deps,
    id: &<R as Resource>::Id,
) -> Result<Option<R>, NaukaApiError>
where
    R: ResourceOps + DeserializeOwned + Send + Sync + 'static,
{
    let surql = <R as ResourceOps>::get_query(id);
    let rows: Vec<R> = deps.db.query_take(&surql).await?;
    Ok(rows.into_iter().next())
}

fn require_raft<R: Resource>(deps: &Deps) -> Result<&RaftNode, NaukaApiError> {
    deps.raft.as_deref().ok_or_else(|| {
        NaukaApiError::Internal(format!(
            "{} is cluster-scoped — Deps needs a Raft handle to write",
            <R as Resource>::TABLE
        ))
    })
}

/// Route a write-producing closure through Raft for cluster-scoped
/// resources or straight to SurrealKV for local ones — same policy
/// `Writer::execute` enforces at the state layer, surfaced one level
/// up so the handler body stays linear.
async fn write_with_scope<R>(
    deps: &Deps,
    f: impl for<'a> FnOnce(
        Writer<'a>,
    )
        -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<R, nauka_state::StateError>> + Send + 'a>>,
) -> Result<R, NaukaApiError>
where
    R: Resource,
{
    match <R as Resource>::SCOPE {
        Scope::Cluster => {
            let raft = require_raft::<R>(deps)?;
            let writer = Writer::new(&deps.db).with_raft(raft);
            Ok(f(writer).await?)
        }
        Scope::Local => {
            let writer = Writer::new(&deps.db);
            Ok(f(writer).await?)
        }
    }
}
