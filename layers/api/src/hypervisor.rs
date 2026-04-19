//! REST + GraphQL handlers for `Hypervisor`.
//!
//! 342-A wires this one resource end-to-end as the proof-of-concept
//! for the wider #342 epic. 342-B extracts the generic-over-R
//! handlers once `Mesh` lands, and 342-C rolls the pattern out to
//! the IAM resources. The current shape is deliberately concrete so
//! the first pass is easy to audit.

use async_graphql::dynamic::{Field, FieldFuture, FieldValue, InputObject, InputValue, Object, TypeRef};
use async_graphql::Value;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{delete as axum_delete, get, patch, post},
    Json, Router,
};
use nauka_core::resource::{Datetime, ResourceOps};
use nauka_hypervisor::Hypervisor;
use nauka_state::Writer;
use serde::Deserialize;
use tracing::instrument;

use crate::{Deps, NaukaApiError, Principal};

// ---------- REST ----------

pub fn routes() -> Router<Deps> {
    Router::new()
        .route("/v1/hypervisors", post(create).get(list))
        .route("/v1/hypervisors/{id}", get(get_by_id))
        .route("/v1/hypervisors/{id}", patch(update))
        .route("/v1/hypervisors/{id}", axum_delete(delete))
}

#[instrument(name = "api.hypervisor.create", skip_all, fields(id = %body.public_key))]
async fn create(
    State(deps): State<Deps>,
    _p: Principal,
    Json(mut body): Json<Hypervisor>,
) -> Result<Json<Hypervisor>, NaukaApiError> {
    let raft = require_raft(&deps)?;
    let now = Datetime::now();
    body.created_at = now;
    body.updated_at = now;
    body.version = 0;
    Writer::new(&deps.db).with_raft(raft).create(&body).await?;
    Ok(Json(body))
}

#[instrument(name = "api.hypervisor.get", skip_all, fields(id = %id))]
async fn get_by_id(
    State(deps): State<Deps>,
    _p: Principal,
    Path(id): Path<String>,
) -> Result<Json<Hypervisor>, NaukaApiError> {
    let rows = fetch_one(&deps, &id).await?;
    match rows.into_iter().next() {
        Some(row) => Ok(Json(row)),
        None => Err(NaukaApiError::NotFound(format!("hypervisor:{id}"))),
    }
}

#[instrument(name = "api.hypervisor.list", skip_all)]
async fn list(
    State(deps): State<Deps>,
    _p: Principal,
) -> Result<Json<Vec<Hypervisor>>, NaukaApiError> {
    let surql = Hypervisor::list_query();
    let rows: Vec<Hypervisor> = deps.db.query_take(&surql).await?;
    Ok(Json(rows))
}

#[instrument(name = "api.hypervisor.update", skip_all, fields(id = %id))]
async fn update(
    State(deps): State<Deps>,
    _p: Principal,
    Path(id): Path<String>,
    Json(mut body): Json<Hypervisor>,
) -> Result<Json<Hypervisor>, NaukaApiError> {
    let raft = require_raft(&deps)?;
    // The path id is the source of truth — the body's id field is
    // ignored, which matches REST convention for PATCH /resource/:id.
    body.public_key = id;
    let current = fetch_one(&deps, &body.public_key)
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| NaukaApiError::NotFound(format!("hypervisor:{}", body.public_key)))?;
    body.created_at = current.created_at;
    body.updated_at = Datetime::now();
    body.version = current.version + 1;
    Writer::new(&deps.db).with_raft(raft).update(&body).await?;
    Ok(Json(body))
}

#[instrument(name = "api.hypervisor.delete", skip_all, fields(id = %id))]
async fn delete(
    State(deps): State<Deps>,
    _p: Principal,
    Path(id): Path<String>,
) -> Result<StatusCode, NaukaApiError> {
    let raft = require_raft(&deps)?;
    Writer::new(&deps.db)
        .with_raft(raft)
        .delete::<Hypervisor>(&id)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

fn require_raft(deps: &Deps) -> Result<&nauka_state::RaftNode, NaukaApiError> {
    deps.raft.as_deref().ok_or_else(|| {
        NaukaApiError::Internal(
            "hypervisor is cluster-scoped — Deps needs a Raft handle to write".into(),
        )
    })
}

async fn fetch_one(deps: &Deps, id: &str) -> Result<Vec<Hypervisor>, NaukaApiError> {
    let surql = Hypervisor::get_query(&id.to_string());
    let rows: Vec<Hypervisor> = deps.db.query_take(&surql).await?;
    Ok(rows)
}

// ---------- GraphQL ----------

/// Register Hypervisor's GraphQL surface — object type, input type,
/// one query root field (`hypervisor(id: ID!)`), one mutation root
/// field (`createHypervisor(input: …)`). Invoked from
/// [`crate::graphql::build_schema`] when the distributed slice
/// reports `table = "hypervisor"`.
pub fn register_gql(
    builder: async_graphql::dynamic::SchemaBuilder,
    query: Object,
    mutation: Object,
) -> (async_graphql::dynamic::SchemaBuilder, Object, Object) {
    let hypervisor_object = Object::new("Hypervisor")
        .field(scalar_field("publicKey", TypeRef::STRING))
        .field(scalar_field("nodeId", TypeRef::STRING)) // serialise u64 as string to keep the response JSON-safe
        .field(scalar_field("raftAddr", TypeRef::STRING))
        .field(scalar_field("address", TypeRef::STRING))
        .field(scalar_field("endpoint", TypeRef::STRING))
        .field(scalar_field("allowedIps", TypeRef::STRING)) // join with comma for 342-A — refined in 342-B
        .field(scalar_field("keepalive", TypeRef::STRING))
        .field(scalar_field("createdAt", TypeRef::STRING))
        .field(scalar_field("updatedAt", TypeRef::STRING))
        .field(scalar_field("version", TypeRef::STRING));

    let hypervisor_input = InputObject::new("HypervisorInput")
        .field(InputValue::new("publicKey", TypeRef::named_nn(TypeRef::STRING)))
        .field(InputValue::new("nodeId", TypeRef::named_nn(TypeRef::STRING)))
        .field(InputValue::new("raftAddr", TypeRef::named_nn(TypeRef::STRING)))
        .field(InputValue::new("address", TypeRef::named_nn(TypeRef::STRING)))
        .field(InputValue::new("endpoint", TypeRef::named(TypeRef::STRING)))
        .field(InputValue::new("allowedIps", TypeRef::named(TypeRef::STRING)))
        .field(InputValue::new("keepalive", TypeRef::named(TypeRef::STRING)));

    let query_field = Field::new(
        "hypervisor",
        TypeRef::named("Hypervisor"),
        |ctx| {
            FieldFuture::new(async move {
                let deps = ctx.data::<Deps>()?;
                let id: String = ctx.args.try_get("id")?.string()?.to_string();
                let rows = fetch_one(deps, &id)
                    .await
                    .map_err(|e| async_graphql::Error::new(e.to_string()))?;
                match rows.into_iter().next() {
                    Some(hv) => Ok(Some(FieldValue::owned_any(hv))),
                    None => Ok(None),
                }
            })
        },
    )
    .argument(InputValue::new("id", TypeRef::named_nn(TypeRef::STRING)));

    let create_mut = Field::new(
        "createHypervisor",
        TypeRef::named_nn("Hypervisor"),
        |ctx| {
            FieldFuture::new(async move {
                let deps = ctx.data::<Deps>()?;
                let input = ctx.args.try_get("input")?;
                let raw: serde_json::Value = input.deserialize()?;
                let body = decode_input(raw)
                    .map_err(|e| async_graphql::Error::new(format!("invalid input: {e}")))?;
                let hv = create_via_gql(deps, body)
                    .await
                    .map_err(|e| async_graphql::Error::new(e.to_string()))?;
                Ok(Some(FieldValue::owned_any(hv)))
            })
        },
    )
    .argument(InputValue::new(
        "input",
        TypeRef::named_nn("HypervisorInput"),
    ));

    (
        builder
            .register(hypervisor_object)
            .register(hypervisor_input),
        query.field(query_field),
        mutation.field(create_mut),
    )
}

/// Object-type field that reads a property off the parent
/// `FieldValue::owned_any::<Hypervisor>`. Every scalar gets stringified
/// so the 342-A schema doesn't have to juggle GraphQL's 32-bit Int
/// limit on u64 identifiers — we'll introduce proper scalar types in
/// 342-B once the test coverage is broader.
fn scalar_field(name: &'static str, ty: &'static str) -> Field {
    Field::new(name, TypeRef::named(ty), move |ctx| {
        FieldFuture::new(async move {
            let parent = ctx.parent_value.try_downcast_ref::<Hypervisor>()?;
            Ok(Some(Value::String(field_string(parent, name))))
        })
    })
}

fn field_string(hv: &Hypervisor, name: &str) -> String {
    match name {
        "publicKey" => hv.public_key.clone(),
        "nodeId" => hv.node_id.to_string(),
        "raftAddr" => hv.raft_addr.clone(),
        "address" => hv.address.clone(),
        "endpoint" => hv.endpoint.clone().unwrap_or_default(),
        "allowedIps" => hv.allowed_ips.join(","),
        "keepalive" => hv
            .keepalive
            .map(|k| k.to_string())
            .unwrap_or_default(),
        "createdAt" => hv.created_at.to_string(),
        "updatedAt" => hv.updated_at.to_string(),
        "version" => hv.version.to_string(),
        _ => String::new(),
    }
}

#[derive(Debug, Deserialize)]
struct HypervisorInput {
    #[serde(rename = "publicKey")]
    public_key: String,
    #[serde(rename = "nodeId")]
    node_id: String,
    #[serde(rename = "raftAddr")]
    raft_addr: String,
    address: String,
    endpoint: Option<String>,
    #[serde(rename = "allowedIps", default)]
    allowed_ips: Option<String>,
    keepalive: Option<String>,
}

fn decode_input(raw: serde_json::Value) -> Result<Hypervisor, serde_json::Error> {
    let input: HypervisorInput = serde_json::from_value(raw)?;
    Ok(Hypervisor {
        public_key: input.public_key,
        node_id: input.node_id.parse().unwrap_or(0),
        raft_addr: input.raft_addr,
        address: input.address,
        endpoint: input.endpoint,
        allowed_ips: input
            .allowed_ips
            .map(|s| s.split(',').map(str::trim).map(String::from).collect())
            .unwrap_or_default(),
        keepalive: input.keepalive.and_then(|s| s.parse().ok()),
        created_at: Datetime::now(),
        updated_at: Datetime::now(),
        version: 0,
    })
}

async fn create_via_gql(deps: &Deps, mut body: Hypervisor) -> Result<Hypervisor, NaukaApiError> {
    let raft = require_raft(deps)?;
    let now = Datetime::now();
    body.created_at = now;
    body.updated_at = now;
    body.version = 0;
    Writer::new(&deps.db).with_raft(raft).create(&body).await?;
    Ok(body)
}
