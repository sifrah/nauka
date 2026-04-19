//! REST + GraphQL handlers for `Hypervisor`.
//!
//! REST now flows through the generic [`crate::crud::mount_crud`] —
//! the create/get/list/update/delete bodies the 342-A commit
//! hand-rolled here are gone, replaced by one `mount_crud::<Hypervisor>`
//! call. GraphQL resolvers remain hand-written per resource because
//! async-graphql's dynamic API needs concrete field-name → field-value
//! wiring that the handler side does not.

use async_graphql::dynamic::{Field, FieldFuture, FieldValue, InputObject, InputValue, Object, TypeRef};
use async_graphql::Value;
use axum::Router;
use nauka_core::resource::Datetime;
use nauka_hypervisor::Hypervisor;
use nauka_state::Writer;
use serde::Deserialize;

use crate::crud::{self, Verb};
use crate::{Deps, NaukaApiError};

// ---------- REST ----------

pub fn routes() -> Router<Deps> {
    crud::mount_crud::<Hypervisor>(
        Router::new(),
        "/v1/hypervisors",
        &[Verb::Create, Verb::Get, Verb::List, Verb::Update, Verb::Delete],
    )
}

// ---------- GraphQL ----------

pub fn register_gql(
    builder: async_graphql::dynamic::SchemaBuilder,
    query: Object,
    mutation: Object,
) -> (async_graphql::dynamic::SchemaBuilder, Object, Object) {
    let hypervisor_object = Object::new("Hypervisor")
        .field(scalar_field("publicKey", TypeRef::STRING))
        .field(scalar_field("nodeId", TypeRef::STRING))
        .field(scalar_field("raftAddr", TypeRef::STRING))
        .field(scalar_field("address", TypeRef::STRING))
        .field(scalar_field("endpoint", TypeRef::STRING))
        .field(scalar_field("allowedIps", TypeRef::STRING))
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

    let query_field = Field::new("hypervisor", TypeRef::named("Hypervisor"), |ctx| {
        FieldFuture::new(async move {
            let deps = ctx.data::<Deps>()?;
            let id: String = ctx.args.try_get("id")?.string()?.to_string();
            let row = crud::fetch_one::<Hypervisor>(deps, &id)
                .await
                .map_err(|e| async_graphql::Error::new(e.to_string()))?;
            Ok(row.map(FieldValue::owned_any))
        })
    })
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
    .argument(InputValue::new("input", TypeRef::named_nn("HypervisorInput")));

    (
        builder
            .register(hypervisor_object)
            .register(hypervisor_input),
        query.field(query_field),
        mutation.field(create_mut),
    )
}

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
        "keepalive" => hv.keepalive.map(|k| k.to_string()).unwrap_or_default(),
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
    let raft = deps.raft.as_deref().ok_or_else(|| {
        NaukaApiError::Internal(
            "hypervisor is cluster-scoped — Deps needs a Raft handle to write".into(),
        )
    })?;
    let now = Datetime::now();
    body.created_at = now;
    body.updated_at = now;
    body.version = 0;
    Writer::new(&deps.db).with_raft(raft).create(&body).await?;
    Ok(body)
}

