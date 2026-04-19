//! REST + GraphQL handlers for `Org`.
//!
//! First IAM resource on the generated API surface (342-C1). Full
//! CRUD: `mount_crud::<Org>` drops every REST handler into one line.
//! GraphQL stays hand-written — the field list differs per struct
//! and async-graphql's dynamic API needs concrete `Field::new` calls.
//!
//! Auth / PERMISSIONS: `Org` has `scope_by = "self"`, so the
//! underlying SurrealDB table enforces per-record access via
//! `fn::iam::can` at query time. 342-C1's middleware still only
//! guarantees *an* authenticated principal — per-verb enforcement
//! piggybacks on the DB permission layer today, with application-
//! level can() wiring tracked as an IAM follow-up.

use async_graphql::dynamic::{Field, FieldFuture, FieldValue, InputObject, InputValue, Object, TypeRef};
use async_graphql::Value;
use axum::Router;
use nauka_core::resource::{Datetime, Ref};
use nauka_iam::{Org, User};
use nauka_state::Writer;
use serde::Deserialize;

use crate::crud::{self, Verb};
use crate::{Deps, NaukaApiError};

// ---------- REST ----------

pub fn routes() -> Router<Deps> {
    crud::mount_crud::<Org>(
        Router::new(),
        "/v1/orgs",
        &[Verb::Create, Verb::Get, Verb::List, Verb::Update, Verb::Delete],
    )
}

// ---------- GraphQL ----------

pub fn register_gql(
    builder: async_graphql::dynamic::SchemaBuilder,
    query: Object,
    mutation: Object,
) -> (async_graphql::dynamic::SchemaBuilder, Object, Object) {
    let org_object = Object::new("Org")
        .field(scalar_field("slug", TypeRef::STRING))
        .field(scalar_field("displayName", TypeRef::STRING))
        .field(scalar_field("owner", TypeRef::STRING))
        .field(scalar_field("createdAt", TypeRef::STRING))
        .field(scalar_field("updatedAt", TypeRef::STRING))
        .field(scalar_field("version", TypeRef::STRING));

    let org_input = InputObject::new("OrgInput")
        .field(InputValue::new("slug", TypeRef::named_nn(TypeRef::STRING)))
        .field(InputValue::new("displayName", TypeRef::named_nn(TypeRef::STRING)))
        .field(InputValue::new("owner", TypeRef::named_nn(TypeRef::STRING)));

    let query_field = Field::new("org", TypeRef::named("Org"), |ctx| {
        FieldFuture::new(async move {
            let deps = ctx.data::<Deps>()?;
            let id: String = ctx.args.try_get("id")?.string()?.to_string();
            let row = crud::fetch_one::<Org>(deps, &id)
                .await
                .map_err(|e| async_graphql::Error::new(e.to_string()))?;
            Ok(row.map(FieldValue::owned_any))
        })
    })
    .argument(InputValue::new("id", TypeRef::named_nn(TypeRef::STRING)));

    let list_field = Field::new("orgs", TypeRef::named_list_nn("Org"), |ctx| {
        FieldFuture::new(async move {
            let deps = ctx.data::<Deps>()?;
            let surql = <Org as nauka_core::resource::ResourceOps>::list_query();
            let rows: Vec<Org> = deps
                .db
                .query_take(&surql)
                .await
                .map_err(|e| async_graphql::Error::new(e.to_string()))?;
            Ok(Some(FieldValue::list(
                rows.into_iter().map(FieldValue::owned_any),
            )))
        })
    });

    let create_mut = Field::new("createOrg", TypeRef::named_nn("Org"), |ctx| {
        FieldFuture::new(async move {
            let deps = ctx.data::<Deps>()?;
            let input = ctx.args.try_get("input")?;
            let raw: serde_json::Value = input.deserialize()?;
            let body = decode_input(raw)
                .map_err(|e| async_graphql::Error::new(format!("invalid input: {e}")))?;
            let org = create_via_gql(deps, body)
                .await
                .map_err(|e| async_graphql::Error::new(e.to_string()))?;
            Ok(Some(FieldValue::owned_any(org)))
        })
    })
    .argument(InputValue::new("input", TypeRef::named_nn("OrgInput")));

    (
        builder.register(org_object).register(org_input),
        query.field(query_field).field(list_field),
        mutation.field(create_mut),
    )
}

fn scalar_field(name: &'static str, ty: &'static str) -> Field {
    Field::new(name, TypeRef::named(ty), move |ctx| {
        FieldFuture::new(async move {
            let parent = ctx.parent_value.try_downcast_ref::<Org>()?;
            Ok(Some(Value::String(field_string(parent, name))))
        })
    })
}

fn field_string(o: &Org, name: &str) -> String {
    match name {
        "slug" => o.slug.clone(),
        "displayName" => o.display_name.clone(),
        "owner" => o.owner.id().to_string(),
        "createdAt" => o.created_at.to_string(),
        "updatedAt" => o.updated_at.to_string(),
        "version" => o.version.to_string(),
        _ => String::new(),
    }
}

#[derive(Debug, Deserialize)]
struct OrgInput {
    slug: String,
    #[serde(rename = "displayName")]
    display_name: String,
    /// Owner is a `Ref<User>`; the GraphQL input accepts the bare
    /// email (= User's `#[id]`) and we wrap it here to keep the
    /// client shape flat.
    owner: String,
}

fn decode_input(raw: serde_json::Value) -> Result<Org, serde_json::Error> {
    let input: OrgInput = serde_json::from_value(raw)?;
    let now = Datetime::now();
    Ok(Org {
        slug: input.slug,
        display_name: input.display_name,
        owner: Ref::<User>::new(input.owner),
        created_at: now,
        updated_at: now,
        version: 0,
    })
}

async fn create_via_gql(deps: &Deps, mut body: Org) -> Result<Org, NaukaApiError> {
    let raft = deps.raft.as_deref().ok_or_else(|| {
        NaukaApiError::Internal(
            "org is cluster-scoped — Deps needs a Raft handle to write".into(),
        )
    })?;
    let now = Datetime::now();
    body.created_at = now;
    body.updated_at = now;
    body.version = 0;
    Writer::new(&deps.db).with_raft(raft).create(&body).await?;
    Ok(body)
}
