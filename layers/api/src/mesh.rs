//! REST + GraphQL handlers for `Mesh` — read-only surface.
//!
//! Mesh creation is wired to `nauka hypervisor init`, not to an HTTP
//! route: minting a mesh requires WireGuard interface + systemd
//! setup that can't happen from a remote call. The `api_verbs =
//! "get, list"` on `#[resource]` enforces this at the macro level,
//! so no create/update/delete handlers live here either.
//!
//! Encrypted fields (`private_key`, `ca_key`, `tls_key`,
//! `peering_pin`) carry `#[serde(skip)]` on the struct itself
//! (342-B) so the axum JSON layer cannot serialise them even by
//! accident — this file assumes that masking and does not re-check.

use async_graphql::dynamic::{Field, FieldFuture, FieldValue, InputValue, Object, TypeRef};
use async_graphql::Value;
use axum::Router;
use nauka_core::resource::ResourceOps;
// `Mesh` in `nauka_hypervisor::mesh::mod` is the runtime struct;
// the DB resource is re-exported as `MeshRecord`. Alias locally so
// the handler code matches the symmetric Hypervisor shape above.
use nauka_hypervisor::MeshRecord as Mesh;

use crate::crud::{self, Verb};
use crate::Deps;

// ---------- REST ----------

pub fn routes() -> Router<Deps> {
    crud::mount_crud::<Mesh>(Router::new(), "/v1/meshes", &[Verb::Get, Verb::List])
}

// ---------- GraphQL ----------

pub fn register_gql(
    builder: async_graphql::dynamic::SchemaBuilder,
    query: Object,
    mutation: Object,
) -> (async_graphql::dynamic::SchemaBuilder, Object, Object) {
    let mesh_object = Object::new("Mesh")
        .field(scalar_field("meshId", TypeRef::STRING))
        .field(scalar_field("interfaceName", TypeRef::STRING))
        .field(scalar_field("listenPort", TypeRef::STRING))
        .field(scalar_field("caCert", TypeRef::STRING))
        .field(scalar_field("tlsCert", TypeRef::STRING))
        .field(scalar_field("createdAt", TypeRef::STRING))
        .field(scalar_field("updatedAt", TypeRef::STRING))
        .field(scalar_field("version", TypeRef::STRING));

    let query_field = Field::new("mesh", TypeRef::named("Mesh"), |ctx| {
        FieldFuture::new(async move {
            let deps = ctx.data::<Deps>()?;
            let id: String = ctx.args.try_get("id")?.string()?.to_string();
            let row = crud::fetch_one::<Mesh>(deps, &id)
                .await
                .map_err(|e| async_graphql::Error::new(e.to_string()))?;
            Ok(row.map(FieldValue::owned_any))
        })
    })
    .argument(InputValue::new("id", TypeRef::named_nn(TypeRef::STRING)));

    let list_field = Field::new("meshes", TypeRef::named_list_nn("Mesh"), |ctx| {
        FieldFuture::new(async move {
            let deps = ctx.data::<Deps>()?;
            let surql = Mesh::list_query();
            let rows: Vec<Mesh> = deps
                .db
                .query_take(&surql)
                .await
                .map_err(|e| async_graphql::Error::new(e.to_string()))?;
            Ok(Some(FieldValue::list(
                rows.into_iter().map(FieldValue::owned_any),
            )))
        })
    });

    (
        builder.register(mesh_object),
        query.field(query_field).field(list_field),
        // Mesh exposes no mutations (api_verbs = "get, list"), so
        // we return the mutation root unchanged.
        mutation,
    )
}

fn scalar_field(name: &'static str, ty: &'static str) -> Field {
    Field::new(name, TypeRef::named(ty), move |ctx| {
        FieldFuture::new(async move {
            let parent = ctx.parent_value.try_downcast_ref::<Mesh>()?;
            Ok(Some(Value::String(field_string(parent, name))))
        })
    })
}

fn field_string(m: &Mesh, name: &str) -> String {
    match name {
        "meshId" => m.mesh_id.clone(),
        "interfaceName" => m.interface_name.clone(),
        "listenPort" => m.listen_port.to_string(),
        "caCert" => m.ca_cert.clone().unwrap_or_default(),
        "tlsCert" => m.tls_cert.clone().unwrap_or_default(),
        "createdAt" => m.created_at.to_string(),
        "updatedAt" => m.updated_at.to_string(),
        "version" => m.version.to_string(),
        _ => String::new(),
    }
}
