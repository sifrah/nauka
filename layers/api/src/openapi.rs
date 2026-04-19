//! Runtime OpenAPI 3.1 document built from
//! [`nauka_core::api::ALL_API_RESOURCES`].
//!
//! 342-D1 generates the JSON at server startup rather than committing
//! a static `openapi.json`. The tradeoff: we lose the git-diff-as-
//! drift-detection trick the issue suggested, but every resource the
//! `#[resource]` macro registers automatically appears in the spec
//! — no second place to update when a verb list changes.
//!
//! Schemas are deliberately **shallow** in this first pass: each
//! resource's request / response body is `{type: "object"}` rather
//! than a full field catalogue. Enriching the schema to carry
//! field-level types belongs in a follow-up once the macro grows a
//! lightweight type-reflection slot — it's a strictly additive change
//! and the Scalar UI already renders method / path / status overlays
//! without it.

use nauka_core::api::{ApiResourceDescriptor, Verb, ALL_API_RESOURCES};
use serde_json::{json, Value};

/// Build the full OpenAPI document.
pub fn build_openapi() -> Value {
    let mut paths = serde_json::Map::new();
    let mut schemas = serde_json::Map::new();

    for desc in ALL_API_RESOURCES.iter().copied() {
        add_resource(&mut paths, &mut schemas, desc);
    }

    json!({
        "openapi": "3.1.0",
        "info": {
            "title": "Nauka",
            "version": env!("CARGO_PKG_VERSION"),
            "description": "Cloud control-plane API generated from the #[resource] contract. \
                            See https://github.com/sifrah/nauka#readme for usage."
        },
        "servers": [
            { "url": "https://localhost:4000", "description": "Local daemon (TLS lands in 342-D2)" }
        ],
        "components": {
            "schemas": schemas,
            "securitySchemes": {
                "bearerAuth": {
                    "type": "http",
                    "scheme": "bearer",
                    "bearerFormat": "JWT"
                }
            }
        },
        "security": [ { "bearerAuth": [] } ],
        "paths": paths,
    })
}

fn add_resource(
    paths: &mut serde_json::Map<String, Value>,
    schemas: &mut serde_json::Map<String, Value>,
    desc: &'static ApiResourceDescriptor,
) {
    let pascal = snake_to_pascal(desc.table);
    let tag = desc.table.to_string();

    // Each resource gets one shallow schema object. Future commits
    // can deepen this as the macro exposes field metadata.
    schemas.insert(
        pascal.clone(),
        json!({
            "type": "object",
            "x-nauka-table": desc.table,
        }),
    );

    let has_create = desc.verbs.contains(&Verb::Create);
    let has_list = desc.verbs.contains(&Verb::List);
    let has_get = desc.verbs.contains(&Verb::Get);
    let has_update = desc.verbs.contains(&Verb::Update);
    let has_delete = desc.verbs.contains(&Verb::Delete);

    // ---- Collection path (/v1/…) ----
    if has_create || has_list {
        let mut ops = serde_json::Map::new();
        if has_create {
            ops.insert(
                "post".to_string(),
                json!({
                    "tags": [tag],
                    "summary": format!("Create a {pascal}"),
                    "requestBody": {
                        "required": true,
                        "content": { "application/json": { "schema": ref_schema(&pascal) } }
                    },
                    "responses": default_responses(&pascal, "200"),
                }),
            );
        }
        if has_list {
            ops.insert(
                "get".to_string(),
                json!({
                    "tags": [tag],
                    "summary": format!("List every {pascal}"),
                    "responses": {
                        "200": {
                            "description": "OK",
                            "content": { "application/json": {
                                "schema": { "type": "array", "items": ref_schema(&pascal) }
                            } }
                        },
                        "401": unauth_response(),
                    }
                }),
            );
        }
        paths.insert(desc.path.to_string(), Value::Object(ops));
    }

    // ---- Item path (/v1/…/{id}) ----
    if has_get || has_update || has_delete {
        let mut ops = serde_json::Map::new();
        let id_param = json!({
            "name": "id",
            "in": "path",
            "required": true,
            "schema": { "type": "string" }
        });
        if has_get {
            ops.insert(
                "get".to_string(),
                json!({
                    "tags": [tag],
                    "summary": format!("Fetch a {pascal} by id"),
                    "parameters": [id_param.clone()],
                    "responses": default_responses(&pascal, "200"),
                }),
            );
        }
        if has_update {
            ops.insert(
                "patch".to_string(),
                json!({
                    "tags": [tag],
                    "summary": format!("Update a {pascal}"),
                    "parameters": [id_param.clone()],
                    "requestBody": {
                        "required": true,
                        "content": { "application/json": { "schema": ref_schema(&pascal) } }
                    },
                    "responses": default_responses(&pascal, "200"),
                }),
            );
        }
        if has_delete {
            ops.insert(
                "delete".to_string(),
                json!({
                    "tags": [tag],
                    "summary": format!("Delete a {pascal}"),
                    "parameters": [id_param],
                    "responses": {
                        "204": { "description": "Deleted" },
                        "401": unauth_response(),
                        "404": not_found_response(),
                    }
                }),
            );
        }
        paths.insert(format!("{}/{{id}}", desc.path), Value::Object(ops));
    }

    // ---- Custom action paths (POST …/{id}/{action}) ----
    for action in desc.custom_actions {
        paths.insert(
            format!("{}/{{id}}/{action}", desc.path),
            json!({
                "post": {
                    "tags": [tag],
                    "summary": format!("{action} — {pascal} custom action"),
                    "parameters": [{
                        "name": "id",
                        "in": "path",
                        "required": true,
                        "schema": { "type": "string" }
                    }],
                    "responses": default_responses(&pascal, "200"),
                }
            }),
        );
    }
}

fn ref_schema(pascal: &str) -> Value {
    json!({ "$ref": format!("#/components/schemas/{pascal}") })
}

fn default_responses(pascal: &str, ok_status: &str) -> Value {
    json!({
        ok_status: {
            "description": "OK",
            "content": { "application/json": { "schema": ref_schema(pascal) } }
        },
        "401": unauth_response(),
        "404": not_found_response(),
        "422": validation_response(),
    })
}

fn unauth_response() -> Value {
    json!({
        "description": "Missing or invalid Bearer token",
        "content": { "application/json": { "schema": { "type": "object" } } }
    })
}

fn not_found_response() -> Value {
    json!({
        "description": "Resource not found",
        "content": { "application/json": { "schema": { "type": "object" } } }
    })
}

fn validation_response() -> Value {
    json!({
        "description": "Validation failure (bad id / shape)",
        "content": { "application/json": { "schema": { "type": "object" } } }
    })
}

/// `hypervisor` → `Hypervisor`, `api_token` → `ApiToken`. Duplicated
/// from `nauka_core::resource::pascal_to_snake`'s inverse.
fn snake_to_pascal(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut upper_next = true;
    for c in s.chars() {
        if c == '_' {
            upper_next = true;
        } else if upper_next {
            out.extend(c.to_uppercase());
            upper_next = false;
        } else {
            out.push(c);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snake_to_pascal_round_trips_the_common_cases() {
        assert_eq!(snake_to_pascal("hypervisor"), "Hypervisor");
        assert_eq!(snake_to_pascal("api_token"), "ApiToken");
        assert_eq!(snake_to_pascal("role_binding"), "RoleBinding");
        assert_eq!(
            snake_to_pascal("password_reset_token"),
            "PasswordResetToken"
        );
    }
}
