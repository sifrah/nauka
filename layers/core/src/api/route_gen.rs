//! Auto-generate axum routes from ResourceDef.
//!
//! Fixes:
//! - #11: GET and DELETE on same /{id} path use method routing (not separate routes)
//! - #10: Scoped routes supported via parent refs
//! - #3: Pagination via query params on list endpoints

use axum::extract::{Json, Path, Query};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::Router;
use std::collections::HashMap;
use std::sync::Arc;

use super::error_response::ApiError;
use crate::error::NaukaError;
use crate::resource::validation;
use crate::resource::{
    OperationRequest, OperationResponse, OperationSemantics, ResourceRegistration, ScopeValues,
};

/// Build an axum Router from resource registrations (including children).
///
/// Flattens the registration tree and generates routes for each resource.
/// Children inherit their parent scope through their ResourceDef parents field.
pub fn build_router(registrations: Vec<ResourceRegistration>, prefix: &str) -> Router {
    // Flatten tree into a list of Arc<ResourceRegistration>
    let mut all = Vec::new();
    for reg in registrations {
        flatten_registrations(reg, &mut all);
    }

    let mut router = Router::new();
    for reg in &all {
        router = add_resource_routes(router, reg, prefix);
    }
    router
}

/// Flatten a registration tree into a flat list.
fn flatten_registrations(reg: ResourceRegistration, out: &mut Vec<Arc<ResourceRegistration>>) {
    let ResourceRegistration {
        def,
        handler,
        children,
    } = reg;
    for child in children {
        flatten_registrations(child, out);
    }
    out.push(Arc::new(ResourceRegistration {
        def,
        handler,
        children: vec![],
    }));
}

/// Add routes for a single (flattened) resource.
fn add_resource_routes(
    mut router: Router,
    reg: &Arc<ResourceRegistration>,
    prefix: &str,
) -> Router {
    let plural = reg.def.identity.plural;
    let parents = &reg.def.scope.parents;
    let base = build_base_path(prefix, plural, parents);
    let has_parents = !parents.is_empty();

    // Collect parent param names for scope extraction
    let parent_kinds: Vec<String> = parents.iter().map(|p| format!("{}_id", p.kind)).collect();

    // ── Collection routes (list + create on /base) ──

    for op in &reg.def.operations {
        let r = Arc::clone(reg);
        let op_name = op.name.to_string();
        let pkinds = parent_kinds.clone();

        match &op.semantics {
            OperationSemantics::List => {
                if has_parents {
                    let handler = get(
                        move |Path(path_params): Path<HashMap<String, String>>,
                              Query(query): Query<HashMap<String, String>>| {
                            let r = Arc::clone(&r);
                            let op = op_name.clone();
                            let pk = pkinds.clone();
                            async move {
                                let scope = extract_scope(&path_params, &pk);
                                handle_scoped(&r, &op, None, query, scope).await
                            }
                        },
                    );
                    router = router.route(&base, handler);
                } else {
                    let handler = get(move |Query(query): Query<HashMap<String, String>>| {
                        let r = Arc::clone(&r);
                        let op = op_name.clone();
                        async move { handle_scoped(&r, &op, None, query, ScopeValues::default()).await }
                    });
                    router = router.route(&base, handler);
                }
            }
            OperationSemantics::Create => {
                if has_parents {
                    let handler = post(
                        move |Path(path_params): Path<HashMap<String, String>>,
                              Json(body): Json<HashMap<String, String>>| {
                            let r = Arc::clone(&r);
                            let op = op_name.clone();
                            let pk = pkinds.clone();
                            async move {
                                let scope = extract_scope(&path_params, &pk);
                                let name = body.get("name").cloned();
                                handle_scoped(&r, &op, name, body, scope).await
                            }
                        },
                    );
                    router = router.route(&base, handler);
                } else {
                    let handler = post(move |Json(body): Json<HashMap<String, String>>| {
                        let r = Arc::clone(&r);
                        let op = op_name.clone();
                        async move {
                            let name = body.get("name").cloned();
                            handle_scoped(&r, &op, name, body, ScopeValues::default()).await
                        }
                    });
                    router = router.route(&base, handler);
                }
            }
            _ => {}
        }
    }

    // ── Instance routes (get + delete on /base/{id}) ──

    let instance_path = format!("{base}/{{id}}");

    for op in &reg.def.operations {
        let r = Arc::clone(reg);
        let op_name = op.name.to_string();
        let pkinds = parent_kinds.clone();

        match &op.semantics {
            OperationSemantics::Get => {
                if has_parents {
                    let handler = get(move |Path(path_params): Path<HashMap<String, String>>| {
                        let r = Arc::clone(&r);
                        let op = op_name.clone();
                        let pk = pkinds.clone();
                        async move {
                            let scope = extract_scope(&path_params, &pk);
                            let id = path_params.get("id").cloned();
                            handle_scoped(&r, &op, id, HashMap::new(), scope).await
                        }
                    });
                    router = router.route(&instance_path, handler);
                } else {
                    let handler = get(move |Path(id): Path<String>| {
                        let r = Arc::clone(&r);
                        let op = op_name.clone();
                        async move {
                            handle_scoped(&r, &op, Some(id), HashMap::new(), ScopeValues::default())
                                .await
                        }
                    });
                    router = router.route(&instance_path, handler);
                }
            }
            OperationSemantics::Delete => {
                if has_parents {
                    let handler = axum::routing::delete(
                        move |Path(path_params): Path<HashMap<String, String>>| {
                            let r = Arc::clone(&r);
                            let op = op_name.clone();
                            let pk = pkinds.clone();
                            async move {
                                let scope = extract_scope(&path_params, &pk);
                                let id = path_params.get("id").cloned();
                                handle_scoped(&r, &op, id, HashMap::new(), scope).await
                            }
                        },
                    );
                    router = router.route(&instance_path, handler);
                } else {
                    let handler = axum::routing::delete(move |Path(id): Path<String>| {
                        let r = Arc::clone(&r);
                        let op = op_name.clone();
                        async move {
                            handle_scoped(&r, &op, Some(id), HashMap::new(), ScopeValues::default())
                                .await
                        }
                    });
                    router = router.route(&instance_path, handler);
                }
            }
            OperationSemantics::Update { .. } => {
                if has_parents {
                    let handler = axum::routing::patch(
                        move |Path(path_params): Path<HashMap<String, String>>,
                              Json(body): Json<HashMap<String, String>>| {
                            let r = Arc::clone(&r);
                            let op = op_name.clone();
                            let pk = pkinds.clone();
                            async move {
                                let scope = extract_scope(&path_params, &pk);
                                let id = path_params.get("id").cloned();
                                handle_scoped(&r, &op, id, body, scope).await
                            }
                        },
                    );
                    router = router.route(&instance_path, handler);
                } else {
                    let handler = axum::routing::patch(
                        move |Path(id): Path<String>, Json(body): Json<HashMap<String, String>>| {
                            let r = Arc::clone(&r);
                            let op = op_name.clone();
                            async move {
                                handle_scoped(&r, &op, Some(id), body, ScopeValues::default()).await
                            }
                        },
                    );
                    router = router.route(&instance_path, handler);
                }
            }
            OperationSemantics::Action => {
                let route = format!("{base}/{}", op.name);
                if has_parents {
                    let handler = post(
                        move |Path(path_params): Path<HashMap<String, String>>,
                              Json(body): Json<HashMap<String, String>>| {
                            let r = Arc::clone(&r);
                            let op = op_name.clone();
                            let pk = pkinds.clone();
                            async move {
                                let scope = extract_scope(&path_params, &pk);
                                let name = body.get("name").cloned();
                                handle_scoped(&r, &op, name, body, scope).await
                            }
                        },
                    );
                    router = router.route(&route, handler);
                } else {
                    let handler = post(move |Json(body): Json<HashMap<String, String>>| {
                        let r = Arc::clone(&r);
                        let op = op_name.clone();
                        async move {
                            let name = body.get("name").cloned();
                            handle_scoped(&r, &op, name, body, ScopeValues::default()).await
                        }
                    });
                    router = router.route(&route, handler);
                }
            }
            _ => {}
        }
    }

    router
}

/// Extract scope values from path params (e.g., org_id → org).
fn extract_scope(path_params: &HashMap<String, String>, parent_kinds: &[String]) -> ScopeValues {
    let mut scope = ScopeValues::default();
    for param in parent_kinds {
        if let Some(value) = path_params.get(param) {
            // param is "org_id" → scope key is "org"
            let key = param.strip_suffix("_id").unwrap_or(param);
            scope.set(key, value.clone());
        }
    }
    scope
}

/// #10: Build a route path incorporating scope parents.
///
/// No parents: `/cloud/v1/orgs`
/// With parents: `/cloud/v1/orgs/{org_id}/projects/{project_id}/environments`
fn build_base_path(prefix: &str, plural: &str, parents: &[crate::resource::ParentRef]) -> String {
    if parents.is_empty() {
        return format!("{prefix}/{plural}");
    }

    let mut path = prefix.to_string();
    for parent in parents {
        let parent_plural = format!("{}s", parent.kind);
        let param = format!("{{{}_id}}", parent.kind);
        path = format!("{path}/{parent_plural}/{param}");
    }
    format!("{path}/{plural}")
}

/// Classify an anyhow::Error into a properly-typed NaukaError.
///
/// First tries to downcast to NaukaError (preserving the original code).
/// Falls back to message-based classification so that handler errors
/// produced via `anyhow::bail!` get the correct HTTP status.
fn classify_anyhow(err: anyhow::Error) -> NaukaError {
    // If the handler already returned a typed NaukaError, use it directly.
    if let Some(nauka_err) = err.downcast_ref::<NaukaError>() {
        return nauka_err.clone();
    }

    let msg = err.to_string();

    // "already exists" → 409
    if msg.contains("already exists") {
        return NaukaError::new(crate::error::ErrorCode::ResourceAlreadyExists, msg);
    }

    // "not found" → 404
    if msg.contains("not found") {
        return NaukaError::new(crate::error::ErrorCode::ResourceNotFound, msg);
    }

    // "has N …. Delete them first" → 422
    if msg.contains("Delete them first") {
        return NaukaError::new(crate::error::ErrorCode::HasDependents, msg);
    }

    // CIDR / validation errors → 400
    if msg.contains("CIDR ") || msg.contains("CIDRs ") || msg.contains("must be") {
        return NaukaError::validation(msg);
    }

    // Missing / required field → 400
    if msg.contains("missing required field")
        || msg.contains("is required")
        || msg.contains("missing name")
    {
        return NaukaError::validation(msg);
    }

    // Invalid name → 400
    if msg.contains("invalid name") || msg.contains("InvalidName") {
        return NaukaError::validation(msg);
    }

    // General validation errors → 400
    if msg.contains("invalid") || msg.contains("cannot be empty") {
        return NaukaError::validation(msg);
    }

    // Permission denied → 403
    if msg.contains("permission denied") || msg.contains("not allowed") || msg.contains("forbidden")
    {
        return NaukaError::new(crate::error::ErrorCode::PermissionDenied, msg);
    }

    // Timeout → 504
    if msg.contains("timed out") || msg.contains("timeout") {
        return NaukaError::new(crate::error::ErrorCode::Timeout, msg);
    }

    // Default: 500
    NaukaError::internal(msg)
}

/// Handle an operation with scope values pre-populated.
async fn handle_scoped(
    reg: &ResourceRegistration,
    operation: &str,
    name: Option<String>,
    fields: HashMap<String, String>,
    scope: ScopeValues,
) -> impl IntoResponse {
    let span = tracing::info_span!(
        "api_operation",
        resource = reg.def.identity.kind,
        operation = operation,
    );
    let _guard = span.enter();

    tracing::debug!(
        resource = reg.def.identity.kind,
        operation = operation,
        name = ?name,
        "handling API request"
    );

    drop(_guard);

    let op_def = reg.def.operations.iter().find(|o| o.name == operation);

    // ── Pre-handler validation pipeline ──
    let mut fields = fields; // make mutable
    if let Some(op_def) = op_def {
        // Filter ReadOnly/Internal fields from API input
        validation::filter_readonly_fields(&reg.def, &mut fields);

        // Apply defaults for missing optional fields
        validation::apply_defaults(&reg.def, op_def, &mut fields);

        // Validate name
        validation::validate_name(&name, &op_def.semantics).map_err(ApiError)?;

        // Validate scope parents
        validation::validate_scope(&reg.def, op_def, &scope).map_err(ApiError)?;

        // Validate required fields
        validation::validate_required_fields(&reg.def, op_def, &fields).map_err(ApiError)?;

        // Validate field types
        validation::validate_field_types(&reg.def, op_def, &fields).map_err(ApiError)?;

        // Validate constraints
        for constraint in &op_def.constraints {
            if let Err(msg) = constraint.validate(&fields) {
                return Err(ApiError(NaukaError::validation(msg)));
            }
        }
    }

    // Extract pagination params before moving fields into the request
    let pagination_page = if let Some(raw) = fields.get("page") {
        Some(raw.parse::<usize>().map_err(|_| {
            ApiError(NaukaError::validation(format!(
                "invalid pagination parameter 'page': expected a positive integer, got '{raw}'"
            )))
        })?)
    } else {
        None
    };
    let pagination_per_page = if let Some(raw) = fields.get("per_page") {
        Some(raw.parse::<usize>().map_err(|_| {
            ApiError(NaukaError::validation(format!(
                "invalid pagination parameter 'per_page': expected a positive integer, got '{raw}'"
            )))
        })?)
    } else {
        None
    };

    let request = OperationRequest {
        operation: operation.to_string(),
        name,
        scope,
        fields,
    };

    let response = match tokio::time::timeout(
        std::time::Duration::from_secs(30),
        (reg.handler)(request),
    )
    .await
    {
        Ok(result) => result.map_err(|e: anyhow::Error| ApiError(classify_anyhow(e)))?,
        Err(_elapsed) => {
            return Err(ApiError(NaukaError::new(
                crate::error::ErrorCode::Timeout,
                "handler timed out after 30s".to_string(),
            )));
        }
    };

    // ── Post-handler output pipeline ──
    let mut response = response;
    validation::validate_response_contract(reg.def.identity.kind, &response).map_err(ApiError)?;
    validation::filter_response_secrets(&reg.def, &mut response);
    validation::normalize_timestamps(&mut response);

    match response {
        OperationResponse::Resource(v) => {
            let status = if operation == "create" {
                axum::http::StatusCode::CREATED
            } else {
                axum::http::StatusCode::OK
            };
            let mut response = (status, axum::Json(v.clone())).into_response();
            // Add Location header for created resources
            if operation == "create" {
                if let Some(id) = v.get("id").and_then(|v| v.as_str()) {
                    let location = format!("/{}/{}", reg.def.identity.plural, id);
                    if let Ok(loc) = axum::http::HeaderValue::from_str(&location) {
                        response
                            .headers_mut()
                            .insert(axum::http::header::LOCATION, loc);
                    }
                }
            }
            Ok(response)
        }
        OperationResponse::ResourceList(items) => {
            let total = items.len();
            let per_page = pagination_per_page.unwrap_or(25).clamp(1, 100);
            let total_pages = if total == 0 {
                1
            } else {
                total.div_ceil(per_page)
            };
            let page = pagination_page.unwrap_or(1).clamp(1, total_pages);
            let start = (page - 1) * per_page;
            let end = (start + per_page).min(total);
            let page_items = &items[start..end];
            let next_page = if page < total_pages {
                Some(page + 1)
            } else {
                None
            };
            let previous_page = if page > 1 { Some(page - 1) } else { None };
            Ok(axum::Json(serde_json::json!({
                "data": page_items,
                "pagination": {
                    "page": page,
                    "per_page": per_page,
                    "total_pages": total_pages,
                    "total_entries": total,
                    "next_page": next_page,
                    "previous_page": previous_page
                }
            }))
            .into_response())
        }
        OperationResponse::Message(msg) => {
            if operation == "delete" {
                Ok(axum::http::StatusCode::NO_CONTENT.into_response())
            } else {
                Ok(axum::Json(serde_json::json!({"message": msg})).into_response())
            }
        }
        OperationResponse::None => Ok(axum::http::StatusCode::NO_CONTENT.into_response()),
    }
}

/// Generate a list of all routes for documentation.
pub fn list_routes(registrations: &[ResourceRegistration], prefix: &str) -> Vec<RouteInfo> {
    let mut routes = Vec::new();

    for reg in registrations {
        collect_routes(reg, prefix, &mut routes);
    }

    routes
}

fn collect_routes(reg: &ResourceRegistration, prefix: &str, routes: &mut Vec<RouteInfo>) {
    let kind = reg.def.identity.cli_name;
    let base = build_base_path(prefix, reg.def.identity.plural, &reg.def.scope.parents);

    for op in &reg.def.operations {
        let (method, path) = match &op.semantics {
            OperationSemantics::List => ("GET", base.clone()),
            OperationSemantics::Create => ("POST", base.clone()),
            OperationSemantics::Get => ("GET", format!("{base}/{{id}}")),
            OperationSemantics::Delete => ("DELETE", format!("{base}/{{id}}")),
            OperationSemantics::Update { .. } => ("PATCH", format!("{base}/{{id}}")),
            OperationSemantics::Action => ("POST", format!("{base}/{}", op.name)),
        };

        routes.push(RouteInfo {
            method: method.to_string(),
            path,
            operation: op.name.to_string(),
            resource: kind.to_string(),
            description: op.description.to_string(),
        });
    }

    // Recurse into children
    for child in &reg.children {
        collect_routes(child, prefix, routes);
    }
}

/// #7: OpenAPI-compatible route listing.
#[derive(Debug, Clone, serde::Serialize)]
pub struct RouteInfo {
    pub method: String,
    pub path: String,
    pub operation: String,
    pub resource: String,
    pub description: String,
}

/// Markdown intro for the OpenAPI spec `info.description`.
const OPENAPI_DESCRIPTION: &str = "The Nauka API is a RESTful interface for turning bare-metal servers into a programmable cloud. Every hypervisor in your mesh exposes the same API, providing a single control plane for compute, networking, and storage resources.";

/// Build the introduction tags — each appears as a top-level section in the sidebar.
fn build_intro_tags() -> Vec<serde_json::Value> {
    vec![
        serde_json::json!({
            "name": "Authentication",
            "x-traitTag": true,
            "description": r#"The API authenticates requests using Bearer tokens passed in the `Authorization` header.

```http
GET /cloud/v1/orgs HTTP/1.1
Host: your-server:8443
Authorization: Bearer nk_live_a1b2c3d4e5f6...
```

All API requests must be made over HTTPS. Requests without a valid token will return `403 Forbidden`.

There are two types of tokens:

| Type | Prefix | Scope |
|------|--------|-------|
| Admin | `nk_live_` | Full access to all resources in the mesh |
| Service | `nk_svc_` | Scoped to a specific organization and project |

> Tokens are issued during `nauka hypervisor init` and can be rotated with `nauka token rotate`."#
        }),
        serde_json::json!({
            "name": "Errors",
            "x-traitTag": true,
            "description": r#"The API uses conventional HTTP status codes to indicate the outcome of a request. Codes in the `2xx` range indicate success, `4xx` indicate a client error, and `5xx` indicate a server error.

All errors return a consistent JSON body with a machine-readable `code` and a human-readable `message`:

```json
{
  "error": {
    "code": "ResourceNotFound",
    "message": "vpc 'web' not found"
  }
}
```

### Error codes

| Status | Code | Description |
|--------|------|-------------|
| `400` | `ValidationError` | The request body is invalid. Check field types, required fields, and naming rules. |
| `403` | `PermissionDenied` | The token does not have permission for this operation. |
| `404` | `ResourceNotFound` | The requested resource does not exist. Verify the name or ID. |
| `409` | `ResourceAlreadyExists` | A resource with this name already exists in the same scope. |
| `415` | `UnsupportedMediaType` | The request is missing `Content-Type: application/json`. |
| `422` | `HasDependents` | The resource cannot be deleted because other resources depend on it. Delete them first. |
| `429` | `RateLimited` | You have exceeded the rate limit. Wait and retry with exponential backoff. |
| `500` | `InternalError` | An unexpected server error occurred. Retry the request or contact support with the `x-request-id`. |
| `504` | `Timeout` | The operation did not complete within the server timeout (30s). |

### Handling errors

We recommend writing code that gracefully handles all possible error codes. Below is a typical pattern:

```bash
response=$(curl -s -w "\n%{http_code}" \
        -H "Authorization: Bearer $TOKEN" \
  https://your-server:8443/cloud/v1/vpcs)

http_code=$(echo "$response" | tail -1)
body=$(echo "$response" | sed '$d')

case $http_code in
  200) echo "$body" | jq '.data' ;;
  429) sleep 5 && retry ;;
  4*)  echo "$body" | jq -r '.error.message' >&2 ;;
  5*)  echo "Server error (request-id in headers), retrying..." >&2 ;;
esac
```"#
        }),
        serde_json::json!({
            "name": "Pagination",
            "x-traitTag": true,
            "description": r#"All list endpoints return paginated responses. Pagination is cursor-based using page numbers.

### Request parameters

| Parameter | Type | Default | Max | Description |
|-----------|------|---------|-----|-------------|
| `page` | integer | `1` | — | Page number to retrieve |
| `per_page` | integer | `25` | `100` | Number of items per page |

### Response format

```json
{
  "data": [
    {"id": "vpc-01abc", "name": "production", "cidr": "10.0.0.0/16"},
    {"id": "vpc-02def", "name": "staging", "cidr": "10.1.0.0/16"}
  ],
  "pagination": {
    "page": 1,
    "per_page": 25,
    "total_pages": 4,
    "total_entries": 87,
    "next_page": 2,
    "previous_page": null
  }
}
```

The `next_page` and `previous_page` fields are `null` when there is no corresponding page. Use them to navigate through results.

### Example

```bash
# First page
curl https://your-server:8443/cloud/v1/orgs?page=1&per_page=10

# Next page
curl https://your-server:8443/cloud/v1/orgs?page=2&per_page=10
```"#
        }),
        serde_json::json!({
            "name": "Rate Limiting",
            "x-traitTag": true,
            "description": r#"The API enforces a rate limit to protect the cluster from excessive load. Limits are applied per source IP across a sliding time window.

### Response headers

Every response includes rate limit information:

| Header | Description |
|--------|-------------|
| `x-ratelimit-remaining` | Number of requests remaining in the current window |

### Exceeding the limit

When the limit is exceeded, the API returns `429 Too Many Requests`:

```json
{
  "error": {
    "code": "RateLimited",
    "message": "Too many requests. Please retry later."
  }
}
```

### Best practices

- Cache responses when possible to reduce the number of API calls
- Implement exponential backoff when you receive a `429` response
- Use `per_page` to fetch more items in a single request instead of making many small requests"#
        }),
        serde_json::json!({
            "name": "Request IDs",
            "x-traitTag": true,
            "description": r#"Every API response includes an `x-request-id` header containing a unique identifier for the request.

```http
HTTP/1.1 200 OK
x-request-id: req-0000004a38bf
x-nauka-version: 2.0.0
content-type: application/json
```

Include this identifier when contacting support or reporting an issue. It allows the team to trace the exact request through the server logs.

> The request ID is also attached to structured log entries on the server side, making it possible to correlate API calls with internal operations."#
        }),
        serde_json::json!({
            "name": "Versioning",
            "x-traitTag": true,
            "description": r#"### URL prefix

The API version is embedded in the URL path. The current version is **v1**. URLs use two prefixes: `/platform/v1` for infrastructure, `/cloud/v1` for cloud resources.

```
https://your-server:8443/platform/v1/hypervisors
https://your-server:8443/cloud/v1/orgs
```

| Prefix | Scope | Description |
|--------|-------|-------------|
| `/platform/v1` | Infrastructure | Hypervisor management, mesh operations, node lifecycle. |
| `/cloud/v1` | Cloud resources | Organizations, VPCs, subnets, VMs, images, and all tenant resources. |

Both prefixes are served on the same port (`127.0.0.1:8443` by default). This documentation covers both the **Platform API** and the **Cloud API**.

### Server version header

Every response includes an `x-nauka-version` header with the exact server build version:

```http
HTTP/1.1 200 OK
x-nauka-version: 2.0.0
content-type: application/json
```

The version follows [Semantic Versioning](https://semver.org/) — `MAJOR.MINOR.PATCH`. Pre-release builds append a channel suffix:

| Format | Example | Channel |
|--------|---------|---------|
| `MAJOR.MINOR.PATCH` | `2.0.0` | Stable |
| `MAJOR.MINOR.PATCH-beta.N` | `2.1.0-beta.3` | Beta |
| `MAJOR.MINOR.PATCH-nightly.N` | `2.1.0-nightly.47` | Nightly |
| `MAJOR.MINOR.PATCH-rc.N` | `2.1.0-rc.1` | Release candidate |

### Release channels

Nauka maintains four release channels. Each channel receives a different level of testing:

| Channel | Stability | Use case |
|---------|-----------|----------|
| **Stable** | Production-ready | Production clusters |
| **RC** | Feature-complete, final testing | Staging environments |
| **Beta** | New features, may contain bugs | Development and testing |
| **Nightly** | Bleeding edge, no stability guarantees | Contributors and early adopters |

The channel determines which updates `nauka update` will install. A node running `2.0.0` (stable) will only receive stable updates unless `--channel` is specified.

### Compatibility

Nodes in the same mesh must share the same **major** version. Minor and patch differences are tolerated for rolling upgrades.

| Local | Remote | Compatible |
|-------|--------|------------|
| `2.0.0` | `2.1.3` | Yes — same major |
| `2.0.0` | `2.0.0-beta.5` | Yes — same major |
| `2.0.0` | `3.0.0` | **No** — different major |

When a node joins a mesh, the peering protocol verifies version compatibility. Incompatible nodes are rejected.

### Stability guarantees

The `v1` API provides these guarantees:

- **Non-breaking changes** are made without incrementing the URL prefix. This includes: new fields in response bodies, new endpoints, new optional request parameters, new error codes, and new query filters.
- **Breaking changes** will increment the prefix to `/platform/v2` and `/cloud/v2`. This includes: removing or renaming response fields, changing field types, removing endpoints, and making optional parameters required.
- **Deprecation** of an endpoint or field is announced in the changelog at least one major release before removal. Deprecated features continue to function until the next major version."#
        }),
    ]
}

/// #7: Generate a production-quality OpenAPI 3.0 spec from resource registrations.
pub fn openapi_spec(registrations: &[ResourceRegistration], prefix: &str) -> serde_json::Value {
    let paths = build_openapi_paths(registrations, prefix);

    let mut schemas = serde_json::Map::new();
    collect_resource_schemas(registrations, &mut schemas);

    // Error schema
    schemas.insert(
        "Error".to_string(),
        serde_json::json!({
            "type": "object",
            "properties": {
                "error": {
                    "type": "object",
                    "properties": {
                        "code": {"type": "string", "description": "Machine-readable error code"},
                        "message": {"type": "string", "description": "Human-readable error message"}
                    },
                    "required": ["code", "message"]
                }
            },
            "required": ["error"]
        }),
    );

    // Paginated response wrapper
    schemas.insert(
        "PaginatedResponse".to_string(),
        serde_json::json!({
            "type": "object",
            "properties": {
                "data": {"type": "array", "items": {}},
                "pagination": {
                    "type": "object",
                    "properties": {
                        "page": {"type": "integer"},
                        "per_page": {"type": "integer"},
                        "total_pages": {"type": "integer"},
                        "total_entries": {"type": "integer"},
                        "next_page": {"type": "integer", "nullable": true},
                        "previous_page": {"type": "integer", "nullable": true}
                    },
                    "required": ["page", "per_page", "total_pages", "total_entries"]
                }
            },
            "required": ["data", "pagination"]
        }),
    );

    let responses = build_shared_responses();

    let intro_tags = build_intro_tags();
    let resource_tags = build_tags(registrations);

    let intro_tag_names: Vec<String> = intro_tags
        .iter()
        .filter_map(|t| t.get("name").and_then(|n| n.as_str()).map(String::from))
        .collect();
    let resource_tag_names: Vec<String> = resource_tags
        .iter()
        .filter_map(|t| t.get("name").and_then(|n| n.as_str()).map(String::from))
        .collect();

    let mut tags = intro_tags;
    tags.extend(resource_tags);

    let tag_groups = serde_json::json!([
        {
            "name": "",
            "tags": intro_tag_names,
        },
        {
            "name": "Resources",
            "tags": resource_tag_names,
        }
    ]);

    serde_json::json!({
        "openapi": "3.0.0",
        "info": {
            "title": "Nauka API",
            "version": env!("CARGO_PKG_VERSION"),
            "description": OPENAPI_DESCRIPTION,
            "x-logo": {
                "url": "/logo-dark.svg",
                "altText": "Nauka",
            },
        },
        "x-tagGroups": tag_groups,
        "tags": tags,
        "paths": paths,
        "components": {
            "schemas": schemas,
            "responses": responses,
        },
    })
}

/// Build a combined OpenAPI spec covering two sets of resources under different prefixes.
pub fn combined_openapi_spec(
    platform_resources: &[ResourceRegistration],
    platform_prefix: &str,
    cloud_resources: &[ResourceRegistration],
    cloud_prefix: &str,
) -> serde_json::Value {
    // Build paths from both sets
    let mut paths = build_openapi_paths(platform_resources, platform_prefix);
    let cloud_paths = build_openapi_paths(cloud_resources, cloud_prefix);
    paths.extend(cloud_paths);

    // Merge schemas
    let mut schemas = serde_json::Map::new();
    collect_resource_schemas(platform_resources, &mut schemas);
    collect_resource_schemas(cloud_resources, &mut schemas);

    schemas.insert(
        "Error".to_string(),
        serde_json::json!({
            "type": "object",
            "properties": {
                "error": {
                    "type": "object",
                    "properties": {
                        "code": {"type": "string", "description": "Machine-readable error code"},
                        "message": {"type": "string", "description": "Human-readable error message"}
                    },
                    "required": ["code", "message"]
                }
            },
            "required": ["error"]
        }),
    );

    schemas.insert(
        "PaginatedResponse".to_string(),
        serde_json::json!({
            "type": "object",
            "properties": {
                "data": {"type": "array", "items": {}},
                "pagination": {
                    "type": "object",
                    "properties": {
                        "page": {"type": "integer"},
                        "per_page": {"type": "integer"},
                        "total_pages": {"type": "integer"},
                        "total_entries": {"type": "integer"},
                        "next_page": {"type": "integer", "nullable": true},
                        "previous_page": {"type": "integer", "nullable": true}
                    },
                    "required": ["page", "per_page", "total_pages", "total_entries"]
                }
            },
            "required": ["data", "pagination"]
        }),
    );

    let responses = build_shared_responses();

    let intro_tags = build_intro_tags();
    let mut platform_tags = build_tags(platform_resources);
    let cloud_tags = build_tags(cloud_resources);

    let intro_tag_names: Vec<String> = intro_tags
        .iter()
        .filter_map(|t| t.get("name").and_then(|n| n.as_str()).map(String::from))
        .collect();
    let platform_tag_names: Vec<String> = platform_tags
        .iter()
        .filter_map(|t| t.get("name").and_then(|n| n.as_str()).map(String::from))
        .collect();
    let cloud_tag_names: Vec<String> = cloud_tags
        .iter()
        .filter_map(|t| t.get("name").and_then(|n| n.as_str()).map(String::from))
        .collect();

    let mut all_tags = intro_tags;
    all_tags.append(&mut platform_tags);
    all_tags.extend(cloud_tags);

    let mut tag_groups = vec![serde_json::json!({
        "name": "",
        "tags": intro_tag_names,
    })];
    if !platform_tag_names.is_empty() {
        tag_groups.push(serde_json::json!({
            "name": "Platform",
            "tags": platform_tag_names,
        }));
    }
    if !cloud_tag_names.is_empty() {
        tag_groups.push(serde_json::json!({
            "name": "Cloud",
            "tags": cloud_tag_names,
        }));
    }

    serde_json::json!({
        "openapi": "3.0.0",
        "info": {
            "title": "Nauka API",
            "version": env!("CARGO_PKG_VERSION"),
            "description": OPENAPI_DESCRIPTION,
            "x-logo": {
                "url": "/logo-dark.svg",
                "altText": "Nauka",
            },
        },
        "x-tagGroups": tag_groups,
        "tags": all_tags,
        "paths": paths,
        "components": {
            "schemas": schemas,
            "responses": responses,
        },
    })
}

/// Build the top-level `tags` array from all resource registrations.
fn build_tags(registrations: &[ResourceRegistration]) -> Vec<serde_json::Value> {
    let mut tags = Vec::new();
    fn capitalize(s: &str) -> String {
        let mut c = s.chars();
        match c.next() {
            None => String::new(),
            Some(f) => f.to_uppercase().to_string() + c.as_str(),
        }
    }
    fn collect_tags(regs: &[ResourceRegistration], tags: &mut Vec<serde_json::Value>) {
        for reg in regs {
            let display = reg
                .def
                .identity
                .kind
                .split('-')
                .map(capitalize)
                .collect::<Vec<_>>()
                .join(" ");
            tags.push(serde_json::json!({
                "name": reg.def.identity.kind,
                "x-displayName": display,
                "description": reg.def.identity.description
            }));
            collect_tags(&reg.children, tags);
        }
    }
    collect_tags(registrations, &mut tags);
    tags
}

/// Collect resource schemas recursively into the schemas map.
fn collect_resource_schemas(
    registrations: &[ResourceRegistration],
    schemas: &mut serde_json::Map<String, serde_json::Value>,
) {
    for reg in registrations {
        schemas.insert(reg.def.identity.kind.to_string(), resource_schema(&reg.def));
        collect_resource_schemas(&reg.children, schemas);
    }
}

/// Build shared error responses for `components.responses`.
fn build_shared_responses() -> serde_json::Map<String, serde_json::Value> {
    let error_ref = serde_json::json!({"$ref": "#/components/schemas/Error"});
    let mut responses = serde_json::Map::new();

    let entries: &[(&str, &str, u16)] = &[
        ("ValidationError", "Invalid input", 400),
        ("NotFound", "Resource not found", 404),
        ("Conflict", "Resource already exists", 409),
        (
            "HasDependents",
            "Cannot delete — resource has dependents",
            422,
        ),
        ("InternalError", "Internal server error", 500),
    ];

    for (name, desc, _status) in entries {
        responses.insert(
            name.to_string(),
            serde_json::json!({
                "description": desc,
                "content": {
                    "application/json": {
                        "schema": error_ref
                    }
                }
            }),
        );
    }
    responses
}

/// Build the OpenAPI `paths` object by iterating registrations recursively.
fn build_openapi_paths(
    registrations: &[ResourceRegistration],
    prefix: &str,
) -> serde_json::Map<String, serde_json::Value> {
    let mut paths = serde_json::Map::new();

    fn collect_paths(
        regs: &[ResourceRegistration],
        prefix: &str,
        paths: &mut serde_json::Map<String, serde_json::Value>,
    ) {
        for reg in regs {
            let def = &reg.def;
            let kind = def.identity.kind;
            let base = build_base_path(prefix, def.identity.plural, &def.scope.parents);

            for op in &def.operations {
                let (method, path) = match &op.semantics {
                    OperationSemantics::List => ("get", base.clone()),
                    OperationSemantics::Create => ("post", base.clone()),
                    OperationSemantics::Get => ("get", format!("{base}/{{id}}")),
                    OperationSemantics::Delete => ("delete", format!("{base}/{{id}}")),
                    OperationSemantics::Update { .. } => ("patch", format!("{base}/{{id}}")),
                    OperationSemantics::Action => ("post", format!("{base}/{}", op.name)),
                };

                let mut operation_obj = serde_json::Map::new();
                operation_obj.insert(
                    "summary".to_string(),
                    serde_json::Value::String(op.description.to_string()),
                );
                operation_obj.insert(
                    "operationId".to_string(),
                    serde_json::Value::String(format!("{}_{}", kind, op.name)),
                );
                operation_obj.insert("tags".to_string(), serde_json::json!([kind]));

                // Parameters (path + query)
                let params = operation_parameters(def, op);
                if !params.is_empty() {
                    operation_obj
                        .insert("parameters".to_string(), serde_json::Value::Array(params));
                }

                // Request body
                if let Some(body) = operation_request_body(def, op) {
                    operation_obj.insert("requestBody".to_string(), body);
                }

                // Responses
                operation_obj.insert("responses".to_string(), operation_responses(kind, op));

                let path_entry = paths.entry(path).or_insert_with(|| serde_json::json!({}));
                path_entry[method] = serde_json::Value::Object(operation_obj);
            }

            collect_paths(&reg.children, prefix, paths);
        }
    }

    collect_paths(registrations, prefix, &mut paths);
    paths
}

/// Build the `parameters` array for an operation (path params + query params).
fn operation_parameters(
    def: &crate::resource::ResourceDef,
    op: &crate::resource::OperationDef,
) -> Vec<serde_json::Value> {
    let mut params = Vec::new();

    // Path parameters from scope parents
    for parent in &def.scope.parents {
        params.push(serde_json::json!({
            "name": format!("{}_id", parent.kind),
            "in": "path",
            "required": true,
            "schema": {"type": "string"},
            "description": parent.description
        }));
    }

    // Instance operations (Get, Delete, Update) have an {id} path parameter
    match &op.semantics {
        OperationSemantics::Get
        | OperationSemantics::Delete
        | OperationSemantics::Update { .. } => {
            params.push(serde_json::json!({
                "name": "id",
                "in": "path",
                "required": true,
                "schema": {"type": "string"},
                "description": format!("{} name or ID", def.identity.kind)
            }));
        }
        _ => {}
    }

    // List operations get pagination query parameters
    if matches!(op.semantics, OperationSemantics::List) {
        params.push(serde_json::json!({
            "name": "page",
            "in": "query",
            "required": false,
            "schema": {"type": "integer", "default": 1, "minimum": 1},
            "description": "Page number"
        }));
        params.push(serde_json::json!({
            "name": "per_page",
            "in": "query",
            "required": false,
            "schema": {"type": "integer", "default": 25, "minimum": 1, "maximum": 100},
            "description": "Items per page (max 100)"
        }));
    }

    params
}

/// Build the `requestBody` for an operation, or None if the operation has no body.
fn operation_request_body(
    def: &crate::resource::ResourceDef,
    op: &crate::resource::OperationDef,
) -> Option<serde_json::Value> {
    use crate::resource::{ArgSource, Mutability, OperationSemantics};

    let mut properties = serde_json::Map::new();
    let mut required_fields: Vec<serde_json::Value> = Vec::new();

    match &op.semantics {
        OperationSemantics::Create => {
            // name is always required on create
            properties.insert(
                "name".to_string(),
                serde_json::json!({"type": "string", "description": "Resource name"}),
            );
            required_fields.push(serde_json::json!("name"));

            // Schema fields with CreateOnly or Mutable mutability
            for field in &def.schema.fields {
                if matches!(
                    field.mutability,
                    Mutability::CreateOnly | Mutability::Mutable
                ) {
                    let schema = field_type_to_json_schema(field);
                    properties.insert(field.name.to_string(), schema);
                    if matches!(field.mutability, Mutability::CreateOnly) && field.default.is_none()
                    {
                        required_fields.push(serde_json::json!(field.name));
                    }
                }
            }

            // Operation custom args
            add_operation_args_to_schema(op, def, &mut properties, &mut required_fields);
        }
        OperationSemantics::Update { .. } => {
            // Schema fields with Mutable mutability
            for field in &def.schema.fields {
                if matches!(field.mutability, Mutability::Mutable) {
                    let schema = field_type_to_json_schema(field);
                    properties.insert(field.name.to_string(), schema);
                }
            }

            // Operation custom args
            add_operation_args_to_schema(op, def, &mut properties, &mut required_fields);

            // Update with no mutable fields and no args => no body
            if properties.is_empty() {
                return None;
            }
        }
        OperationSemantics::Action => {
            // Action operations: custom args only
            for arg in &op.args {
                let field_def = match &arg.source {
                    ArgSource::Custom(f) => f,
                    ArgSource::FromSchema(name) => {
                        match def.schema.fields.iter().find(|f| f.name == *name) {
                            Some(f) => f,
                            None => continue,
                        }
                    }
                };
                let schema = field_type_to_json_schema(field_def);
                properties.insert(arg.name.to_string(), schema);
                if arg.required {
                    required_fields.push(serde_json::json!(arg.name));
                }
            }

            // Action with no args => no body
            if properties.is_empty() {
                return None;
            }
        }
        // Get, List, Delete => no request body
        _ => return None,
    }

    let mut schema = serde_json::json!({
        "type": "object",
        "properties": properties
    });
    if !required_fields.is_empty() {
        schema["required"] = serde_json::Value::Array(required_fields);
    }

    Some(serde_json::json!({
        "required": true,
        "content": {
            "application/json": {
                "schema": schema
            }
        }
    }))
}

/// Add operation-specific custom args to a request body schema.
fn add_operation_args_to_schema(
    op: &crate::resource::OperationDef,
    def: &crate::resource::ResourceDef,
    properties: &mut serde_json::Map<String, serde_json::Value>,
    required_fields: &mut Vec<serde_json::Value>,
) {
    use crate::resource::ArgSource;

    for arg in &op.args {
        let field_def = match &arg.source {
            ArgSource::Custom(f) => f,
            ArgSource::FromSchema(name) => {
                match def.schema.fields.iter().find(|f| f.name == *name) {
                    Some(f) => f,
                    None => continue,
                }
            }
        };
        let schema = field_type_to_json_schema(field_def);
        properties.insert(arg.name.to_string(), schema);
        if arg.required {
            required_fields.push(serde_json::json!(arg.name));
        }
    }
}

/// Convert a FieldDef to a JSON Schema object, including description and default.
fn field_type_to_json_schema(field: &crate::resource::FieldDef) -> serde_json::Value {
    use crate::resource::FieldType;

    let mut schema = serde_json::Map::new();

    match &field.field_type {
        FieldType::String
        | FieldType::Secret
        | FieldType::Path
        | FieldType::Duration
        | FieldType::Cidr
        | FieldType::IpAddr
        | FieldType::ResourceRef(_)
        | FieldType::KeyValue => {
            schema.insert("type".to_string(), serde_json::json!("string"));
        }
        FieldType::Integer | FieldType::Port | FieldType::SizeGb | FieldType::SizeMb => {
            schema.insert("type".to_string(), serde_json::json!("integer"));
        }
        FieldType::Flag => {
            schema.insert("type".to_string(), serde_json::json!("boolean"));
        }
        FieldType::Enum(e) => {
            schema.insert("type".to_string(), serde_json::json!("string"));
            schema.insert("enum".to_string(), serde_json::json!(e.values));
        }
    }

    if !field.description.is_empty() {
        schema.insert(
            "description".to_string(),
            serde_json::Value::String(field.description.to_string()),
        );
    }

    if let Some(default) = field.default {
        // Parse numeric defaults for integer types
        match &field.field_type {
            FieldType::Integer | FieldType::Port | FieldType::SizeGb | FieldType::SizeMb => {
                if let Ok(n) = default.parse::<i64>() {
                    schema.insert("default".to_string(), serde_json::json!(n));
                } else {
                    schema.insert(
                        "default".to_string(),
                        serde_json::Value::String(default.to_string()),
                    );
                }
            }
            FieldType::Flag => {
                let v = default == "true";
                schema.insert("default".to_string(), serde_json::json!(v));
            }
            _ => {
                schema.insert(
                    "default".to_string(),
                    serde_json::Value::String(default.to_string()),
                );
            }
        }
    }

    serde_json::Value::Object(schema)
}

/// Build the `responses` object for an operation.
fn operation_responses(kind: &str, op: &crate::resource::OperationDef) -> serde_json::Value {
    use crate::resource::OperationSemantics;

    let resource_ref = serde_json::json!({"$ref": format!("#/components/schemas/{}", kind)});

    match &op.semantics {
        OperationSemantics::Create => {
            serde_json::json!({
                "201": {
                    "description": "Created",
                    "content": {
                        "application/json": {
                            "schema": resource_ref
                        }
                    }
                },
                "400": {"$ref": "#/components/responses/ValidationError"},
                "409": {"$ref": "#/components/responses/Conflict"},
                "500": {"$ref": "#/components/responses/InternalError"}
            })
        }
        OperationSemantics::Get => {
            serde_json::json!({
                "200": {
                    "description": "OK",
                    "content": {
                        "application/json": {
                            "schema": resource_ref
                        }
                    }
                },
                "400": {"$ref": "#/components/responses/ValidationError"},
                "404": {"$ref": "#/components/responses/NotFound"},
                "500": {"$ref": "#/components/responses/InternalError"}
            })
        }
        OperationSemantics::List => {
            serde_json::json!({
                "200": {
                    "description": "OK",
                    "content": {
                        "application/json": {
                            "schema": {
                                "allOf": [
                                    {"$ref": "#/components/schemas/PaginatedResponse"},
                                    {
                                        "type": "object",
                                        "properties": {
                                            "data": {
                                                "type": "array",
                                                "items": resource_ref
                                            }
                                        }
                                    }
                                ]
                            }
                        }
                    }
                },
                "400": {"$ref": "#/components/responses/ValidationError"},
                "500": {"$ref": "#/components/responses/InternalError"}
            })
        }
        OperationSemantics::Delete => {
            serde_json::json!({
                "204": {
                    "description": "No Content"
                },
                "400": {"$ref": "#/components/responses/ValidationError"},
                "404": {"$ref": "#/components/responses/NotFound"},
                "422": {"$ref": "#/components/responses/HasDependents"},
                "500": {"$ref": "#/components/responses/InternalError"}
            })
        }
        OperationSemantics::Update { .. } => {
            serde_json::json!({
                "200": {
                    "description": "OK",
                    "content": {
                        "application/json": {
                            "schema": resource_ref
                        }
                    }
                },
                "400": {"$ref": "#/components/responses/ValidationError"},
                "404": {"$ref": "#/components/responses/NotFound"},
                "500": {"$ref": "#/components/responses/InternalError"}
            })
        }
        OperationSemantics::Action => {
            serde_json::json!({
                "200": {
                    "description": "OK",
                    "content": {
                        "application/json": {
                            "schema": {
                                "type": "object",
                                "properties": {
                                    "message": {"type": "string"}
                                }
                            }
                        }
                    }
                },
                "400": {"$ref": "#/components/responses/ValidationError"},
                "500": {"$ref": "#/components/responses/InternalError"}
            })
        }
    }
}

/// Generate a JSON Schema-like object from a ResourceDef's schema fields.
fn resource_schema(def: &crate::resource::ResourceDef) -> serde_json::Value {
    let mut properties = serde_json::Map::new();
    let mut required = Vec::new();

    // Name is always present
    properties.insert("name".to_string(), serde_json::json!({"type": "string"}));
    required.push(serde_json::json!("name"));

    for field in &def.schema.fields {
        let field_schema = match &field.field_type {
            crate::resource::FieldType::String
            | crate::resource::FieldType::Secret
            | crate::resource::FieldType::Path
            | crate::resource::FieldType::Duration
            | crate::resource::FieldType::Cidr
            | crate::resource::FieldType::IpAddr
            | crate::resource::FieldType::ResourceRef(_)
            | crate::resource::FieldType::KeyValue => {
                serde_json::json!({"type": "string", "description": field.description})
            }
            crate::resource::FieldType::Integer
            | crate::resource::FieldType::Port
            | crate::resource::FieldType::SizeGb
            | crate::resource::FieldType::SizeMb => {
                serde_json::json!({"type": "integer", "description": field.description})
            }
            crate::resource::FieldType::Flag => {
                serde_json::json!({"type": "boolean", "description": field.description})
            }
            crate::resource::FieldType::Enum(e) => {
                serde_json::json!({"type": "string", "enum": e.values, "description": field.description})
            }
        };
        properties.insert(field.name.to_string(), field_schema);

        if matches!(field.mutability, crate::resource::Mutability::CreateOnly)
            && field.default.is_none()
        {
            required.push(serde_json::json!(field.name));
        }
    }

    serde_json::json!({
        "type": "object",
        "properties": properties,
        "required": required,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resource::*;

    fn test_resource() -> ResourceRegistration {
        let def = ResourceDef {
            identity: ResourceIdentity {
                kind: "widget",
                cli_name: "widget",
                plural: "widgets",
                description: "Test widget",
                aliases: &[],
            },
            scope: ScopeDef::global(),
            schema: ResourceSchema::new(),
            operations: vec![
                OperationDef::create(),
                OperationDef::list(),
                OperationDef::get(),
                OperationDef::delete(),
                OperationDef::action("polish", "Polish the widget"),
            ],
            presentation: PresentationDef::none(),
        };

        let handler: HandlerFn = Box::new(|req| {
            Box::pin(async move {
                match req.operation.as_str() {
                    "list" => Ok(OperationResponse::ResourceList(vec![
                        serde_json::json!({"id": "w1-id", "name": "w1"}),
                    ])),
                    "create" => Ok(OperationResponse::Resource(
                        serde_json::json!({"id": "new-id", "name": req.name.unwrap_or_default()}),
                    )),
                    "get" => Ok(OperationResponse::Resource(
                        serde_json::json!({"id": "get-id", "name": req.name.unwrap_or_default()}),
                    )),
                    "delete" => Ok(OperationResponse::Message("deleted".into())),
                    "polish" => Ok(OperationResponse::Message("polished".into())),
                    _ => Ok(OperationResponse::None),
                }
            })
        });

        ResourceRegistration {
            def,
            handler,
            children: vec![],
        }
    }

    fn scoped_resource() -> ResourceRegistration {
        let def = ResourceDef {
            identity: ResourceIdentity {
                kind: "subnet",
                cli_name: "subnet",
                plural: "subnets",
                description: "Subnet",
                aliases: &[],
            },
            scope: ScopeDef::within("vpc", "--vpc", "Parent VPC"),
            schema: ResourceSchema::new(),
            operations: vec![OperationDef::list(), OperationDef::create()],
            presentation: PresentationDef::none(),
        };

        let handler: HandlerFn =
            Box::new(|_req| Box::pin(async move { Ok(OperationResponse::ResourceList(vec![])) }));

        ResourceRegistration {
            def,
            handler,
            children: vec![],
        }
    }

    // ── Route generation ──

    #[test]
    fn list_routes_generates_all() {
        let routes = list_routes(&[test_resource()], "/cloud/v1");
        assert_eq!(routes.len(), 5);
        let methods: Vec<&str> = routes.iter().map(|r| r.method.as_str()).collect();
        assert!(methods.contains(&"GET"));
        assert!(methods.contains(&"POST"));
        assert!(methods.contains(&"DELETE"));
    }

    #[test]
    fn list_routes_empty() {
        assert!(list_routes(&[], "/v1").is_empty());
    }

    // #11: GET + DELETE on same path
    #[test]
    fn build_router_with_get_and_delete() {
        let reg = test_resource();
        let _router = build_router(vec![reg], "/cloud/v1"); // should not panic
    }

    // #10: Scoped routes
    #[test]
    fn scoped_routes_include_parent() {
        let routes = list_routes(&[scoped_resource()], "/cloud/v1");
        let paths: Vec<&str> = routes.iter().map(|r| r.path.as_str()).collect();
        assert!(
            paths
                .iter()
                .any(|p| p.contains("vpcs") && p.contains("subnets")),
            "expected scoped path, got: {paths:?}"
        );
    }

    #[test]
    fn base_path_no_parents() {
        let path = build_base_path("/cloud/v1", "orgs", &[]);
        assert_eq!(path, "/cloud/v1/orgs");
    }

    #[test]
    fn base_path_with_parent() {
        let parents = vec![crate::resource::ParentRef {
            kind: "org",
            flag: "--org",
            required_on_create: true,
            required_on_resolve: false,
            description: "Organization",
        }];
        let path = build_base_path("/cloud/v1", "projects", &parents);
        assert_eq!(path, "/cloud/v1/orgs/{org_id}/projects");
    }

    #[test]
    fn base_path_nested() {
        let parents = vec![
            crate::resource::ParentRef {
                kind: "org",
                flag: "--org",
                required_on_create: true,
                required_on_resolve: false,
                description: "Org",
            },
            crate::resource::ParentRef {
                kind: "project",
                flag: "--project",
                required_on_create: true,
                required_on_resolve: false,
                description: "Project",
            },
        ];
        let path = build_base_path("/v1", "vpcs", &parents);
        assert_eq!(path, "/v1/orgs/{org_id}/projects/{project_id}/vpcs");
    }

    // ── Scope extraction ──

    #[test]
    fn extract_scope_from_path() {
        let mut params = HashMap::new();
        params.insert("org_id".to_string(), "org-123".to_string());
        params.insert("project_id".to_string(), "proj-456".to_string());
        let scope = extract_scope(&params, &["org_id".to_string(), "project_id".to_string()]);
        assert_eq!(scope.get("org"), Some("org-123"));
        assert_eq!(scope.get("project"), Some("proj-456"));
    }

    // ── Handler ──

    #[tokio::test]
    async fn handle_operation_list() {
        let reg = test_resource();
        let resp = handle_scoped(&reg, "list", None, HashMap::new(), ScopeValues::default()).await;
        assert!(resp.into_response().status().is_success());
    }

    #[tokio::test]
    async fn handle_operation_create() {
        let reg = test_resource();
        let resp = handle_scoped(
            &reg,
            "create",
            Some("widget-one".into()),
            HashMap::new(),
            ScopeValues::default(),
        )
        .await;
        assert!(resp.into_response().status().is_success());
    }

    #[tokio::test]
    async fn handle_operation_delete() {
        let reg = test_resource();
        let resp = handle_scoped(
            &reg,
            "delete",
            Some("widget-one".into()),
            HashMap::new(),
            ScopeValues::default(),
        )
        .await;
        assert_eq!(
            resp.into_response().status(),
            axum::http::StatusCode::NO_CONTENT
        );
    }

    // #7: OpenAPI spec
    #[test]
    fn openapi_spec_generates() {
        let spec = openapi_spec(&[test_resource()], "/cloud/v1");
        assert_eq!(spec["openapi"], "3.0.0");
        assert!(spec["paths"]["/cloud/v1/widgets"]["get"].is_object());
        assert!(spec["paths"]["/cloud/v1/widgets"]["post"].is_object());
        assert!(spec["paths"]["/cloud/v1/widgets/{id}"]["get"].is_object());
        assert!(spec["paths"]["/cloud/v1/widgets/{id}"]["delete"].is_object());
        assert!(spec["components"]["schemas"].is_object());

        // Description present
        assert!(
            spec["info"]["description"].as_str().unwrap().len() > 10,
            "expected non-empty description"
        );

        // Intro tags (Authentication, Errors, Pagination, etc.)
        let tags = spec["tags"].as_array().unwrap();
        assert!(!tags.is_empty());
        let tag_names: Vec<&str> = tags.iter().filter_map(|t| t["name"].as_str()).collect();
        assert!(
            tag_names.contains(&"Pagination"),
            "expected Pagination intro tag"
        );
        assert!(tag_names.contains(&"Errors"), "expected Errors intro tag");

        // Resource tags
        assert!(
            tag_names.contains(&"widget"),
            "expected widget resource tag"
        );

        // Shared error responses
        assert!(spec["components"]["responses"]["ValidationError"].is_object());
        assert!(spec["components"]["responses"]["NotFound"].is_object());
        assert!(spec["components"]["responses"]["Conflict"].is_object());
        assert!(spec["components"]["responses"]["HasDependents"].is_object());
        assert!(spec["components"]["responses"]["InternalError"].is_object());

        // Error + PaginatedResponse schemas
        assert!(spec["components"]["schemas"]["Error"].is_object());
        assert!(spec["components"]["schemas"]["PaginatedResponse"].is_object());

        // Create has 201 response + request body
        let create = &spec["paths"]["/cloud/v1/widgets"]["post"];
        assert!(create["responses"]["201"].is_object());
        assert!(create["responses"]["409"].is_object());
        assert!(create["requestBody"].is_object());

        // List has pagination query params
        let list = &spec["paths"]["/cloud/v1/widgets"]["get"];
        let params = list["parameters"].as_array().unwrap();
        let param_names: Vec<&str> = params.iter().map(|p| p["name"].as_str().unwrap()).collect();
        assert!(param_names.contains(&"page"));
        assert!(param_names.contains(&"per_page"));

        // List response uses PaginatedResponse
        assert!(
            list["responses"]["200"]["content"]["application/json"]["schema"]["allOf"].is_array()
        );

        // Get has {id} path param
        let get_op = &spec["paths"]["/cloud/v1/widgets/{id}"]["get"];
        let get_params = get_op["parameters"].as_array().unwrap();
        assert!(
            get_params
                .iter()
                .any(|p| p["name"] == "id" && p["in"] == "path"),
            "expected {{id}} path parameter on get"
        );

        // Delete has 204 + 422
        let delete_op = &spec["paths"]["/cloud/v1/widgets/{id}"]["delete"];
        assert!(delete_op["responses"]["204"].is_object());
        assert!(delete_op["responses"]["422"].is_object());

        // Action has 200 with message schema
        let polish = &spec["paths"]["/cloud/v1/widgets/polish"]["post"];
        assert!(polish["responses"]["200"].is_object());
    }

    #[test]
    fn openapi_spec_empty() {
        let spec = openapi_spec(&[], "/v1");
        assert_eq!(spec["openapi"], "3.0.0");
        assert!(spec["paths"].as_object().unwrap().is_empty());
        assert!(spec["components"]["schemas"].is_object());
        // Shared schemas present even with no resources
        assert!(spec["components"]["schemas"]["Error"].is_object());
        assert!(spec["components"]["schemas"]["PaginatedResponse"].is_object());
        assert!(spec["components"]["responses"].is_object());
        // Intro tags always present, but no resource tags
        let tags = spec["tags"].as_array().unwrap();
        assert!(
            tags.iter().all(|t| t.get("x-traitTag").is_some()),
            "expected only intro tags when no resources"
        );
    }

    // ── Pagination (#116) ──

    fn paginated_resource(count: usize) -> ResourceRegistration {
        let def = ResourceDef {
            identity: ResourceIdentity {
                kind: "widget",
                cli_name: "widget",
                plural: "widgets",
                description: "Test widget",
                aliases: &[],
            },
            scope: ScopeDef::global(),
            schema: ResourceSchema::new(),
            operations: vec![OperationDef::list()],
            presentation: PresentationDef::none(),
        };

        let handler: HandlerFn = Box::new(move |_req| {
            let items: Vec<serde_json::Value> = (0..count)
                .map(|i| serde_json::json!({"id": format!("w{i}-id"), "name": format!("w{i}")}))
                .collect();
            Box::pin(async move { Ok(OperationResponse::ResourceList(items)) })
        });

        ResourceRegistration {
            def,
            handler,
            children: vec![],
        }
    }

    async fn list_with_params(
        reg: &ResourceRegistration,
        params: &[(&str, &str)],
    ) -> serde_json::Value {
        let fields: HashMap<String, String> = params
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let resp = handle_scoped(reg, "list", None, fields, ScopeValues::default()).await;
        let response = resp.into_response();
        let (_, body) = response.into_parts();
        let bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[tokio::test]
    async fn pagination_defaults_page1_per25() {
        let reg = paginated_resource(30);
        let json = list_with_params(&reg, &[]).await;
        let pag = &json["pagination"];
        assert_eq!(pag["page"], 1);
        assert_eq!(pag["per_page"], 25);
        assert_eq!(pag["total_entries"], 30);
        assert_eq!(pag["total_pages"], 2);
        assert_eq!(pag["next_page"], 2);
        assert!(pag["previous_page"].is_null());
        assert_eq!(json["data"].as_array().unwrap().len(), 25);
    }

    #[tokio::test]
    async fn pagination_page2() {
        let reg = paginated_resource(30);
        let json = list_with_params(&reg, &[("page", "2")]).await;
        let pag = &json["pagination"];
        assert_eq!(pag["page"], 2);
        assert_eq!(pag["total_pages"], 2);
        assert_eq!(pag["previous_page"], 1);
        assert!(pag["next_page"].is_null());
        assert_eq!(json["data"].as_array().unwrap().len(), 5);
    }

    #[tokio::test]
    async fn pagination_custom_per_page() {
        let reg = paginated_resource(30);
        let json = list_with_params(&reg, &[("per_page", "10"), ("page", "1")]).await;
        let pag = &json["pagination"];
        assert_eq!(pag["page"], 1);
        assert_eq!(pag["per_page"], 10);
        assert_eq!(pag["total_pages"], 3);
        assert_eq!(json["data"].as_array().unwrap().len(), 10);
    }

    #[tokio::test]
    async fn pagination_per_page_capped_at_100() {
        let reg = paginated_resource(5);
        let json = list_with_params(&reg, &[("per_page", "999")]).await;
        assert_eq!(json["pagination"]["per_page"], 100);
    }

    #[tokio::test]
    async fn pagination_empty_list() {
        let reg = paginated_resource(0);
        let json = list_with_params(&reg, &[]).await;
        let pag = &json["pagination"];
        assert_eq!(pag["total_entries"], 0);
        assert_eq!(pag["total_pages"], 1);
        assert_eq!(pag["page"], 1);
        assert_eq!(json["data"].as_array().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn pagination_page_clamped_to_max() {
        let reg = paginated_resource(10);
        // Only 1 page with per_page=25, requesting page 5 should clamp to 1
        let json = list_with_params(&reg, &[("page", "5")]).await;
        assert_eq!(json["pagination"]["page"], 1);
    }

    #[test]
    fn route_info_serializes() {
        let ri = RouteInfo {
            method: "GET".into(),
            path: "/cloud/v1/widget".into(),
            operation: "list".into(),
            resource: "widget".into(),
            description: "List widgets".into(),
        };
        let json = serde_json::to_string(&ri).unwrap();
        assert!(json.contains("GET"));
    }

    // ── classify_anyhow tests ──

    #[test]
    fn classify_already_exists() {
        let err = anyhow::anyhow!("org 'acme' already exists");
        let classified = classify_anyhow(err);
        assert_eq!(
            classified.code,
            crate::error::ErrorCode::ResourceAlreadyExists
        );
        assert_eq!(classified.http_status(), 409);
    }

    #[test]
    fn classify_not_found() {
        let err = anyhow::anyhow!("vpc 'web' not found");
        let classified = classify_anyhow(err);
        assert_eq!(classified.code, crate::error::ErrorCode::ResourceNotFound);
        assert_eq!(classified.http_status(), 404);
    }

    #[test]
    fn classify_has_dependents() {
        let err = anyhow::anyhow!("vpc 'web' has 2 subnet(s). Delete them first.");
        let classified = classify_anyhow(err);
        assert_eq!(classified.code, crate::error::ErrorCode::HasDependents);
        assert_eq!(classified.http_status(), 422);
    }

    #[test]
    fn classify_cidr_validation() {
        let err = anyhow::anyhow!(
            "CIDR must be a private range (10.0.0.0/8, 172.16.0.0/12, or 192.168.0.0/16)"
        );
        let classified = classify_anyhow(err);
        assert_eq!(classified.code, crate::error::ErrorCode::ValidationError);
        assert_eq!(classified.http_status(), 400);
    }

    #[test]
    fn classify_cidr_overlap() {
        let err = anyhow::anyhow!("VPC CIDRs overlap: 10.0.0.0/16 and 10.0.0.0/24");
        let classified = classify_anyhow(err);
        assert_eq!(classified.code, crate::error::ErrorCode::ValidationError);
        assert_eq!(classified.http_status(), 400);
    }

    #[test]
    fn classify_must_be_validation() {
        let err =
            anyhow::anyhow!("vm must be stopped or pending to delete (current state: running)");
        let classified = classify_anyhow(err);
        assert_eq!(classified.code, crate::error::ErrorCode::ValidationError);
        assert_eq!(classified.http_status(), 400);
    }

    #[test]
    fn classify_downcast_nauka_error() {
        let nauka_err = NaukaError::not_found("vpc", "web");
        let err = anyhow::Error::new(nauka_err);
        let classified = classify_anyhow(err);
        assert_eq!(classified.code, crate::error::ErrorCode::ResourceNotFound);
        assert_eq!(classified.http_status(), 404);
    }

    #[test]
    fn classify_unknown_defaults_to_500() {
        let err = anyhow::anyhow!("something unexpected happened");
        let classified = classify_anyhow(err);
        assert_eq!(classified.code, crate::error::ErrorCode::InternalError);
        assert_eq!(classified.http_status(), 500);
    }

    #[test]
    fn classify_missing_required_field() {
        let err = anyhow::anyhow!("missing required field: s3-endpoint");
        let classified = classify_anyhow(err);
        assert_eq!(classified.code, crate::error::ErrorCode::ValidationError);
        assert_eq!(classified.http_status(), 400);
    }

    #[test]
    fn classify_is_required() {
        let err = anyhow::anyhow!("--org is required");
        let classified = classify_anyhow(err);
        assert_eq!(classified.code, crate::error::ErrorCode::ValidationError);
        assert_eq!(classified.http_status(), 400);
    }

    #[test]
    fn classify_missing_name() {
        let err = anyhow::anyhow!("missing name");
        let classified = classify_anyhow(err);
        assert_eq!(classified.code, crate::error::ErrorCode::ValidationError);
        assert_eq!(classified.http_status(), 400);
    }

    #[test]
    fn classify_invalid_name() {
        let err = anyhow::anyhow!("invalid name 'AB': must start with a lowercase letter");
        let classified = classify_anyhow(err);
        assert_eq!(classified.code, crate::error::ErrorCode::ValidationError);
        assert_eq!(classified.http_status(), 400);
    }

    #[test]
    fn classify_permission_denied() {
        let err = anyhow::anyhow!("permission denied for this operation");
        let classified = classify_anyhow(err);
        assert_eq!(classified.code, crate::error::ErrorCode::PermissionDenied);
        assert_eq!(classified.http_status(), 403);
    }

    #[test]
    fn classify_timeout() {
        let err = anyhow::anyhow!("operation timed out after 30s");
        let classified = classify_anyhow(err);
        assert_eq!(classified.code, crate::error::ErrorCode::Timeout);
        assert_eq!(classified.http_status(), 504);
    }

    #[tokio::test]
    async fn pagination_invalid_page_returns_400() {
        let reg = paginated_resource(10);
        let fields: HashMap<String, String> = [("page".to_string(), "abc".to_string())]
            .into_iter()
            .collect();
        let resp = handle_scoped(&reg, "list", None, fields, ScopeValues::default()).await;
        assert_eq!(resp.into_response().status(), 400);
    }

    #[tokio::test]
    async fn pagination_invalid_per_page_returns_400() {
        let reg = paginated_resource(10);
        let fields: HashMap<String, String> = [("per_page".to_string(), "xyz".to_string())]
            .into_iter()
            .collect();
        let resp = handle_scoped(&reg, "list", None, fields, ScopeValues::default()).await;
        assert_eq!(resp.into_response().status(), 400);
    }
}
