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
/// No parents: `/admin/v1/orgs`
/// With parents: `/admin/v1/orgs/{org_id}/projects/{project_id}/environments`
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
    let op_def = reg.def.operations.iter().find(|o| o.name == operation);
    if let Some(op_def) = op_def {
        for constraint in &op_def.constraints {
            if let Err(msg) = constraint.validate(&fields) {
                return Err(ApiError(NaukaError::validation(msg)));
            }
        }
    }

    // Extract pagination params before moving fields into the request
    let pagination_page = fields.get("page").and_then(|v| v.parse::<usize>().ok());
    let pagination_per_page = fields.get("per_page").and_then(|v| v.parse::<usize>().ok());

    let request = OperationRequest {
        operation: operation.to_string(),
        name,
        scope,
        fields,
    };

    let response = (reg.handler)(request)
        .await
        .map_err(|e: anyhow::Error| ApiError(classify_anyhow(e)))?;

    match response {
        OperationResponse::Resource(v) => {
            let status = if operation == "create" {
                axum::http::StatusCode::CREATED
            } else {
                axum::http::StatusCode::OK
            };
            Ok((status, axum::Json(v)).into_response())
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

/// #7: Generate a minimal OpenAPI-style spec from routes.
pub fn openapi_spec(registrations: &[ResourceRegistration], prefix: &str) -> serde_json::Value {
    let routes = list_routes(registrations, prefix);

    let mut paths = serde_json::Map::new();
    for route in &routes {
        let path_entry = paths
            .entry(route.path.clone())
            .or_insert_with(|| serde_json::json!({}));
        let method_lower = route.method.to_lowercase();
        path_entry[method_lower] = serde_json::json!({
            "summary": route.description,
            "operationId": format!("{}_{}", route.resource, route.operation),
            "tags": [route.resource],
        });
    }

    serde_json::json!({
        "openapi": "3.0.0",
        "info": {
            "title": "Nauka API",
            "version": env!("CARGO_PKG_VERSION"),
        },
        "paths": paths,
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
                        serde_json::json!({"name": "w1"}),
                    ])),
                    "create" => Ok(OperationResponse::Resource(
                        serde_json::json!({"name": req.name.unwrap_or_default()}),
                    )),
                    "get" => Ok(OperationResponse::Resource(
                        serde_json::json!({"name": req.name.unwrap_or_default()}),
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
        let routes = list_routes(&[test_resource()], "/admin/v1");
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
        let _router = build_router(vec![reg], "/admin/v1"); // should not panic
    }

    // #10: Scoped routes
    #[test]
    fn scoped_routes_include_parent() {
        let routes = list_routes(&[scoped_resource()], "/admin/v1");
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
        let path = build_base_path("/admin/v1", "orgs", &[]);
        assert_eq!(path, "/admin/v1/orgs");
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
        let path = build_base_path("/admin/v1", "projects", &parents);
        assert_eq!(path, "/admin/v1/orgs/{org_id}/projects");
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
            Some("w1".into()),
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
            Some("w1".into()),
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
        let spec = openapi_spec(&[test_resource()], "/admin/v1");
        assert_eq!(spec["openapi"], "3.0.0");
        assert!(spec["paths"]["/admin/v1/widgets"]["get"].is_object());
        assert!(spec["paths"]["/admin/v1/widgets"]["post"].is_object());
        assert!(spec["paths"]["/admin/v1/widgets/{id}"]["get"].is_object());
        assert!(spec["paths"]["/admin/v1/widgets/{id}"]["delete"].is_object());
    }

    #[test]
    fn openapi_spec_empty() {
        let spec = openapi_spec(&[], "/v1");
        assert_eq!(spec["openapi"], "3.0.0");
        assert!(spec["paths"].as_object().unwrap().is_empty());
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
                .map(|i| serde_json::json!({"name": format!("w{i}")}))
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
            path: "/admin/v1/widget".into(),
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
}
