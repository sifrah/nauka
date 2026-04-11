//! E2E test: resource framework generates CLI + API from a single definition.

use nauka_core::resource::*;

fn test_resource() -> ResourceRegistration {
    let def = ResourceDef::build("widget", "Test widget")
        .alias("w")
        .plural("widgets")
        .action("create", "Create a widget")
        .op(|op| {
            op.with_arg(OperationArg::required(
                "color",
                FieldDef::string("color", "Widget color"),
            ))
            .with_output(OutputKind::Resource)
        })
        .list()
        .get()
        .column("NAME", "name")
        .column("STATE", "state")
        .column("ID", "id")
        .empty_message("No widgets found.")
        .sort_by("name")
        .detail_section(
            None,
            vec![
                DetailField::new("Name", "name"),
                DetailField::new("ID", "id"),
                DetailField::new("State", "state"),
                DetailField::new("Created", "created_at").with_format(DisplayFormat::Timestamp),
            ],
        )
        .done();

    let handler: HandlerFn = Box::new(|req| {
        Box::pin(async move {
            match req.operation.as_str() {
                "list" => Ok(OperationResponse::ResourceList(vec![
                    serde_json::json!({"id": "wdg-001", "name": "w1", "state": "active"}),
                ])),
                "get" => Ok(OperationResponse::Resource(
                    serde_json::json!({"id": "wdg-001", "name": req.name.unwrap_or_default()}),
                )),
                "create" => Ok(OperationResponse::Resource(
                    serde_json::json!({"id": "wdg-002", "name": req.fields.get("name").cloned().unwrap_or_default()}),
                )),
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

#[test]
fn cli_generation() {
    let reg = test_resource();
    let cmd = generate_command(&reg.def);

    assert_eq!(cmd.get_name(), "widget");

    // Should have subcommands for all operations
    let subs: Vec<&str> = cmd.get_subcommands().map(|c| c.get_name()).collect();
    assert!(subs.contains(&"create"));
    assert!(subs.contains(&"list"));
    assert!(subs.contains(&"get"));
}

#[test]
fn api_route_generation() {
    let reg = test_resource();
    let routes = nauka_core::api::list_routes(&[reg], "/v1");

    let ops: Vec<&str> = routes.iter().map(|r| r.operation.as_str()).collect();
    assert!(ops.contains(&"create"));
    assert!(ops.contains(&"list"));
    assert!(ops.contains(&"get"));

    // List is GET /v1/widgets
    assert!(routes
        .iter()
        .any(|r| r.method == "GET" && r.path == "/v1/widgets"));
}

#[test]
fn openapi_spec_generation() {
    let reg = test_resource();
    let spec = nauka_core::api::openapi_spec(&[reg], "/v1");

    assert_eq!(spec["openapi"], "3.0.0");
    assert!(spec["paths"]["/v1/widgets"].is_object());
}

#[tokio::test]
async fn api_server_serves_routes() {
    use axum::body::Body;
    use http::Request;
    use nauka_core::api::{ApiConfig, ApiServer};
    use tower::ServiceExt;

    let server = ApiServer::new(ApiConfig::default(), vec![test_resource()], vec![]);

    // GET /admin/v1/widgets → list
    let req = Request::builder()
        .uri("/admin/v1/widgets")
        .body(Body::empty())
        .unwrap();
    let resp = server.admin_router().clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), 200);

    // GET /health
    let req = Request::builder()
        .uri("/health")
        .body(Body::empty())
        .unwrap();
    let resp = server.admin_router().clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), 200);
}

#[test]
fn registry_find_by_alias() {
    let mut registry = ResourceRegistry::new();
    registry.register(test_resource());

    assert!(registry.find("widget").is_some());
    assert!(registry.find("w").is_some());
    assert!(registry.find("unknown").is_none());
}
