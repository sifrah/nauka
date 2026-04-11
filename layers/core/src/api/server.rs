//! API server with graceful shutdown.

use axum::Router;
use std::net::SocketAddr;

use super::middleware;
use super::route_gen::build_router;
use crate::resource::ResourceRegistration;

/// API server configuration.
#[derive(Debug, Clone)]
pub struct ApiConfig {
    /// API bind address (serves both platform and cloud routes).
    pub platform_addr: SocketAddr,
    /// URL prefix for platform routes (hypervisor infrastructure).
    pub platform_prefix: String,
    /// URL prefix for cloud routes (org, vpc, vm, etc.).
    pub cloud_prefix: String,
    /// Rate limit: max requests per window.
    pub rate_limit_requests: u64,
    /// Rate limit: window in seconds.
    pub rate_limit_window_secs: u64,
    /// Graceful shutdown drain timeout in seconds.
    pub shutdown_timeout_secs: u64,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            platform_addr: "127.0.0.1:8443".parse().unwrap(),
            platform_prefix: "/platform/v1".to_string(),
            cloud_prefix: "/cloud/v1".to_string(),
            rate_limit_requests: 1000,
            rate_limit_window_secs: 60,
            shutdown_timeout_secs: 30,
        }
    }
}

/// The API server.
pub struct ApiServer {
    pub config: ApiConfig,
    router: Router,
}

impl ApiServer {
    /// Create a new API server.
    pub fn new(
        config: ApiConfig,
        platform_resources: Vec<ResourceRegistration>,
        cloud_resources: Vec<ResourceRegistration>,
    ) -> Self {
        let router = build_api_router(&config, platform_resources, cloud_resources);
        Self { config, router }
    }

    pub fn router(&self) -> &Router {
        &self.router
    }

    /// Run the API server with graceful shutdown on SIGTERM/SIGINT.
    pub async fn run(self) -> Result<(), std::io::Error> {
        let listener = tokio::net::TcpListener::bind(self.config.platform_addr).await?;
        tracing::info!(
            addr = %self.config.platform_addr,
            shutdown_timeout = self.config.shutdown_timeout_secs,
            "API server listening"
        );

        axum::serve(listener, self.router)
            .with_graceful_shutdown(shutdown_signal())
            .await
    }
}

fn build_api_router(
    config: &ApiConfig,
    platform_resources: Vec<ResourceRegistration>,
    cloud_resources: Vec<ResourceRegistration>,
) -> Router {
    use super::route_gen::combined_openapi_spec;

    // Build combined OpenAPI spec
    let spec = combined_openapi_spec(
        &platform_resources,
        &config.platform_prefix,
        &cloud_resources,
        &config.cloud_prefix,
    );

    let platform_routes = build_router(platform_resources, &config.platform_prefix);
    let cloud_routes = build_router(cloud_resources, &config.cloud_prefix);

    let health = Router::new().route(
        "/health",
        axum::routing::get(|| async {
            axum::Json(serde_json::json!({
                "status": "ok",
                "version": env!("CARGO_PKG_VERSION"),
            }))
        }),
    );

    // #7: OpenAPI spec endpoint
    let openapi = Router::new().route(
        "/openapi.json",
        axum::routing::get(move || {
            let spec = spec.clone();
            async move { axum::Json(spec) }
        }),
    );

    let fallback = || async {
        let body = axum::Json(serde_json::json!({
            "error": {
                "code": "NotFound",
                "message": "The requested endpoint does not exist.",
            }
        }));
        (axum::http::StatusCode::NOT_FOUND, body)
    };

    // Rate limiter
    let limiter =
        middleware::RateLimiter::new(config.rate_limit_requests, config.rate_limit_window_secs);

    Router::new()
        .merge(platform_routes)
        .merge(cloud_routes)
        .merge(health)
        .merge(openapi)
        .fallback(fallback)
        .layer(tower_http::trace::TraceLayer::new_for_http())
        .layer(tower_http::cors::CorsLayer::permissive())
        .layer(tower_http::limit::RequestBodyLimitLayer::new(1024 * 1024)) // 1MB
        .layer(axum::middleware::from_fn(middleware::security_headers))
        .layer(axum::middleware::from_fn(
            middleware::require_json_content_type,
        ))
        .layer(axum::middleware::from_fn_with_state(
            limiter,
            middleware::rate_limit,
        ))
        .layer(axum::middleware::from_fn(middleware::request_id))
        .layer(axum::middleware::from_fn(middleware::version_header))
}

/// #8: Wait for SIGTERM or SIGINT.
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => { tracing::info!("received Ctrl+C, shutting down"); }
        _ = terminate => { tracing::info!("received SIGTERM, shutting down"); }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resource::*;
    use axum::body::Body;
    use http::Request;
    use tower::ServiceExt;

    fn test_resource() -> ResourceRegistration {
        let def = ResourceDef {
            identity: ResourceIdentity {
                kind: "thing",
                cli_name: "thing",
                plural: "things",
                description: "Test thing",
                aliases: &[],
            },
            scope: ScopeDef::global(),
            schema: ResourceSchema::new(),
            operations: vec![
                OperationDef::list(),
                OperationDef::get(),
                OperationDef::delete(),
                OperationDef::action("ping", "Ping the thing"),
            ],
            presentation: PresentationDef::none(),
        };

        let handler: HandlerFn = Box::new(|req| {
            Box::pin(async move {
                match req.operation.as_str() {
                    "list" => Ok(OperationResponse::ResourceList(vec![
                        serde_json::json!({"id": "t1-id", "name": "t1"}),
                    ])),
                    "get" => Ok(OperationResponse::Resource(
                        serde_json::json!({"id": "get-id", "name": req.name.unwrap_or_default()}),
                    )),
                    "delete" => Ok(OperationResponse::Message("deleted".into())),
                    "ping" => Ok(OperationResponse::Message("pong".into())),
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
    fn default_config() {
        let c = ApiConfig::default();
        assert_eq!(c.platform_addr.port(), 8443);
        assert_eq!(c.rate_limit_requests, 1000);
    }

    #[test]
    fn server_builds() {
        let _server = ApiServer::new(ApiConfig::default(), vec![], vec![test_resource()]);
    }

    #[tokio::test]
    async fn health_endpoint() {
        let server = ApiServer::new(ApiConfig::default(), vec![], vec![test_resource()]);
        let req = Request::builder()
            .uri("/health")
            .body(Body::empty())
            .unwrap();
        let resp = server.router.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), 200);
    }

    #[tokio::test]
    async fn list_endpoint() {
        let server = ApiServer::new(ApiConfig::default(), vec![], vec![test_resource()]);
        let req = Request::builder()
            .uri("/cloud/v1/things")
            .body(Body::empty())
            .unwrap();
        let resp = server.router.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), 200);
    }

    // #11: GET and DELETE on same /{id} path
    #[tokio::test]
    async fn get_and_delete_same_path() {
        let server = ApiServer::new(ApiConfig::default(), vec![], vec![test_resource()]);

        // GET
        let req = Request::builder()
            .uri("/cloud/v1/things/thing-one")
            .body(Body::empty())
            .unwrap();
        let resp = server.router.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), 200);

        // DELETE
        let req = Request::builder()
            .method("DELETE")
            .uri("/cloud/v1/things/thing-one")
            .body(Body::empty())
            .unwrap();
        let resp = server.router.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), 204);
    }

    #[tokio::test]
    async fn action_endpoint() {
        let server = ApiServer::new(ApiConfig::default(), vec![], vec![test_resource()]);
        let req = Request::builder()
            .method("POST")
            .uri("/cloud/v1/things/ping")
            .header("content-type", "application/json")
            .body(Body::from("{}"))
            .unwrap();
        let resp = server.router.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), 200);
    }

    // #5: Request ID header
    #[tokio::test]
    async fn response_has_request_id() {
        let server = ApiServer::new(ApiConfig::default(), vec![], vec![test_resource()]);
        let req = Request::builder()
            .uri("/health")
            .body(Body::empty())
            .unwrap();
        let resp = server.router.clone().oneshot(req).await.unwrap();
        assert!(resp.headers().contains_key("x-request-id"));
    }

    // #6: Version header
    #[tokio::test]
    async fn response_has_version() {
        let server = ApiServer::new(ApiConfig::default(), vec![], vec![test_resource()]);
        let req = Request::builder()
            .uri("/health")
            .body(Body::empty())
            .unwrap();
        let resp = server.router.clone().oneshot(req).await.unwrap();
        assert!(resp.headers().contains_key("x-nauka-version"));
    }

    // #7: OpenAPI endpoint
    #[tokio::test]
    async fn openapi_endpoint() {
        let server = ApiServer::new(ApiConfig::default(), vec![], vec![test_resource()]);
        let req = Request::builder()
            .uri("/openapi.json")
            .body(Body::empty())
            .unwrap();
        let resp = server.router.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), 200);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let spec: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(spec["openapi"], "3.0.0");
        assert!(spec["paths"]["/cloud/v1/things"]["get"].is_object());
        assert!(spec["components"]["schemas"].is_object());
    }

    #[tokio::test]
    async fn not_found_route() {
        let server = ApiServer::new(ApiConfig::default(), vec![], vec![test_resource()]);
        let req = Request::builder()
            .uri("/cloud/v1/nonexistent")
            .body(Body::empty())
            .unwrap();
        let resp = server.router.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), 404);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["error"]["code"], "NotFound");
    }
}
