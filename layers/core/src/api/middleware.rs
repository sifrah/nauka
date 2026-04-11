//! API middleware: rate limiting, request ID, versioning.

use axum::extract::Request;
use axum::http::HeaderValue;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

// ═══════════════════════════════════════════════════
// 5. Request ID
// ═══════════════════════════════════════════════════

static REQUEST_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Middleware that injects `X-Request-Id` header into every response.
pub async fn request_id(req: Request, next: Next) -> Response {
    let id = REQUEST_COUNTER.fetch_add(1, Ordering::Relaxed);
    let request_id = format!("req-{id:012x}");

    let mut resp = next.run(req).await;
    resp.headers_mut().insert(
        "x-request-id",
        HeaderValue::from_str(&request_id).unwrap_or_else(|_| HeaderValue::from_static("unknown")),
    );
    resp
}

// ═══════════════════════════════════════════════════
// 6. Version header
// ═══════════════════════════════════════════════════

/// Middleware that injects `X-Nauka-Version` header.
pub async fn version_header(req: Request, next: Next) -> Response {
    let mut resp = next.run(req).await;
    resp.headers_mut().insert(
        "x-nauka-version",
        HeaderValue::from_static(env!("CARGO_PKG_VERSION")),
    );
    resp
}

// ═══════════════════════════════════════════════════
// 2. Rate limiting (simple token bucket)
// ═══════════════════════════════════════════════════

/// Simple in-memory rate limiter.
#[derive(Clone)]
pub struct RateLimiter {
    max_requests: u64,
    window_secs: u64,
    state: Arc<RateLimitState>,
}

struct RateLimitState {
    count: AtomicU64,
    window_start: std::sync::Mutex<Instant>,
}

impl RateLimiter {
    /// Create a new rate limiter.
    pub fn new(max_requests: u64, window_secs: u64) -> Self {
        Self {
            max_requests,
            window_secs,
            state: Arc::new(RateLimitState {
                count: AtomicU64::new(0),
                window_start: std::sync::Mutex::new(Instant::now()),
            }),
        }
    }

    /// Check if a request is allowed. Returns remaining requests.
    /// Check if a request is allowed. Returns remaining requests or false if rejected.
    #[allow(clippy::result_unit_err)]
    pub fn check(&self) -> Result<u64, ()> {
        let mut start = self.state.window_start.lock().unwrap();
        let elapsed = start.elapsed().as_secs();

        // Reset window if expired
        if elapsed >= self.window_secs {
            *start = Instant::now();
            self.state.count.store(1, Ordering::Relaxed);
            return Ok(self.max_requests - 1);
        }

        let count = self.state.count.fetch_add(1, Ordering::Relaxed) + 1;
        if count > self.max_requests {
            Err(())
        } else {
            Ok(self.max_requests - count)
        }
    }

    /// Reset the limiter (for testing).
    pub fn reset(&self) {
        self.state.count.store(0, Ordering::Relaxed);
        *self.state.window_start.lock().unwrap() = Instant::now();
    }
}

// ═══════════════════════════════════════════════════
// 7. Content-Type validation
// ═══════════════════════════════════════════════════

/// Middleware that validates Content-Type on POST/PATCH/PUT requests.
/// Returns 415 Unsupported Media Type if Content-Type is not application/json.
pub async fn require_json_content_type(req: Request, next: Next) -> Response {
    if matches!(*req.method(), axum::http::Method::POST | axum::http::Method::PATCH | axum::http::Method::PUT) {
        let content_type = req.headers()
            .get(axum::http::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");

        if !content_type.starts_with("application/json") {
            let body = axum::Json(serde_json::json!({
                "error": {
                    "code": "UnsupportedMediaType",
                    "message": "Content-Type must be application/json",
                }
            }));
            return (axum::http::StatusCode::UNSUPPORTED_MEDIA_TYPE, body).into_response();
        }
    }
    next.run(req).await
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Rate limiter ──

    #[test]
    fn rate_limiter_allows_within_window() {
        let rl = RateLimiter::new(5, 60);
        for _ in 0..5 {
            assert!(rl.check().is_ok());
        }
    }

    #[test]
    fn rate_limiter_rejects_over_limit() {
        let rl = RateLimiter::new(3, 60);
        assert!(rl.check().is_ok());
        assert!(rl.check().is_ok());
        assert!(rl.check().is_ok());
        assert!(rl.check().is_err());
    }

    #[test]
    fn rate_limiter_reset() {
        let rl = RateLimiter::new(2, 60);
        assert!(rl.check().is_ok());
        assert!(rl.check().is_ok());
        assert!(rl.check().is_err());
        rl.reset();
        assert!(rl.check().is_ok());
    }

    #[test]
    fn rate_limiter_returns_remaining() {
        let rl = RateLimiter::new(5, 60);
        assert_eq!(rl.check().unwrap(), 4);
        assert_eq!(rl.check().unwrap(), 3);
    }

    // ── Request counter ──

    #[test]
    fn request_counter_increments() {
        let a = REQUEST_COUNTER.load(Ordering::Relaxed);
        REQUEST_COUNTER.fetch_add(1, Ordering::Relaxed);
        let b = REQUEST_COUNTER.load(Ordering::Relaxed);
        assert_eq!(b, a + 1);
    }

    // ── Content-Type validation ──

    #[tokio::test]
    async fn content_type_rejects_non_json() {
        use axum::{body::Body, routing::post, Router};
        use http::Request;
        use tower::ServiceExt;

        let app = Router::new()
            .route("/test", post(|| async { "ok" }))
            .layer(axum::middleware::from_fn(require_json_content_type));

        let req = Request::builder()
            .method("POST")
            .uri("/test")
            .header("content-type", "text/plain")
            .body(Body::from("hello"))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), 415);
    }

    #[tokio::test]
    async fn content_type_allows_json() {
        use axum::{body::Body, routing::post, Router};
        use http::Request;
        use tower::ServiceExt;

        let app = Router::new()
            .route("/test", post(|| async { "ok" }))
            .layer(axum::middleware::from_fn(require_json_content_type));

        let req = Request::builder()
            .method("POST")
            .uri("/test")
            .header("content-type", "application/json")
            .body(Body::from("{}"))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), 200);
    }

    #[tokio::test]
    async fn content_type_skips_get() {
        use axum::{body::Body, routing::get, Router};
        use http::Request;
        use tower::ServiceExt;

        let app = Router::new()
            .route("/test", get(|| async { "ok" }))
            .layer(axum::middleware::from_fn(require_json_content_type));

        let req = Request::builder()
            .uri("/test")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), 200);
    }
}
