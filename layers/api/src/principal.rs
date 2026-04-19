//! Authentication extractor + `require_auth` middleware.
//!
//! 342-A scope: require a non-empty Bearer token and forward the
//! raw JWT as the [`Principal`]. Signature validation + claim
//! extraction wire in once IAM integration lands (see 342-C).
//!
//! This keeps the middleware contract stable (headers in →
//! `Principal` in extensions → handler extractor), so tightening the
//! check later is a one-file change.

use axum::{
    extract::{FromRequestParts, Request},
    http::{header, request::Parts},
    middleware::Next,
    response::Response,
};

use crate::NaukaApiError;

/// The authenticated caller. Carried in request extensions so
/// handlers (and GraphQL resolvers) can receive it via an extractor
/// without touching the raw header.
#[derive(Debug, Clone)]
pub struct Principal {
    /// Raw JWT forwarded from `Authorization: Bearer …`. Not
    /// signature-verified in 342-A — IAM integration wires real
    /// validation in 342-C. Handlers should treat it as opaque.
    pub jwt: String,
}

/// Reject the request with 401 unless an `Authorization: Bearer …`
/// header is present and non-empty.
pub async fn require_auth(mut req: Request, next: Next) -> Result<Response, NaukaApiError> {
    let jwt = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string);

    match jwt {
        Some(token) => {
            req.extensions_mut().insert(Principal { jwt: token });
            Ok(next.run(req).await)
        }
        None => Err(NaukaApiError::Unauthorized),
    }
}

impl<S: Send + Sync> FromRequestParts<S> for Principal {
    type Rejection = NaukaApiError;

    async fn from_request_parts(parts: &mut Parts, _: &S) -> Result<Self, Self::Rejection> {
        parts
            .extensions
            .get::<Principal>()
            .cloned()
            // Middleware runs before the extractor; a missing
            // extension means the route somehow bypassed
            // `require_auth`. Treat it as an internal bug, not a
            // client error.
            .ok_or_else(|| {
                NaukaApiError::Internal(
                    "Principal extension missing — require_auth middleware not wired".into(),
                )
            })
    }
}
