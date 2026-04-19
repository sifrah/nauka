//! Uniform API error type. One `IntoResponse` impl maps every
//! variant to an HTTP status + a JSON body whose `error` field is
//! the `event_name` the rest of Nauka already uses for structured
//! logging (see #337). No per-handler `match err { ... }` blocks.

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use nauka_core::NaukaError;

#[derive(Debug, thiserror::Error)]
pub enum NaukaApiError {
    #[error("unauthorized")]
    Unauthorized,
    #[error("not found: {0}")]
    NotFound(String),
    #[error("{0}")]
    Conflict(String),
    #[error("{0}")]
    Validation(String),
    #[error("invalid json: {0}")]
    Json(String),
    #[error(transparent)]
    State(#[from] nauka_state::StateError),
    #[error("internal: {0}")]
    Internal(String),
}

impl NaukaError for NaukaApiError {
    fn event_name(&self) -> &'static str {
        match self {
            NaukaApiError::Unauthorized => "api.unauthorized",
            NaukaApiError::NotFound(_) => "api.not_found",
            NaukaApiError::Conflict(_) => "api.conflict",
            NaukaApiError::Validation(_) => "api.validation",
            NaukaApiError::Json(_) => "api.bad_json",
            // Forward the upstream event_name so `state.db` /
            // `state.raft` logs stay recognizable from the
            // caller-facing error code alike.
            NaukaApiError::State(e) => e.event_name(),
            NaukaApiError::Internal(_) => "api.internal",
        }
    }
}

impl IntoResponse for NaukaApiError {
    fn into_response(self) -> Response {
        let status = match &self {
            NaukaApiError::Unauthorized => StatusCode::UNAUTHORIZED,
            NaukaApiError::NotFound(_) => StatusCode::NOT_FOUND,
            NaukaApiError::Conflict(_) => StatusCode::CONFLICT,
            NaukaApiError::Validation(_) => StatusCode::UNPROCESSABLE_ENTITY,
            NaukaApiError::Json(_) => StatusCode::BAD_REQUEST,
            NaukaApiError::State(_) => StatusCode::INTERNAL_SERVER_ERROR,
            NaukaApiError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let code = self.event_name();
        let body = Json(serde_json::json!({
            "error": code,
            "message": self.to_string(),
        }));
        (status, body).into_response()
    }
}
