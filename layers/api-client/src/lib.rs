//! Nauka API SDK — typed HTTPS client that speaks the `layers/api`
//! REST surface. Designed to be the single source-of-truth for how
//! the CLI (and downstream integrators) call the daemon; no
//! `reqwest::post(...)` scattered across `bin/nauka`.
//!
//! 342-A ships one sub-client (`HypervisorClient`). 342-B adds
//! `MeshClient`; 342-C rolls out the full IAM surface.
//!
//! ## Layering
//!
//! The client reuses the resource structs from `nauka-hypervisor`
//! (and eventually `nauka-iam`) so server and client agree on
//! shape at compile time — no hand-maintained DTOs to drift.

use std::sync::Arc;

use nauka_hypervisor::Hypervisor;
use reqwest::header::AUTHORIZATION;
use reqwest::{Client as HttpClient, StatusCode};
use serde::de::DeserializeOwned;
use serde::Serialize;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("http: {0}")]
    Http(#[from] reqwest::Error),
    #[error("unauthorized (401)")]
    Unauthorized,
    #[error("not found (404): {0}")]
    NotFound(String),
    #[error("conflict (409): {0}")]
    Conflict(String),
    #[error("validation (422): {0}")]
    Validation(String),
    #[error("server error ({status}): {body}")]
    Server { status: u16, body: String },
    #[error("invalid url: {0}")]
    Url(String),
}

/// Top-level client. Clone cheaply — the underlying reqwest
/// [`Client`](reqwest::Client) is already `Arc<Inner>`.
#[derive(Clone)]
pub struct Client {
    inner: Arc<Inner>,
}

struct Inner {
    http: HttpClient,
    base_url: String,
    jwt: String,
}

impl Client {
    /// Build a client pointing at `base_url` (e.g. `https://localhost:4000`)
    /// that forwards `Authorization: Bearer <jwt>` on every request.
    ///
    /// TLS is rustls-backed by default (see the `reqwest` feature
    /// flags in Cargo.toml). Accepts self-signed certs in
    /// development via [`Self::danger_accept_invalid_certs`] — real
    /// mesh-CA verification plumbs through in 342-D.
    pub fn new(base_url: impl Into<String>, jwt: impl Into<String>) -> Result<Self, ClientError> {
        let http = HttpClient::builder()
            .build()
            .map_err(ClientError::Http)?;
        Ok(Self::from_parts(http, base_url.into(), jwt.into()))
    }

    /// Escape hatch for local/dev deployments where the mesh CA
    /// hasn't been wired into the CLI's trust store yet.
    pub fn danger_accept_invalid_certs(
        base_url: impl Into<String>,
        jwt: impl Into<String>,
    ) -> Result<Self, ClientError> {
        let http = HttpClient::builder()
            .danger_accept_invalid_certs(true)
            .build()
            .map_err(ClientError::Http)?;
        Ok(Self::from_parts(http, base_url.into(), jwt.into()))
    }

    fn from_parts(http: HttpClient, base_url: String, jwt: String) -> Self {
        Self {
            inner: Arc::new(Inner {
                http,
                base_url: base_url.trim_end_matches('/').to_string(),
                jwt,
            }),
        }
    }

    pub fn hypervisor(&self) -> HypervisorClient<'_> {
        HypervisorClient { client: self }
    }

    async fn get<T: DeserializeOwned>(&self, path: &str) -> Result<T, ClientError> {
        let url = format!("{}{}", self.inner.base_url, path);
        let resp = self
            .inner
            .http
            .get(&url)
            .header(AUTHORIZATION, format!("Bearer {}", self.inner.jwt))
            .send()
            .await?;
        parse_json(resp).await
    }

    async fn post<B: Serialize, T: DeserializeOwned>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<T, ClientError> {
        let url = format!("{}{}", self.inner.base_url, path);
        let resp = self
            .inner
            .http
            .post(&url)
            .header(AUTHORIZATION, format!("Bearer {}", self.inner.jwt))
            .json(body)
            .send()
            .await?;
        parse_json(resp).await
    }

    async fn patch<B: Serialize, T: DeserializeOwned>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<T, ClientError> {
        let url = format!("{}{}", self.inner.base_url, path);
        let resp = self
            .inner
            .http
            .patch(&url)
            .header(AUTHORIZATION, format!("Bearer {}", self.inner.jwt))
            .json(body)
            .send()
            .await?;
        parse_json(resp).await
    }

    async fn delete_empty(&self, path: &str) -> Result<(), ClientError> {
        let url = format!("{}{}", self.inner.base_url, path);
        let resp = self
            .inner
            .http
            .delete(&url)
            .header(AUTHORIZATION, format!("Bearer {}", self.inner.jwt))
            .send()
            .await?;
        status_only(resp).await
    }
}

/// Hypervisor sub-client. See 342-A for the contract; 342-B adds
/// sibling clients once more resources migrate to the generated
/// surface.
pub struct HypervisorClient<'a> {
    client: &'a Client,
}

impl HypervisorClient<'_> {
    pub async fn create(&self, body: &Hypervisor) -> Result<Hypervisor, ClientError> {
        self.client.post("/v1/hypervisors", body).await
    }

    pub async fn get(&self, id: &str) -> Result<Hypervisor, ClientError> {
        self.client
            .get(&format!("/v1/hypervisors/{id}"))
            .await
    }

    pub async fn list(&self) -> Result<Vec<Hypervisor>, ClientError> {
        self.client.get("/v1/hypervisors").await
    }

    pub async fn update(&self, id: &str, body: &Hypervisor) -> Result<Hypervisor, ClientError> {
        self.client
            .patch(&format!("/v1/hypervisors/{id}"), body)
            .await
    }

    pub async fn delete(&self, id: &str) -> Result<(), ClientError> {
        self.client
            .delete_empty(&format!("/v1/hypervisors/{id}"))
            .await
    }
}

async fn parse_json<T: DeserializeOwned>(resp: reqwest::Response) -> Result<T, ClientError> {
    let status = resp.status();
    if status.is_success() {
        return resp.json().await.map_err(ClientError::Http);
    }
    let body = resp.text().await.unwrap_or_default();
    Err(map_error(status, body))
}

async fn status_only(resp: reqwest::Response) -> Result<(), ClientError> {
    let status = resp.status();
    if status.is_success() {
        return Ok(());
    }
    let body = resp.text().await.unwrap_or_default();
    Err(map_error(status, body))
}

fn map_error(status: StatusCode, body: String) -> ClientError {
    match status {
        StatusCode::UNAUTHORIZED => ClientError::Unauthorized,
        StatusCode::NOT_FOUND => ClientError::NotFound(body),
        StatusCode::CONFLICT => ClientError::Conflict(body),
        StatusCode::UNPROCESSABLE_ENTITY => ClientError::Validation(body),
        _ => ClientError::Server {
            status: status.as_u16(),
            body,
        },
    }
}
