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

use nauka_hypervisor::{Hypervisor, MeshRecord};
use nauka_iam::{
    ActiveSession, ApiToken, AuditEvent, Env, Org, Permission, Project, Role, RoleBinding,
    ServiceAccount, User,
};
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
        let http = HttpClient::builder().build().map_err(ClientError::Http)?;
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

    pub fn mesh(&self) -> MeshClient<'_> {
        MeshClient { client: self }
    }

    pub fn org(&self) -> OrgClient<'_> {
        OrgClient { client: self }
    }

    pub fn project(&self) -> ProjectClient<'_> {
        ProjectClient { client: self }
    }

    pub fn env(&self) -> EnvClient<'_> {
        EnvClient { client: self }
    }

    pub fn role(&self) -> RoleClient<'_> {
        RoleClient { client: self }
    }

    pub fn role_binding(&self) -> RoleBindingClient<'_> {
        RoleBindingClient { client: self }
    }

    pub fn permission(&self) -> PermissionClient<'_> {
        PermissionClient { client: self }
    }

    pub fn service_account(&self) -> ServiceAccountClient<'_> {
        ServiceAccountClient { client: self }
    }

    pub fn api_token(&self) -> ApiTokenClient<'_> {
        ApiTokenClient { client: self }
    }

    pub fn active_session(&self) -> ActiveSessionClient<'_> {
        ActiveSessionClient { client: self }
    }

    pub fn audit_event(&self) -> AuditEventClient<'_> {
        AuditEventClient { client: self }
    }

    pub fn user(&self) -> UserClient<'_> {
        UserClient { client: self }
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
            .get(&format!("/v1/hypervisors/{}", encode_path_segment(id)))
            .await
    }

    pub async fn list(&self) -> Result<Vec<Hypervisor>, ClientError> {
        self.client.get("/v1/hypervisors").await
    }

    pub async fn update(&self, id: &str, body: &Hypervisor) -> Result<Hypervisor, ClientError> {
        self.client
            .patch(
                &format!("/v1/hypervisors/{}", encode_path_segment(id)),
                body,
            )
            .await
    }

    pub async fn delete(&self, id: &str) -> Result<(), ClientError> {
        self.client
            .delete_empty(&format!("/v1/hypervisors/{}", encode_path_segment(id)))
            .await
    }
}

/// Mesh sub-client. Read-only surface (`api_verbs = "get, list"` on
/// the resource) — creation flows through `nauka hypervisor init`,
/// not through the API. Encrypted/secret fields
/// (`private_key`, `ca_key`, `tls_key`, `peering_pin`) are masked
/// server-side via `#[serde(skip)]`; the deserialised values the
/// SDK returns populate those fields with their `Default` values,
/// which is correct — a remote caller has no business handling the
/// ciphertexts.
pub struct MeshClient<'a> {
    client: &'a Client,
}

impl MeshClient<'_> {
    pub async fn get(&self, id: &str) -> Result<MeshRecord, ClientError> {
        self.client
            .get(&format!("/v1/meshes/{}", encode_path_segment(id)))
            .await
    }

    pub async fn list(&self) -> Result<Vec<MeshRecord>, ClientError> {
        self.client.get("/v1/meshes").await
    }
}

/// Org sub-client. First IAM resource on the generated SDK surface
/// (342-C1). Identical shape to the Hypervisor client — the
/// generic `mount_crud::<Org>` handler on the server keeps the
/// wire format aligned by construction.
pub struct OrgClient<'a> {
    client: &'a Client,
}

impl OrgClient<'_> {
    pub async fn create(&self, body: &Org) -> Result<Org, ClientError> {
        self.client.post("/v1/orgs", body).await
    }

    pub async fn get(&self, id: &str) -> Result<Org, ClientError> {
        self.client
            .get(&format!("/v1/orgs/{}", encode_path_segment(id)))
            .await
    }

    pub async fn list(&self) -> Result<Vec<Org>, ClientError> {
        self.client.get("/v1/orgs").await
    }

    pub async fn update(&self, id: &str, body: &Org) -> Result<Org, ClientError> {
        self.client
            .patch(&format!("/v1/orgs/{}", encode_path_segment(id)), body)
            .await
    }

    pub async fn delete(&self, id: &str) -> Result<(), ClientError> {
        self.client
            .delete_empty(&format!("/v1/orgs/{}", encode_path_segment(id)))
            .await
    }
}

/// Boilerplate-free CRUD sub-client generator. Every sub-client
/// exposes all five verbs; the server decides which ones actually
/// work (verbs the resource opted out of return 405).
///
/// Client-side we don't hide the methods — a hidden method would
/// force the SDK to know the `api_verbs` policy, which is the
/// server's job. Calling a verb the server doesn't expose just
/// bubbles up `ClientError::Server { status: 405 }` like any
/// other mismatched request.
macro_rules! impl_crud_client {
    ($client:ident, $res:path, $prefix:expr) => {
        pub struct $client<'a> {
            client: &'a Client,
        }

        impl $client<'_> {
            pub async fn create(&self, body: &$res) -> Result<$res, ClientError> {
                self.client.post($prefix, body).await
            }

            pub async fn get(&self, id: &str) -> Result<$res, ClientError> {
                self.client
                    .get(&format!("{}/{}", $prefix, encode_path_segment(id)))
                    .await
            }

            pub async fn list(&self) -> Result<Vec<$res>, ClientError> {
                self.client.get($prefix).await
            }

            pub async fn update(&self, id: &str, body: &$res) -> Result<$res, ClientError> {
                self.client
                    .patch(&format!("{}/{}", $prefix, encode_path_segment(id)), body)
                    .await
            }

            pub async fn delete(&self, id: &str) -> Result<(), ClientError> {
                self.client
                    .delete_empty(&format!("{}/{}", $prefix, encode_path_segment(id)))
                    .await
            }
        }
    };
}

impl_crud_client!(ProjectClient, Project, "/v1/projects");
impl_crud_client!(EnvClient, Env, "/v1/envs");
impl_crud_client!(RoleClient, Role, "/v1/roles");
impl_crud_client!(RoleBindingClient, RoleBinding, "/v1/role-bindings");
impl_crud_client!(PermissionClient, Permission, "/v1/permissions");
impl_crud_client!(ServiceAccountClient, ServiceAccount, "/v1/service-accounts");
impl_crud_client!(ApiTokenClient, ApiToken, "/v1/api-tokens");
impl_crud_client!(ActiveSessionClient, ActiveSession, "/v1/sessions");
impl_crud_client!(AuditEventClient, AuditEvent, "/v1/audit-events");
impl_crud_client!(UserClient, User, "/v1/users");

/// Percent-encode a path segment (RFC 3986 unreserved set kept
/// as-is, everything else `%XX`). Used when the resource id
/// contains `/`, `:`, or other characters that would otherwise
/// break the URL routing — Mesh IDs like `fdaa:bbbb:cccc::/48`
/// are the motivating case.
pub fn encode_path_segment(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '.' | '_' | '~' => out.push(c),
            _ => {
                let mut buf = [0u8; 4];
                for &byte in c.encode_utf8(&mut buf).as_bytes() {
                    out.push_str(&format!("%{byte:02X}"));
                }
            }
        }
    }
    out
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
