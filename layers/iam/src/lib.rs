//! Nauka IAM layer — identity, authentication, authorization.
//!
//! IAM-1 scope (issue #344 epic, phase 1):
//!
//! - [`User`] resource — `scope = "cluster"`, replicated via Raft.
//!   Password stored as a PHC-encoded Argon2id hash
//!   (`$argon2id$v=19$…`). SurrealDB's `crypto::argon2::compare`
//!   verifies hashes produced by this layer's [`hash_password`].
//! - `#[access]`-emitted `DEFINE ACCESS user ON DATABASE TYPE RECORD`
//!   — the authoritative signin path. The SIGNUP clause is provided
//!   for single-node / dev usage only; production signups go through
//!   [`signup`] so the record lands on the Raft leader and replicates.
//! - [`signup`] / [`signin`] / [`Jwt`] — daemon-side helpers the IPC
//!   handlers call. The CLI talks to the daemon over the join-port
//!   TCP loopback channel and never touches the DB directly.
//! - [`token`] — file-backed JWT storage at
//!   `~/.config/nauka/token` (`mode 0600`).
//!
//! Later IAM phases (roles, bindings, audit, MFA, sessions, …) plug
//! in as new resources + accesses; the epic deliberately orders them
//! so the cluster stays usable after each phase merges.
#![deny(clippy::print_stdout, clippy::print_stderr)]

pub mod audit;
pub mod auth;
pub mod can;
pub mod definition;
pub mod error;
pub mod ops;
pub mod seed;
pub mod token;

pub use audit::{audit_write, list_audit, try_audit};
pub use auth::{decode_claims, hash_password, signin, signup, verify_password, Claims, Jwt};
pub use can::IAM_CAN_DDL;
pub use definition::{
    ApiToken, AuditEvent, Env, Org, Permission, Project, Role, RoleBinding, ServiceAccount, User,
};
pub use error::IamError;
pub use ops::{
    authenticate, bind_role, create_api_token, create_env, create_org, create_project,
    create_service_account, list_api_tokens, list_bindings, list_envs, list_orgs, list_projects,
    list_roles, list_service_accounts, parse_api_token, revoke_api_token, unbind_role, AuthContext,
    MintedToken,
};
pub use seed::bootstrap;
pub use token::{delete_token, load_token, save_token, token_path};
