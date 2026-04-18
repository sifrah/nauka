//! IAM resources — see epic #344.
//!
//! **IAM-1** (#345): [`User`] + `DEFINE ACCESS user` for record-type
//! authentication.
//!
//! **IAM-2** (#346): [`Org`], [`Project`], [`Env`] scope tree.
//! `Org.owner` anchors the chain; `Project.org` and `Env.project`
//! pull PERMISSIONS in via `scope_by`. The per-record authorization
//! rule lives in [`crate::can::IAM_CAN_DDL`] so every resource shares
//! one decision point.

mod api_token;
mod audit_event;
mod env;
mod org;
mod permission;
mod project;
mod role;
mod role_binding;
mod service_account;
mod user;

pub use api_token::ApiToken;
pub use audit_event::AuditEvent;
pub use env::Env;
pub use org::Org;
pub use permission::Permission;
pub use project::Project;
pub use role::Role;
pub use role_binding::RoleBinding;
pub use service_account::ServiceAccount;
pub use user::User;
