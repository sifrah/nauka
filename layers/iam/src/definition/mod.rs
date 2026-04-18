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

mod env;
mod org;
mod project;
mod user;

pub use env::Env;
pub use org::Org;
pub use project::Project;
pub use user::User;
