//! `Env` — third level of the IAM scope tree (e.g. `production` or
//! `staging` inside a `Project`).
//!
//! Authorization delegates to the owning `Project` via `scope_by =
//! "project"`. `fn::iam::can` walks `$this.project.org.owner` in one
//! expression — SurrealDB dereferences record links transparently.

use nauka_core::resource::{Ref, SurrealValue};
use nauka_core_macros::resource;
use serde::{Deserialize, Serialize};

use super::project::Project;

#[resource(table = "env", scope = "cluster", scope_by = "project")]
#[derive(Serialize, Deserialize, SurrealValue, Debug, Clone)]
pub struct Env {
    #[id]
    pub uid: String,
    pub slug: String,
    pub project: Ref<Project>,
    pub display_name: String,
}
