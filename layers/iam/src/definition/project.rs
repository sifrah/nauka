//! `Project` — second level of the IAM scope tree.
//!
//! Authorization delegates to the owning `Org` via `scope_by = "org"`
//! — the macro emits `PERMISSIONS FOR <verb> WHERE fn::iam::can(
//! '<verb>', $this.org)` for each CRUD verb, and the function walks
//! `$this.org.owner` to reach the authoritative owner.
//!
//! Why a UUID-shaped `#[id]` instead of slug: two orgs may both have
//! a `web` project — only `(org, slug)` is unique, not `slug` alone.
//! A synthetic id keeps the primary key single-column without
//! forcing compound-key gymnastics in SurrealDB.
//!
//! The Rust field is named `uid`, not `id`, to avoid shadowing
//! SurrealDB's implicit `id` column (always a record link). Every
//! resource in this codebase follows the same rule — see
//! `Hypervisor.public_key`, `User.email`, `Org.slug`.

use nauka_core::resource::{Ref, SurrealValue};
use nauka_core_macros::resource;
use serde::{Deserialize, Serialize};

use super::org::Org;

#[resource(table = "project", scope = "cluster", scope_by = "org")]
#[derive(Serialize, Deserialize, SurrealValue, Debug, Clone)]
pub struct Project {
    #[id]
    pub uid: String,
    pub slug: String,
    pub org: Ref<Org>,
    pub display_name: String,
}
