//! `Permission` — one record per `<table>.<verb>` the cluster
//! recognises. Not edited by users; the `crate::seed` module
//! auto-populates this table at boot from `ALL_RESOURCES`.
//!
//! Why a resource and not a static registry? `Role.permissions` is
//! `Vec<Ref<Permission>>`, and SurrealDB's DDL enforces the reference
//! constraint at the database layer. Having permissions as actual
//! records also makes `nauka permission list` trivial — no special
//! CLI wiring around a compile-time slice.
//!
//! PERMISSIONS: readable by any authenticated principal. Modifying
//! the catalog is reserved to the daemon's bootstrap path, which
//! runs without `$auth` — covered by the `$auth = NONE` arm.

use nauka_core::resource::SurrealValue;
use nauka_core_macros::resource;
use serde::{Deserialize, Serialize};

#[resource(
    table = "permission",
    scope = "cluster",
    permissions = "$auth = NONE OR $auth != NONE"
)]
#[derive(Serialize, Deserialize, SurrealValue, Debug, Clone)]
pub struct Permission {
    /// Permission identifier, e.g. `"org.select"` or `"vm.start"`.
    /// Canonical form: `<table>.<verb>`.
    #[id]
    pub name: String,
    /// The table this permission applies to. Same value as the
    /// `ResourceDescriptor.table` that seeded it.
    pub table: String,
    /// The verb — `"select"`, `"create"`, `"update"`, `"delete"`,
    /// or a `custom_actions` entry.
    pub verb: String,
}
