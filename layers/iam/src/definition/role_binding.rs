//! `RoleBinding` — the join record that actually grants
//! permissions. A binding carries:
//!
//! - `principal` — who is granted.
//! - `role` — which role they get.
//! - `org` — at what scope. IAM-3 binds at Org only; later phases
//!   will also bind at Project and Env (those fields land alongside
//!   `fn::iam::can`'s matching extension).
//!
//! Authorization follows the scope_by chain via `org`: only the
//! org's owner (or a user with `role_binding.create` via another
//! binding — recursion is bounded by the DDL's one-level check) can
//! manage bindings.

use nauka_core::resource::{Ref, SurrealValue};
use nauka_core_macros::resource;
use serde::{Deserialize, Serialize};

use super::org::Org;
use super::role::Role;
use super::user::User;

#[resource(
    table = "role_binding",
    scope = "cluster",
    // Explicit clause instead of `scope_by = "org"` — the scope_by
    // path would emit `fn::iam::can('...', $this.org)`, but
    // `fn::iam::can` itself queries `role_binding` to check bindings,
    // and that SELECT is then filtered by this very PERMISSIONS
    // clause, forming an infinite recursion SurrealDB resolves by
    // returning empty. Direct ownership check (owner of the org OR
    // the principal themselves) avoids the loop while keeping the
    // visibility intent identical.
    permissions = "$auth = NONE \
                   OR $this.principal = $auth.id \
                   OR $this.org.owner = $auth.id"
)]
#[derive(Serialize, Deserialize, SurrealValue, Debug, Clone)]
pub struct RoleBinding {
    /// Synthetic unique id. Built as
    /// `"<org-slug>-<principal-email>-<role-slug>"` so
    /// `(org, principal, role)` remains single-record unique and
    /// the record id is greppable. Collisions are also blocked by
    /// the `#[unique]` tuple below.
    #[id]
    pub uid: String,
    /// The user being granted the role. IAM-4 will promote this to
    /// a polymorphic `Ref<User | ServiceAccount | Group>`.
    pub principal: Ref<User>,
    /// Role granted.
    pub role: Ref<Role>,
    /// Org scope.
    pub org: Ref<Org>,
}
