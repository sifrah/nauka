//! `Role` — a named bundle of permissions. Three kinds:
//!
//! - **primitive** (`owner` / `editor` / `viewer`) — seeded at boot,
//!   not org-scoped. Immutable.
//! - **predefined** — nauka-provided richer bundles (e.g.
//!   `vm.operator`). Global, not org-scoped. Future work.
//! - **custom** — created by an org's owner to bundle a subset of
//!   permissions specific to their org.
//!
//! PERMISSIONS rule: primitive / predefined are globally visible;
//! custom roles are scoped to their owning `Org`. The daemon's
//! `$auth = NONE` bootstrap path bypasses the check so the seeder
//! can write records regardless of session state.

use nauka_core::resource::{Ref, SurrealValue};
use nauka_core_macros::resource;
use serde::{Deserialize, Serialize};

use super::org::Org;
use super::permission::Permission;

#[resource(
    table = "role",
    scope = "cluster",
    // Three visibility regimes collapsed into one clause:
    //   - `$auth = NONE` — bootstrap / state machine.
    //   - Non-custom roles — visible to any authenticated principal.
    //   - Custom roles — only the owning org's owner.
    // Custom roles without an `org` are rejected by application
    // code (`ops::create_custom_role`); the DDL only enforces the
    // visibility rule.
    permissions = "$auth = NONE OR $this.kind != 'custom' \
                   OR ($this.org != NONE AND $this.org.owner = $auth.id)"
)]
#[derive(Serialize, Deserialize, SurrealValue, Debug, Clone)]
pub struct Role {
    /// Role slug. Primitive roles use their bare kind name
    /// (`"owner"`, `"editor"`, `"viewer"`). Custom roles use a
    /// `"<org-slug>-<slug>"` format to disambiguate across orgs.
    #[id]
    pub slug: String,
    /// `"primitive"` | `"predefined"` | `"custom"`.
    pub kind: String,
    /// `Some(org)` for custom roles, `None` for primitive /
    /// predefined.
    pub org: Option<Ref<Org>>,
    /// Permissions granted by this role. Evaluated by
    /// `fn::iam::can` against `<table>.<action>` names.
    pub permissions: Vec<Ref<Permission>>,
}
