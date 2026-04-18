//! `Org` — top-level tenancy unit. Anchors the IAM scope chain:
//! every `Project` points at an `Org`, every `Env` points at a
//! `Project`, and `fn::iam::can` walks up the chain to check the
//! `Org.owner` field.
//!
//! Why `slug` as `#[id]`: the slug is the stable identifier operators
//! type into CLI commands (`nauka org create --slug acme`). It
//! doubles as the record-id so `org:acme` appears in URLs, logs, and
//! audit events — easier to eyeball than a UUID.
//!
//! Why `permissions` and not `scope_by`: an `Org` IS its own scope.
//! `scope_by = "owner"` would emit `$this.owner.owner` at the
//! function call site, which is nonsense — `$auth.id` is already a
//! `User` record id. A flat `$this.owner = $auth.id` check is what
//! we actually want, and `permissions` expresses it directly.

use nauka_core::resource::{Ref, SurrealValue};
use nauka_core_macros::resource;
use serde::{Deserialize, Serialize};

use super::user::User;

#[resource(
    table = "org",
    scope = "cluster",
    // Two allowed paths:
    //   - `$auth = NONE` — the Raft state machine applying a write
    //     after the daemon's handler already authorized the caller
    //     in Rust. Background tasks also run this way.
    //   - `$auth.id = $this.owner` — a user-session query; the
    //     owner can do anything to their own org.
    // Other sessions (including impersonation attempts via record
    // access) are rejected.
    permissions = "$auth = NONE OR $this.owner = $auth.id"
)]
#[derive(Serialize, Deserialize, SurrealValue, Debug, Clone)]
pub struct Org {
    #[id]
    #[unique]
    pub slug: String,
    pub display_name: String,
    pub owner: Ref<User>,
}
