//! `ActiveSession` — one row per successful signin. IAM-8 (#352)
//! ships the observability slice: operators can see who is signed
//! in from where and when the session was last active. Server-side
//! revocation + refresh-token reuse detection arrive in follow-ups
//! (IAM-8b / IAM-8c).
//!
//! The Raft state-machine path writes rows on every signin. The
//! owner can read their own rows via the table's PERMISSIONS
//! clause; cross-user visibility requires a role binding with the
//! right permission (not yet wired, but the rule will slot in
//! alongside IAM-9 audit filtering).

use nauka_core::resource::{Datetime, Ref, SurrealValue};
use nauka_core_macros::resource;
use serde::{Deserialize, Serialize};

use super::user::User;

#[resource(
    table = "active_session",
    scope = "cluster",
    // Owner-only SELECT (plus state-machine writes via the
    // `$auth = NONE` arm). No cross-user view in IAM-8 — listing
    // someone else's sessions would require an explicit permission
    // once RoleBinding catches up.
    permissions = "$auth = NONE OR $this.user = $auth.id",
    // Create happens at signin time on the leader, not over the
    // API. Delete = explicit revoke (future: `nauka session
    // revoke <uid>`). Update never makes sense.
    api_verbs = "get, list, delete"
)]
#[derive(Serialize, Deserialize, SurrealValue, Debug, Clone)]
pub struct ActiveSession {
    /// ULID-shaped so newest-first sort works without a secondary
    /// index — same scheme `AuditEvent` uses.
    #[id]
    pub uid: String,
    /// The user this session belongs to.
    pub user: Ref<User>,
    /// Peer IP the daemon saw when the JWT was minted. `"loopback"`
    /// when the signin came from the CLI on the same host.
    pub ip: String,
    /// Free-form client descriptor. Always `"cli"` today; the REST
    /// surface (ResourceDef #342) will populate it from the
    /// `User-Agent` header.
    pub user_agent: String,
    /// Bumped on every successful IPC call that uses the JWT.
    /// IAM-8 only writes it at signin time; future IAM-8b updates
    /// it from the `authenticate` helper once per request.
    pub last_active_at: Datetime,
}
