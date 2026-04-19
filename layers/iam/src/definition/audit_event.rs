//! `AuditEvent` — append-only, hash-chained log of every
//! authorization-relevant mutation in the cluster.
//!
//! Writes come only from the daemon's `audit::audit_write` helper,
//! which pulls the previous event's hash, builds the new event
//! record with `prev_hash` + a fresh `hash`, and routes the write
//! through `Writer::create` so it replicates via Raft alongside the
//! mutation it describes.
//!
//! ## PERMISSIONS caveat
//!
//! IAM-5 uses the blanket `$auth = NONE` rule to restrict writes to
//! the Raft state-machine path. User sessions still can't CREATE /
//! UPDATE / DELETE rows (Raft applies run without `$auth`, but any
//! user-originated write would have a session and hit the
//! `WHERE false` arm). SELECT is allowed from any authenticated
//! principal for now — IAM-6 will narrow that to per-org filtering
//! once field-level PERMISSIONS land.

use nauka_core::resource::{Datetime, Ref, SurrealValue};
use nauka_core_macros::resource;
use serde::{Deserialize, Serialize};

use super::org::Org;

#[resource(
    table = "audit_event",
    scope = "cluster",
    // Read-only API surface — writes come only from the
    // daemon's hash-chained `audit::audit_write` helper. Exposing
    // create / update / delete over HTTP would let a caller break
    // the chain's invariants.
    api_verbs = "get, list",
    api_path = "/v1/audit-events",
    // Any non-NONE session is rejected for CREATE / UPDATE /
    // DELETE — audit rows arrive only through the state machine.
    // SELECT currently follows the same rule; IAM-6 will split
    // per-verb clauses and open read to authenticated callers.
    permissions = "$auth = NONE"
)]
#[derive(Serialize, Deserialize, SurrealValue, Debug, Clone)]
pub struct AuditEvent {
    /// ULID-shaped identifier. Monotonic-ish (timestamp-prefixed)
    /// so listing newest-first works by record id. The full
    /// 26-char Crockford ULID isn't necessary here — we use our
    /// own 24-char form (`<12 hex ms><12 hex random>`) that sorts
    /// the same way and stays in the snake-id-safe alphabet.
    #[id]
    pub uid: String,
    /// Principal that initiated the mutation — a full record id
    /// string (`user:alice@example.com` / `service_account:acme-ci`)
    /// so we can later filter across principal kinds without
    /// polymorphic refs.
    pub actor: String,
    /// `"create"`, `"update"`, `"delete"`.
    pub action: String,
    /// Record id of the subject row, e.g. `org:acme`.
    pub target: String,
    /// Org the mutation lives under, for per-org log filtering.
    /// `None` for cluster-level subjects (hypervisors, mesh) where
    /// org isn't meaningful yet.
    pub org: Option<Ref<Org>>,
    /// `"success"` if the write committed, `"failure"` otherwise.
    /// IAM-5 only records successes — failure logging belongs to
    /// IAM-5b + IAM-9 governance.
    pub outcome: String,
    /// Hash of the preceding audit event. First event in the chain
    /// uses `"0"` repeated — same width as a hex SHA-256 so the
    /// format stays uniform.
    pub prev_hash: String,
    /// `sha256(prev_hash || canonical_json({uid, actor, action,
    /// target, outcome, at}))`. Hex-encoded, lowercase.
    pub hash: String,
    /// Wall-clock timestamp at write time. Set on the Raft leader
    /// so every follower applies the byte-identical value.
    pub at: Datetime,
}
