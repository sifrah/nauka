//! `User` resource + `DEFINE ACCESS user` — see IAM epic #344 / IAM-1.
//!
//! ## Why email is the `#[id]`
//!
//! Email is both the login identifier and a stable natural key — it
//! changes rarely (password reset / identity merge, not routine
//! operations). Using email as the record id means the SurrealDB
//! `DEFINE ACCESS` SIGNIN clause can locate a user by email without a
//! secondary index, and the JWT's record-id subject (`user:⟨…⟩`)
//! carries the same identifier the CLI uses for `--email`.
//!
//! ## Why SIGNUP is "dev-only"
//!
//! Our cluster resources must go through the Raft state machine so
//! every node applies the identical SurrealQL statement. SurrealDB's
//! built-in SIGNUP clause writes directly to the local KV, which
//! would silently drop-on-the-floor on non-leader nodes. Callers must
//! route signups through [`crate::signup`], which hashes the password
//! in Rust and uses `Writer::create`. The SIGNUP clause here exists
//! so `db.signup()` works in single-node integration tests and dev
//! REPLs.
//!
//! ## `password_hash` is hidden from user SELECTs (IAM-6)
//!
//! `#[hidden]` emits a field-level
//! `PERMISSIONS FOR select WHERE $auth = NONE`, so a record-level
//! session querying `user` sees `NONE` in the `password_hash`
//! column. DEFINE ACCESS SIGNIN still reads it because SurrealDB
//! runs that query with `$auth = NONE` (elevated) internally. The
//! `#[secret]` attribute — KMS/Vault-backed encryption at rest —
//! is a later phase.

use nauka_core::resource::{Datetime, SurrealValue};
use nauka_core_macros::{access, resource};
use serde::{Deserialize, Serialize};

#[resource(
    table = "user",
    scope = "cluster",
    // IAM-6: users can read their own record (display name,
    // verified flags once they land in IAM-7). `password_hash`
    // stays hidden via the field-level `#[hidden]` clause even
    // when the outer record is visible. Root / state-machine
    // queries keep full access through the `$auth = NONE` arm.
    permissions = "$auth = NONE OR $this.id = $auth.id",
    // `create` via the API would short-circuit password hashing —
    // signup has to hash with Argon2id *on the leader* so every
    // replica applies an identical SurQL. Keep the bespoke
    // `nauka user create` / `nauka login` path for that; the API
    // surfaces the rest.
    api_verbs = "get, list, update, delete"
)]
#[access(
    name = "user",
    type = "record",
    // SIGNUP = dev/single-node path. `crypto::argon2::generate` hashes
    // locally with a fresh salt, so the write must NOT travel through
    // Raft — each follower would compute a different hash and the
    // state machine would diverge.
    signup = "CREATE type::record('user', $email) \
              SET email = $email, \
                  password_hash = crypto::argon2::generate($password), \
                  display_name = $display_name, \
                  email_verified_at = NONE, \
                  active = true, \
                  created_at = time::now(), \
                  updated_at = time::now(), \
                  version = 0",
    // IAM-9 (#353): `AND active = true` keeps deactivated users
    // from ever signing in, even if the password still matches —
    // same error surface as \"unknown user\" / \"wrong password\" so
    // the CLI can't tell the three cases apart.
    signin = "SELECT * FROM user \
              WHERE email = $email \
                AND active = true \
                AND crypto::argon2::compare(password_hash, $password)",
    jwt_duration = "1h",
    session_duration = "24h",
)]
#[derive(Serialize, Deserialize, SurrealValue, Debug, Clone)]
pub struct User {
    #[id]
    pub email: String,
    /// PHC-encoded Argon2id hash (`$argon2id$v=19$…`). Compatible
    /// with SurrealDB's `crypto::argon2::compare`. Hidden from
    /// user-session SELECTs via `#[hidden]` (IAM-6 / #350).
    /// `#[serde(skip)]` additionally keeps the hash out of
    /// REST/GraphQL response bodies — even code paths that don't
    /// go through the DB permission layer (a hypothetical
    /// administrative bypass) can't leak the digest to the wire.
    #[hidden]
    #[serde(skip)]
    pub password_hash: String,
    pub display_name: String,
    /// Set once the user has proven they own the address —
    /// currently done out-of-band by an admin (future IAM-7b wires
    /// up email delivery). IAM-9 governance will gate critical
    /// actions (`EmergencyAccess`, role escalation) on this being
    /// populated.
    pub email_verified_at: Option<Datetime>,
    /// IAM-9 (#353) deactivation flag. `auth::signin` rejects
    /// credentials when `active = false`; the DB-layer record
    /// stays around so audit history + re-activation remain
    /// possible. Follow-up IAM-9e handles 30-day soft-delete / GDPR
    /// purge on top of this.
    pub active: bool,
    // `created_at`, `updated_at`, `version` — injected by `#[resource]`.
}
