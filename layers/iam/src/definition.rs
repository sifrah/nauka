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
//! ## Why password_hash is a plain field (not `#[secret]`)
//!
//! IAM-6 introduces `#[secret]` (encryption at rest) and field-level
//! PERMISSIONS that hide `password_hash` from SELECT. For IAM-1 we
//! keep the field visible — the integration test needs to assert the
//! PHC string was stored correctly. We'll tighten visibility when the
//! permission layer lands.

use nauka_core::resource::SurrealValue;
use nauka_core_macros::{access, resource};
use serde::{Deserialize, Serialize};

#[resource(table = "user", scope = "cluster")]
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
                  created_at = time::now(), \
                  updated_at = time::now(), \
                  version = 0",
    signin = "SELECT * FROM user \
              WHERE email = $email \
                AND crypto::argon2::compare(password_hash, $password)",
    jwt_duration = "1h",
    session_duration = "24h",
)]
#[derive(Serialize, Deserialize, SurrealValue, Debug, Clone)]
pub struct User {
    #[id]
    pub email: String,
    /// PHC-encoded Argon2id hash (`$argon2id$v=19$…`). Compatible with
    /// SurrealDB's `crypto::argon2::compare`.
    pub password_hash: String,
    pub display_name: String,
    // `created_at`, `updated_at`, `version` — injected by `#[resource]`.
}
