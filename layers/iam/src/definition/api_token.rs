//! `ApiToken` — the secret a machine presents to act as a
//! [`ServiceAccount`]. Stored Stripe-style: the token that the
//! client actually sends is `nk_live_<token_id>_<secret>`, where
//! `token_id` is 16 URL-safe-base64 chars used purely as an O(1)
//! lookup key, and `secret` is 48 URL-safe-base64 chars whose
//! Argon2id PHC hash lives in `hash`.
//!
//! The DEFINE ACCESS for `service_account` (see
//! [`crate::can::SERVICE_ACCOUNT_ACCESS_DDL`]… actually emitted from
//! `#[access]` on this type) performs the hash verification and
//! signs in as the target `service_account` record — so `$auth.id`
//! on authenticated queries is the SA's record id, not the token.
//!
//! Why a separate `token_id` instead of hashing and searching by
//! hash: Argon2id salts are per-call, so two hashes of the same
//! plaintext diverge byte-for-byte. Storing an indexed plaintext id
//! gives us a cheap lookup that does not leak the secret.

use nauka_core::resource::{Ref, SurrealValue};
use nauka_core_macros::{access, resource};
use serde::{Deserialize, Serialize};

use super::service_account::ServiceAccount;

#[resource(
    table = "api_token",
    scope = "cluster",
    // Tokens follow their service account's org scope. A principal
    // who can see the SA can see / revoke its tokens.
    scope_by = "service_account"
)]
#[access(
    name = "service_account",
    type = "record",
    // SIGNIN looks up the token row by its public id, then verifies
    // the secret. On match, returns the service account record so
    // SurrealDB issues the JWT for that SA; on mismatch the SELECT
    // yields NONE and signin fails with "invalid credentials".
    //
    // `[0]` unwraps the SELECT's array; SurrealDB then coerces the
    // single-record value to the access record.
    signin = "(SELECT VALUE service_account FROM api_token \
               WHERE token_id = $token_id \
                 AND crypto::argon2::compare(hash, $secret))[0]",
    jwt_duration = "15m",
    session_duration = "15m",
)]
#[derive(Serialize, Deserialize, SurrealValue, Debug, Clone)]
pub struct ApiToken {
    /// Public portion of the token — 16 chars, URL-safe. Never
    /// treated as secret; included in the plaintext token given to
    /// the operator so the daemon can look the row up without
    /// scanning the table.
    #[id]
    pub token_id: String,
    /// Human-readable name for the token — shown in `nauka token
    /// list`, never used for authentication.
    pub name: String,
    /// The service account this token authenticates as. JWT's
    /// `$auth` will be this record once signin succeeds.
    pub service_account: Ref<ServiceAccount>,
    /// Argon2id PHC hash of the secret portion. Never reversible;
    /// the plaintext is only displayed once at creation time.
    pub hash: String,
}
