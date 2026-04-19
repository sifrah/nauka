//! `PasswordResetToken` — opaque single-use token minted by
//! `nauka password reset-request` and redeemed by `nauka password
//! reset`. See IAM-7 (#351).
//!
//! - `token_id` is 32 URL-safe-base64 chars. Not derived from the
//!   user's email, so possessing a token leaks no information
//!   about whether the account exists.
//! - `expires_at` is 15 minutes past creation. IAM-7 does not
//!   implement server-side revocation beyond `consumed` and
//!   expiry — both are checked on every redeem.
//! - `consumed` flips to `true` inside the same Raft transaction
//!   that updates `User.password_hash`, so a replay of the token
//!   deterministically fails on every node.
//!
//! PERMISSIONS = `\$auth = NONE`: tokens never surface to any
//! user-level SELECT. The daemon path is the only reader /
//! writer.

use nauka_core::resource::{Datetime, Ref, SurrealValue};
use nauka_core_macros::resource;
use serde::{Deserialize, Serialize};

use super::user::User;

#[resource(
    table = "password_reset_token",
    scope = "cluster",
    permissions = "$auth = NONE",
    // Fully internal — daemon path is the only reader/writer.
    // `api_verbs = ""` keeps the resource off every generated
    // surface (REST, GraphQL, CLI). It still registers its DDL.
    api_verbs = ""
)]
#[derive(Serialize, Deserialize, SurrealValue, Debug, Clone)]
pub struct PasswordResetToken {
    /// 32 URL-safe chars of OS randomness. Also the record id.
    #[id]
    pub token_id: String,
    /// The user the token resets. Intentionally not `Option<>` —
    /// a reset-request for a non-existent email never creates a
    /// row; the daemon just pretends it did.
    pub user: Ref<User>,
    /// Wall-clock deadline. Set on the leader at mint time so
    /// every replica agrees on the value. The 15-minute window
    /// is IAM-7 default; IAM-9 governance may shorten it.
    pub expires_at: Datetime,
    /// Flipped to `true` on successful redeem. Redeeming a
    /// consumed token always fails.
    pub consumed: bool,
}
