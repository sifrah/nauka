//! `ServiceAccount` — machine identity inside an Org.
//!
//! A service account is a first-class principal, but never signs in
//! via password: it authenticates through an [`ApiToken`] record
//! whose secret is Argon2id-hashed. Every SA belongs to exactly one
//! Org; bindings and tokens live in that Org's scope.
//!
//! The Rust field is named `slug` (not `id`) to avoid shadowing
//! SurrealDB's implicit `id` column, same convention as User.email /
//! Org.slug / Project.uid.

use nauka_core::resource::{Ref, SurrealValue};
use nauka_core_macros::resource;
use serde::{Deserialize, Serialize};

use super::org::Org;

#[resource(table = "service_account", scope = "cluster", scope_by = "org")]
#[derive(Serialize, Deserialize, SurrealValue, Debug, Clone)]
pub struct ServiceAccount {
    /// Per-org unique slug. Global uniqueness is guaranteed by
    /// prefixing with `<org>-` at creation time
    /// (`ops::create_service_account`), same pattern as Project.uid.
    #[id]
    pub slug: String,
    /// Display-only; the service account's machine-readable slug is
    /// `slug` above.
    pub display_name: String,
    /// Parent org.
    pub org: Ref<Org>,
}
