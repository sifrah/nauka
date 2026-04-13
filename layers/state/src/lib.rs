#![allow(clippy::result_large_err)]
//! State persistence for Nauka.
//!
//! Two backends, one local and one distributed, that together cover every
//! piece of state a Nauka deployment needs. The two never hold the same
//! data: bootstrap state lives locally on each node, everything else lives
//! in the cluster store.
//!
//! - [`EmbeddedDb`] — embedded SurrealDB on disk via the SurrealKV backend.
//!   The single source of truth for per-node bootstrap state that must be
//!   readable before any cluster exists: mesh identity, hypervisor identity,
//!   peers, WireGuard keys, and the storage region registry.
//! - [`ClusterDb`] — TiKV-backed distributed raw KV store for shared state
//!   (orgs, projects, VMs, VPCs, etc.). Transitional: Phase 2
//!   (sifrah/nauka#206 / sifrah/nauka#220) replaces it with `EmbeddedDb`
//!   over the SurrealDB SDK's `kv-tikv` backend, after which the two
//!   stores share the same API.
//!
//! See [`README.md`](https://github.com/sifrah/nauka/blob/main/layers/state/README.md)
//! for the full crate guide.
//!
//! # SurrealDB namespace / database conventions
//!
//! Per ADR 0003 (sifrah/nauka#190):
//!
//! - Local SurrealKV: namespace `nauka`, database `bootstrap`
//! - Distributed TiKV (Phase 2): namespace `nauka`, database `cluster`
//!
//! These literals are exported as the [`NAUKA_NS`], [`BOOTSTRAP_DB`], and
//! [`CLUSTER_DB`] constants so call sites never inline the strings.
//!
//! # Usage — `EmbeddedDb` (SurrealKV-backed bootstrap state)
//!
//! ```no_run
//! use nauka_state::EmbeddedDb;
//! use std::path::Path;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let db = EmbeddedDb::open(Path::new("/var/lib/nauka/bootstrap.skv")).await?;
//! let _: Vec<surrealdb::types::Value> =
//!     db.client().query("SELECT * FROM peer").await?.take(0)?;
//! db.shutdown().await?;
//! # Ok(()) }
//! ```

pub mod cluster;
pub mod embedded;

pub use cluster::ClusterDb;
pub use embedded::{pd_endpoints_for, EmbeddedDb};

// ═══════════════════════════════════════════════════
// SurrealDB namespace / database constants (ADR 0003)
// ═══════════════════════════════════════════════════

/// SurrealDB namespace used by all Nauka SurrealDB instances.
///
/// Per ADR 0003 (sifrah/nauka#190), Nauka writes only to this single
/// namespace. Multi-tenancy lives at the row level inside the cluster
/// database, not at the namespace level.
pub const NAUKA_NS: &str = "nauka";

/// SurrealDB database name for the local SurrealKV-backed bootstrap state.
///
/// Used by [`EmbeddedDb::open`].
pub const BOOTSTRAP_DB: &str = "bootstrap";

/// SurrealDB database name for the distributed TiKV-backed cluster state.
///
/// Reserved for the Phase 2 TiKV-backed `EmbeddedDb` constructor (P2.2,
/// sifrah/nauka#206). Defined here in P1.2 so the constants live in one
/// place and the cluster-side issue can refer to the existing symbol.
pub const CLUSTER_DB: &str = "cluster";

// ═══════════════════════════════════════════════════
// Errors
// ═══════════════════════════════════════════════════

/// Error type returned by every fallible nauka-state operation.
///
/// Variants are deliberately coarse-grained: callers either care about the
/// specific failure mode (e.g. `NotFound` vs everything else) or they
/// propagate the error opaquely. Anything finer than these four variants
/// stays in the inner message string.
#[derive(Debug, thiserror::Error)]
pub enum StateError {
    /// A SurrealDB-side or generic backend error: connection failures,
    /// query errors, internal engine errors, anything that does not fit
    /// the more specific variants below.
    #[error("database error: {0}")]
    Database(String),

    /// A schema-level error: a `DEFINE TABLE` constraint was violated, an
    /// `ASSERT` clause failed, a unique index conflicted, a SCHEMAFULL field
    /// type mismatched, etc. The inner string is the human-readable detail.
    ///
    /// Mapped from `surrealdb::Error` variants where the engine reports
    /// `is_validation()` (parse / invalid params / SCHEMAFULL violations) or
    /// `is_already_exists()` (unique-index conflicts during writes).
    #[error("schema error: {0}")]
    Schema(String),

    /// A "record / table / namespace / database does not exist" error.
    ///
    /// Mapped from `surrealdb::Error::is_not_found()`.
    #[error("not found: {0}")]
    NotFound(String),

    /// A serde / JSON serialization error.
    ///
    /// Surfaced today by [`ClusterDb`] (which still serializes values as
    /// raw JSON under TiKV) and by the JSON-bridge paths on `EmbeddedDb`
    /// that round-trip Rust structs through `serde_json::Value` before
    /// handing them to SurrealDB. Phase 2 (sifrah/nauka#220) retires the
    /// `ClusterDb` half; Phase 3 codegen (sifrah/nauka#225 ff) retires
    /// the JSON-bridge half, at which point this variant goes away.
    #[error("serialization error: {0}")]
    Serialization(String),

    /// A filesystem-level error.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

impl From<surrealdb::Error> for StateError {
    /// Best-effort classification of `surrealdb::Error` into the right
    /// `StateError` variant.
    ///
    /// The mapping is conservative: only `is_not_found()`, `is_validation()`,
    /// and `is_already_exists()` get specific variants. Everything else
    /// (query errors, connection errors, internal errors, thrown errors,
    /// not-allowed, configuration, serialization-on-the-wire) becomes
    /// `Database` because callers shouldn't need to distinguish them — they
    /// either want to know "did the record exist?" / "did the schema
    /// reject this?" or they propagate the error opaquely.
    fn from(err: surrealdb::Error) -> Self {
        let msg = format!("{err}");
        let details = err.details();
        if details.is_not_found() {
            StateError::NotFound(msg)
        } else if details.is_validation() || details.is_already_exists() {
            StateError::Schema(msg)
        } else {
            StateError::Database(msg)
        }
    }
}

pub type Result<T> = std::result::Result<T, StateError>;

#[cfg(test)]
mod tests {
    use super::*;

    // ─── Error mapping (P1.3, sifrah/nauka#193) ──────────────────────

    #[test]
    fn error_mapping_not_found() {
        // surrealdb::Error::not_found is the canonical "record/table/ns/db
        // does not exist" constructor. It must round-trip into our
        // StateError::NotFound variant.
        let surreal_err =
            surrealdb::Error::not_found("record `vm:does_not_exist`".to_string(), None);
        let state_err: StateError = surreal_err.into();
        assert!(
            matches!(state_err, StateError::NotFound(_)),
            "expected NotFound, got: {state_err:?}"
        );
    }

    #[test]
    fn error_mapping_validation_to_schema() {
        // SCHEMAFULL constraint failures, parse errors, and ASSERT failures
        // all surface as `Error::validation`. They map to StateError::Schema
        // because the caller's recourse is "fix the schema or the input",
        // not "retry against the backend".
        let surreal_err =
            surrealdb::Error::validation("DEFINE FIELD ... ASSERT failed".to_string(), None);
        let state_err: StateError = surreal_err.into();
        assert!(
            matches!(state_err, StateError::Schema(_)),
            "expected Schema, got: {state_err:?}"
        );
    }

    #[test]
    fn error_mapping_already_exists_to_schema() {
        // Unique-index conflicts on writes are also schema-level signals
        // for the caller (the constraint exists, the input violates it).
        let surreal_err =
            surrealdb::Error::already_exists("unique index `vm_name` violated".to_string(), None);
        let state_err: StateError = surreal_err.into();
        assert!(
            matches!(state_err, StateError::Schema(_)),
            "expected Schema, got: {state_err:?}"
        );
    }

    #[test]
    fn error_mapping_internal_to_database() {
        // Internal/connection/query/etc. errors all collapse to Database.
        // Internal is the most "catch-all" of the surrealdb variants and
        // is the right canary for the default-mapping path.
        let surreal_err = surrealdb::Error::internal("transaction conflict".to_string());
        let state_err: StateError = surreal_err.into();
        assert!(
            matches!(state_err, StateError::Database(_)),
            "expected Database, got: {state_err:?}"
        );
    }

    #[test]
    fn error_mapping_connection_to_database() {
        // Connection errors are operationally distinct ("the cluster is
        // gone") but the call site doesn't get a different recovery path
        // — it has to retry or surface the error to the user. Database
        // is the right bucket.
        let surreal_err = surrealdb::Error::connection("router uninitialised".to_string(), None);
        let state_err: StateError = surreal_err.into();
        assert!(
            matches!(state_err, StateError::Database(_)),
            "expected Database, got: {state_err:?}"
        );
    }

    #[test]
    fn error_mapping_question_mark_via_into() {
        // Sanity check that the `?` operator picks up the From impl
        // automatically (i.e. that it's a `From`, not a one-off helper).
        fn returns_state_result() -> Result<()> {
            Err(surrealdb::Error::not_found("missing".to_string(), None))?;
            Ok(())
        }
        let err = returns_state_result().unwrap_err();
        assert!(matches!(err, StateError::NotFound(_)));
    }

    #[test]
    fn error_mapping_preserves_message() {
        // The inner String of each variant should carry the original
        // message so logs and CLI output stay readable.
        let surreal_err =
            surrealdb::Error::not_found("record `vm:abc` does not exist".to_string(), None);
        let state_err: StateError = surreal_err.into();
        let rendered = format!("{state_err}");
        assert!(
            rendered.contains("vm:abc"),
            "rendered error should keep the original detail, got: {rendered}"
        );
    }
}
