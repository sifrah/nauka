//! Cluster-state SurrealQL schema application.
//!
//! ADR 0004 (sifrah/nauka#210) chose Option A: the bootstrap node applies
//! the cluster `.surql` schemas to TiKV exactly once, after PD/TiKV come
//! up and before the storage region is written. Joining nodes do NOT
//! call this function — they assume the schemas are already in place.
//!
//! P2.5 (sifrah/nauka#209) shipped the schema files; this module
//! consumes them at compile time via `include_str!` so the binary has
//! no runtime filesystem dependency on the source tree.
//!
//! Idempotency: every `DEFINE` in every schema file uses `IF NOT EXISTS`,
//! so re-running this function against an already-initialised cluster
//! is a no-op. The test
//! [`tests::apply_cluster_schemas_is_idempotent`] pins that contract.

use crate::{EmbeddedDb, Result, StateError};

/// Compile-time-embedded copies of every P2.5 cluster schema, keyed by
/// short name for diagnostics. The order is deliberate: parent tables
/// come before children, so even a SCHEMAFULL backend that doesn't
/// support forward references would still apply cleanly.
///
/// `include_str!` resolves relative to *this* source file
/// (`layers/state/src/schema.rs`), so the paths climb out of
/// `state/src` and back down into each sibling layer's `schemas/`
/// directory.
/// Table names defined by the cluster schemas, in application order.
///
/// Exposed publicly so callers (`nauka hypervisor doctor`, the migration
/// runner, etc.) can verify that every expected table is present in a
/// running cluster without having to re-parse the `.surql` files.
/// Kept in lockstep with [`CLUSTER_SCHEMAS`] by
/// [`tests::cluster_table_names_matches_schemas`].
pub const CLUSTER_TABLE_NAMES: &[&str] = &[
    "user",
    "org",
    "project",
    "env",
    "vpc",
    "subnet",
    "vpc_peering",
    "natgw",
    "vm",
];

const CLUSTER_SCHEMAS: &[(&str, &str)] = &[
    ("user", include_str!("../../org/schemas/user.surql")),
    ("org", include_str!("../../org/schemas/org.surql")),
    ("project", include_str!("../../org/schemas/project.surql")),
    ("env", include_str!("../../org/schemas/env.surql")),
    ("vpc", include_str!("../../network/schemas/vpc.surql")),
    ("subnet", include_str!("../../network/schemas/subnet.surql")),
    // P2.12 (sifrah/nauka#216): peering + natgw schemas ship alongside
    // the network layer's SurrealDB-SDK migration so the new stores
    // write to SCHEMAFULL tables instead of legacy raw-KV catch-alls.
    (
        "vpc_peering",
        include_str!("../../network/schemas/peering.surql"),
    ),
    ("natgw", include_str!("../../network/schemas/natgw.surql")),
    ("vm", include_str!("../../compute/schemas/vm.surql")),
];

/// Apply every cluster schema to the given [`EmbeddedDb`] in order.
///
/// Idempotent — every `DEFINE` uses `IF NOT EXISTS`, so callers can
/// invoke this against either a freshly-bootstrapped TiKV cluster or
/// one that already has the schemas applied. Returns an error on the
/// first schema that fails to apply, with a message that names the
/// offending schema for fast triage.
///
/// # Errors
///
/// Returns [`StateError::Database`] if any schema fails to apply or
/// if the SurrealDB response's `.check()` surfaces a per-statement
/// error (e.g. a parse error, an `ASSERT` failure, a SCHEMAFULL
/// violation). The error message is prefixed with the name of the
/// offending schema (`"apply vm schema: ..."` or
/// `"check vm schema: ..."`) so the operator can immediately tell
/// which file is at fault.
pub async fn apply_cluster_schemas(db: &EmbeddedDb) -> Result<()> {
    for (name, schema) in CLUSTER_SCHEMAS {
        db.client()
            .query(*schema)
            .await
            .map_err(|e| StateError::Database(format!("apply {name} schema: {e}")))?
            .check()
            .map_err(|e| StateError::Database(format!("check {name} schema: {e}")))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::EmbeddedDb;

    /// P2.7 — `apply_cluster_schemas` runs cleanly against a fresh
    /// `EmbeddedDb` (using the SurrealKV backend for the test, since
    /// we don't have a TiKV cluster in unit tests). Re-running against
    /// the same handle must also succeed — that's the idempotency
    /// contract the acceptance criteria calls out as "re-running on an
    /// existing cluster does not fail".
    ///
    /// The schemas define `cluster`-scoped tables that don't collide
    /// with the bootstrap tables already applied by `EmbeddedDb::open`,
    /// so we can safely share the same SurrealDB instance here.
    #[tokio::test]
    async fn apply_cluster_schemas_is_idempotent() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = EmbeddedDb::open(&dir.path().join("p2_7.skv"))
            .await
            .expect("open");

        // First apply — must succeed against an empty db.
        apply_cluster_schemas(&db)
            .await
            .expect("first apply should succeed");

        // Second apply — must be a no-op thanks to `IF NOT EXISTS` on
        // every DEFINE. Any regression that drops the guard surfaces
        // here as an "already exists" error.
        apply_cluster_schemas(&db)
            .await
            .expect("second apply (idempotency) should succeed");

        db.shutdown().await.expect("shutdown");
    }

    /// P2.17 (sifrah/nauka#221) — the public [`CLUSTER_TABLE_NAMES`]
    /// list must stay in sync with the schemas actually applied. If
    /// somebody adds a `.surql` file to [`CLUSTER_SCHEMAS`] without
    /// updating the names list (or vice versa), the doctor's
    /// schema-presence check would silently miss the new table.
    #[test]
    fn cluster_table_names_matches_schemas() {
        let schema_names: Vec<&str> = CLUSTER_SCHEMAS.iter().map(|(n, _)| *n).collect();
        assert_eq!(
            CLUSTER_TABLE_NAMES,
            &schema_names[..],
            "CLUSTER_TABLE_NAMES drifted from CLUSTER_SCHEMAS — \
             update layers/state/src/schema.rs to keep them in lockstep"
        );
    }
}
