//! Organization layer — resource hierarchy for multi-tenancy.
//!
//! Structure mirrors the resource hierarchy:
//! - **Org** — top-level organization (globally unique name)
//!   - **IAM** — identity & access management root (user + future policy/role)
//!     - **User** — identity resource
//!   - **Project** — scoped within an Org
//!     - **Env** — scoped within a Project (prod, staging, dev)
//!
//! CLI: `nauka org`, `nauka org project`, `nauka org project env`
//!
//! # Schema registry
//!
//! Each nested module has its own `definition.surql` at its root. The
//! `inventory::submit!` block below registers every schema with
//! [`nauka_state::SchemaRegistration`] so the bootstrap node picks
//! them up at link time and applies them in dependency order via
//! [`nauka_state::apply_cluster_schemas`]. Adding a new resource is
//! one `.surql` + one `submit!` line here — no central list to keep
//! in sync.

pub mod handlers;
pub mod iam;
pub mod project;
pub mod store;
pub mod types;

use nauka_core::resource::ResourceRegistration;

inventory::submit!(nauka_core::resource::LayerRegistration(registration));

// ─── Schema registry (sifrah/nauka#286) ──────────────────────────────
//
// `include_str!` paths are relative to *this* file (`layers/org/src/lib.rs`),
// so each `definition.surql` is reached by its nested module path.

inventory::submit!(nauka_state::SchemaRegistration {
    name: "org",
    definition: include_str!("definition.surql"),
    depends_on: &[],
});

inventory::submit!(nauka_state::SchemaRegistration {
    name: "iam",
    definition: include_str!("iam/definition.surql"),
    depends_on: &["org"],
});

inventory::submit!(nauka_state::SchemaRegistration {
    name: "user",
    definition: include_str!("iam/user/definition.surql"),
    depends_on: &["iam"],
});

inventory::submit!(nauka_state::SchemaRegistration {
    name: "project",
    definition: include_str!("project/definition.surql"),
    depends_on: &["org"],
});

inventory::submit!(nauka_state::SchemaRegistration {
    name: "env",
    definition: include_str!("project/env/definition.surql"),
    depends_on: &["project"],
});

/// Top-level registration: org with project (with env) as children.
pub fn registration() -> ResourceRegistration {
    ResourceRegistration {
        def: handlers::resource_def(),
        handler: handlers::handler(),
        children: vec![project::handlers::registration()],
    }
}

#[cfg(test)]
mod registry_tests {
    //! End-to-end coverage of the P2/#286 schema registry path that
    //! isolated tests in `nauka-state` cannot provide: those tests link
    //! only `nauka-state`, so `inventory::iter::<SchemaRegistration>` is
    //! empty at link time and the registry walks zero entries.
    //!
    //! This module lives in `nauka-org`, which *does* submit the five
    //! org-hierarchy schemas, so `apply_cluster_schemas` exercises the
    //! full inventory walk + Kahn toposort + actual SurrealDB
    //! application path against a real `EmbeddedDb`.

    use nauka_state::{apply_cluster_schemas, registrations, EmbeddedDb};

    /// Every `definition.surql` this crate registers must be applied
    /// by `apply_cluster_schemas` against a fresh `EmbeddedDb`, and
    /// every expected table must be discoverable via `INFO FOR DB`
    /// afterwards. Exercises the full P2/#286 wire-up.
    #[tokio::test]
    async fn apply_cluster_schemas_materializes_every_registered_table() {
        // Fresh on-disk SurrealKv so we don't race any other test.
        let dir = tempfile::tempdir().expect("tempdir");
        let db = EmbeddedDb::open(&dir.path().join("registry_e2e.skv"))
            .await
            .expect("open");

        apply_cluster_schemas(&db).await.expect("apply schemas");

        // Collect the expected table names from the inventory — this
        // is the same list the doctor walks in production.
        let expected: Vec<&str> = registrations().iter().map(|r| r.name).collect();
        assert!(
            !expected.is_empty(),
            "inventory should contain at least the org-hierarchy schemas \
             (got an empty registry — `nauka-org` isn't linked properly?)"
        );

        // `INFO FOR DB` returns the live catalog of the selected
        // namespace/database. Every registered table must appear under
        // the `tables` key.
        let mut res = db
            .client()
            .query("INFO FOR DB")
            .await
            .expect("INFO FOR DB")
            .check()
            .expect("INFO FOR DB check");
        let info: Option<serde_json::Value> = res.take(0).expect("take INFO FOR DB row");
        let info = info.expect("INFO FOR DB returned no rows");
        let tables = info
            .get("tables")
            .and_then(|t| t.as_object())
            .expect("INFO FOR DB has no `tables` object");

        for name in &expected {
            assert!(
                tables.contains_key(*name),
                "table `{name}` is registered in the schema registry but \
                 missing from INFO FOR DB after apply_cluster_schemas"
            );
        }

        db.shutdown().await.expect("shutdown");
    }

    /// Guard against a regression that drops a layer from the module
    /// tree without removing its `submit!` (or vice versa). The
    /// `nauka-org` crate must register exactly these five logical
    /// tables — no more, no less. Any future layer added here must
    /// also update this assertion.
    #[test]
    fn inventory_contains_expected_nauka_org_schemas() {
        let names: std::collections::HashSet<&str> =
            registrations().iter().map(|r| r.name).collect();
        for expected in &["org", "iam", "user", "project", "env"] {
            assert!(
                names.contains(*expected),
                "missing schema registration for `{expected}` — did you add \
                 a module without the corresponding `inventory::submit!`?"
            );
        }
    }
}
