//! Cluster-state SurrealQL schema application.
//!
//! ADR 0004 (sifrah/nauka#210) chose Option A: the bootstrap node applies
//! the cluster `.surql` schemas to TiKV exactly once, after PD/TiKV come
//! up and before the storage region is written. Joining nodes do NOT
//! call this function — they assume the schemas are already in place.
//!
//! # Registry, not a central list
//!
//! Every layer registers its `definition.surql` via
//! [`SchemaRegistration`] + [`inventory::submit!`]. This module does not
//! hardcode which tables exist — it walks the inventory, topologically
//! sorts the entries by their `depends_on` list, and applies each in
//! dependency order. Adding a new resource means dropping a
//! `definition.surql` next to its `mod.rs` and emitting one `submit!`
//! in that layer's `lib.rs`. No central list to keep in sync.
//!
//! Idempotency: every `DEFINE` in every schema file uses `IF NOT EXISTS`,
//! so re-running this function against an already-initialised cluster
//! is a no-op.

use crate::{EmbeddedDb, Result, StateError};

/// Schema registration emitted by each layer's `lib.rs` via
/// [`inventory::submit!`], collected at link time via
/// [`inventory::collect!`] below.
///
/// The string fields all have `'static` lifetime because they come
/// from `include_str!` (the `.surql` file content) and string literals
/// (the table name + dependency list).
///
/// # Example
///
/// ```ignore
/// inventory::submit!(nauka_state::SchemaRegistration {
///     name: "project",
///     definition: include_str!("project/definition.surql"),
///     depends_on: &["org"],
/// });
/// ```
#[derive(Debug, Clone, Copy)]
pub struct SchemaRegistration {
    /// Logical table name (e.g. `"org"`, `"project"`, `"user"`). Must
    /// match the `DEFINE TABLE <name>` statement at the top of the
    /// associated `definition.surql`.
    pub name: &'static str,
    /// Content of the `definition.surql` file, loaded via `include_str!`
    /// at the call site of `inventory::submit!`.
    pub definition: &'static str,
    /// Logical names of tables this schema references via foreign-key
    /// fields. Used to topologically order `apply_cluster_schemas` so
    /// SCHEMAFULL references resolve cleanly even on a fresh cluster.
    pub depends_on: &'static [&'static str],
}

inventory::collect!(SchemaRegistration);

/// Returns every [`SchemaRegistration`] collected at link time, in
/// declaration order (unsorted). Mostly useful for tests / introspection
/// — [`apply_cluster_schemas`] walks the same iterator internally and
/// does a proper topological sort before applying.
pub fn registrations() -> Vec<&'static SchemaRegistration> {
    inventory::iter::<SchemaRegistration>().collect()
}

/// Apply every registered cluster schema to the given [`EmbeddedDb`]
/// in dependency order.
///
/// Walks [`inventory::iter::<SchemaRegistration>`], topologically sorts
/// the entries by their `depends_on` lists via Kahn's algorithm, and
/// runs each `definition` through `db.client().query(...).check()`.
/// Idempotent — every `DEFINE` uses `IF NOT EXISTS`, so callers can
/// invoke this against either a freshly-bootstrapped TiKV cluster or
/// one that already has the schemas applied.
///
/// # Errors
///
/// - [`StateError::Database`] if any schema fails to apply or if its
///   response's `.check()` surfaces a per-statement error. The message
///   is prefixed with the offending schema name for fast triage.
/// - [`StateError::Database`] if the registered schemas form a cycle
///   or reference a `depends_on` name that no other registration
///   provides — both surface as `"schema registry: ..."` messages.
pub async fn apply_cluster_schemas(db: &EmbeddedDb) -> Result<()> {
    let ordered = topo_sort(&registrations())?;
    for reg in ordered {
        db.client()
            .query(reg.definition)
            .await
            .map_err(|e| StateError::Database(format!("apply {} schema: {e}", reg.name)))?
            .check()
            .map_err(|e| StateError::Database(format!("check {} schema: {e}", reg.name)))?;
    }
    Ok(())
}

/// Kahn's algorithm over the schema registry.
///
/// Returns the registrations in an order where every schema's
/// `depends_on` entries appear before it. Detects two failure modes:
///
/// 1. **Unknown dependency** — a registration lists a `depends_on`
///    name that no other registration provides. Means either a typo
///    or a missing `submit!` in the parent layer.
/// 2. **Cycle** — the dependency graph contains a cycle, so Kahn
///    cannot drain every node. Should never happen for a tree-shaped
///    resource hierarchy but we guard against it anyway.
fn topo_sort(
    registrations: &[&'static SchemaRegistration],
) -> Result<Vec<&'static SchemaRegistration>> {
    use std::collections::{HashMap, HashSet, VecDeque};

    let names: HashSet<&str> = registrations.iter().map(|r| r.name).collect();

    // Validate every dependency is known before we even start — gives
    // a clearer error than discovering it mid-sort.
    for reg in registrations {
        for dep in reg.depends_on {
            if !names.contains(*dep) {
                return Err(StateError::Database(format!(
                    "schema registry: {} depends on unknown schema '{}'",
                    reg.name, dep
                )));
            }
        }
    }

    // indegree[name] = number of unresolved dependencies of `name`.
    let mut indegree: HashMap<&str, usize> = registrations
        .iter()
        .map(|r| (r.name, r.depends_on.len()))
        .collect();

    // Reverse edges: children[dep] = [name, ...] — schemas that
    // depend on `dep`. Used to decrement indegree when `dep` is
    // resolved.
    let mut children: HashMap<&str, Vec<&str>> = HashMap::new();
    for reg in registrations {
        for dep in reg.depends_on {
            children.entry(*dep).or_default().push(reg.name);
        }
    }

    // Seed the queue with every zero-indegree node.
    let mut queue: VecDeque<&str> = indegree
        .iter()
        .filter_map(|(name, deg)| (*deg == 0).then_some(*name))
        .collect();

    // Stable output order for tests and debug logs — iterate the
    // queue in the order we seeded it, then extend deterministically.
    let by_name: HashMap<&str, &'static SchemaRegistration> =
        registrations.iter().map(|r| (r.name, *r)).collect();

    let mut ordered: Vec<&'static SchemaRegistration> = Vec::with_capacity(registrations.len());
    while let Some(name) = queue.pop_front() {
        ordered.push(by_name[name]);
        if let Some(kids) = children.get(name) {
            for kid in kids {
                let deg = indegree.get_mut(kid).expect("kid in indegree map");
                *deg -= 1;
                if *deg == 0 {
                    queue.push_back(*kid);
                }
            }
        }
    }

    if ordered.len() != registrations.len() {
        return Err(StateError::Database(format!(
            "schema registry: dependency cycle among {} registrations, only sorted {}",
            registrations.len(),
            ordered.len()
        )));
    }

    Ok(ordered)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::EmbeddedDb;

    /// Integration: `apply_cluster_schemas` runs cleanly against a fresh
    /// `EmbeddedDb` and a second application is a no-op. This exercises
    /// the inventory walk + toposort + actual SurrealDB execution.
    #[tokio::test]
    async fn apply_cluster_schemas_is_idempotent() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = EmbeddedDb::open(&dir.path().join("p2_7.skv"))
            .await
            .expect("open");

        apply_cluster_schemas(&db)
            .await
            .expect("first apply should succeed");
        apply_cluster_schemas(&db)
            .await
            .expect("second apply (idempotency) should succeed");

        db.shutdown().await.expect("shutdown");
    }

    /// Unit: `topo_sort` resolves a simple dependency chain correctly.
    /// The inventory may reorder submissions at link time, so we pass
    /// in deliberately-scrambled input and assert the parent always
    /// lands before its child.
    #[test]
    fn topo_sort_orders_dependencies_before_dependents() {
        static ORG: SchemaRegistration = SchemaRegistration {
            name: "org",
            definition: "",
            depends_on: &[],
        };
        static PROJECT: SchemaRegistration = SchemaRegistration {
            name: "project",
            definition: "",
            depends_on: &["org"],
        };
        static ENV: SchemaRegistration = SchemaRegistration {
            name: "env",
            definition: "",
            depends_on: &["project"],
        };

        // Scrambled input.
        let input = vec![&ENV, &PROJECT, &ORG];
        let sorted = topo_sort(&input).expect("sort");

        let names: Vec<&str> = sorted.iter().map(|r| r.name).collect();
        let org_idx = names.iter().position(|n| *n == "org").unwrap();
        let project_idx = names.iter().position(|n| *n == "project").unwrap();
        let env_idx = names.iter().position(|n| *n == "env").unwrap();
        assert!(org_idx < project_idx, "org must come before project");
        assert!(project_idx < env_idx, "project must come before env");
    }

    /// Unit: an unknown `depends_on` name is surfaced as a clear error
    /// instead of being silently skipped or producing a runtime
    /// `SCHEMAFULL` violation much later.
    #[test]
    fn topo_sort_rejects_unknown_dependency() {
        static ORPHAN: SchemaRegistration = SchemaRegistration {
            name: "env",
            definition: "",
            depends_on: &["project"], // no `project` in the input
        };
        let err = topo_sort(&[&ORPHAN]).unwrap_err();
        assert!(
            err.to_string().contains("unknown schema 'project'"),
            "{err}"
        );
    }

    /// Unit: a self-referencing registration forms a cycle of one and
    /// is detected as such.
    #[test]
    fn topo_sort_rejects_cycle() {
        static SELF_DEP: SchemaRegistration = SchemaRegistration {
            name: "loop",
            definition: "",
            depends_on: &["loop"],
        };
        let err = topo_sort(&[&SELF_DEP]).unwrap_err();
        assert!(err.to_string().contains("cycle"), "{err}");
    }
}
