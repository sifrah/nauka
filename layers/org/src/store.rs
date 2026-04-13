//! Org persistence on the SurrealDB-backed cluster store.
//!
//! P2.9 (sifrah/nauka#213) migrated this module from the legacy raw-KV
//! cluster-DB surface (`db.put` / `db.get` / `db.scan_keys`) to the
//! native SurrealDB SDK on top of [`EmbeddedDb`]. Every method now
//! reaches the SDK via `self.db.client()` and writes/reads against the
//! SCHEMAFULL `org` table defined in
//! `layers/state/schemas/cluster/org.surql` (the canonical schema
//! shipped by P2.5 in `layers/org/schemas/org.surql`, mirrored into the
//! state crate by P2.7 so the bootstrap node can `apply_cluster_schemas`
//! it without an upward layer dependency).
//!
//! The store holds an [`EmbeddedDb`] directly rather than the legacy
//! cluster wrapper type — the previous version stored a
//! cluster-wrapper value and reached the SDK via a
//! `.embedded().client()` chain, but that kept the legacy type's name
//! in the file even though the methods had all migrated. P2.9 drops
//! the indirection entirely: call sites now pass
//! `cluster_db.embedded().clone()` at construction time, which gives
//! the store a cheap `Arc`-shared handle onto the same underlying
//! `Surreal<Db>` the rest of the cluster path sees.
//!
//! The legacy `_reg_v2` registry / `org-idx` sidecar keys that the
//! pre-P2.9 version of this file maintained are gone: the schema's
//! unique index on `org.name` is now the source of truth for "have we
//! seen this name before?", and `SELECT * FROM org` walks every row
//! directly without a separate key list.
//!
//! The Org → SurrealDB JSON bridge is still hand-written rather than
//! `SurrealValue`-derived because `Org` embeds [`ResourceMeta`] which
//! in turn carries an `id: String` field that collides with SurrealDB's
//! reserved record-id slot. We bind each column explicitly in the
//! `CREATE … SET …` form so the offending `id` field is dropped on the
//! way in, and we cast the `created_at` / `updated_at` Unix-epoch
//! values through `<datetime>$iso8601_string` so they match the
//! schema's `TYPE datetime` constraint without requiring a
//! `surrealdb::types::SurrealValue` derive cascade. The same
//! JSON-bridge pattern is documented at length on
//! `nauka_hypervisor::fabric::state::FabricState::save`.

use nauka_core::id::OrgId;
use nauka_core::resource::epoch_to_iso8601;
use nauka_core::resource::ResourceMeta;
use nauka_state::sdk_bridge::{classify_create_error, iso8601_to_epoch, thing_to_id_string};
use nauka_state::EmbeddedDb;
use serde::Deserialize;

use crate::types::Org;

/// SurrealDB table backing this store. Defined in
/// `layers/state/schemas/cluster/org.surql` (mirror of
/// `layers/org/schemas/org.surql`) as `DEFINE TABLE org SCHEMAFULL`.
const ORG_TABLE: &str = "org";

pub struct OrgStore {
    db: EmbeddedDb,
}

impl OrgStore {
    /// Build an [`OrgStore`] over a SurrealDB handle.
    ///
    /// Call sites typically have a cluster-DB wrapper on hand and pass
    /// `cluster_db.embedded().clone()` as the argument. The
    /// [`EmbeddedDb`] is cheap to clone (`Arc`-shared internally), so
    /// constructing per-request `OrgStore` values off the same cluster
    /// handle is free.
    pub fn new(db: EmbeddedDb) -> Self {
        Self { db }
    }

    /// Create a new org with the given human-readable name.
    ///
    /// Builds an [`Org`] with a freshly-generated [`OrgId`] and writes
    /// it to the `org` table via
    /// `CREATE type::record($tbl, $id) SET …`. The record id is the
    /// ULID-prefixed `OrgId` (e.g. `org-01J…`); the schema's unique
    /// index on `name` rejects duplicate human names at the database
    /// level, so the previous "check then insert" race is closed.
    ///
    /// Returns the unique-name conflict as a flat `anyhow::Error` with
    /// a human-friendly message so the CLI keeps producing the same
    /// "org '<name>' already exists" wording it did before P2.9.
    pub async fn create(&self, name: &str) -> anyhow::Result<Org> {
        let org = Org {
            meta: ResourceMeta::new(OrgId::generate().to_string(), name),
        };

        let created_at_iso = epoch_to_iso8601(org.meta.created_at);
        let updated_at_iso = epoch_to_iso8601(org.meta.updated_at);

        // Build the row via explicit SET fields so the SurrealDB
        // SCHEMAFULL constraints are honoured: `id` is dropped (it
        // lives in the record id), `created_at`/`updated_at` are cast
        // through `<datetime>` so the ISO 8601 strings parse into the
        // schema's native `datetime` type, `labels` is bound as a JSON
        // object, and `name`/`status` are bound as plain strings.
        let labels_json = serde_json::to_value(&org.meta.labels)
            .map_err(|e| anyhow::anyhow!("serialise labels: {e}"))?;

        let query_result = self
            .db
            .client()
            .query(
                "CREATE type::record($tbl, $id) SET \
                 name = $name, \
                 status = $status, \
                 labels = $labels, \
                 created_at = <datetime>$created_at, \
                 updated_at = <datetime>$updated_at",
            )
            .bind(("tbl", ORG_TABLE))
            .bind(("id", org.meta.id.clone()))
            .bind(("name", org.meta.name.clone()))
            .bind(("status", org.meta.status.clone()))
            .bind(("labels", labels_json))
            .bind(("created_at", created_at_iso))
            .bind(("updated_at", updated_at_iso))
            .await;
        let response = match query_result {
            Ok(r) => r,
            Err(e) => {
                return Err(anyhow::anyhow!(classify_create_error(
                    "org",
                    name,
                    &e.to_string()
                )))
            }
        };
        if let Err(e) = response.check() {
            return Err(anyhow::anyhow!(classify_create_error(
                "org",
                name,
                &e.to_string()
            )));
        }

        Ok(org)
    }

    /// Look up an org by id (when the input looks like an `OrgId`)
    /// or by human name (otherwise).
    ///
    /// The id path is a direct record-id `SELECT`; the name path uses
    /// the schema's unique `org_name` index for an O(1) lookup. Both
    /// paths return `Ok(None)` when no row matches — neither one is
    /// an error.
    pub async fn get(&self, name_or_id: &str) -> anyhow::Result<Option<Org>> {
        if OrgId::looks_like_id(name_or_id) {
            self.get_by_id(name_or_id).await
        } else {
            self.get_by_name(name_or_id).await
        }
    }

    async fn get_by_id(&self, id: &str) -> anyhow::Result<Option<Org>> {
        let mut response = self
            .db
            .client()
            .query("SELECT * FROM type::record($tbl, $id)")
            .bind(("tbl", ORG_TABLE))
            .bind(("id", id.to_string()))
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        // Round-trip through `serde_json::Value` so callers don't have
        // to derive `SurrealValue` on `OrgRow`. See the rationale on
        // [`OrgRow`] and the `FabricState::load` JSON-bridge pattern.
        let raw: Vec<serde_json::Value> = response.take(0).map_err(|e| anyhow::anyhow!("{e}"))?;
        match raw.into_iter().next() {
            None => Ok(None),
            Some(v) => {
                let row: OrgRow = serde_json::from_value(v)
                    .map_err(|e| anyhow::anyhow!("deserialise org row: {e}"))?;
                Ok(Some(row.into_org()))
            }
        }
    }

    async fn get_by_name(&self, name: &str) -> anyhow::Result<Option<Org>> {
        let mut response = self
            .db
            .client()
            .query("SELECT * FROM org WHERE name = $name LIMIT 1")
            .bind(("name", name.to_string()))
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        // Same JSON-bridge as `get_by_id` — see the comment there and
        // the rationale block on [`OrgRow`].
        let raw: Vec<serde_json::Value> = response.take(0).map_err(|e| anyhow::anyhow!("{e}"))?;
        match raw.into_iter().next() {
            None => Ok(None),
            Some(v) => {
                let row: OrgRow = serde_json::from_value(v)
                    .map_err(|e| anyhow::anyhow!("deserialise org row: {e}"))?;
                Ok(Some(row.into_org()))
            }
        }
    }

    /// List every org in the cluster, in unspecified order.
    ///
    /// The SCHEMAFULL `org` table is the source of truth — the legacy
    /// `_reg_v2` sidecar registry the previous version of this file
    /// maintained is gone with P2.9.
    pub async fn list(&self) -> anyhow::Result<Vec<Org>> {
        let mut response = self
            .db
            .client()
            .query("SELECT * FROM org")
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let raw: Vec<serde_json::Value> = response.take(0).map_err(|e| anyhow::anyhow!("{e}"))?;
        let orgs: Result<Vec<Org>, _> = raw
            .into_iter()
            .map(|v| {
                serde_json::from_value::<OrgRow>(v)
                    .map(OrgRow::into_org)
                    .map_err(|e| anyhow::anyhow!("deserialise org row: {e}"))
            })
            .collect();
        orgs
    }

    /// Delete an org by name or id. Refuses to delete an org that
    /// still owns any project — the caller must remove the children
    /// first.
    pub async fn delete(&self, name_or_id: &str) -> anyhow::Result<()> {
        let org = self
            .get(name_or_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("org '{name_or_id}' not found"))?;

        // Refuse to delete an org that still has projects. P2.10
        // (sifrah/nauka#214) migrated the project store to the native
        // SurrealDB SDK on top of the SCHEMAFULL `project` table, so
        // we can count the surviving children with a direct query on
        // the owning-org id. This store still doesn't import
        // `ProjectStore` because that would force a circular module
        // dependency (`ProjectStore::delete` reaches back into
        // `OrgStore::get` to resolve the parent org) — the inline
        // query is cheaper.
        let remaining_projects = self.count_projects_in_org(&org.meta.id).await?;
        if remaining_projects > 0 {
            anyhow::bail!(
                "org '{}' has {} project(s). Delete them first.",
                org.meta.name,
                remaining_projects
            );
        }

        let result = self
            .db
            .client()
            .query("DELETE type::record($tbl, $id)")
            .bind(("tbl", ORG_TABLE))
            .bind(("id", org.meta.id.clone()))
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        result.check().map_err(|e| anyhow::anyhow!("{e}"))?;
        Ok(())
    }

    /// Count the projects currently owned by the org whose record-id
    /// is `org_id`.
    ///
    /// Uses the SCHEMAFULL `project` table that P2.10 migrated to, so
    /// the query is a single `SELECT name FROM project WHERE org =
    /// $org`. The `project` table is defined by
    /// `nauka_state::apply_cluster_schemas` at bootstrap, so a fresh
    /// database already has the table — no `DEFINE TABLE IF NOT
    /// EXISTS` dance is needed. If the schema hasn't been applied at
    /// all (e.g. a pre-bootstrap sanity test), the SurrealDB error
    /// bubbles up as an `anyhow::Error` and the caller surfaces it.
    async fn count_projects_in_org(&self, org_id: &str) -> anyhow::Result<usize> {
        let mut response = self
            .db
            .client()
            .query("SELECT name FROM project WHERE org = $org")
            .bind(("org", org_id.to_string()))
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let rows: Vec<serde_json::Value> = response.take(0).map_err(|e| anyhow::anyhow!("{e}"))?;
        Ok(rows.len())
    }
}

/// Row shape returned by `SELECT * FROM org`. Mirrors the SCHEMAFULL
/// `org` table's column set with one ergonomic conversion: SurrealDB
/// hands the record id back as a typed `Thing` (table + id), which
/// serde_json renders as `{"tb": "org", "id": {"String": "..."}}`. We
/// bridge through `serde_json::Value` and pull the inner string out in
/// [`OrgRow::into_org`] so callers see the same flat `org-<ulid>` id
/// string that they always have.
///
/// `created_at` / `updated_at` arrive as RFC 3339 / ISO 8601 strings
/// from SurrealDB; we parse them back into Unix-epoch seconds with
/// [`iso8601_to_epoch`] so the resulting [`Org`] is bit-identical to
/// what `OrgStore::create` originally produced.
#[derive(Debug, Deserialize)]
struct OrgRow {
    id: serde_json::Value,
    name: String,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    labels: Option<serde_json::Value>,
    created_at: String,
    updated_at: String,
}

impl OrgRow {
    fn into_org(self) -> Org {
        let id = thing_to_id_string("org:", &self.id);
        let labels = self
            .labels
            .and_then(|v| serde_json::from_value(v).ok())
            .unwrap_or_default();
        Org {
            meta: ResourceMeta {
                id,
                name: self.name,
                status: self.status.unwrap_or_else(|| "active".to_string()),
                labels,
                created_at: iso8601_to_epoch(&self.created_at),
                updated_at: iso8601_to_epoch(&self.updated_at),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build an in-process `OrgStore` backed by a fresh SurrealKV
    /// datastore at a temporary path, with the `org` schema applied.
    ///
    /// The tempdir handle has to live as long as the store, so we
    /// return it together. SurrealDB's SCHEMAFULL semantics are
    /// identical across SurrealKv and TiKv (the Phase-2 contract is
    /// that one `EmbeddedDb` wrapper fronts both, with the same SDK
    /// surface), so this is a faithful rehearsal of production cluster
    /// state without needing a live TiKV cluster on the test host.
    async fn temp_store() -> (tempfile::TempDir, OrgStore) {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = EmbeddedDb::open(&dir.path().join("orgs.skv"))
            .await
            .expect("open EmbeddedDb at temp path");
        // Apply the cluster schema bundle so the SCHEMAFULL `org` table
        // (and every parent/child it depends on) is live before the
        // store touches it.
        nauka_state::apply_cluster_schemas(&db)
            .await
            .expect("apply cluster schemas");
        (dir, OrgStore::new(db))
    }

    #[tokio::test]
    async fn create_then_get_by_name() {
        let (_d, store) = temp_store().await;
        let created = store.create("acme").await.expect("create org");
        assert_eq!(created.meta.name, "acme");
        assert!(created.meta.id.starts_with("org-"));

        let got = store
            .get("acme")
            .await
            .expect("get by name")
            .expect("missing");
        assert_eq!(got.meta.name, "acme");
        assert_eq!(got.meta.id, created.meta.id);
        assert_eq!(got.meta.status, "active");
    }

    #[tokio::test]
    async fn create_then_get_by_id() {
        let (_d, store) = temp_store().await;
        let created = store.create("acme").await.expect("create org");

        let got = store
            .get(&created.meta.id)
            .await
            .expect("get by id")
            .expect("missing");
        assert_eq!(got.meta.id, created.meta.id);
        assert_eq!(got.meta.name, "acme");
    }

    #[tokio::test]
    async fn create_duplicate_name_is_rejected() {
        let (_d, store) = temp_store().await;
        store.create("acme").await.expect("first create");
        let err = store
            .create("acme")
            .await
            .expect_err("duplicate name should fail");
        assert!(
            err.to_string().contains("already exists"),
            "expected duplicate error, got: {err}",
        );
    }

    #[tokio::test]
    async fn list_returns_all_orgs() {
        let (_d, store) = temp_store().await;
        store.create("acme").await.unwrap();
        store.create("globex").await.unwrap();
        store.create("initech").await.unwrap();

        let orgs = store.list().await.expect("list");
        assert_eq!(orgs.len(), 3);
        let mut names: Vec<&str> = orgs.iter().map(|o| o.meta.name.as_str()).collect();
        names.sort();
        assert_eq!(names, vec!["acme", "globex", "initech"]);
    }

    #[tokio::test]
    async fn list_empty_returns_empty() {
        let (_d, store) = temp_store().await;
        let orgs = store.list().await.expect("list");
        assert!(orgs.is_empty());
    }

    #[tokio::test]
    async fn get_missing_returns_none() {
        let (_d, store) = temp_store().await;
        assert!(store.get("does-not-exist").await.unwrap().is_none());
        assert!(store
            .get("org-01J00000000000000000000000")
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn delete_removes_org() {
        let (_d, store) = temp_store().await;
        store.create("acme").await.unwrap();
        store.delete("acme").await.expect("delete");
        assert!(store.get("acme").await.unwrap().is_none());
        assert!(store.list().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn delete_missing_org_errors() {
        let (_d, store) = temp_store().await;
        let err = store
            .delete("does-not-exist")
            .await
            .expect_err("missing org should fail");
        assert!(err.to_string().contains("not found"), "got: {err}");
    }

    /// Delete refuses to drop an org that still has a project pointing
    /// at it.
    ///
    /// We seed the SCHEMAFULL `project` table directly (via a `CREATE`
    /// bypassing `ProjectStore::create`) so this test stays
    /// self-contained — it doesn't pull in `ProjectStore`, which
    /// reaches back into `OrgStore::get` to resolve the parent org
    /// and would force a circular import dance into this file.
    #[tokio::test]
    async fn delete_refuses_when_project_remains() {
        let (_d, store) = temp_store().await;
        let org = store.create("acme").await.expect("create org");

        // Seed a `project` row directly. The SCHEMAFULL `project`
        // table is created by `apply_cluster_schemas` in `temp_store`,
        // so every column has to be populated to satisfy the ASSERT
        // constraints.
        store
            .db
            .client()
            .query(
                "CREATE type::record('project', 'project-fake') SET \
                 name = 'fakeproj', \
                 status = 'active', \
                 labels = {}, \
                 org = $org, \
                 org_name = 'acme', \
                 created_at = time::now(), \
                 updated_at = time::now()",
            )
            .bind(("org", org.meta.id.clone()))
            .await
            .expect("seed project row")
            .check()
            .expect("seed project row check");

        // Now `delete` should refuse the org — there's a project still
        // pointing at it via `org = <org.meta.id>`.
        let err = store
            .delete("acme")
            .await
            .expect_err("delete should refuse while a project still exists");
        let msg = err.to_string();
        assert!(
            msg.contains("project") && msg.contains("Delete them first"),
            "expected 'has N project(s). Delete them first' message, got: {msg}",
        );

        // The org is still there — the failed delete must not have
        // removed it as a side-effect.
        assert!(store.get("acme").await.unwrap().is_some());
    }
}
