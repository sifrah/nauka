//! Project persistence on the SurrealDB-backed cluster store.
//!
//! P2.10 (sifrah/nauka#214) migrated this module from the legacy raw-KV
//! cluster-DB surface (`db.put` / `db.get` / `db.scan_keys`) to the
//! native SurrealDB SDK on top of [`EmbeddedDb`]. Every method now
//! reaches the SDK via `self.db.client()` and writes/reads against the
//! SCHEMAFULL `project` table defined in
//! `layers/org/schemas/project.surql` (applied to the cluster by
//! `nauka_state::apply_cluster_schemas` at bootstrap per ADR 0004).
//!
//! The store holds an [`EmbeddedDb`] directly, same as the post-P2.9
//! `OrgStore`. Call sites pass `cluster_db.embedded().clone()` at
//! construction time; the clone is cheap (`Arc`-shared `Surreal<Db>`
//! router) and lets the store share the underlying connection with the
//! rest of the cluster path.
//!
//! The legacy `_reg_v2` / `proj-idx` sidecar keys are gone: the
//! schema's composite unique index on `(org, name)` is now the source
//! of truth for "have we seen this (org, name) pair before?", and
//! `SELECT * FROM project` walks every row directly without a separate
//! key list.
//!
//! The `Project` → SurrealDB JSON bridge is hand-written rather than
//! `SurrealValue`-derived for the same reason `OrgStore` bridges
//! manually: `Project` embeds [`ResourceMeta`] which carries an
//! `id: String` field that collides with SurrealDB's reserved
//! record-id slot. We bind each column explicitly in the
//! `CREATE … SET …` form so the offending `id` field is dropped on the
//! way in, and we cast the `created_at` / `updated_at` Unix-epoch
//! values through `<datetime>$iso8601_string` to match the schema's
//! `TYPE datetime` constraint. The JSON-bridge pattern is documented
//! at length on `nauka_hypervisor::fabric::state::FabricState::save`.

use nauka_core::id::{OrgId, ProjectId};
use nauka_core::resource::epoch_to_iso8601;
use nauka_core::resource::ResourceMeta;
use nauka_state::EmbeddedDb;
use serde::Deserialize;

use crate::sdk_bridge::{classify_create_error, iso8601_to_epoch, thing_to_id_string};

use super::types::Project;

/// SurrealDB table backing this store. Defined by
/// `layers/org/schemas/project.surql` as `DEFINE TABLE project
/// SCHEMAFULL` and applied at bootstrap via
/// `nauka_state::apply_cluster_schemas`.
const PROJECT_TABLE: &str = "project";

pub struct ProjectStore {
    db: EmbeddedDb,
}

impl ProjectStore {
    /// Build a [`ProjectStore`] over a SurrealDB handle.
    ///
    /// Call sites that already hold a cluster-DB wrapper pass
    /// `cluster_db.embedded().clone()` — the [`EmbeddedDb`] is cheap
    /// to clone (`Arc`-shared internally), so constructing per-request
    /// stores off the same cluster handle is free.
    pub fn new(db: EmbeddedDb) -> Self {
        Self { db }
    }

    /// Create a new project within an org.
    ///
    /// Resolves the owning org by its human-readable name, builds a
    /// `Project` with a fresh [`ProjectId`], and writes it to the
    /// `project` table via `CREATE type::record($tbl, $id) SET …`. The
    /// record id is the ULID-prefixed `ProjectId` (e.g.
    /// `project-01J…`); the schema's composite unique index on
    /// `(org, name)` rejects duplicate `(org, name)` pairs at the
    /// database level, closing the old "check then insert" race.
    ///
    /// Duplicate-name conflicts surface as a flat `anyhow::Error` with
    /// a human-friendly `"project '<name>' already exists in org
    /// '<org>'"` message so the CLI keeps producing the same wording
    /// it did before P2.10.
    pub async fn create(&self, name: &str, org_name: &str) -> anyhow::Result<Project> {
        // P2.9 (sifrah/nauka#213) migrated `OrgStore` to take an
        // `EmbeddedDb` directly; reach the inner handle via a cheap
        // clone of our own.
        let org_store = crate::store::OrgStore::new(self.db.clone());
        let org = org_store
            .get(org_name)
            .await?
            .ok_or_else(|| anyhow::anyhow!("org '{org_name}' not found"))?;

        let project = Project {
            meta: ResourceMeta::new(ProjectId::generate().to_string(), name),
            org_id: org.meta.id.clone().into(),
            org_name: org.meta.name.clone(),
        };

        let created_at_iso = epoch_to_iso8601(project.meta.created_at);
        let updated_at_iso = epoch_to_iso8601(project.meta.updated_at);
        let labels_json = serde_json::to_value(&project.meta.labels)
            .map_err(|e| anyhow::anyhow!("serialise labels: {e}"))?;

        let query_result = self
            .db
            .client()
            .query(
                "CREATE type::record($tbl, $id) SET \
                 name = $name, \
                 status = $status, \
                 labels = $labels, \
                 org = $org, \
                 org_name = $org_name, \
                 created_at = <datetime>$created_at, \
                 updated_at = <datetime>$updated_at",
            )
            .bind(("tbl", PROJECT_TABLE))
            .bind(("id", project.meta.id.clone()))
            .bind(("name", project.meta.name.clone()))
            .bind(("status", project.meta.status.clone()))
            .bind(("labels", labels_json))
            .bind(("org", project.org_id.as_str().to_string()))
            .bind(("org_name", project.org_name.clone()))
            .bind(("created_at", created_at_iso))
            .bind(("updated_at", updated_at_iso))
            .await;
        let response = match query_result {
            Ok(r) => r,
            Err(e) => {
                return Err(classify_create_error_with_org(
                    name,
                    org_name,
                    &e.to_string(),
                ))
            }
        };
        if let Err(e) = response.check() {
            return Err(classify_create_error_with_org(
                name,
                org_name,
                &e.to_string(),
            ));
        }

        Ok(project)
    }

    /// Look up a project by id (when the input looks like a
    /// [`ProjectId`]) or by human name (otherwise).
    ///
    /// The id path is a direct record-id `SELECT`; the name path
    /// requires `org_name` so the lookup goes through the composite
    /// `(org, name)` index. Both paths return `Ok(None)` when no row
    /// matches.
    pub async fn get(
        &self,
        name_or_id: &str,
        org_name: Option<&str>,
    ) -> anyhow::Result<Option<Project>> {
        if ProjectId::looks_like_id(name_or_id) {
            return self.get_by_id(name_or_id).await;
        }

        let org_name =
            org_name.ok_or_else(|| anyhow::anyhow!("--org required to resolve project by name"))?;

        // Resolve the owning org so we can query by its record id via
        // the composite `(org, name)` unique index.
        let org_store = crate::store::OrgStore::new(self.db.clone());
        let org = org_store
            .get(org_name)
            .await?
            .ok_or_else(|| anyhow::anyhow!("org '{org_name}' not found"))?;

        self.get_by_org_and_name(&org.meta.id, name_or_id).await
    }

    async fn get_by_id(&self, id: &str) -> anyhow::Result<Option<Project>> {
        let mut response = self
            .db
            .client()
            .query("SELECT * FROM type::record($tbl, $id)")
            .bind(("tbl", PROJECT_TABLE))
            .bind(("id", id.to_string()))
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let raw: Vec<serde_json::Value> = response.take(0).map_err(|e| anyhow::anyhow!("{e}"))?;
        decode_first(raw)
    }

    async fn get_by_org_and_name(
        &self,
        org_id: &str,
        name: &str,
    ) -> anyhow::Result<Option<Project>> {
        let mut response = self
            .db
            .client()
            .query("SELECT * FROM project WHERE org = $org AND name = $name LIMIT 1")
            .bind(("org", org_id.to_string()))
            .bind(("name", name.to_string()))
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let raw: Vec<serde_json::Value> = response.take(0).map_err(|e| anyhow::anyhow!("{e}"))?;
        decode_first(raw)
    }

    /// List every project in the cluster, optionally filtered by the
    /// owning org's human name (or id).
    pub async fn list(&self, org_name: Option<&str>) -> anyhow::Result<Vec<Project>> {
        let mut response = self
            .db
            .client()
            .query("SELECT * FROM project")
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let raw: Vec<serde_json::Value> = response.take(0).map_err(|e| anyhow::anyhow!("{e}"))?;
        let projects: Vec<Project> = raw
            .into_iter()
            .filter_map(|v| {
                serde_json::from_value::<ProjectRow>(v)
                    .ok()
                    .map(ProjectRow::into_project)
            })
            .collect();

        match org_name {
            Some(name) => Ok(projects
                .into_iter()
                .filter(|p| p.org_name == name || p.org_id.as_str() == name)
                .collect()),
            None => Ok(projects),
        }
    }

    /// Delete a project by name or id. Refuses to delete a project
    /// that still has any environment — the caller must remove the
    /// children first.
    pub async fn delete(&self, name_or_id: &str, org_name: &str) -> anyhow::Result<()> {
        let project = self.get(name_or_id, Some(org_name)).await?.ok_or_else(|| {
            anyhow::anyhow!("project '{name_or_id}' not found in org '{org_name}'")
        })?;

        // Refuse to delete a project that still owns environments. We
        // query the SCHEMAFULL `env` table (from the P2.5 bundle,
        // applied at bootstrap by `nauka_state::apply_cluster_schemas`)
        // directly on the `project` column rather than constructing
        // an `EnvStore` here — `EnvStore` still goes through the
        // legacy raw-KV cluster wrapper (P2.11, sifrah/nauka#215
        // migrates it), whose `data` wrapper column would collide
        // with the SCHEMAFULL `env` table's declared fields once the
        // schema bundle is in place. Querying by the owning
        // `project` record-id keeps this path layer-clean and lets
        // the same check work before and after P2.11 lands.
        let remaining_envs = self.count_envs_in_project(&project.meta.id).await?;
        if remaining_envs > 0 {
            anyhow::bail!(
                "project '{}' has {} environment(s). Delete them first.",
                project.meta.name,
                remaining_envs
            );
        }

        let result = self
            .db
            .client()
            .query("DELETE type::record($tbl, $id)")
            .bind(("tbl", PROJECT_TABLE))
            .bind(("id", project.meta.id.clone()))
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        result.check().map_err(|e| anyhow::anyhow!("{e}"))?;
        Ok(())
    }

    /// Count the environments currently owned by the project whose
    /// record-id is `project_id`.
    ///
    /// Uses the SCHEMAFULL `env` table from the P2.5 bundle. The
    /// table is created by `apply_cluster_schemas` at bootstrap, so a
    /// freshly-bootstrapped database already has it.
    async fn count_envs_in_project(&self, project_id: &str) -> anyhow::Result<usize> {
        let mut response = self
            .db
            .client()
            .query("SELECT name FROM env WHERE project = $project")
            .bind(("project", project_id.to_string()))
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let rows: Vec<serde_json::Value> = response.take(0).map_err(|e| anyhow::anyhow!("{e}"))?;
        Ok(rows.len())
    }
}

/// Row shape returned by `SELECT * FROM project`. Same conversion
/// pattern as [`crate::store::OrgStore`]'s `OrgRow`: SurrealDB hands
/// the record id back as a typed `Thing` (table + id) which
/// serde_json renders as one of several shapes; we bridge through
/// `serde_json::Value` and pull the inner string out in
/// [`ProjectRow::into_project`] via
/// [`crate::sdk_bridge::thing_to_id_string`].
#[derive(Debug, Deserialize)]
struct ProjectRow {
    id: serde_json::Value,
    name: String,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    labels: Option<serde_json::Value>,
    org: String,
    org_name: String,
    created_at: String,
    updated_at: String,
}

impl ProjectRow {
    fn into_project(self) -> Project {
        let id = thing_to_id_string("project:", &self.id);
        let labels = self
            .labels
            .and_then(|v| serde_json::from_value(v).ok())
            .unwrap_or_default();
        Project {
            meta: ResourceMeta {
                id,
                name: self.name,
                status: self.status.unwrap_or_else(|| "active".to_string()),
                labels,
                created_at: iso8601_to_epoch(&self.created_at),
                updated_at: iso8601_to_epoch(&self.updated_at),
            },
            org_id: OrgId::from(self.org),
            org_name: self.org_name,
        }
    }
}

/// Deserialize the first row from a `SELECT` result set, returning
/// `Ok(None)` when the set is empty.
fn decode_first(raw: Vec<serde_json::Value>) -> anyhow::Result<Option<Project>> {
    match raw.into_iter().next() {
        None => Ok(None),
        Some(v) => {
            let row: ProjectRow = serde_json::from_value(v)
                .map_err(|e| anyhow::anyhow!("deserialise project row: {e}"))?;
            Ok(Some(row.into_project()))
        }
    }
}

/// Wrap [`crate::sdk_bridge::classify_create_error`] so the user-facing
/// duplicate message includes the owning org. The composite unique
/// index on `(org, name)` rejects `(org_a, "web")` + `(org_a, "web")`
/// but allows `(org_a, "web")` + `(org_b, "web")`, so the error
/// wording "project 'web' already exists in org 'acme'" is both
/// accurate and matches the pre-P2.10 wording the CLI tests expect.
fn classify_create_error_with_org(name: &str, org_name: &str, err_msg: &str) -> anyhow::Error {
    let lowered = err_msg.to_lowercase();
    if lowered.contains("already contains")
        || lowered.contains("already exists")
        || lowered.contains("duplicate")
    {
        anyhow::anyhow!("project '{name}' already exists in org '{org_name}'")
    } else {
        // Fall back to the shared helper for anything that isn't a
        // duplicate — it preserves the raw error text.
        classify_create_error("project", name, err_msg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build an in-process `ProjectStore` backed by a fresh SurrealKV
    /// datastore at a temporary path, with the cluster schema bundle
    /// applied. Returns the tempdir guard, the store, and a ready-made
    /// `OrgStore` over the same handle so individual tests can seed
    /// the parent orgs they need.
    async fn temp_store() -> (tempfile::TempDir, ProjectStore, crate::store::OrgStore) {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = EmbeddedDb::open(&dir.path().join("projects.skv"))
            .await
            .expect("open EmbeddedDb at temp path");
        nauka_state::apply_cluster_schemas(&db)
            .await
            .expect("apply cluster schemas");
        let store = ProjectStore::new(db.clone());
        let org_store = crate::store::OrgStore::new(db);
        (dir, store, org_store)
    }

    #[tokio::test]
    async fn create_then_get_by_name() {
        let (_d, store, org_store) = temp_store().await;
        org_store.create("acme").await.expect("create org");

        let project = store.create("web", "acme").await.expect("create project");
        assert_eq!(project.meta.name, "web");
        // `ProjectId`'s prefix is `proj`, not `project`; the SurrealDB
        // record ends up at `project:proj-01J…` — the record-id table
        // part is `project` (from `PROJECT_TABLE`), the record-id part
        // is the ULID-prefixed `ProjectId`.
        assert!(
            project.meta.id.starts_with("proj-"),
            "expected proj- prefix, got: {}",
            project.meta.id
        );
        assert_eq!(project.org_name, "acme");

        let got = store
            .get("web", Some("acme"))
            .await
            .expect("get by name")
            .expect("missing");
        assert_eq!(got.meta.id, project.meta.id);
        assert_eq!(got.meta.name, "web");
        assert_eq!(got.org_name, "acme");
    }

    #[tokio::test]
    async fn create_then_get_by_id() {
        let (_d, store, org_store) = temp_store().await;
        org_store.create("acme").await.expect("create org");
        let project = store.create("web", "acme").await.expect("create project");

        let got = store
            .get(&project.meta.id, None)
            .await
            .expect("get by id")
            .expect("missing");
        assert_eq!(got.meta.id, project.meta.id);
        assert_eq!(got.meta.name, "web");
    }

    #[tokio::test]
    async fn create_without_parent_org_errors() {
        let (_d, store, _os) = temp_store().await;
        let err = store
            .create("web", "nonexistent")
            .await
            .expect_err("missing org should fail");
        assert!(
            err.to_string().contains("not found"),
            "expected 'not found', got: {err}"
        );
    }

    #[tokio::test]
    async fn duplicate_name_in_same_org_is_rejected() {
        let (_d, store, org_store) = temp_store().await;
        org_store.create("acme").await.expect("create org");
        store.create("web", "acme").await.expect("first create");

        let err = store
            .create("web", "acme")
            .await
            .expect_err("duplicate (org, name) should fail");
        let msg = err.to_string();
        assert!(
            msg.contains("already exists"),
            "expected duplicate error, got: {msg}"
        );
    }

    #[tokio::test]
    async fn same_name_in_different_orgs_is_allowed() {
        let (_d, store, org_store) = temp_store().await;
        org_store.create("acme").await.expect("create acme");
        org_store.create("globex").await.expect("create globex");

        // Two orgs may both have a project called "web" — the unique
        // index is on `(org, name)`, not just `name`.
        store.create("web", "acme").await.expect("create acme/web");
        store
            .create("web", "globex")
            .await
            .expect("create globex/web");

        // Fetching by (name, org) must find each one.
        let acme_web = store
            .get("web", Some("acme"))
            .await
            .expect("get acme/web")
            .expect("missing acme/web");
        let globex_web = store
            .get("web", Some("globex"))
            .await
            .expect("get globex/web")
            .expect("missing globex/web");
        assert_ne!(acme_web.meta.id, globex_web.meta.id);
        assert_eq!(acme_web.org_name, "acme");
        assert_eq!(globex_web.org_name, "globex");
    }

    #[tokio::test]
    async fn list_returns_all_projects() {
        let (_d, store, org_store) = temp_store().await;
        org_store.create("acme").await.expect("create acme");
        org_store.create("globex").await.expect("create globex");
        store.create("web", "acme").await.expect("create acme/web");
        store.create("api", "acme").await.expect("create acme/api");
        store
            .create("web", "globex")
            .await
            .expect("create globex/web");

        let all = store.list(None).await.expect("list all");
        assert_eq!(all.len(), 3);

        let acme_projects = store.list(Some("acme")).await.expect("list acme");
        assert_eq!(acme_projects.len(), 2);
        let mut names: Vec<&str> = acme_projects.iter().map(|p| p.meta.name.as_str()).collect();
        names.sort();
        assert_eq!(names, vec!["api", "web"]);
    }

    #[tokio::test]
    async fn list_empty_returns_empty() {
        let (_d, store, _os) = temp_store().await;
        let all = store.list(None).await.expect("list");
        assert!(all.is_empty());
    }

    #[tokio::test]
    async fn get_missing_returns_none() {
        let (_d, store, org_store) = temp_store().await;
        org_store.create("acme").await.expect("create acme");
        assert!(store
            .get("does-not-exist", Some("acme"))
            .await
            .unwrap()
            .is_none());
        // `ProjectId::looks_like_id` checks for the `proj-` prefix.
        assert!(store
            .get("proj-01J00000000000000000000000", None)
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn delete_removes_project() {
        let (_d, store, org_store) = temp_store().await;
        org_store.create("acme").await.expect("create org");
        store.create("web", "acme").await.expect("create project");

        store.delete("web", "acme").await.expect("delete");
        assert!(store.get("web", Some("acme")).await.unwrap().is_none());
        assert!(store.list(None).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn delete_missing_project_errors() {
        let (_d, store, org_store) = temp_store().await;
        org_store.create("acme").await.expect("create org");
        let err = store
            .delete("does-not-exist", "acme")
            .await
            .expect_err("missing project should fail");
        assert!(err.to_string().contains("not found"), "got: {err}");
    }

    /// Org delete is forbidden while a project still points at it —
    /// mirrors the test under `OrgStore` but uses the real project
    /// store to seed the child instead of a hand-crafted row.
    #[tokio::test]
    async fn org_delete_refuses_while_child_project_exists() {
        let (_d, store, org_store) = temp_store().await;
        org_store.create("acme").await.expect("create org");
        store.create("web", "acme").await.expect("create project");

        let err = org_store
            .delete("acme")
            .await
            .expect_err("org delete should refuse while a project exists");
        let msg = err.to_string();
        assert!(
            msg.contains("project") && msg.contains("Delete them first"),
            "expected 'has N project(s). Delete them first' message, got: {msg}",
        );

        // Removing the project unblocks the org delete.
        store.delete("web", "acme").await.expect("delete project");
        org_store.delete("acme").await.expect("delete org");
    }
}
