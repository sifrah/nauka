//! Environment persistence on the SurrealDB-backed cluster store.
//!
//! P2.11 (sifrah/nauka#215) migrated this module from the legacy raw-KV
//! cluster path to the native SurrealDB SDK on top of [`EmbeddedDb`],
//! following the same pattern that P2.9 / P2.10 established for
//! [`crate::store::OrgStore`] and
//! [`crate::project::store::ProjectStore`]. Every method now reaches
//! the SDK via `self.db.client()` and writes/reads against the
//! SCHEMAFULL `env` table defined in `layers/org/schemas/env.surql`
//! (applied to the cluster by `nauka_state::apply_cluster_schemas` at
//! bootstrap per ADR 0004).
//!
//! The store holds an [`EmbeddedDb`] directly, same as the other two
//! org-layer stores. Call sites pass `cluster_db.embedded().clone()`
//! at construction time; the clone is cheap (`Arc`-shared
//! `Surreal<Db>` router) and lets the store share the underlying
//! connection with the rest of the cluster path.
//!
//! The legacy `env` / `env-idx` / `_reg_v2` sidecar namespaces are
//! gone: the schema's composite unique index on `(project, name)` is
//! now the source of truth for "have we seen this (project, name)
//! pair before?", and `SELECT * FROM env` walks every row directly
//! without a separate key list.
//!
//! The `Environment` → SurrealDB JSON bridge is hand-written rather
//! than `SurrealValue`-derived for the same reason the sibling stores
//! bridge manually: `Environment` embeds [`ResourceMeta`] which carries
//! an `id: String` field that collides with SurrealDB's reserved
//! record-id slot. We bind each column explicitly in the
//! `CREATE … SET …` form so the offending `id` field is dropped on the
//! way in, and we cast the `created_at` / `updated_at` Unix-epoch
//! values through `<datetime>$iso8601_string` to match the schema's
//! `TYPE datetime` constraint.

use nauka_core::id::{EnvId, OrgId, ProjectId};
use nauka_core::resource::epoch_to_iso8601;
use nauka_core::resource::ResourceMeta;
use nauka_state::sdk_bridge::{classify_create_error, iso8601_to_epoch, thing_to_id_string};
use nauka_state::EmbeddedDb;
use serde::Deserialize;

use crate::project;

use super::types::Environment;

/// SurrealDB table backing this store. Defined by
/// `layers/org/schemas/env.surql` as `DEFINE TABLE env SCHEMAFULL`
/// and applied at bootstrap via `nauka_state::apply_cluster_schemas`.
const ENV_TABLE: &str = "env";

pub struct EnvStore {
    db: EmbeddedDb,
}

impl EnvStore {
    /// Build an [`EnvStore`] over a SurrealDB handle.
    ///
    /// Call sites that already hold a cluster-DB wrapper pass
    /// `cluster_db.embedded().clone()` — the [`EmbeddedDb`] is cheap
    /// to clone (`Arc`-shared internally), so constructing per-request
    /// stores off the same cluster handle is free.
    pub fn new(db: EmbeddedDb) -> Self {
        Self { db }
    }

    /// Create a new environment within a project.
    ///
    /// Resolves the owning project by its human-readable name (scoped
    /// to `org_name`), builds an `Environment` with a fresh [`EnvId`],
    /// and writes it to the `env` table via
    /// `CREATE type::record($tbl, $id) SET …`. The schema's composite
    /// unique index on `(project, name)` rejects duplicate
    /// `(project, name)` pairs at the database level, closing the old
    /// "check then insert" race.
    ///
    /// Duplicate-name conflicts surface as a flat `anyhow::Error` with
    /// a human-friendly `"environment '<name>' already exists in
    /// project '<project>'"` message so the CLI keeps producing the
    /// same wording it did before P2.11.
    pub async fn create(
        &self,
        name: &str,
        project_name: &str,
        org_name: &str,
    ) -> anyhow::Result<Environment> {
        // Resolve the owning project via the post-P2.10 store that
        // takes an `EmbeddedDb` directly.
        let proj_store = project::store::ProjectStore::new(self.db.clone());
        let project = proj_store
            .get(project_name, Some(org_name))
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!("project '{project_name}' not found in org '{org_name}'")
            })?;

        let env = Environment {
            meta: ResourceMeta::new(EnvId::generate().to_string(), name),
            project_id: project.meta.id.clone().into(),
            project_name: project.meta.name.clone(),
            org_id: project.org_id.clone(),
            org_name: project.org_name.clone(),
        };

        let created_at_iso = epoch_to_iso8601(env.meta.created_at);
        let updated_at_iso = epoch_to_iso8601(env.meta.updated_at);
        let labels_json = serde_json::to_value(&env.meta.labels)
            .map_err(|e| anyhow::anyhow!("serialise labels: {e}"))?;

        let query_result = self
            .db
            .client()
            .query(
                "CREATE type::record($tbl, $id) SET \
                 name = $name, \
                 status = $status, \
                 labels = $labels, \
                 project = $project, \
                 project_name = $project_name, \
                 org = $org, \
                 org_name = $org_name, \
                 created_at = <datetime>$created_at, \
                 updated_at = <datetime>$updated_at",
            )
            .bind(("tbl", ENV_TABLE))
            .bind(("id", env.meta.id.clone()))
            .bind(("name", env.meta.name.clone()))
            .bind(("status", env.meta.status.clone()))
            .bind(("labels", labels_json))
            .bind(("project", env.project_id.as_str().to_string()))
            .bind(("project_name", env.project_name.clone()))
            .bind(("org", env.org_id.as_str().to_string()))
            .bind(("org_name", env.org_name.clone()))
            .bind(("created_at", created_at_iso))
            .bind(("updated_at", updated_at_iso))
            .await;
        let response = match query_result {
            Ok(r) => r,
            Err(e) => {
                return Err(classify_create_error_with_project(
                    name,
                    project_name,
                    &e.to_string(),
                ))
            }
        };
        if let Err(e) = response.check() {
            return Err(classify_create_error_with_project(
                name,
                project_name,
                &e.to_string(),
            ));
        }

        Ok(env)
    }

    /// Look up an environment by id (when the input looks like an
    /// [`EnvId`]) or by human name (otherwise).
    ///
    /// The id path is a direct record-id `SELECT`; the name path
    /// requires both `project_name` and `org_name` so the lookup can
    /// resolve the parent project first and then use the composite
    /// `(project, name)` index. Both paths return `Ok(None)` when no
    /// row matches.
    pub async fn get(
        &self,
        name_or_id: &str,
        project_name: Option<&str>,
        org_name: Option<&str>,
    ) -> anyhow::Result<Option<Environment>> {
        if EnvId::looks_like_id(name_or_id) {
            return self.get_by_id(name_or_id).await;
        }

        let project_name = project_name
            .ok_or_else(|| anyhow::anyhow!("--project required to resolve environment by name"))?;

        // Resolve the owning project so we can query by its record id
        // via the composite `(project, name)` unique index.
        let proj_store = project::store::ProjectStore::new(self.db.clone());
        let project = proj_store
            .get(project_name, org_name)
            .await?
            .ok_or_else(|| anyhow::anyhow!("project '{project_name}' not found"))?;

        self.get_by_project_and_name(&project.meta.id, name_or_id)
            .await
    }

    async fn get_by_id(&self, id: &str) -> anyhow::Result<Option<Environment>> {
        let mut response = self
            .db
            .client()
            .query("SELECT * FROM type::record($tbl, $id)")
            .bind(("tbl", ENV_TABLE))
            .bind(("id", id.to_string()))
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let raw: Vec<serde_json::Value> = response.take(0).map_err(|e| anyhow::anyhow!("{e}"))?;
        decode_first(raw)
    }

    async fn get_by_project_and_name(
        &self,
        project_id: &str,
        name: &str,
    ) -> anyhow::Result<Option<Environment>> {
        let mut response = self
            .db
            .client()
            .query("SELECT * FROM env WHERE project = $project AND name = $name LIMIT 1")
            .bind(("project", project_id.to_string()))
            .bind(("name", name.to_string()))
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let raw: Vec<serde_json::Value> = response.take(0).map_err(|e| anyhow::anyhow!("{e}"))?;
        decode_first(raw)
    }

    /// List every environment in the cluster, optionally filtered by
    /// the owning project and/or org (by human name or by id).
    pub async fn list(
        &self,
        project_name: Option<&str>,
        org_name: Option<&str>,
    ) -> anyhow::Result<Vec<Environment>> {
        let mut response = self
            .db
            .client()
            .query("SELECT * FROM env")
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let raw: Vec<serde_json::Value> = response.take(0).map_err(|e| anyhow::anyhow!("{e}"))?;
        let envs: Vec<Environment> = raw
            .into_iter()
            .filter_map(|v| {
                serde_json::from_value::<EnvRow>(v)
                    .ok()
                    .map(EnvRow::into_environment)
            })
            .collect();

        match (project_name, org_name) {
            (Some(proj), Some(org)) => Ok(envs
                .into_iter()
                .filter(|e| {
                    (e.project_name == proj || e.project_id.as_str() == proj)
                        && (e.org_name == org || e.org_id.as_str() == org)
                })
                .collect()),
            (Some(proj), None) => Ok(envs
                .into_iter()
                .filter(|e| e.project_name == proj || e.project_id.as_str() == proj)
                .collect()),
            (None, Some(org)) => Ok(envs
                .into_iter()
                .filter(|e| e.org_name == org || e.org_id.as_str() == org)
                .collect()),
            (None, None) => Ok(envs),
        }
    }

    /// Delete an environment by name or id.
    ///
    /// `env` is the bottom of the org → project → env hierarchy, so
    /// there is no child-count check here — once the env is gone, the
    /// parent project can be deleted whenever the operator wants.
    pub async fn delete(
        &self,
        name_or_id: &str,
        project_name: &str,
        org_name: &str,
    ) -> anyhow::Result<()> {
        let env = self
            .get(name_or_id, Some(project_name), Some(org_name))
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!("environment '{name_or_id}' not found in project '{project_name}'")
            })?;

        let result = self
            .db
            .client()
            .query("DELETE type::record($tbl, $id)")
            .bind(("tbl", ENV_TABLE))
            .bind(("id", env.meta.id.clone()))
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        result.check().map_err(|e| anyhow::anyhow!("{e}"))?;
        Ok(())
    }
}

/// Row shape returned by `SELECT * FROM env`. Same conversion pattern
/// as the sibling [`ProjectRow`](crate::project::store) / `OrgRow`:
/// SurrealDB hands the record id back as a typed `Thing` (table + id)
/// which serde_json renders as one of several shapes; we bridge
/// through `serde_json::Value` and pull the inner string out in
/// [`EnvRow::into_environment`] via
/// [`nauka_state::sdk_bridge::thing_to_id_string`].
#[derive(Debug, Deserialize)]
struct EnvRow {
    id: serde_json::Value,
    name: String,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    labels: Option<serde_json::Value>,
    project: String,
    project_name: String,
    org: String,
    org_name: String,
    created_at: String,
    updated_at: String,
}

impl EnvRow {
    fn into_environment(self) -> Environment {
        let id = thing_to_id_string("env:", &self.id);
        let labels = self
            .labels
            .and_then(|v| serde_json::from_value(v).ok())
            .unwrap_or_default();
        Environment {
            meta: ResourceMeta {
                id,
                name: self.name,
                status: self.status.unwrap_or_else(|| "active".to_string()),
                labels,
                created_at: iso8601_to_epoch(&self.created_at),
                updated_at: iso8601_to_epoch(&self.updated_at),
            },
            project_id: ProjectId::from(self.project),
            project_name: self.project_name,
            org_id: OrgId::from(self.org),
            org_name: self.org_name,
        }
    }
}

/// Deserialize the first row from a `SELECT` result set, returning
/// `Ok(None)` when the set is empty.
fn decode_first(raw: Vec<serde_json::Value>) -> anyhow::Result<Option<Environment>> {
    match raw.into_iter().next() {
        None => Ok(None),
        Some(v) => {
            let row: EnvRow = serde_json::from_value(v)
                .map_err(|e| anyhow::anyhow!("deserialise env row: {e}"))?;
            Ok(Some(row.into_environment()))
        }
    }
}

/// Wrap [`nauka_state::sdk_bridge::classify_create_error`] so the user-facing
/// duplicate message includes the owning project. The composite
/// unique index on `(project, name)` rejects
/// `(proj_a, "prod")` + `(proj_a, "prod")` but allows
/// `(proj_a, "prod")` + `(proj_b, "prod")`, so the error wording
/// "environment 'prod' already exists in project 'web'" is both
/// accurate and matches the pre-P2.11 wording the CLI tests expect.
fn classify_create_error_with_project(
    name: &str,
    project_name: &str,
    err_msg: &str,
) -> anyhow::Error {
    let lowered = err_msg.to_lowercase();
    if lowered.contains("already contains")
        || lowered.contains("already exists")
        || lowered.contains("duplicate")
    {
        anyhow::anyhow!("environment '{name}' already exists in project '{project_name}'")
    } else {
        // Fall back to the shared helper for anything that isn't a
        // duplicate — it preserves the raw error text.
        anyhow::anyhow!(classify_create_error("environment", name, err_msg))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build an in-process `EnvStore` backed by a fresh SurrealKV
    /// datastore at a temporary path, with the cluster schema bundle
    /// applied. Returns the tempdir guard, the env store, the parent
    /// project store, and the grand-parent org store so individual
    /// tests can seed the hierarchy they need.
    async fn temp_store() -> (
        tempfile::TempDir,
        EnvStore,
        project::store::ProjectStore,
        crate::store::OrgStore,
    ) {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = EmbeddedDb::open(&dir.path().join("envs.skv"))
            .await
            .expect("open EmbeddedDb at temp path");
        nauka_state::apply_cluster_schemas(&db)
            .await
            .expect("apply cluster schemas");
        let env_store = EnvStore::new(db.clone());
        let proj_store = project::store::ProjectStore::new(db.clone());
        let org_store = crate::store::OrgStore::new(db);
        (dir, env_store, proj_store, org_store)
    }

    async fn seed_hierarchy(
        proj_store: &project::store::ProjectStore,
        org_store: &crate::store::OrgStore,
    ) {
        org_store.create("acme").await.expect("create org");
        proj_store
            .create("web", "acme")
            .await
            .expect("create project");
    }

    #[tokio::test]
    async fn create_then_get_by_name() {
        let (_d, envs, projs, orgs) = temp_store().await;
        seed_hierarchy(&projs, &orgs).await;

        let env = envs
            .create("prod", "web", "acme")
            .await
            .expect("create env");
        assert_eq!(env.meta.name, "prod");
        // `EnvId`'s prefix is `env`, not `environment`; the SurrealDB
        // record ends up at `env:env-01J…`.
        assert!(
            env.meta.id.starts_with("env-"),
            "expected env- prefix, got: {}",
            env.meta.id
        );
        assert_eq!(env.project_name, "web");
        assert_eq!(env.org_name, "acme");

        let got = envs
            .get("prod", Some("web"), Some("acme"))
            .await
            .expect("get by name")
            .expect("missing");
        assert_eq!(got.meta.id, env.meta.id);
        assert_eq!(got.meta.name, "prod");
    }

    #[tokio::test]
    async fn create_then_get_by_id() {
        let (_d, envs, projs, orgs) = temp_store().await;
        seed_hierarchy(&projs, &orgs).await;
        let env = envs
            .create("prod", "web", "acme")
            .await
            .expect("create env");

        let got = envs
            .get(&env.meta.id, None, None)
            .await
            .expect("get by id")
            .expect("missing");
        assert_eq!(got.meta.id, env.meta.id);
        assert_eq!(got.meta.name, "prod");
    }

    #[tokio::test]
    async fn create_without_parent_project_errors() {
        let (_d, envs, _projs, orgs) = temp_store().await;
        orgs.create("acme").await.expect("create org");
        let err = envs
            .create("prod", "nonexistent", "acme")
            .await
            .expect_err("missing project should fail");
        assert!(
            err.to_string().contains("not found"),
            "expected 'not found', got: {err}"
        );
    }

    #[tokio::test]
    async fn duplicate_name_in_same_project_is_rejected() {
        let (_d, envs, projs, orgs) = temp_store().await;
        seed_hierarchy(&projs, &orgs).await;
        envs.create("prod", "web", "acme")
            .await
            .expect("first create");

        let err = envs
            .create("prod", "web", "acme")
            .await
            .expect_err("duplicate (project, name) should fail");
        let msg = err.to_string();
        assert!(
            msg.contains("already exists"),
            "expected duplicate error, got: {msg}"
        );
    }

    #[tokio::test]
    async fn same_name_in_different_projects_is_allowed() {
        let (_d, envs, projs, orgs) = temp_store().await;
        orgs.create("acme").await.expect("create org");
        projs.create("web", "acme").await.expect("create acme/web");
        projs.create("api", "acme").await.expect("create acme/api");

        // Two projects within the same org may both have a `prod` env
        // — the unique index is on `(project, name)`, not
        // `(org, name)` or just `name`.
        envs.create("prod", "web", "acme")
            .await
            .expect("create web/prod");
        envs.create("prod", "api", "acme")
            .await
            .expect("create api/prod");

        let web_prod = envs
            .get("prod", Some("web"), Some("acme"))
            .await
            .expect("get web/prod")
            .expect("missing");
        let api_prod = envs
            .get("prod", Some("api"), Some("acme"))
            .await
            .expect("get api/prod")
            .expect("missing");
        assert_ne!(web_prod.meta.id, api_prod.meta.id);
        assert_eq!(web_prod.project_name, "web");
        assert_eq!(api_prod.project_name, "api");
    }

    #[tokio::test]
    async fn list_returns_all_envs() {
        let (_d, envs, projs, orgs) = temp_store().await;
        seed_hierarchy(&projs, &orgs).await;
        projs.create("api", "acme").await.expect("create acme/api");
        envs.create("prod", "web", "acme").await.unwrap();
        envs.create("staging", "web", "acme").await.unwrap();
        envs.create("prod", "api", "acme").await.unwrap();

        let all = envs.list(None, None).await.expect("list all");
        assert_eq!(all.len(), 3);

        let web_envs = envs
            .list(Some("web"), Some("acme"))
            .await
            .expect("list web envs");
        assert_eq!(web_envs.len(), 2);

        let acme_envs = envs.list(None, Some("acme")).await.expect("list acme envs");
        assert_eq!(acme_envs.len(), 3);
    }

    #[tokio::test]
    async fn list_empty_returns_empty() {
        let (_d, envs, _p, _o) = temp_store().await;
        let all = envs.list(None, None).await.expect("list");
        assert!(all.is_empty());
    }

    #[tokio::test]
    async fn get_missing_returns_none() {
        let (_d, envs, projs, orgs) = temp_store().await;
        seed_hierarchy(&projs, &orgs).await;
        assert!(envs
            .get("does-not-exist", Some("web"), Some("acme"))
            .await
            .unwrap()
            .is_none());
        // `EnvId::looks_like_id` checks for the `env-` prefix.
        assert!(envs
            .get("env-01J00000000000000000000000", None, None)
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn delete_removes_env() {
        let (_d, envs, projs, orgs) = temp_store().await;
        seed_hierarchy(&projs, &orgs).await;
        envs.create("prod", "web", "acme")
            .await
            .expect("create env");

        envs.delete("prod", "web", "acme").await.expect("delete");
        assert!(envs
            .get("prod", Some("web"), Some("acme"))
            .await
            .unwrap()
            .is_none());
        assert!(envs.list(None, None).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn delete_missing_env_errors() {
        let (_d, envs, projs, orgs) = temp_store().await;
        seed_hierarchy(&projs, &orgs).await;
        let err = envs
            .delete("does-not-exist", "web", "acme")
            .await
            .expect_err("missing env should fail");
        assert!(err.to_string().contains("not found"), "got: {err}");
    }

    /// Project delete is forbidden while a child env still exists —
    /// mirrors the test under `ProjectStore` but now uses the real
    /// env store to seed the child instead of a hand-crafted row.
    #[tokio::test]
    async fn project_delete_refuses_while_child_env_exists() {
        let (_d, envs, projs, orgs) = temp_store().await;
        seed_hierarchy(&projs, &orgs).await;
        envs.create("prod", "web", "acme")
            .await
            .expect("create env");

        let err = projs
            .delete("web", "acme")
            .await
            .expect_err("project delete should refuse while an env exists");
        let msg = err.to_string();
        assert!(
            msg.contains("environment") && msg.contains("Delete them first"),
            "expected 'has N environment(s). Delete them first' message, got: {msg}",
        );

        // Removing the env unblocks the project delete.
        envs.delete("prod", "web", "acme")
            .await
            .expect("delete env");
        projs.delete("web", "acme").await.expect("delete project");
    }
}
