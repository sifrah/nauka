//! Persistence for Org/Project/Environment in ClusterDb (TiKV).
//!
//! Key layout:
//! - `orgs/{org_id}`                     → Org JSON
//! - `orgs_idx/{name}`                   → org_id
//! - `projects/{project_id}`             → Project JSON
//! - `projects_idx/{org_id}/{name}`      → project_id
//! - `envs/{env_id}`                     → Environment JSON
//! - `envs_idx/{project_id}/{name}`      → env_id

use nauka_core::id::{EnvId, OrgId, ProjectId};
use nauka_hypervisor::controlplane::ClusterDb;

use crate::types::{Environment, Org, Project};

/// Namespaces in TiKV.
///
/// Index namespaces use "idx:" prefix (not "_") to avoid colliding with
/// data namespace prefix scans. In TiKV, `list("orgs", "")` scans
/// `orgs/` → `orgs0`, and `_` (0x5F) falls in that range, but `:` (0x3A)
/// does not since `0` (0x30) < `:` (0x3A).
/// Wait — that's actually still in range. Use completely different prefixes.
const NS_ORGS: &str = "org.data";
const NS_ORGS_IDX: &str = "org.name";
const NS_PROJECTS: &str = "proj.data";
const NS_PROJECTS_IDX: &str = "proj.name";
const NS_ENVS: &str = "env.data";
const NS_ENVS_IDX: &str = "env.name";

/// Store wrapping ClusterDb for org hierarchy CRUD.
pub struct OrgStore {
    db: ClusterDb,
}

impl OrgStore {
    pub fn new(db: ClusterDb) -> Self {
        Self { db }
    }

    // ═══════════════════════════════════════════════════
    // Org
    // ═══════════════════════════════════════════════════

    pub async fn create_org(&self, name: &str) -> anyhow::Result<Org> {
        // Check uniqueness
        if self.get_org_by_name(name).await?.is_some() {
            anyhow::bail!("org '{name}' already exists");
        }

        let org = Org {
            id: OrgId::generate(),
            name: name.to_string(),
            created_at: now(),
        };

        self.db.put(NS_ORGS, org.id.as_str(), &org).await?;
        self.db
            .put(NS_ORGS_IDX, &org.name, &org.id.as_str().to_string())
            .await?;

        Ok(org)
    }

    pub async fn get_org(&self, name_or_id: &str) -> anyhow::Result<Option<Org>> {
        if OrgId::looks_like_id(name_or_id) {
            self.db.get(NS_ORGS, name_or_id).await.map_err(Into::into)
        } else {
            self.get_org_by_name(name_or_id).await
        }
    }

    async fn get_org_by_name(&self, name: &str) -> anyhow::Result<Option<Org>> {
        let id: Option<String> = self.db.get(NS_ORGS_IDX, name).await?;
        match id {
            Some(id) => self.db.get(NS_ORGS, &id).await.map_err(Into::into),
            None => Ok(None),
        }
    }

    pub async fn list_orgs(&self) -> anyhow::Result<Vec<Org>> {
        let pairs: Vec<(String, Org)> = self.db.list(NS_ORGS, "").await?;
        Ok(pairs.into_iter().map(|(_, v)| v).collect())
    }

    pub async fn delete_org(&self, name_or_id: &str) -> anyhow::Result<()> {
        let org = self
            .get_org(name_or_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("org '{name_or_id}' not found"))?;

        // Check for child projects
        let projects = self.list_projects(Some(&org.name)).await?;
        if !projects.is_empty() {
            anyhow::bail!(
                "org '{}' has {} project(s). Delete them first.",
                org.name,
                projects.len()
            );
        }

        self.db.delete(NS_ORGS, org.id.as_str()).await?;
        self.db.delete(NS_ORGS_IDX, &org.name).await?;
        Ok(())
    }

    // ═══════════════════════════════════════════════════
    // Project
    // ═══════════════════════════════════════════════════

    pub async fn create_project(&self, name: &str, org_name: &str) -> anyhow::Result<Project> {
        let org = self
            .get_org(org_name)
            .await?
            .ok_or_else(|| anyhow::anyhow!("org '{org_name}' not found"))?;

        // Check uniqueness within org
        let idx_key = format!("{}/{}", org.id.as_str(), name);
        let existing: Option<String> = self.db.get(NS_PROJECTS_IDX, &idx_key).await?;
        if existing.is_some() {
            anyhow::bail!("project '{name}' already exists in org '{org_name}'");
        }

        let project = Project {
            id: ProjectId::generate(),
            name: name.to_string(),
            org_id: org.id.clone(),
            org_name: org.name.clone(),
            created_at: now(),
        };

        self.db
            .put(NS_PROJECTS, project.id.as_str(), &project)
            .await?;
        self.db
            .put(NS_PROJECTS_IDX, &idx_key, &project.id.as_str().to_string())
            .await?;

        Ok(project)
    }

    pub async fn get_project(
        &self,
        name_or_id: &str,
        org_name: Option<&str>,
    ) -> anyhow::Result<Option<Project>> {
        if ProjectId::looks_like_id(name_or_id) {
            return self
                .db
                .get(NS_PROJECTS, name_or_id)
                .await
                .map_err(Into::into);
        }

        // Resolve by name — need org to build index key
        let org_name =
            org_name.ok_or_else(|| anyhow::anyhow!("--org required to resolve project by name"))?;
        let org = self
            .get_org(org_name)
            .await?
            .ok_or_else(|| anyhow::anyhow!("org '{org_name}' not found"))?;

        let idx_key = format!("{}/{}", org.id.as_str(), name_or_id);
        let id: Option<String> = self.db.get(NS_PROJECTS_IDX, &idx_key).await?;
        match id {
            Some(id) => self.db.get(NS_PROJECTS, &id).await.map_err(Into::into),
            None => Ok(None),
        }
    }

    pub async fn list_projects(&self, org_name: Option<&str>) -> anyhow::Result<Vec<Project>> {
        let pairs: Vec<(String, Project)> = self.db.list(NS_PROJECTS, "").await?;
        let projects: Vec<Project> = pairs.into_iter().map(|(_, v)| v).collect();

        match org_name {
            Some(name) => Ok(projects
                .into_iter()
                .filter(|p| p.org_name == name)
                .collect()),
            None => Ok(projects),
        }
    }

    pub async fn delete_project(&self, name_or_id: &str, org_name: &str) -> anyhow::Result<()> {
        let project = self
            .get_project(name_or_id, Some(org_name))
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!("project '{name_or_id}' not found in org '{org_name}'")
            })?;

        // Check for child environments
        let envs = self.list_envs(Some(&project.name), Some(org_name)).await?;
        if !envs.is_empty() {
            anyhow::bail!(
                "project '{}' has {} environment(s). Delete them first.",
                project.name,
                envs.len()
            );
        }

        let idx_key = format!("{}/{}", project.org_id.as_str(), project.name);
        self.db.delete(NS_PROJECTS, project.id.as_str()).await?;
        self.db.delete(NS_PROJECTS_IDX, &idx_key).await?;
        Ok(())
    }

    // ═══════════════════════════════════════════════════
    // Environment
    // ═══════════════════════════════════════════════════

    pub async fn create_env(
        &self,
        name: &str,
        project_name: &str,
        org_name: &str,
    ) -> anyhow::Result<Environment> {
        let project = self
            .get_project(project_name, Some(org_name))
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!("project '{project_name}' not found in org '{org_name}'")
            })?;

        // Check uniqueness within project
        let idx_key = format!("{}/{}", project.id.as_str(), name);
        let existing: Option<String> = self.db.get(NS_ENVS_IDX, &idx_key).await?;
        if existing.is_some() {
            anyhow::bail!("environment '{name}' already exists in project '{project_name}'");
        }

        let env = Environment {
            id: EnvId::generate(),
            name: name.to_string(),
            project_id: project.id.clone(),
            project_name: project.name.clone(),
            org_id: project.org_id.clone(),
            org_name: project.org_name.clone(),
            created_at: now(),
        };

        self.db.put(NS_ENVS, env.id.as_str(), &env).await?;
        self.db
            .put(NS_ENVS_IDX, &idx_key, &env.id.as_str().to_string())
            .await?;

        Ok(env)
    }

    pub async fn get_env(
        &self,
        name_or_id: &str,
        project_name: Option<&str>,
        org_name: Option<&str>,
    ) -> anyhow::Result<Option<Environment>> {
        if EnvId::looks_like_id(name_or_id) {
            return self.db.get(NS_ENVS, name_or_id).await.map_err(Into::into);
        }

        let project_name = project_name
            .ok_or_else(|| anyhow::anyhow!("--project required to resolve environment by name"))?;
        let project = self
            .get_project(project_name, org_name)
            .await?
            .ok_or_else(|| anyhow::anyhow!("project '{project_name}' not found"))?;

        let idx_key = format!("{}/{}", project.id.as_str(), name_or_id);
        let id: Option<String> = self.db.get(NS_ENVS_IDX, &idx_key).await?;
        match id {
            Some(id) => self.db.get(NS_ENVS, &id).await.map_err(Into::into),
            None => Ok(None),
        }
    }

    pub async fn list_envs(
        &self,
        project_name: Option<&str>,
        org_name: Option<&str>,
    ) -> anyhow::Result<Vec<Environment>> {
        let pairs: Vec<(String, Environment)> = self.db.list(NS_ENVS, "").await?;
        let envs: Vec<Environment> = pairs.into_iter().map(|(_, v)| v).collect();

        match (project_name, org_name) {
            (Some(proj), Some(org)) => Ok(envs
                .into_iter()
                .filter(|e| e.project_name == proj && e.org_name == org)
                .collect()),
            (Some(proj), None) => Ok(envs
                .into_iter()
                .filter(|e| e.project_name == proj)
                .collect()),
            (None, Some(org)) => Ok(envs.into_iter().filter(|e| e.org_name == org).collect()),
            (None, None) => Ok(envs),
        }
    }

    pub async fn delete_env(
        &self,
        name_or_id: &str,
        project_name: &str,
        org_name: &str,
    ) -> anyhow::Result<()> {
        let env = self
            .get_env(name_or_id, Some(project_name), Some(org_name))
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!("environment '{name_or_id}' not found in project '{project_name}'")
            })?;

        let idx_key = format!("{}/{}", env.project_id.as_str(), env.name);
        self.db.delete(NS_ENVS, env.id.as_str()).await?;
        self.db.delete(NS_ENVS_IDX, &idx_key).await?;
        Ok(())
    }
}

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
