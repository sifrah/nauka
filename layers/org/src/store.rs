//! Persistence for Org/Project/Environment in ClusterDb (TiKV).
//!
//! Key layout:
//! - `org/{org_id}`                          → Org JSON
//! - `org-idx/{name}`                        → org_id string
//! - `org-ids`                               → Vec<String> of all org IDs
//! - `proj/{project_id}`                     → Project JSON
//! - `proj-idx/{org_id}/{name}`              → project_id string
//! - `proj-ids`                              → Vec<String> of all project IDs
//! - `env/{env_id}`                          → Environment JSON
//! - `env-idx/{project_id}/{name}`           → env_id string
//! - `env-ids`                               → Vec<String> of all env IDs
//!
//! We avoid TiKV scan (broken in tikv-client 0.4) by maintaining
//! a registry key (`*-ids`) with all IDs. List fetches the registry
//! then gets each entry individually.

use nauka_core::id::{EnvId, OrgId, ProjectId};
use nauka_hypervisor::controlplane::ClusterDb;

use crate::types::{Environment, Org, Project};

/// Namespaces — each resource type gets a unique prefix.
const NS_ORG: &str = "org";
const NS_ORG_IDX: &str = "org-idx";
const NS_PROJ: &str = "proj";
const NS_PROJ_IDX: &str = "proj-idx";
const NS_ENV: &str = "env";
const NS_ENV_IDX: &str = "env-idx";

/// Registry keys — single key holding all IDs for each type.
const REG_ORGS: (&str, &str) = ("_reg", "org-ids");
const REG_PROJECTS: (&str, &str) = ("_reg", "proj-ids");
const REG_ENVS: (&str, &str) = ("_reg", "env-ids");

/// Store wrapping ClusterDb for org hierarchy CRUD.
pub struct OrgStore {
    db: ClusterDb,
}

impl OrgStore {
    pub fn new(db: ClusterDb) -> Self {
        Self { db }
    }

    // ── Registry helpers ──

    async fn load_ids(&self, reg: (&str, &str)) -> anyhow::Result<Vec<String>> {
        let ids: Option<Vec<String>> = self.db.get(reg.0, reg.1).await?;
        Ok(ids.unwrap_or_default())
    }

    async fn save_ids(&self, reg: (&str, &str), ids: &[String]) -> anyhow::Result<()> {
        self.db.put(reg.0, reg.1, &ids.to_vec()).await?;
        Ok(())
    }

    async fn add_id(&self, reg: (&str, &str), id: &str) -> anyhow::Result<()> {
        let mut ids = self.load_ids(reg).await?;
        ids.push(id.to_string());
        self.save_ids(reg, &ids).await
    }

    async fn remove_id(&self, reg: (&str, &str), id: &str) -> anyhow::Result<()> {
        let mut ids = self.load_ids(reg).await?;
        ids.retain(|i| i != id);
        self.save_ids(reg, &ids).await
    }

    // ═══════════════════════════════════════════════════
    // Org
    // ═══════════════════════════════════════════════════

    pub async fn create_org(&self, name: &str) -> anyhow::Result<Org> {
        if self.get_org_by_name(name).await?.is_some() {
            anyhow::bail!("org '{name}' already exists");
        }

        let org = Org {
            id: OrgId::generate(),
            name: name.to_string(),
            created_at: now(),
        };

        self.db.put(NS_ORG, org.id.as_str(), &org).await?;
        self.db
            .put(NS_ORG_IDX, &org.name, &org.id.as_str().to_string())
            .await?;
        self.add_id(REG_ORGS, org.id.as_str()).await?;

        Ok(org)
    }

    pub async fn get_org(&self, name_or_id: &str) -> anyhow::Result<Option<Org>> {
        if OrgId::looks_like_id(name_or_id) {
            self.db.get(NS_ORG, name_or_id).await.map_err(Into::into)
        } else {
            self.get_org_by_name(name_or_id).await
        }
    }

    async fn get_org_by_name(&self, name: &str) -> anyhow::Result<Option<Org>> {
        let id: Option<String> = self.db.get(NS_ORG_IDX, name).await?;
        match id {
            Some(id) => self.db.get(NS_ORG, &id).await.map_err(Into::into),
            None => Ok(None),
        }
    }

    pub async fn list_orgs(&self) -> anyhow::Result<Vec<Org>> {
        let ids = self.load_ids(REG_ORGS).await?;
        let mut orgs = Vec::new();
        for id in &ids {
            if let Some(org) = self.db.get::<Org>(NS_ORG, id).await? {
                orgs.push(org);
            }
        }
        Ok(orgs)
    }

    pub async fn delete_org(&self, name_or_id: &str) -> anyhow::Result<()> {
        let org = self
            .get_org(name_or_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("org '{name_or_id}' not found"))?;

        let projects = self.list_projects(Some(&org.name)).await?;
        if !projects.is_empty() {
            anyhow::bail!(
                "org '{}' has {} project(s). Delete them first.",
                org.name,
                projects.len()
            );
        }

        self.db.delete(NS_ORG, org.id.as_str()).await?;
        self.db.delete(NS_ORG_IDX, &org.name).await?;
        self.remove_id(REG_ORGS, org.id.as_str()).await?;
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

        let idx_key = format!("{}/{}", org.id.as_str(), name);
        let existing: Option<String> = self.db.get(NS_PROJ_IDX, &idx_key).await?;
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

        self.db.put(NS_PROJ, project.id.as_str(), &project).await?;
        self.db
            .put(NS_PROJ_IDX, &idx_key, &project.id.as_str().to_string())
            .await?;
        self.add_id(REG_PROJECTS, project.id.as_str()).await?;

        Ok(project)
    }

    pub async fn get_project(
        &self,
        name_or_id: &str,
        org_name: Option<&str>,
    ) -> anyhow::Result<Option<Project>> {
        if ProjectId::looks_like_id(name_or_id) {
            return self.db.get(NS_PROJ, name_or_id).await.map_err(Into::into);
        }

        let org_name =
            org_name.ok_or_else(|| anyhow::anyhow!("--org required to resolve project by name"))?;
        let org = self
            .get_org(org_name)
            .await?
            .ok_or_else(|| anyhow::anyhow!("org '{org_name}' not found"))?;

        let idx_key = format!("{}/{}", org.id.as_str(), name_or_id);
        let id: Option<String> = self.db.get(NS_PROJ_IDX, &idx_key).await?;
        match id {
            Some(id) => self.db.get(NS_PROJ, &id).await.map_err(Into::into),
            None => Ok(None),
        }
    }

    pub async fn list_projects(&self, org_name: Option<&str>) -> anyhow::Result<Vec<Project>> {
        let ids = self.load_ids(REG_PROJECTS).await?;
        let mut projects = Vec::new();
        for id in &ids {
            if let Some(p) = self.db.get::<Project>(NS_PROJ, id).await? {
                projects.push(p);
            }
        }

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

        let envs = self.list_envs(Some(&project.name), Some(org_name)).await?;
        if !envs.is_empty() {
            anyhow::bail!(
                "project '{}' has {} environment(s). Delete them first.",
                project.name,
                envs.len()
            );
        }

        let idx_key = format!("{}/{}", project.org_id.as_str(), project.name);
        self.db.delete(NS_PROJ, project.id.as_str()).await?;
        self.db.delete(NS_PROJ_IDX, &idx_key).await?;
        self.remove_id(REG_PROJECTS, project.id.as_str()).await?;
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

        let idx_key = format!("{}/{}", project.id.as_str(), name);
        let existing: Option<String> = self.db.get(NS_ENV_IDX, &idx_key).await?;
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

        self.db.put(NS_ENV, env.id.as_str(), &env).await?;
        self.db
            .put(NS_ENV_IDX, &idx_key, &env.id.as_str().to_string())
            .await?;
        self.add_id(REG_ENVS, env.id.as_str()).await?;

        Ok(env)
    }

    pub async fn get_env(
        &self,
        name_or_id: &str,
        project_name: Option<&str>,
        org_name: Option<&str>,
    ) -> anyhow::Result<Option<Environment>> {
        if EnvId::looks_like_id(name_or_id) {
            return self.db.get(NS_ENV, name_or_id).await.map_err(Into::into);
        }

        let project_name = project_name
            .ok_or_else(|| anyhow::anyhow!("--project required to resolve environment by name"))?;
        let project = self
            .get_project(project_name, org_name)
            .await?
            .ok_or_else(|| anyhow::anyhow!("project '{project_name}' not found"))?;

        let idx_key = format!("{}/{}", project.id.as_str(), name_or_id);
        let id: Option<String> = self.db.get(NS_ENV_IDX, &idx_key).await?;
        match id {
            Some(id) => self.db.get(NS_ENV, &id).await.map_err(Into::into),
            None => Ok(None),
        }
    }

    pub async fn list_envs(
        &self,
        project_name: Option<&str>,
        org_name: Option<&str>,
    ) -> anyhow::Result<Vec<Environment>> {
        let ids = self.load_ids(REG_ENVS).await?;
        let mut envs = Vec::new();
        for id in &ids {
            if let Some(e) = self.db.get::<Environment>(NS_ENV, id).await? {
                envs.push(e);
            }
        }

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
        self.db.delete(NS_ENV, env.id.as_str()).await?;
        self.db.delete(NS_ENV_IDX, &idx_key).await?;
        self.remove_id(REG_ENVS, env.id.as_str()).await?;
        Ok(())
    }
}

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
