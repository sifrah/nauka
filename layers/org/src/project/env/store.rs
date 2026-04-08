//! Environment persistence in ClusterDb (TiKV).

use nauka_core::id::EnvId;
use nauka_hypervisor::controlplane::ClusterDb;

use super::types::Environment;
use crate::project;

const NS_ENV: &str = "env";
const NS_ENV_IDX: &str = "env-idx";
const REG_ENVS: (&str, &str) = ("_reg", "env-ids");

pub struct EnvStore {
    db: ClusterDb,
}

impl EnvStore {
    pub fn new(db: ClusterDb) -> Self {
        Self { db }
    }

    pub async fn create(
        &self,
        name: &str,
        project_name: &str,
        org_name: &str,
    ) -> anyhow::Result<Environment> {
        let proj_store = project::store::ProjectStore::new(self.db.clone());
        let project = proj_store
            .get(project_name, Some(org_name))
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
            created_at: crate::now(),
        };

        self.db.put(NS_ENV, env.id.as_str(), &env).await?;
        self.db
            .put(NS_ENV_IDX, &idx_key, &env.id.as_str().to_string())
            .await?;
        add_id(&self.db, env.id.as_str()).await?;

        Ok(env)
    }

    pub async fn get(
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
        let proj_store = project::store::ProjectStore::new(self.db.clone());
        let project = proj_store
            .get(project_name, org_name)
            .await?
            .ok_or_else(|| anyhow::anyhow!("project '{project_name}' not found"))?;

        let idx_key = format!("{}/{}", project.id.as_str(), name_or_id);
        let id: Option<String> = self.db.get(NS_ENV_IDX, &idx_key).await?;
        match id {
            Some(id) => self.db.get(NS_ENV, &id).await.map_err(Into::into),
            None => Ok(None),
        }
    }

    pub async fn list(
        &self,
        project_name: Option<&str>,
        org_name: Option<&str>,
    ) -> anyhow::Result<Vec<Environment>> {
        let ids = load_ids(&self.db).await?;
        let mut envs = Vec::new();
        for id in &ids {
            if let Some(e) = self.db.get::<Environment>(NS_ENV, id).await? {
                envs.push(e);
            }
        }

        // Filter accepts both names and IDs (API passes IDs, CLI passes names)
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

        let idx_key = format!("{}/{}", env.project_id.as_str(), env.name);
        self.db.delete(NS_ENV, env.id.as_str()).await?;
        self.db.delete(NS_ENV_IDX, &idx_key).await?;
        remove_id(&self.db, env.id.as_str()).await?;
        Ok(())
    }
}

async fn load_ids(db: &ClusterDb) -> anyhow::Result<Vec<String>> {
    let ids: Option<Vec<String>> = db.get(REG_ENVS.0, REG_ENVS.1).await?;
    Ok(ids.unwrap_or_default())
}

async fn add_id(db: &ClusterDb, id: &str) -> anyhow::Result<()> {
    let mut ids = load_ids(db).await?;
    ids.push(id.to_string());
    db.put(REG_ENVS.0, REG_ENVS.1, &ids).await?;
    Ok(())
}

async fn remove_id(db: &ClusterDb, id: &str) -> anyhow::Result<()> {
    let mut ids = load_ids(db).await?;
    ids.retain(|i| i != id);
    db.put(REG_ENVS.0, REG_ENVS.1, &ids).await?;
    Ok(())
}
