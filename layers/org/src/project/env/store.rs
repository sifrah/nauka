//! Environment persistence in ClusterDb (TiKV).

use nauka_core::id::EnvId;
use nauka_core::resource::ResourceMeta;
use nauka_hypervisor::controlplane::ClusterDb;

use super::types::Environment;
use crate::project;

const NS_ENV: &str = "env";
const NS_ENV_IDX: &str = "env-idx";
const REG_V2_NS: &str = "_reg_v2";
const REG_V2_PREFIX: &str = "env/";
const REG_V1: (&str, &str) = ("_reg", "env-ids");

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

        let idx_key = format!("{}/{}", project.meta.id, name);
        let existing: Option<String> = self.db.get(NS_ENV_IDX, &idx_key).await?;
        if existing.is_some() {
            anyhow::bail!("environment '{name}' already exists in project '{project_name}'");
        }

        let env = Environment {
            meta: ResourceMeta::new(EnvId::generate().to_string(), name),
            project_id: project.meta.id.clone().into(),
            project_name: project.meta.name.clone(),
            org_id: project.org_id.clone(),
            org_name: project.org_name.clone(),
        };

        self.db.put(NS_ENV, &env.meta.id, &env).await?;
        self.db.put(NS_ENV_IDX, &idx_key, &env.meta.id).await?;
        add_id(&self.db, &env.meta.id).await?;

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

        let idx_key = format!("{}/{}", project.meta.id, name_or_id);
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

        let idx_key = format!("{}/{}", env.project_id.as_str(), env.meta.name);
        self.db.delete(NS_ENV, &env.meta.id).await?;
        self.db.delete(NS_ENV_IDX, &idx_key).await?;
        remove_id(&self.db, &env.meta.id).await?;
        Ok(())
    }
}

async fn load_ids(db: &ClusterDb) -> anyhow::Result<Vec<String>> {
    let keys = db.scan_keys(REG_V2_NS, REG_V2_PREFIX).await?;
    let mut ids: Vec<String> = keys
        .into_iter()
        .filter_map(|k| k.strip_prefix(REG_V2_PREFIX).map(|s| s.to_string()))
        .collect();

    if let Some(v1_ids) = db.get::<Vec<String>>(REG_V1.0, REG_V1.1).await? {
        for old_id in v1_ids {
            if !ids.contains(&old_id) {
                let key = format!("{REG_V2_PREFIX}{old_id}");
                db.put(REG_V2_NS, &key, &true).await?;
                ids.push(old_id);
            }
        }
        db.delete(REG_V1.0, REG_V1.1).await?;
    }

    Ok(ids)
}

async fn add_id(db: &ClusterDb, id: &str) -> anyhow::Result<()> {
    let key = format!("{REG_V2_PREFIX}{id}");
    db.put(REG_V2_NS, &key, &true).await?;
    Ok(())
}

async fn remove_id(db: &ClusterDb, id: &str) -> anyhow::Result<()> {
    let key = format!("{REG_V2_PREFIX}{id}");
    db.delete(REG_V2_NS, &key).await?;
    Ok(())
}
