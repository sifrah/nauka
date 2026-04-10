//! Project persistence in ClusterDb (TiKV).

use nauka_core::id::ProjectId;
use nauka_core::resource::ResourceMeta;
use nauka_hypervisor::controlplane::ClusterDb;

use super::types::Project;

const NS_PROJ: &str = "proj";
const NS_PROJ_IDX: &str = "proj-idx";
const REG_V2_NS: &str = "_reg_v2";
const REG_V2_PREFIX: &str = "proj/";
const REG_V1: (&str, &str) = ("_reg", "proj-ids");

pub struct ProjectStore {
    db: ClusterDb,
}

impl ProjectStore {
    pub fn new(db: ClusterDb) -> Self {
        Self { db }
    }

    pub async fn create(&self, name: &str, org_name: &str) -> anyhow::Result<Project> {
        let org_store = crate::store::OrgStore::new(self.db.clone());
        let org = org_store
            .get(org_name)
            .await?
            .ok_or_else(|| anyhow::anyhow!("org '{org_name}' not found"))?;

        let idx_key = format!("{}/{}", org.meta.id, name);
        let existing: Option<String> = self.db.get(NS_PROJ_IDX, &idx_key).await?;
        if existing.is_some() {
            anyhow::bail!("project '{name}' already exists in org '{org_name}'");
        }

        let project = Project {
            meta: ResourceMeta::new(ProjectId::generate().to_string(), name),
            org_id: org.meta.id.clone().into(),
            org_name: org.meta.name.clone(),
        };

        self.db.put(NS_PROJ, &project.meta.id, &project).await?;
        self.db.put(NS_PROJ_IDX, &idx_key, &project.meta.id).await?;
        add_id(&self.db, &project.meta.id).await?;

        Ok(project)
    }

    pub async fn get(
        &self,
        name_or_id: &str,
        org_name: Option<&str>,
    ) -> anyhow::Result<Option<Project>> {
        if ProjectId::looks_like_id(name_or_id) {
            return self.db.get(NS_PROJ, name_or_id).await.map_err(Into::into);
        }

        let org_name =
            org_name.ok_or_else(|| anyhow::anyhow!("--org required to resolve project by name"))?;
        let org_store = crate::store::OrgStore::new(self.db.clone());
        let org = org_store
            .get(org_name)
            .await?
            .ok_or_else(|| anyhow::anyhow!("org '{org_name}' not found"))?;

        let idx_key = format!("{}/{}", org.meta.id, name_or_id);
        let id: Option<String> = self.db.get(NS_PROJ_IDX, &idx_key).await?;
        match id {
            Some(id) => self.db.get(NS_PROJ, &id).await.map_err(Into::into),
            None => Ok(None),
        }
    }

    pub async fn list(&self, org_name: Option<&str>) -> anyhow::Result<Vec<Project>> {
        let ids = load_ids(&self.db).await?;
        let mut projects = Vec::new();
        for id in &ids {
            if let Some(p) = self.db.get::<Project>(NS_PROJ, id).await? {
                projects.push(p);
            }
        }

        match org_name {
            Some(name) => Ok(projects
                .into_iter()
                .filter(|p| p.org_name == name || p.org_id.as_str() == name)
                .collect()),
            None => Ok(projects),
        }
    }

    pub async fn delete(&self, name_or_id: &str, org_name: &str) -> anyhow::Result<()> {
        let project = self.get(name_or_id, Some(org_name)).await?.ok_or_else(|| {
            anyhow::anyhow!("project '{name_or_id}' not found in org '{org_name}'")
        })?;

        let env_store = super::env::store::EnvStore::new(self.db.clone());
        let envs = env_store
            .list(Some(&project.meta.name), Some(org_name))
            .await?;
        if !envs.is_empty() {
            anyhow::bail!(
                "project '{}' has {} environment(s). Delete them first.",
                project.meta.name,
                envs.len()
            );
        }

        let idx_key = format!("{}/{}", project.org_id.as_str(), project.meta.name);
        self.db.delete(NS_PROJ, &project.meta.id).await?;
        self.db.delete(NS_PROJ_IDX, &idx_key).await?;
        remove_id(&self.db, &project.meta.id).await?;
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
