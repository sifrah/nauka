//! Project persistence in ClusterDb (TiKV).

use nauka_core::id::ProjectId;
use nauka_hypervisor::controlplane::ClusterDb;

use super::types::Project;

const NS_PROJ: &str = "proj";
const NS_PROJ_IDX: &str = "proj-idx";
const REG_PROJECTS: (&str, &str) = ("_reg", "proj-ids");

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
            created_at: crate::now(),
        };

        self.db.put(NS_PROJ, project.id.as_str(), &project).await?;
        self.db
            .put(NS_PROJ_IDX, &idx_key, &project.id.as_str().to_string())
            .await?;
        add_id(&self.db, project.id.as_str()).await?;

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

        let idx_key = format!("{}/{}", org.id.as_str(), name_or_id);
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
                .filter(|p| p.org_name == name)
                .collect()),
            None => Ok(projects),
        }
    }

    pub async fn delete(&self, name_or_id: &str, org_name: &str) -> anyhow::Result<()> {
        let project = self.get(name_or_id, Some(org_name)).await?.ok_or_else(|| {
            anyhow::anyhow!("project '{name_or_id}' not found in org '{org_name}'")
        })?;

        // Check for child environments
        let env_store = super::env::store::EnvStore::new(self.db.clone());
        let envs = env_store.list(Some(&project.name), Some(org_name)).await?;
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
        remove_id(&self.db, project.id.as_str()).await?;
        Ok(())
    }
}

async fn load_ids(db: &ClusterDb) -> anyhow::Result<Vec<String>> {
    let ids: Option<Vec<String>> = db.get(REG_PROJECTS.0, REG_PROJECTS.1).await?;
    Ok(ids.unwrap_or_default())
}

async fn add_id(db: &ClusterDb, id: &str) -> anyhow::Result<()> {
    let mut ids = load_ids(db).await?;
    ids.push(id.to_string());
    db.put(REG_PROJECTS.0, REG_PROJECTS.1, &ids).await?;
    Ok(())
}

async fn remove_id(db: &ClusterDb, id: &str) -> anyhow::Result<()> {
    let mut ids = load_ids(db).await?;
    ids.retain(|i| i != id);
    db.put(REG_PROJECTS.0, REG_PROJECTS.1, &ids).await?;
    Ok(())
}
