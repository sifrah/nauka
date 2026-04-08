//! Org persistence in ClusterDb (TiKV).

use nauka_core::id::OrgId;
use nauka_hypervisor::controlplane::ClusterDb;

use crate::types::Org;

const NS_ORG: &str = "org";
const NS_ORG_IDX: &str = "org-idx";
const REG_ORGS: (&str, &str) = ("_reg", "org-ids");

pub struct OrgStore {
    db: ClusterDb,
}

impl OrgStore {
    pub fn new(db: ClusterDb) -> Self {
        Self { db }
    }

    pub async fn create(&self, name: &str) -> anyhow::Result<Org> {
        if self.get_by_name(name).await?.is_some() {
            anyhow::bail!("org '{name}' already exists");
        }

        let org = Org {
            id: OrgId::generate(),
            name: name.to_string(),
            created_at: crate::now(),
        };

        self.db.put(NS_ORG, org.id.as_str(), &org).await?;
        self.db
            .put(NS_ORG_IDX, &org.name, &org.id.as_str().to_string())
            .await?;
        add_id(&self.db, org.id.as_str()).await?;

        Ok(org)
    }

    pub async fn get(&self, name_or_id: &str) -> anyhow::Result<Option<Org>> {
        if OrgId::looks_like_id(name_or_id) {
            self.db.get(NS_ORG, name_or_id).await.map_err(Into::into)
        } else {
            self.get_by_name(name_or_id).await
        }
    }

    async fn get_by_name(&self, name: &str) -> anyhow::Result<Option<Org>> {
        let id: Option<String> = self.db.get(NS_ORG_IDX, name).await?;
        match id {
            Some(id) => self.db.get(NS_ORG, &id).await.map_err(Into::into),
            None => Ok(None),
        }
    }

    pub async fn list(&self) -> anyhow::Result<Vec<Org>> {
        let ids = load_ids(&self.db).await?;
        let mut orgs = Vec::new();
        for id in &ids {
            if let Some(org) = self.db.get::<Org>(NS_ORG, id).await? {
                orgs.push(org);
            }
        }
        Ok(orgs)
    }

    pub async fn delete(&self, name_or_id: &str) -> anyhow::Result<()> {
        let org = self
            .get(name_or_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("org '{name_or_id}' not found"))?;

        // Check for child projects
        let proj_store = crate::project::store::ProjectStore::new(self.db.clone());
        let projects = proj_store.list(Some(&org.name)).await?;
        if !projects.is_empty() {
            anyhow::bail!(
                "org '{}' has {} project(s). Delete them first.",
                org.name,
                projects.len()
            );
        }

        self.db.delete(NS_ORG, org.id.as_str()).await?;
        self.db.delete(NS_ORG_IDX, &org.name).await?;
        remove_id(&self.db, org.id.as_str()).await?;
        Ok(())
    }
}

async fn load_ids(db: &ClusterDb) -> anyhow::Result<Vec<String>> {
    let ids: Option<Vec<String>> = db.get(REG_ORGS.0, REG_ORGS.1).await?;
    Ok(ids.unwrap_or_default())
}

async fn add_id(db: &ClusterDb, id: &str) -> anyhow::Result<()> {
    let mut ids = load_ids(db).await?;
    ids.push(id.to_string());
    db.put(REG_ORGS.0, REG_ORGS.1, &ids).await?;
    Ok(())
}

async fn remove_id(db: &ClusterDb, id: &str) -> anyhow::Result<()> {
    let mut ids = load_ids(db).await?;
    ids.retain(|i| i != id);
    db.put(REG_ORGS.0, REG_ORGS.1, &ids).await?;
    Ok(())
}
