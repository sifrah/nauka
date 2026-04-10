//! Org persistence in ClusterDb (TiKV).

use nauka_core::id::OrgId;
use nauka_core::resource::ResourceMeta;
use nauka_hypervisor::controlplane::ClusterDb;

use crate::types::Org;

const NS_ORG: &str = "org";
const NS_ORG_IDX: &str = "org-idx";
const REG_V2_NS: &str = "_reg_v2";
const REG_V2_PREFIX: &str = "org/";
/// Legacy v1 registry key — read during migration, never written.
const REG_V1: (&str, &str) = ("_reg", "org-ids");

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
            meta: ResourceMeta::new(OrgId::generate().to_string(), name),
        };

        self.db.put(NS_ORG, &org.meta.id, &org).await?;
        self.db
            .put(NS_ORG_IDX, &org.meta.name, &org.meta.id)
            .await?;
        add_id(&self.db, &org.meta.id).await?;

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

        let proj_store = crate::project::store::ProjectStore::new(self.db.clone());
        let projects = proj_store.list(Some(&org.meta.name)).await?;
        if !projects.is_empty() {
            anyhow::bail!(
                "org '{}' has {} project(s). Delete them first.",
                org.meta.name,
                projects.len()
            );
        }

        self.db.delete(NS_ORG, &org.meta.id).await?;
        self.db.delete(NS_ORG_IDX, &org.meta.name).await?;
        remove_id(&self.db, &org.meta.id).await?;
        Ok(())
    }
}

async fn load_ids(db: &ClusterDb) -> anyhow::Result<Vec<String>> {
    // Primary: per-key scan (race-free)
    let keys = db.scan_keys(REG_V2_NS, REG_V2_PREFIX).await?;
    let mut ids: Vec<String> = keys
        .into_iter()
        .filter_map(|k| k.strip_prefix(REG_V2_PREFIX).map(|s| s.to_string()))
        .collect();

    // Backwards compat: merge any IDs from the legacy v1 list that aren't in v2 yet
    if let Some(v1_ids) = db.get::<Vec<String>>(REG_V1.0, REG_V1.1).await? {
        for old_id in v1_ids {
            if !ids.contains(&old_id) {
                // Migrate to v2 on the fly
                let key = format!("{REG_V2_PREFIX}{old_id}");
                db.put(REG_V2_NS, &key, &true).await?;
                ids.push(old_id);
            }
        }
        // Clean up legacy key after full migration
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
