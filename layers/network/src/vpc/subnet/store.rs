use nauka_core::id::SubnetId;
use nauka_core::resource::ResourceMeta;
use nauka_hypervisor::controlplane::ClusterDb;

use super::types::Subnet;

const NS_SUB: &str = "sub";
const NS_SUB_IDX: &str = "sub-idx";
const REG_V2_NS: &str = "_reg_v2";
const REG_V2_PREFIX: &str = "sub/";
const REG_V1: (&str, &str) = ("_reg", "sub-ids");

pub struct SubnetStore {
    db: ClusterDb,
}

impl SubnetStore {
    pub fn new(db: ClusterDb) -> Self {
        Self { db }
    }

    pub async fn create(
        &self,
        name: &str,
        vpc_name_or_id: &str,
        org_name: Option<&str>,
        cidr: &str,
    ) -> anyhow::Result<Subnet> {
        let vpc_store = crate::vpc::store::VpcStore::new(self.db.clone());
        let vpc = vpc_store
            .get(vpc_name_or_id, org_name)
            .await?
            .ok_or_else(|| anyhow::anyhow!("vpc '{vpc_name_or_id}' not found"))?;

        // Validate subnet is within VPC
        let subnet_net = crate::validate::subnet_within_vpc(cidr, &vpc.cidr)?;

        // Check overlap with existing subnets
        let existing_subs = self.list(Some(&vpc.meta.id), None).await?;
        let existing_cidrs: Vec<String> = existing_subs.iter().map(|s| s.cidr.clone()).collect();
        crate::validate::no_overlap(&subnet_net, &existing_cidrs)?;

        // Check uniqueness within VPC
        let idx_key = format!("{}/{}", vpc.meta.id, name);
        let existing: Option<String> = self.db.get(NS_SUB_IDX, &idx_key).await?;
        if existing.is_some() {
            anyhow::bail!("subnet '{name}' already exists in vpc '{}'", vpc.meta.name);
        }

        let gw = crate::validate::gateway(&subnet_net);

        let subnet = Subnet {
            meta: ResourceMeta::new(SubnetId::generate().to_string(), name),
            vpc_id: vpc.meta.id.clone().into(),
            vpc_name: vpc.meta.name.clone(),
            cidr: cidr.to_string(),
            gateway: gw,
        };

        self.db.put(NS_SUB, &subnet.meta.id, &subnet).await?;
        self.db.put(NS_SUB_IDX, &idx_key, &subnet.meta.id).await?;
        add_id(&self.db, &subnet.meta.id).await?;

        Ok(subnet)
    }

    pub async fn get(
        &self,
        name_or_id: &str,
        vpc_name_or_id: Option<&str>,
        org_name: Option<&str>,
    ) -> anyhow::Result<Option<Subnet>> {
        if SubnetId::looks_like_id(name_or_id) {
            return self.db.get(NS_SUB, name_or_id).await.map_err(Into::into);
        }
        let vpc_name = vpc_name_or_id
            .ok_or_else(|| anyhow::anyhow!("--vpc required to resolve subnet by name"))?;
        let vpc_store = crate::vpc::store::VpcStore::new(self.db.clone());
        let vpc = vpc_store
            .get(vpc_name, org_name)
            .await?
            .ok_or_else(|| anyhow::anyhow!("vpc '{vpc_name}' not found"))?;
        let idx_key = format!("{}/{}", vpc.meta.id, name_or_id);
        let id: Option<String> = self.db.get(NS_SUB_IDX, &idx_key).await?;
        match id {
            Some(id) => self.db.get(NS_SUB, &id).await.map_err(Into::into),
            None => Ok(None),
        }
    }

    pub async fn list(
        &self,
        vpc_name: Option<&str>,
        _org_name: Option<&str>,
    ) -> anyhow::Result<Vec<Subnet>> {
        let ids = load_ids(&self.db).await?;
        let mut subs = Vec::new();
        for id in &ids {
            if let Some(s) = self.db.get::<Subnet>(NS_SUB, id).await? {
                subs.push(s);
            }
        }
        match vpc_name {
            Some(name) => Ok(subs
                .into_iter()
                .filter(|s| s.vpc_name == name || s.vpc_id.as_str() == name)
                .collect()),
            None => Ok(subs),
        }
    }

    pub async fn delete(
        &self,
        name_or_id: &str,
        vpc_name: &str,
        org_name: Option<&str>,
    ) -> anyhow::Result<()> {
        let subnet = self
            .get(name_or_id, Some(vpc_name), org_name)
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!("subnet '{name_or_id}' not found in vpc '{vpc_name}'")
            })?;
        let idx_key = format!("{}/{}", subnet.vpc_id.as_str(), subnet.meta.name);
        self.db.delete(NS_SUB, &subnet.meta.id).await?;
        self.db.delete(NS_SUB_IDX, &idx_key).await?;
        remove_id(&self.db, &subnet.meta.id).await?;
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
