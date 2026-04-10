use nauka_core::id::VpcId;
use nauka_core::resource::ResourceMeta;
use nauka_hypervisor::controlplane::ClusterDb;

use super::types::Vpc;

const NS_VPC: &str = "vpc";
const NS_VPC_IDX: &str = "vpc-idx";
const REG_V2_NS: &str = "_reg_v2";
const REG_V2_PREFIX: &str = "vpc/";
/// Legacy v1 registry key — read during migration, never written.
const REG_V1: (&str, &str) = ("_reg", "vpc-ids");
const VNI_COUNTER: (&str, &str) = ("_reg", "vni-counter");
const VNI_START: u32 = 100;

pub struct VpcStore {
    db: ClusterDb,
}

impl VpcStore {
    pub fn new(db: ClusterDb) -> Self {
        Self { db }
    }

    async fn next_vni(&self) -> anyhow::Result<u32> {
        let current: Option<u32> = self.db.get(VNI_COUNTER.0, VNI_COUNTER.1).await?;
        let vni = current.unwrap_or(VNI_START);
        self.db
            .put(VNI_COUNTER.0, VNI_COUNTER.1, &(vni + 1))
            .await?;
        Ok(vni)
    }

    pub async fn create(
        &self,
        name: &str,
        org_name: &str,
        cidr: &str,
        project_id: Option<String>,
        env_id: Option<String>,
    ) -> anyhow::Result<Vpc> {
        // Resolve org
        let org_store = nauka_org::store::OrgStore::new(self.db.clone());
        let org = org_store
            .get(org_name)
            .await?
            .ok_or_else(|| anyhow::anyhow!("org '{org_name}' not found"))?;

        // Validate CIDR
        crate::validate::private_cidr(cidr)?;

        // Check uniqueness within org
        let idx_key = format!("{}/{}", org.meta.id, name);
        let existing: Option<String> = self.db.get(NS_VPC_IDX, &idx_key).await?;
        if existing.is_some() {
            anyhow::bail!("vpc '{name}' already exists in org '{org_name}'");
        }

        let vni = self.next_vni().await?;

        let vpc = Vpc {
            meta: ResourceMeta::new(VpcId::generate().to_string(), name),
            cidr: cidr.to_string(),
            org_id: org.meta.id.clone().into(),
            org_name: org.meta.name.clone(),
            vni,
            project_id: project_id.map(|s| s.into()),
            env_id: env_id.map(|s| s.into()),
        };

        self.db.put(NS_VPC, &vpc.meta.id, &vpc).await?;
        self.db.put(NS_VPC_IDX, &idx_key, &vpc.meta.id).await?;
        add_id(&self.db, &vpc.meta.id).await?;

        Ok(vpc)
    }

    pub async fn get(
        &self,
        name_or_id: &str,
        org_name: Option<&str>,
    ) -> anyhow::Result<Option<Vpc>> {
        if VpcId::looks_like_id(name_or_id) {
            return self.db.get(NS_VPC, name_or_id).await.map_err(Into::into);
        }
        let org_name =
            org_name.ok_or_else(|| anyhow::anyhow!("--org required to resolve VPC by name"))?;
        let org_store = nauka_org::store::OrgStore::new(self.db.clone());
        let org = org_store
            .get(org_name)
            .await?
            .ok_or_else(|| anyhow::anyhow!("org '{org_name}' not found"))?;
        let idx_key = format!("{}/{}", org.meta.id, name_or_id);
        let id: Option<String> = self.db.get(NS_VPC_IDX, &idx_key).await?;
        match id {
            Some(id) => self.db.get(NS_VPC, &id).await.map_err(Into::into),
            None => Ok(None),
        }
    }

    pub async fn list(&self, org_name: Option<&str>) -> anyhow::Result<Vec<Vpc>> {
        let ids = load_ids(&self.db).await?;
        let mut vpcs = Vec::new();
        for id in &ids {
            if let Some(v) = self.db.get::<Vpc>(NS_VPC, id).await? {
                vpcs.push(v);
            }
        }
        match org_name {
            Some(name) => Ok(vpcs
                .into_iter()
                .filter(|v| v.org_name == name || v.org_id.as_str() == name)
                .collect()),
            None => Ok(vpcs),
        }
    }

    pub async fn delete(&self, name_or_id: &str, org_name: &str) -> anyhow::Result<()> {
        let vpc = self
            .get(name_or_id, Some(org_name))
            .await?
            .ok_or_else(|| anyhow::anyhow!("vpc '{name_or_id}' not found in org '{org_name}'"))?;

        // Check for child subnets
        let sub_store = super::subnet::store::SubnetStore::new(self.db.clone());
        let subs = sub_store.list(Some(&vpc.meta.name), Some(org_name)).await?;
        if !subs.is_empty() {
            anyhow::bail!(
                "vpc '{}' has {} subnet(s). Delete them first.",
                vpc.meta.name,
                subs.len()
            );
        }

        // Check for child NAT gateways
        let natgw_store = super::natgw::store::NatGwStore::new(self.db.clone());
        let natgws = natgw_store.list(Some(&vpc.meta.name)).await?;
        if !natgws.is_empty() {
            anyhow::bail!(
                "vpc '{}' has a NAT gateway. Delete it first.",
                vpc.meta.name,
            );
        }

        let idx_key = format!("{}/{}", vpc.org_id.as_str(), vpc.meta.name);
        self.db.delete(NS_VPC, &vpc.meta.id).await?;
        self.db.delete(NS_VPC_IDX, &idx_key).await?;
        remove_id(&self.db, &vpc.meta.id).await?;
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
