use nauka_core::id::NatGwId;
use nauka_core::resource::ResourceMeta;
use nauka_hypervisor::controlplane::ClusterDb;

use super::ipv6_alloc;
use super::types::{NatGw, NatGwState};

const NS_NATGW: &str = "natgw";
const NS_NATGW_IDX: &str = "natgw-idx";
const REG_V2_NS: &str = "_reg_v2";
const REG_V2_PREFIX: &str = "natgw/";
const REG_V1: (&str, &str) = ("_reg", "natgw-ids");

pub struct NatGwStore {
    db: ClusterDb,
}

impl NatGwStore {
    pub fn new(db: ClusterDb) -> Self {
        Self { db }
    }

    /// Create a NAT gateway for a VPC.
    ///
    /// Requires that the selected hypervisor has an `ipv6_block` configured.
    /// Only one NAT gateway per VPC is allowed.
    pub async fn create(
        &self,
        name: &str,
        vpc_name_or_id: &str,
        org_name: &str,
        hypervisor_id: &str,
        ipv6_block: &str,
    ) -> anyhow::Result<NatGw> {
        let vpc_store = crate::vpc::store::VpcStore::new(self.db.clone());
        let vpc = vpc_store
            .get(vpc_name_or_id, Some(org_name))
            .await?
            .ok_or_else(|| anyhow::anyhow!("vpc '{vpc_name_or_id}' not found"))?;

        // Check: one NAT GW per VPC
        let existing = self.list_by_vpc(Some(&vpc.meta.id)).await?;
        if !existing.is_empty() {
            anyhow::bail!(
                "vpc '{}' already has a NAT gateway '{}'",
                vpc.meta.name,
                existing[0].meta.name
            );
        }

        let nat_gw_id = NatGwId::generate();

        // Allocate a public IPv6 from the hypervisor's /64 block
        let public_ipv6 =
            ipv6_alloc::allocate(&self.db, hypervisor_id, ipv6_block, nat_gw_id.as_str()).await?;

        let natgw = NatGw {
            meta: ResourceMeta::new(nat_gw_id.to_string(), name),
            vpc_id: vpc.meta.id.clone().into(),
            vpc_name: vpc.meta.name.clone(),
            public_ipv6,
            hypervisor_id: hypervisor_id.to_string(),
            state: NatGwState::Provisioning,
        };

        // Persist
        self.db.put(NS_NATGW, &natgw.meta.id, &natgw).await?;
        let idx_key = format!("{}/{}", vpc.meta.id, name);
        self.db.put(NS_NATGW_IDX, &idx_key, &natgw.meta.id).await?;
        add_id(&self.db, &natgw.meta.id).await?;

        Ok(natgw)
    }

    pub async fn get(
        &self,
        name_or_id: &str,
        vpc_name_or_id: Option<&str>,
        org_name: Option<&str>,
    ) -> anyhow::Result<Option<NatGw>> {
        // Try direct ID lookup first
        if let Some(natgw) = self.db.get::<NatGw>(NS_NATGW, name_or_id).await? {
            return Ok(Some(natgw));
        }

        // Try index lookup by vpc + name
        if let Some(vpc_ref) = vpc_name_or_id {
            let vpc_store = crate::vpc::store::VpcStore::new(self.db.clone());
            if let Some(vpc) = vpc_store.get(vpc_ref, org_name).await? {
                let idx_key = format!("{}/{}", vpc.meta.id, name_or_id);
                if let Some(id) = self.db.get::<String>(NS_NATGW_IDX, &idx_key).await? {
                    return self
                        .db
                        .get::<NatGw>(NS_NATGW, &id)
                        .await
                        .map_err(Into::into);
                }
            }
        }

        Ok(None)
    }

    pub async fn list(&self, vpc_name: Option<&str>) -> anyhow::Result<Vec<NatGw>> {
        let all = self.list_by_vpc(None).await?;
        match vpc_name {
            Some(name) => Ok(all
                .into_iter()
                .filter(|n| n.vpc_name == name || n.vpc_id.as_str() == name)
                .collect()),
            None => Ok(all),
        }
    }

    async fn list_by_vpc(&self, vpc_id: Option<&str>) -> anyhow::Result<Vec<NatGw>> {
        let ids = load_ids(&self.db).await?;
        let mut items = Vec::new();
        for id in &ids {
            if let Some(natgw) = self.db.get::<NatGw>(NS_NATGW, id).await? {
                if let Some(vid) = vpc_id {
                    if natgw.vpc_id.as_str() == vid {
                        items.push(natgw);
                    }
                } else {
                    items.push(natgw);
                }
            }
        }
        Ok(items)
    }

    pub async fn delete(
        &self,
        name_or_id: &str,
        vpc_name: &str,
        org_name: &str,
    ) -> anyhow::Result<()> {
        let natgw = self
            .get(name_or_id, Some(vpc_name), Some(org_name))
            .await?
            .ok_or_else(|| anyhow::anyhow!("nat-gateway '{name_or_id}' not found"))?;

        // Release the allocated IPv6
        ipv6_alloc::release(&self.db, &natgw.hypervisor_id, &natgw.meta.id).await?;

        // Remove from store
        let idx_key = format!("{}/{}", natgw.vpc_id.as_str(), natgw.meta.name);
        self.db.delete(NS_NATGW, &natgw.meta.id).await?;
        self.db.delete(NS_NATGW_IDX, &idx_key).await?;
        remove_id(&self.db, &natgw.meta.id).await?;

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
