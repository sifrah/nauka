use nauka_core::id::PeeringId;
use nauka_core::resource::ResourceMeta;
use nauka_hypervisor::controlplane::ClusterDb;

use super::types::{PeeringState, VpcPeering};

const NS_PEER: &str = "vpcpeer";
const NS_PEER_IDX: &str = "vpcpeer-idx";
const REG_PEERS: (&str, &str) = ("_reg", "vpcpeer-ids");

pub struct PeeringStore {
    db: ClusterDb,
}

impl PeeringStore {
    pub fn new(db: ClusterDb) -> Self {
        Self { db }
    }

    pub async fn create(
        &self,
        vpc_name_or_id: &str,
        peer_vpc_name_or_id: &str,
        org_name: Option<&str>,
    ) -> anyhow::Result<VpcPeering> {
        let vpc_store = crate::vpc::store::VpcStore::new(self.db.clone());
        let vpc = vpc_store
            .get(vpc_name_or_id, org_name)
            .await?
            .ok_or_else(|| anyhow::anyhow!("vpc '{vpc_name_or_id}' not found"))?;
        let peer_vpc = vpc_store
            .get(peer_vpc_name_or_id, org_name)
            .await?
            .ok_or_else(|| anyhow::anyhow!("vpc '{peer_vpc_name_or_id}' not found"))?;

        // Can't peer with self
        if vpc.meta.id == peer_vpc.meta.id {
            anyhow::bail!("cannot peer a VPC with itself");
        }

        // CIDRs must not overlap
        if let (Ok(a), Ok(b)) = (
            vpc.cidr.parse::<ipnet::Ipv4Net>(),
            peer_vpc.cidr.parse::<ipnet::Ipv4Net>(),
        ) {
            if a.contains(&b) || b.contains(&a) {
                anyhow::bail!("VPC CIDRs overlap: {} and {}", vpc.cidr, peer_vpc.cidr);
            }
        }

        // Check no existing peering between these two
        let idx_key = format!("{}/{}", vpc.meta.id, peer_vpc.meta.id);
        let reverse_key = format!("{}/{}", peer_vpc.meta.id, vpc.meta.id);
        let existing: Option<String> = self.db.get(NS_PEER_IDX, &idx_key).await?;
        let existing_rev: Option<String> = self.db.get(NS_PEER_IDX, &reverse_key).await?;
        if existing.is_some() || existing_rev.is_some() {
            anyhow::bail!(
                "peering already exists between '{}' and '{}'",
                vpc.meta.name,
                peer_vpc.meta.name
            );
        }

        let auto_name = format!("{}-to-{}", vpc.meta.name, peer_vpc.meta.name);
        let peering = VpcPeering {
            meta: ResourceMeta::new(PeeringId::generate().to_string(), &auto_name),
            vpc_id: vpc.meta.id.clone().into(),
            vpc_name: vpc.meta.name.clone(),
            peer_vpc_id: peer_vpc.meta.id.clone().into(),
            peer_vpc_name: peer_vpc.meta.name.clone(),
            state: PeeringState::Active,
        };

        self.db.put(NS_PEER, &peering.meta.id, &peering).await?;
        self.db.put(NS_PEER_IDX, &idx_key, &peering.meta.id).await?;
        self.db
            .put(NS_PEER_IDX, &reverse_key, &peering.meta.id)
            .await?;
        add_id(&self.db, &peering.meta.id).await?;

        Ok(peering)
    }

    pub async fn get(&self, name_or_id: &str) -> anyhow::Result<Option<VpcPeering>> {
        // Peerings are always looked up by ID (names are auto-generated)
        self.db.get(NS_PEER, name_or_id).await.map_err(Into::into)
    }

    pub async fn list(&self, vpc_name: Option<&str>) -> anyhow::Result<Vec<VpcPeering>> {
        let ids = load_ids(&self.db).await?;
        let mut peers = Vec::new();
        for id in &ids {
            if let Some(p) = self.db.get::<VpcPeering>(NS_PEER, id).await? {
                peers.push(p);
            }
        }
        match vpc_name {
            Some(name) => Ok(peers
                .into_iter()
                .filter(|p| {
                    p.vpc_name == name
                        || p.vpc_id.as_str() == name
                        || p.peer_vpc_name == name
                        || p.peer_vpc_id.as_str() == name
                })
                .collect()),
            None => Ok(peers),
        }
    }

    pub async fn delete(&self, name_or_id: &str) -> anyhow::Result<()> {
        let peering = self
            .get(name_or_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("peering '{name_or_id}' not found"))?;
        let idx_key = format!(
            "{}/{}",
            peering.vpc_id.as_str(),
            peering.peer_vpc_id.as_str()
        );
        let reverse_key = format!(
            "{}/{}",
            peering.peer_vpc_id.as_str(),
            peering.vpc_id.as_str()
        );
        self.db.delete(NS_PEER, &peering.meta.id).await?;
        self.db.delete(NS_PEER_IDX, &idx_key).await?;
        self.db.delete(NS_PEER_IDX, &reverse_key).await?;
        remove_id(&self.db, &peering.meta.id).await?;
        Ok(())
    }
}

async fn load_ids(db: &ClusterDb) -> anyhow::Result<Vec<String>> {
    let ids: Option<Vec<String>> = db.get(REG_PEERS.0, REG_PEERS.1).await?;
    Ok(ids.unwrap_or_default())
}

async fn add_id(db: &ClusterDb, id: &str) -> anyhow::Result<()> {
    let mut ids = load_ids(db).await?;
    ids.push(id.to_string());
    db.put(REG_PEERS.0, REG_PEERS.1, &ids).await?;
    Ok(())
}

async fn remove_id(db: &ClusterDb, id: &str) -> anyhow::Result<()> {
    let mut ids = load_ids(db).await?;
    ids.retain(|i| i != id);
    db.put(REG_PEERS.0, REG_PEERS.1, &ids).await?;
    Ok(())
}
