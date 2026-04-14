//! VPC peering persistence on the SurrealDB-backed cluster store.
//!
//! P2.12 (sifrah/nauka#216) migrated this module from the legacy
//! raw-KV cluster path to the native SurrealDB SDK on top of
//! [`EmbeddedDb`], and shipped the SCHEMAFULL `vpc_peering` table in
//! `layers/network/schemas/peering.surql` — both are net-new in this
//! PR because P2.5 didn't include peering in its initial schema
//! bundle.
//!
//! The `(vpc, peer_vpc)` pair is enforced unique at the schema level;
//! the reverse pair `(peer_vpc, vpc)` is enforced at the application
//! level here because SurrealDB can't express "unique on an unordered
//! pair of fields" directly. That check runs before the SDK `CREATE`
//! so we return the classic `"peering already exists between '<a>'
//! and '<b>'"` error text without racing the schema.

use nauka_core::id::PeeringId;
use nauka_core::resource::epoch_to_iso8601;
use nauka_core::resource::ResourceMeta;
use nauka_state::sdk_bridge::{iso8601_to_epoch, thing_to_id_string};
use nauka_state::EmbeddedDb;
use serde::Deserialize;

use super::types::{PeeringState, VpcPeering};

/// SurrealDB table backing this store.
const PEERING_TABLE: &str = "vpc_peering";

pub struct PeeringStore {
    db: EmbeddedDb,
}

impl PeeringStore {
    /// Build a [`PeeringStore`] over a SurrealDB handle.
    ///
    /// Call sites that already hold a cluster-DB wrapper pass
    /// `db.clone()`.
    pub fn new(db: EmbeddedDb) -> Self {
        Self { db }
    }

    /// Create a new peering between two VPCs in the same org.
    ///
    /// Rejects self-peering, rejects overlapping CIDRs, and rejects
    /// any attempt to create a pairing that already exists in either
    /// direction. The schema's composite unique index on
    /// `(vpc, peer_vpc)` closes the "forward" race; this method
    /// additionally checks the reverse direction before hitting the
    /// schema.
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

        // Can't peer with self.
        if vpc.meta.id == peer_vpc.meta.id {
            anyhow::bail!("cannot peer a VPC with itself");
        }

        // CIDRs must not overlap (one containing the other would
        // make routing ambiguous).
        if let (Ok(a), Ok(b)) = (
            vpc.cidr.parse::<ipnet::Ipv4Net>(),
            peer_vpc.cidr.parse::<ipnet::Ipv4Net>(),
        ) {
            if a.contains(&b) || b.contains(&a) {
                anyhow::bail!("VPC CIDRs overlap: {} and {}", vpc.cidr, peer_vpc.cidr);
            }
        }

        // Check no existing peering in either direction. The schema
        // enforces the forward direction on write; we short-circuit
        // early so the error message matches the pre-P2.12 wording.
        if self.exists_pair(&vpc.meta.id, &peer_vpc.meta.id).await?
            || self.exists_pair(&peer_vpc.meta.id, &vpc.meta.id).await?
        {
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

        let created_at_iso = epoch_to_iso8601(peering.meta.created_at);
        let updated_at_iso = epoch_to_iso8601(peering.meta.updated_at);
        let labels_json = serde_json::to_value(&peering.meta.labels)
            .map_err(|e| anyhow::anyhow!("serialise labels: {e}"))?;
        let state_str = match peering.state {
            PeeringState::Active => "active",
            PeeringState::Pending => "pending",
        };

        let response = self
            .db
            .client()
            .query(
                "CREATE type::record($tbl, $id) SET \
                 name = $name, \
                 status = $status, \
                 labels = $labels, \
                 vpc = $vpc, \
                 vpc_name = $vpc_name, \
                 peer_vpc = $peer_vpc, \
                 peer_vpc_name = $peer_vpc_name, \
                 state = $state, \
                 created_at = <datetime>$created_at, \
                 updated_at = <datetime>$updated_at",
            )
            .bind(("tbl", PEERING_TABLE))
            .bind(("id", peering.meta.id.clone()))
            .bind(("name", peering.meta.name.clone()))
            .bind(("status", peering.meta.status.clone()))
            .bind(("labels", labels_json))
            .bind(("vpc", peering.vpc_id.as_str().to_string()))
            .bind(("vpc_name", peering.vpc_name.clone()))
            .bind(("peer_vpc", peering.peer_vpc_id.as_str().to_string()))
            .bind(("peer_vpc_name", peering.peer_vpc_name.clone()))
            .bind(("state", state_str))
            .bind(("created_at", created_at_iso))
            .bind(("updated_at", updated_at_iso))
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        response.check().map_err(|e| anyhow::anyhow!("{e}"))?;

        Ok(peering)
    }

    async fn exists_pair(&self, vpc_id: &str, peer_vpc_id: &str) -> anyhow::Result<bool> {
        let mut response = self
            .db
            .client()
            .query(
                "SELECT name FROM vpc_peering \
                 WHERE vpc = $vpc AND peer_vpc = $peer_vpc LIMIT 1",
            )
            .bind(("vpc", vpc_id.to_string()))
            .bind(("peer_vpc", peer_vpc_id.to_string()))
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let rows: Vec<serde_json::Value> = response.take(0).map_err(|e| anyhow::anyhow!("{e}"))?;
        Ok(!rows.is_empty())
    }

    /// Look up a peering by id. Peerings don't have a user-facing
    /// name so they're always retrieved by their auto-generated
    /// record id.
    pub async fn get(&self, name_or_id: &str) -> anyhow::Result<Option<VpcPeering>> {
        let mut response = self
            .db
            .client()
            .query("SELECT * FROM type::record($tbl, $id)")
            .bind(("tbl", PEERING_TABLE))
            .bind(("id", name_or_id.to_string()))
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let raw: Vec<serde_json::Value> = response.take(0).map_err(|e| anyhow::anyhow!("{e}"))?;
        decode_first(raw)
    }

    /// List peerings, optionally filtered to those touching a
    /// specific VPC (by name or record id) on either side of the
    /// pair.
    pub async fn list(&self, vpc_name: Option<&str>) -> anyhow::Result<Vec<VpcPeering>> {
        let mut response = self
            .db
            .client()
            .query("SELECT * FROM vpc_peering")
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let raw: Vec<serde_json::Value> = response.take(0).map_err(|e| anyhow::anyhow!("{e}"))?;
        let peers: Vec<VpcPeering> = raw
            .into_iter()
            .filter_map(|v| {
                serde_json::from_value::<PeeringRow>(v)
                    .ok()
                    .map(PeeringRow::into_peering)
            })
            .collect();
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

    /// Delete a peering. Peerings have no children, so this is a
    /// single record-id DELETE.
    pub async fn delete(&self, name_or_id: &str) -> anyhow::Result<()> {
        let peering = self
            .get(name_or_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("peering '{name_or_id}' not found"))?;
        let result = self
            .db
            .client()
            .query("DELETE type::record($tbl, $id)")
            .bind(("tbl", PEERING_TABLE))
            .bind(("id", peering.meta.id.clone()))
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        result.check().map_err(|e| anyhow::anyhow!("{e}"))?;
        Ok(())
    }
}

/// Row shape returned by `SELECT * FROM vpc_peering`.
#[derive(Debug, Deserialize)]
struct PeeringRow {
    id: serde_json::Value,
    name: String,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    labels: Option<serde_json::Value>,
    vpc: String,
    vpc_name: String,
    peer_vpc: String,
    peer_vpc_name: String,
    state: String,
    created_at: String,
    updated_at: String,
}

impl PeeringRow {
    fn into_peering(self) -> VpcPeering {
        let id = thing_to_id_string("vpc_peering:", &self.id);
        let state = match self.state.as_str() {
            "pending" => PeeringState::Pending,
            // Default to Active for anything else so a malformed row
            // doesn't crash the list. The schema's ASSERT keeps this
            // branch unreachable in practice.
            _ => PeeringState::Active,
        };
        VpcPeering {
            meta: ResourceMeta {
                id,
                name: self.name,
                status: self.status.unwrap_or_else(|| "active".to_string()),
                labels: self
                    .labels
                    .and_then(|v| serde_json::from_value(v).ok())
                    .unwrap_or_default(),
                created_at: iso8601_to_epoch(&self.created_at),
                updated_at: iso8601_to_epoch(&self.updated_at),
            },
            vpc_id: self.vpc.into(),
            vpc_name: self.vpc_name,
            peer_vpc_id: self.peer_vpc.into(),
            peer_vpc_name: self.peer_vpc_name,
            state,
        }
    }
}

fn decode_first(raw: Vec<serde_json::Value>) -> anyhow::Result<Option<VpcPeering>> {
    match raw.into_iter().next() {
        None => Ok(None),
        Some(v) => {
            let row: PeeringRow = serde_json::from_value(v)
                .map_err(|e| anyhow::anyhow!("deserialise peering row: {e}"))?;
            Ok(Some(row.into_peering()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn temp_store() -> (
        tempfile::TempDir,
        PeeringStore,
        crate::vpc::store::VpcStore,
        nauka_org::store::OrgStore,
    ) {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = EmbeddedDb::open(&dir.path().join("peerings.skv"))
            .await
            .expect("open EmbeddedDb");
        nauka_state::apply_cluster_schemas(&db)
            .await
            .expect("apply cluster schemas");
        let peerings = PeeringStore::new(db.clone());
        let vpcs = crate::vpc::store::VpcStore::new(db.clone());
        let orgs = nauka_org::store::OrgStore::new(db);
        (dir, peerings, vpcs, orgs)
    }

    async fn seed_two_vpcs(
        vpcs: &crate::vpc::store::VpcStore,
        orgs: &nauka_org::store::OrgStore,
    ) -> (crate::vpc::types::Vpc, crate::vpc::types::Vpc) {
        orgs.create("acme").await.unwrap();
        let a = vpcs
            .create("web", "acme", "10.0.0.0/16", None, None)
            .await
            .unwrap();
        let b = vpcs
            .create("db", "acme", "10.1.0.0/16", None, None)
            .await
            .unwrap();
        (a, b)
    }

    #[tokio::test]
    async fn create_then_get_by_id() {
        let (_d, peerings, vpcs, orgs) = temp_store().await;
        seed_two_vpcs(&vpcs, &orgs).await;
        let p = peerings
            .create("web", "db", Some("acme"))
            .await
            .expect("create");
        assert_eq!(p.meta.name, "web-to-db");
        // `PeeringId` uses the `peer` prefix; the SurrealDB record
        // ends up at `vpc_peering:peer-01J…`.
        assert!(p.meta.id.starts_with("peer-"), "got: {}", p.meta.id);
        let got = peerings.get(&p.meta.id).await.unwrap().expect("missing");
        assert_eq!(got.meta.id, p.meta.id);
    }

    #[tokio::test]
    async fn self_peering_is_rejected() {
        let (_d, peerings, vpcs, orgs) = temp_store().await;
        seed_two_vpcs(&vpcs, &orgs).await;
        let err = peerings
            .create("web", "web", Some("acme"))
            .await
            .expect_err("self-peering");
        assert!(err.to_string().contains("cannot peer"), "got: {err}");
    }

    #[tokio::test]
    async fn overlapping_cidr_peering_is_rejected_upstream() {
        // The VPC layer already rejects overlapping CIDRs at creation
        // time, so a peering between two overlapping VPCs is
        // unreachable in practice — the second `VpcStore::create`
        // fails first. This test pins that upstream invariant so a
        // future change that relaxes `VpcStore` overlap checking
        // fails here instead of silently widening the surface.
        let (_d, _peerings, vpcs, orgs) = temp_store().await;
        orgs.create("acme").await.unwrap();
        vpcs.create("a", "acme", "10.0.0.0/16", None, None)
            .await
            .unwrap();
        vpcs.create("b", "acme", "10.0.0.0/24", None, None)
            .await
            .expect_err("overlap at vpc creation");
    }

    #[tokio::test]
    async fn duplicate_pair_rejected_forward() {
        let (_d, peerings, vpcs, orgs) = temp_store().await;
        seed_two_vpcs(&vpcs, &orgs).await;
        peerings.create("web", "db", Some("acme")).await.unwrap();
        let err = peerings
            .create("web", "db", Some("acme"))
            .await
            .expect_err("duplicate");
        assert!(err.to_string().contains("already exists"), "got: {err}");
    }

    #[tokio::test]
    async fn duplicate_pair_rejected_reverse() {
        let (_d, peerings, vpcs, orgs) = temp_store().await;
        seed_two_vpcs(&vpcs, &orgs).await;
        peerings.create("web", "db", Some("acme")).await.unwrap();
        let err = peerings
            .create("db", "web", Some("acme"))
            .await
            .expect_err("reverse duplicate");
        assert!(err.to_string().contains("already exists"), "got: {err}");
    }

    #[tokio::test]
    async fn list_touching_vpc() {
        let (_d, peerings, vpcs, orgs) = temp_store().await;
        orgs.create("acme").await.unwrap();
        vpcs.create("a", "acme", "10.0.0.0/16", None, None)
            .await
            .unwrap();
        vpcs.create("b", "acme", "10.1.0.0/16", None, None)
            .await
            .unwrap();
        vpcs.create("c", "acme", "10.2.0.0/16", None, None)
            .await
            .unwrap();
        peerings.create("a", "b", Some("acme")).await.unwrap();
        peerings.create("b", "c", Some("acme")).await.unwrap();

        let touching_b = peerings.list(Some("b")).await.unwrap();
        assert_eq!(touching_b.len(), 2);
        let touching_a = peerings.list(Some("a")).await.unwrap();
        assert_eq!(touching_a.len(), 1);
    }

    #[tokio::test]
    async fn delete_removes_peering() {
        let (_d, peerings, vpcs, orgs) = temp_store().await;
        seed_two_vpcs(&vpcs, &orgs).await;
        let p = peerings.create("web", "db", Some("acme")).await.unwrap();
        peerings.delete(&p.meta.id).await.expect("delete");
        assert!(peerings.get(&p.meta.id).await.unwrap().is_none());
    }
}
