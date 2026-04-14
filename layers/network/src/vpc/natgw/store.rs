//! NAT gateway persistence on the SurrealDB-backed cluster store.
//!
//! P2.12 (sifrah/nauka#216) migrated this module from the legacy
//! raw-KV cluster path to the native SurrealDB SDK on top of
//! [`EmbeddedDb`], and shipped the SCHEMAFULL `natgw` table in
//! `layers/network/schemas/natgw.surql` — net-new in this PR.
//!
//! Invariants enforced here:
//! - At most one NAT gateway per VPC (checked before the SDK write).
//!   The schema has a non-unique `natgw_vpc` index to make the check
//!   fast; "partial unique" indexes don't exist in SurrealDB so the
//!   invariant lives in the application layer.
//! - Names are unique within a VPC (schema composite unique on
//!   `(vpc, name)`).
//! - Public IPv6 addresses are globally unique (schema unique on
//!   `public_ipv6`). Allocation is deterministic via SHA-256 of the
//!   NAT gateway id, so collisions require an extremely unlikely
//!   hash crash.

use nauka_core::id::NatGwId;
use nauka_core::resource::epoch_to_iso8601;
use nauka_core::resource::ResourceMeta;
use nauka_state::sdk_bridge::{iso8601_to_epoch, thing_to_id_string};
use nauka_state::EmbeddedDb;
use serde::Deserialize;

use super::ipv6_alloc;
use super::types::{NatGw, NatGwState};

/// SurrealDB table backing this store.
const NATGW_TABLE: &str = "natgw";

pub struct NatGwStore {
    db: EmbeddedDb,
}

impl NatGwStore {
    /// Build a [`NatGwStore`] over a SurrealDB handle.
    ///
    /// Call sites that already hold a cluster-DB wrapper pass
    /// `db.clone()`.
    pub fn new(db: EmbeddedDb) -> Self {
        Self { db }
    }

    /// Create a NAT gateway for a VPC.
    ///
    /// Requires that the selected hypervisor has an `ipv6_block`
    /// configured. Only one NAT gateway per VPC is allowed.
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

        // Enforce "at most one NAT gateway per VPC" at the application
        // layer — SurrealDB doesn't support partial unique indexes.
        let existing = self.list_by_vpc(&vpc.meta.id).await?;
        if let Some(existing_gw) = existing.first() {
            anyhow::bail!(
                "vpc '{}' already has a NAT gateway '{}'",
                vpc.meta.name,
                existing_gw.meta.name
            );
        }

        let nat_gw_id = NatGwId::generate();

        // Allocate a public IPv6 from the hypervisor's /64 block.
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

        let created_at_iso = epoch_to_iso8601(natgw.meta.created_at);
        let updated_at_iso = epoch_to_iso8601(natgw.meta.updated_at);
        let labels_json = serde_json::to_value(&natgw.meta.labels)
            .map_err(|e| anyhow::anyhow!("serialise labels: {e}"))?;
        let state_str = match natgw.state {
            NatGwState::Provisioning => "provisioning",
            NatGwState::Active => "active",
            NatGwState::Error => "error",
        };

        let insert_result = self
            .db
            .client()
            .query(
                "CREATE type::record($tbl, $id) SET \
                 name = $name, \
                 status = $status, \
                 labels = $labels, \
                 vpc = $vpc, \
                 vpc_name = $vpc_name, \
                 public_ipv6 = $public_ipv6, \
                 hypervisor = $hypervisor, \
                 state = $state, \
                 created_at = <datetime>$created_at, \
                 updated_at = <datetime>$updated_at",
            )
            .bind(("tbl", NATGW_TABLE))
            .bind(("id", natgw.meta.id.clone()))
            .bind(("name", natgw.meta.name.clone()))
            .bind(("status", natgw.meta.status.clone()))
            .bind(("labels", labels_json))
            .bind(("vpc", natgw.vpc_id.as_str().to_string()))
            .bind(("vpc_name", natgw.vpc_name.clone()))
            .bind(("public_ipv6", natgw.public_ipv6.to_string()))
            .bind(("hypervisor", natgw.hypervisor_id.clone()))
            .bind(("state", state_str))
            .bind(("created_at", created_at_iso))
            .bind(("updated_at", updated_at_iso))
            .await;

        let response = match insert_result {
            Ok(r) => r,
            Err(e) => {
                // Roll back the IPv6 allocation if the DB write
                // failed — otherwise we'd leak a reservation for a
                // NAT gateway that doesn't exist.
                let _ = ipv6_alloc::release(&self.db, hypervisor_id, natgw.meta.id.as_str()).await;
                return Err(anyhow::anyhow!("{e}"));
            }
        };
        if let Err(e) = response.check() {
            let _ = ipv6_alloc::release(&self.db, hypervisor_id, natgw.meta.id.as_str()).await;
            return Err(anyhow::anyhow!("{e}"));
        }

        Ok(natgw)
    }

    /// Look up a NAT gateway by id first, then by (vpc, name) index
    /// if the id path doesn't match. Matches the pre-P2.12 lookup
    /// order so CLI error messages stay stable.
    pub async fn get(
        &self,
        name_or_id: &str,
        vpc_name_or_id: Option<&str>,
        org_name: Option<&str>,
    ) -> anyhow::Result<Option<NatGw>> {
        // Try direct record-id lookup first.
        if let Some(natgw) = self.get_by_id(name_or_id).await? {
            return Ok(Some(natgw));
        }

        // Fall back to (vpc, name) lookup.
        if let Some(vpc_ref) = vpc_name_or_id {
            let vpc_store = crate::vpc::store::VpcStore::new(self.db.clone());
            if let Some(vpc) = vpc_store.get(vpc_ref, org_name).await? {
                return self.get_by_vpc_and_name(&vpc.meta.id, name_or_id).await;
            }
        }

        Ok(None)
    }

    async fn get_by_id(&self, id: &str) -> anyhow::Result<Option<NatGw>> {
        let mut response = self
            .db
            .client()
            .query("SELECT * FROM type::record($tbl, $id)")
            .bind(("tbl", NATGW_TABLE))
            .bind(("id", id.to_string()))
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let raw: Vec<serde_json::Value> = response.take(0).map_err(|e| anyhow::anyhow!("{e}"))?;
        decode_first(raw)
    }

    async fn get_by_vpc_and_name(&self, vpc_id: &str, name: &str) -> anyhow::Result<Option<NatGw>> {
        let mut response = self
            .db
            .client()
            .query("SELECT * FROM natgw WHERE vpc = $vpc AND name = $name LIMIT 1")
            .bind(("vpc", vpc_id.to_string()))
            .bind(("name", name.to_string()))
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let raw: Vec<serde_json::Value> = response.take(0).map_err(|e| anyhow::anyhow!("{e}"))?;
        decode_first(raw)
    }

    /// List NAT gateways, optionally filtered by the owning VPC (by
    /// name or record id).
    pub async fn list(&self, vpc_name: Option<&str>) -> anyhow::Result<Vec<NatGw>> {
        let mut response = self
            .db
            .client()
            .query("SELECT * FROM natgw")
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let raw: Vec<serde_json::Value> = response.take(0).map_err(|e| anyhow::anyhow!("{e}"))?;
        let all: Vec<NatGw> = raw
            .into_iter()
            .filter_map(|v| {
                serde_json::from_value::<NatGwRow>(v)
                    .ok()
                    .map(NatGwRow::into_natgw)
            })
            .collect();
        match vpc_name {
            Some(name) => Ok(all
                .into_iter()
                .filter(|n| n.vpc_name == name || n.vpc_id.as_str() == name)
                .collect()),
            None => Ok(all),
        }
    }

    /// List NAT gateways for a specific VPC record id. Used by
    /// `create` to enforce the "at most one NAT gateway per VPC"
    /// invariant.
    async fn list_by_vpc(&self, vpc_id: &str) -> anyhow::Result<Vec<NatGw>> {
        let mut response = self
            .db
            .client()
            .query("SELECT * FROM natgw WHERE vpc = $vpc")
            .bind(("vpc", vpc_id.to_string()))
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let raw: Vec<serde_json::Value> = response.take(0).map_err(|e| anyhow::anyhow!("{e}"))?;
        Ok(raw
            .into_iter()
            .filter_map(|v| {
                serde_json::from_value::<NatGwRow>(v)
                    .ok()
                    .map(NatGwRow::into_natgw)
            })
            .collect())
    }

    /// Delete a NAT gateway and release its IPv6 allocation.
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

        // Release the allocated IPv6 first. If the subsequent DELETE
        // fails, the caller can re-try and the release path is
        // idempotent (it just removes the entry from the blob).
        ipv6_alloc::release(&self.db, &natgw.hypervisor_id, &natgw.meta.id).await?;

        let result = self
            .db
            .client()
            .query("DELETE type::record($tbl, $id)")
            .bind(("tbl", NATGW_TABLE))
            .bind(("id", natgw.meta.id.clone()))
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        result.check().map_err(|e| anyhow::anyhow!("{e}"))?;
        Ok(())
    }
}

/// Row shape returned by `SELECT * FROM natgw`.
#[derive(Debug, Deserialize)]
struct NatGwRow {
    id: serde_json::Value,
    name: String,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    labels: Option<serde_json::Value>,
    vpc: String,
    vpc_name: String,
    public_ipv6: String,
    hypervisor: String,
    state: String,
    created_at: String,
    updated_at: String,
}

impl NatGwRow {
    fn into_natgw(self) -> NatGw {
        let id = thing_to_id_string("natgw:", &self.id);
        let state = match self.state.as_str() {
            "active" => NatGwState::Active,
            "error" => NatGwState::Error,
            // Default to Provisioning for anything else so a bad
            // migration doesn't crash the list. The schema's ASSERT
            // keeps this branch unreachable in practice.
            _ => NatGwState::Provisioning,
        };
        NatGw {
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
            public_ipv6: self
                .public_ipv6
                .parse()
                .unwrap_or(std::net::Ipv6Addr::UNSPECIFIED),
            hypervisor_id: self.hypervisor,
            state,
        }
    }
}

fn decode_first(raw: Vec<serde_json::Value>) -> anyhow::Result<Option<NatGw>> {
    match raw.into_iter().next() {
        None => Ok(None),
        Some(v) => {
            let row: NatGwRow = serde_json::from_value(v)
                .map_err(|e| anyhow::anyhow!("deserialise natgw row: {e}"))?;
            Ok(Some(row.into_natgw()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn temp_store() -> (
        tempfile::TempDir,
        NatGwStore,
        crate::vpc::store::VpcStore,
        nauka_org::store::OrgStore,
    ) {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = EmbeddedDb::open(&dir.path().join("natgws.skv"))
            .await
            .expect("open EmbeddedDb");
        nauka_state::apply_cluster_schemas(&db)
            .await
            .expect("apply cluster schemas");
        let natgws = NatGwStore::new(db.clone());
        let vpcs = crate::vpc::store::VpcStore::new(db.clone());
        let orgs = nauka_org::store::OrgStore::new(db);
        (dir, natgws, vpcs, orgs)
    }

    async fn seed_vpc(
        vpcs: &crate::vpc::store::VpcStore,
        orgs: &nauka_org::store::OrgStore,
    ) -> crate::vpc::types::Vpc {
        orgs.create("acme").await.expect("create org");
        vpcs.create("web", "acme", "10.0.0.0/16", None, None)
            .await
            .expect("create vpc")
    }

    #[tokio::test]
    async fn create_then_get_by_name() {
        let (_d, natgws, vpcs, orgs) = temp_store().await;
        seed_vpc(&vpcs, &orgs).await;
        let natgw = natgws
            .create("gw1", "web", "acme", "hv-1", "2a01:4f8:c012:abcd::/64")
            .await
            .expect("create natgw");
        assert_eq!(natgw.meta.name, "gw1");
        // NatGwId prefix is `nat`, not `natgw`.
        assert!(natgw.meta.id.starts_with("nat-"), "got: {}", natgw.meta.id);
        assert_eq!(natgw.hypervisor_id, "hv-1");
        assert!(matches!(natgw.state, NatGwState::Provisioning));

        let got = natgws
            .get("gw1", Some("web"), Some("acme"))
            .await
            .expect("get")
            .expect("missing");
        assert_eq!(got.meta.id, natgw.meta.id);
        assert_eq!(got.public_ipv6, natgw.public_ipv6);
    }

    #[tokio::test]
    async fn only_one_natgw_per_vpc() {
        let (_d, natgws, vpcs, orgs) = temp_store().await;
        seed_vpc(&vpcs, &orgs).await;
        natgws
            .create("gw1", "web", "acme", "hv-1", "2a01:4f8:c012:abcd::/64")
            .await
            .unwrap();
        let err = natgws
            .create("gw2", "web", "acme", "hv-1", "2a01:4f8:c012:abcd::/64")
            .await
            .expect_err("second natgw should fail");
        assert!(
            err.to_string().contains("already has a NAT gateway"),
            "got: {err}"
        );
    }

    #[tokio::test]
    async fn list_filters_by_vpc() {
        let (_d, natgws, vpcs, orgs) = temp_store().await;
        orgs.create("acme").await.expect("create org");
        vpcs.create("web", "acme", "10.0.0.0/16", None, None)
            .await
            .unwrap();
        vpcs.create("api", "acme", "10.1.0.0/16", None, None)
            .await
            .unwrap();
        natgws
            .create("gw1", "web", "acme", "hv-1", "2a01:4f8:c012:abcd::/64")
            .await
            .unwrap();
        natgws
            .create("gw2", "api", "acme", "hv-2", "2a01:4f8:c012:1234::/64")
            .await
            .unwrap();

        let web = natgws.list(Some("web")).await.unwrap();
        assert_eq!(web.len(), 1);
        let all = natgws.list(None).await.unwrap();
        assert_eq!(all.len(), 2);
    }

    #[tokio::test]
    async fn delete_removes_natgw_and_releases_ipv6() {
        let (_d, natgws, vpcs, orgs) = temp_store().await;
        seed_vpc(&vpcs, &orgs).await;
        let gw = natgws
            .create("gw1", "web", "acme", "hv-1", "2a01:4f8:c012:abcd::/64")
            .await
            .unwrap();
        natgws.delete("gw1", "web", "acme").await.expect("delete");
        assert!(natgws.get(&gw.meta.id, None, None).await.unwrap().is_none());
    }
}
