//! Subnet persistence on the SurrealDB-backed cluster store.
//!
//! P2.12 (sifrah/nauka#216) migrated this module from the legacy
//! raw-KV cluster path to the native SurrealDB SDK on top of
//! [`EmbeddedDb`]. Every method reaches the SDK via
//! `self.db.client()` and writes/reads against the SCHEMAFULL
//! `subnet` table defined in `layers/network/schemas/subnet.surql`.
//!
//! The legacy `sub` / `sub-idx` / `_reg_v2` sidecar namespaces are
//! gone: the schema's composite unique index on `(vpc, name)` plus
//! the secondary uniqueness on `(vpc, cidr)` enforce every invariant
//! the pre-P2.12 store checked manually.

use nauka_core::id::SubnetId;
use nauka_core::resource::epoch_to_iso8601;
use nauka_core::resource::ResourceMeta;
use nauka_state::sdk_bridge::{classify_create_error, iso8601_to_epoch, thing_to_id_string};
use nauka_state::EmbeddedDb;
use serde::Deserialize;

use super::types::Subnet;

/// SurrealDB table backing this store.
const SUBNET_TABLE: &str = "subnet";

pub struct SubnetStore {
    db: EmbeddedDb,
}

impl SubnetStore {
    /// Build a [`SubnetStore`] over a SurrealDB handle.
    ///
    /// Call sites that already hold a cluster-DB wrapper pass
    /// `db.clone()`.
    pub fn new(db: EmbeddedDb) -> Self {
        Self { db }
    }

    /// Create a new subnet within a VPC.
    ///
    /// Resolves the owning VPC, validates the CIDR is inside the
    /// VPC's address space and doesn't overlap any sibling subnet,
    /// then writes the row via `CREATE` against the SCHEMAFULL
    /// `subnet` table. Duplicate-name + duplicate-CIDR conflicts are
    /// rejected by the schema's composite unique indexes.
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

        // Validate subnet is within VPC.
        let subnet_net = crate::validate::subnet_within_vpc(cidr, &vpc.cidr)?;

        // Check overlap with existing sibling subnets in the VPC.
        let existing_subs = self.list(Some(&vpc.meta.id), None).await?;
        let existing_cidrs: Vec<String> = existing_subs.iter().map(|s| s.cidr.clone()).collect();
        crate::validate::no_overlap(&subnet_net, &existing_cidrs)?;

        let gateway = crate::validate::gateway(&subnet_net);

        let subnet = Subnet {
            meta: ResourceMeta::new(SubnetId::generate().to_string(), name),
            vpc_id: vpc.meta.id.clone().into(),
            vpc_name: vpc.meta.name.clone(),
            cidr: cidr.to_string(),
            gateway,
        };

        let created_at_iso = epoch_to_iso8601(subnet.meta.created_at);
        let updated_at_iso = epoch_to_iso8601(subnet.meta.updated_at);
        let labels_json = serde_json::to_value(&subnet.meta.labels)
            .map_err(|e| anyhow::anyhow!("serialise labels: {e}"))?;

        let query_result = self
            .db
            .client()
            .query(
                "CREATE type::record($tbl, $id) SET \
                 name = $name, \
                 status = $status, \
                 labels = $labels, \
                 cidr = $cidr, \
                 gateway = $gateway, \
                 vpc = $vpc, \
                 vpc_name = $vpc_name, \
                 created_at = <datetime>$created_at, \
                 updated_at = <datetime>$updated_at",
            )
            .bind(("tbl", SUBNET_TABLE))
            .bind(("id", subnet.meta.id.clone()))
            .bind(("name", subnet.meta.name.clone()))
            .bind(("status", subnet.meta.status.clone()))
            .bind(("labels", labels_json))
            .bind(("cidr", subnet.cidr.clone()))
            .bind(("gateway", subnet.gateway.clone()))
            .bind(("vpc", subnet.vpc_id.as_str().to_string()))
            .bind(("vpc_name", subnet.vpc_name.clone()))
            .bind(("created_at", created_at_iso))
            .bind(("updated_at", updated_at_iso))
            .await;
        let response = match query_result {
            Ok(r) => r,
            Err(e) => return Err(classify_subnet_error(name, &vpc.meta.name, &e.to_string())),
        };
        if let Err(e) = response.check() {
            return Err(classify_subnet_error(name, &vpc.meta.name, &e.to_string()));
        }

        Ok(subnet)
    }

    /// Look up a subnet by id (when the input looks like a
    /// [`SubnetId`]) or by name (otherwise).
    pub async fn get(
        &self,
        name_or_id: &str,
        vpc_name_or_id: Option<&str>,
        org_name: Option<&str>,
    ) -> anyhow::Result<Option<Subnet>> {
        if SubnetId::looks_like_id(name_or_id) {
            return self.get_by_id(name_or_id).await;
        }
        let vpc_ref = vpc_name_or_id
            .ok_or_else(|| anyhow::anyhow!("--vpc required to resolve subnet by name"))?;
        let vpc_store = crate::vpc::store::VpcStore::new(self.db.clone());
        let vpc = vpc_store
            .get(vpc_ref, org_name)
            .await?
            .ok_or_else(|| anyhow::anyhow!("vpc '{vpc_ref}' not found"))?;

        self.get_by_vpc_and_name(&vpc.meta.id, name_or_id).await
    }

    async fn get_by_id(&self, id: &str) -> anyhow::Result<Option<Subnet>> {
        let mut response = self
            .db
            .client()
            .query("SELECT * FROM type::record($tbl, $id)")
            .bind(("tbl", SUBNET_TABLE))
            .bind(("id", id.to_string()))
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let raw: Vec<serde_json::Value> = response.take(0).map_err(|e| anyhow::anyhow!("{e}"))?;
        decode_first(raw)
    }

    async fn get_by_vpc_and_name(
        &self,
        vpc_id: &str,
        name: &str,
    ) -> anyhow::Result<Option<Subnet>> {
        let mut response = self
            .db
            .client()
            .query("SELECT * FROM subnet WHERE vpc = $vpc AND name = $name LIMIT 1")
            .bind(("vpc", vpc_id.to_string()))
            .bind(("name", name.to_string()))
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let raw: Vec<serde_json::Value> = response.take(0).map_err(|e| anyhow::anyhow!("{e}"))?;
        decode_first(raw)
    }

    /// List every subnet, optionally filtered by the owning VPC (by
    /// name or record id).
    pub async fn list(
        &self,
        vpc_name: Option<&str>,
        _org_name: Option<&str>,
    ) -> anyhow::Result<Vec<Subnet>> {
        let mut response = self
            .db
            .client()
            .query("SELECT * FROM subnet")
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let raw: Vec<serde_json::Value> = response.take(0).map_err(|e| anyhow::anyhow!("{e}"))?;
        let subnets: Vec<Subnet> = raw
            .into_iter()
            .filter_map(|v| {
                serde_json::from_value::<SubnetRow>(v)
                    .ok()
                    .map(SubnetRow::into_subnet)
            })
            .collect();
        match vpc_name {
            Some(name) => Ok(subnets
                .into_iter()
                .filter(|s| s.vpc_name == name || s.vpc_id.as_str() == name)
                .collect()),
            None => Ok(subnets),
        }
    }

    /// Delete a subnet. `subnet` is effectively the bottom of the
    /// network hierarchy — VMs reference a subnet via IPAM rows, but
    /// those rows are cleared with the subnet's `ipam` side table (P2.13
    /// will migrate the VM store to take ownership of its IP release),
    /// so `delete` is a single DELETE today.
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

        let result = self
            .db
            .client()
            .query("DELETE type::record($tbl, $id)")
            .bind(("tbl", SUBNET_TABLE))
            .bind(("id", subnet.meta.id.clone()))
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        result.check().map_err(|e| anyhow::anyhow!("{e}"))?;
        Ok(())
    }
}

/// Row shape returned by `SELECT * FROM subnet`.
#[derive(Debug, Deserialize)]
struct SubnetRow {
    id: serde_json::Value,
    name: String,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    labels: Option<serde_json::Value>,
    cidr: String,
    gateway: String,
    vpc: String,
    vpc_name: String,
    created_at: String,
    updated_at: String,
}

impl SubnetRow {
    fn into_subnet(self) -> Subnet {
        let id = thing_to_id_string("subnet:", &self.id);
        Subnet {
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
            cidr: self.cidr,
            gateway: self.gateway,
        }
    }
}

fn decode_first(raw: Vec<serde_json::Value>) -> anyhow::Result<Option<Subnet>> {
    match raw.into_iter().next() {
        None => Ok(None),
        Some(v) => {
            let row: SubnetRow = serde_json::from_value(v)
                .map_err(|e| anyhow::anyhow!("deserialise subnet row: {e}"))?;
            Ok(Some(row.into_subnet()))
        }
    }
}

/// Map a SurrealDB create error into a friendly message. Duplicate
/// `(vpc, name)` pairs get the classic "subnet '<name>' already exists
/// in vpc '<vpc>'" wording; everything else falls through the shared
/// [`classify_create_error`] helper.
fn classify_subnet_error(name: &str, vpc_name: &str, err_msg: &str) -> anyhow::Error {
    let lowered = err_msg.to_lowercase();
    if lowered.contains("already contains")
        || lowered.contains("already exists")
        || lowered.contains("duplicate")
    {
        anyhow::anyhow!("subnet '{name}' already exists in vpc '{vpc_name}'")
    } else {
        anyhow::anyhow!(classify_create_error("subnet", name, err_msg))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn temp_store() -> (
        tempfile::TempDir,
        SubnetStore,
        crate::vpc::store::VpcStore,
        nauka_org::store::OrgStore,
    ) {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = EmbeddedDb::open(&dir.path().join("subnets.skv"))
            .await
            .expect("open EmbeddedDb at temp path");
        nauka_state::apply_cluster_schemas(&db)
            .await
            .expect("apply cluster schemas");
        let subnets = SubnetStore::new(db.clone());
        let vpcs = crate::vpc::store::VpcStore::new(db.clone());
        let orgs = nauka_org::store::OrgStore::new(db);
        (dir, subnets, vpcs, orgs)
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
        let (_d, subs, vpcs, orgs) = temp_store().await;
        seed_vpc(&vpcs, &orgs).await;
        let sub = subs
            .create("public", "web", Some("acme"), "10.0.1.0/24")
            .await
            .expect("create subnet");
        assert_eq!(sub.meta.name, "public");
        // `SubnetId` uses the `sub` prefix (not `subnet`); the
        // SurrealDB record ends up at `subnet:sub-01J…`.
        assert!(sub.meta.id.starts_with("sub-"), "got: {}", sub.meta.id);
        assert_eq!(sub.cidr, "10.0.1.0/24");
        assert_eq!(sub.gateway, "10.0.1.1");
        let got = subs
            .get("public", Some("web"), Some("acme"))
            .await
            .expect("get")
            .expect("missing");
        assert_eq!(got.meta.id, sub.meta.id);
    }

    #[tokio::test]
    async fn create_duplicate_name_in_same_vpc_fails() {
        let (_d, subs, vpcs, orgs) = temp_store().await;
        seed_vpc(&vpcs, &orgs).await;
        subs.create("public", "web", Some("acme"), "10.0.1.0/24")
            .await
            .unwrap();
        let err = subs
            .create("public", "web", Some("acme"), "10.0.2.0/24")
            .await
            .expect_err("duplicate name should fail");
        assert!(err.to_string().contains("already exists"), "got: {err}");
    }

    #[tokio::test]
    async fn overlapping_cidr_fails() {
        let (_d, subs, vpcs, orgs) = temp_store().await;
        seed_vpc(&vpcs, &orgs).await;
        subs.create("a", "web", Some("acme"), "10.0.1.0/24")
            .await
            .unwrap();
        let err = subs
            .create("b", "web", Some("acme"), "10.0.1.128/25")
            .await
            .expect_err("overlap should fail");
        assert!(
            err.to_string().to_lowercase().contains("overlap"),
            "got: {err}"
        );
    }

    #[tokio::test]
    async fn subnet_outside_vpc_fails() {
        let (_d, subs, vpcs, orgs) = temp_store().await;
        seed_vpc(&vpcs, &orgs).await;
        let err = subs
            .create("a", "web", Some("acme"), "192.168.0.0/24")
            .await
            .expect_err("outside vpc should fail");
        let _ = err;
    }

    #[tokio::test]
    async fn list_filters_by_vpc() {
        let (_d, subs, vpcs, orgs) = temp_store().await;
        seed_vpc(&vpcs, &orgs).await;
        vpcs.create("api", "acme", "10.1.0.0/16", None, None)
            .await
            .unwrap();
        subs.create("a", "web", Some("acme"), "10.0.1.0/24")
            .await
            .unwrap();
        subs.create("b", "web", Some("acme"), "10.0.2.0/24")
            .await
            .unwrap();
        subs.create("a", "api", Some("acme"), "10.1.1.0/24")
            .await
            .unwrap();

        let web_subs = subs.list(Some("web"), Some("acme")).await.unwrap();
        assert_eq!(web_subs.len(), 2);
        let all = subs.list(None, None).await.unwrap();
        assert_eq!(all.len(), 3);
    }

    #[tokio::test]
    async fn delete_removes_subnet() {
        let (_d, subs, vpcs, orgs) = temp_store().await;
        seed_vpc(&vpcs, &orgs).await;
        subs.create("a", "web", Some("acme"), "10.0.1.0/24")
            .await
            .unwrap();
        subs.delete("a", "web", Some("acme")).await.expect("delete");
        assert!(subs
            .get("a", Some("web"), Some("acme"))
            .await
            .unwrap()
            .is_none());
    }
}
