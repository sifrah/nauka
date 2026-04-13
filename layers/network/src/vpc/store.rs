//! VPC persistence on the SurrealDB-backed cluster store.
//!
//! P2.12 (sifrah/nauka#216) migrated this module from the legacy
//! raw-KV cluster path to the native SurrealDB SDK on top of
//! [`EmbeddedDb`], following the same pattern P2.9/P2.10/P2.11
//! established for the org-layer stores. Every method now reaches the
//! SDK via `self.db.client()` and writes/reads against the SCHEMAFULL
//! `vpc` table defined in `layers/network/schemas/vpc.surql` (applied
//! to the cluster by `nauka_state::apply_cluster_schemas` at
//! bootstrap per ADR 0004).
//!
//! The store holds an [`EmbeddedDb`] directly. Call sites pass
//! `cluster_db.embedded().clone()` at construction time; the clone is
//! cheap (`Arc`-shared `Surreal<Db>` router).
//!
//! The legacy `vpc` / `vpc-idx` / `_reg_v2` sidecar namespaces are
//! gone: the schema's composite unique index on `(org, name)` is now
//! the source of truth for "have we seen this (org, name) pair before?",
//! and `SELECT * FROM vpc` walks every row directly.
//!
//! The VNI counter, which the pre-P2.12 version kept in a raw-KV
//! `_reg:vni-counter` key, is now computed as `max(vni) + 1` across
//! the `vpc` table, falling back to [`VNI_START`] (100) on an empty
//! table. Concurrent creators still race, but the SCHEMAFULL `vpc_vni`
//! unique index guarantees at most one of the racing `CREATE`s wins
//! and the other gets a clean "already exists" rollback — the store
//! retries the allocation in a loop to stay obviously-correct under
//! concurrency.

use nauka_core::id::VpcId;
use nauka_core::resource::epoch_to_iso8601;
use nauka_core::resource::ResourceMeta;
use nauka_state::sdk_bridge::{classify_create_error, iso8601_to_epoch, thing_to_id_string};
use nauka_state::EmbeddedDb;
use serde::Deserialize;

use super::types::Vpc;

/// SurrealDB table backing this store. Defined by
/// `layers/network/schemas/vpc.surql` as `DEFINE TABLE vpc SCHEMAFULL`
/// and applied at bootstrap via `nauka_state::apply_cluster_schemas`.
const VPC_TABLE: &str = "vpc";

/// First VNI value handed out on a fresh cluster. VNIs below 100 are
/// reserved for future system use.
const VNI_START: u32 = 100;

/// Maximum number of VNI allocation retries before giving up. A real
/// failure mode is extreme concurrency on `create`, which is not a
/// production path today (VPCs are created via operator CLI). 8
/// retries is plenty of headroom without risking a runaway loop.
const VNI_RETRY_LIMIT: u32 = 8;

pub struct VpcStore {
    db: EmbeddedDb,
}

impl VpcStore {
    /// Build a [`VpcStore`] over a SurrealDB handle.
    ///
    /// Call sites that already hold a cluster-DB wrapper pass
    /// `cluster_db.embedded().clone()`.
    pub fn new(db: EmbeddedDb) -> Self {
        Self { db }
    }

    /// Pick the next VNI to allocate.
    ///
    /// Reads `math::max(vni)` from the `vpc` table and returns
    /// `max + 1`, or [`VNI_START`] on an empty table. The SCHEMAFULL
    /// `vpc_vni` unique index closes the concurrent-allocation race:
    /// if two creators see the same `max`, one of their `CREATE`s
    /// will fail the unique-index check, and the caller retries.
    async fn next_vni(&self) -> anyhow::Result<u32> {
        let mut response = self
            .db
            .client()
            .query("SELECT math::max(vni) AS max_vni FROM vpc GROUP ALL")
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let rows: Vec<serde_json::Value> = response.take(0).map_err(|e| anyhow::anyhow!("{e}"))?;
        let max_vni = rows
            .into_iter()
            .next()
            .and_then(|row| row.get("max_vni").cloned())
            .and_then(|v| v.as_u64())
            .map(|v| v as u32);
        Ok(max_vni.map(|v| v + 1).unwrap_or(VNI_START))
    }

    /// Create a new VPC within an org.
    ///
    /// The duplicate-name path is closed by the SCHEMAFULL
    /// `vpc_org_name` composite unique index. The duplicate-VNI path
    /// is closed by the `vpc_vni` unique index; on a rare race (two
    /// creators observing the same `max(vni)`) the loser retries the
    /// whole allocation. [`VNI_RETRY_LIMIT`] caps the retry count.
    pub async fn create(
        &self,
        name: &str,
        org_name: &str,
        cidr: &str,
        project_id: Option<String>,
        env_id: Option<String>,
    ) -> anyhow::Result<Vpc> {
        // Resolve the owning org via the post-P2.9 OrgStore API.
        let org_store = nauka_org::store::OrgStore::new(self.db.clone());
        let org = org_store
            .get(org_name)
            .await?
            .ok_or_else(|| anyhow::anyhow!("org '{org_name}' not found"))?;

        // Validate CIDR is in the private-address space.
        let new_net = crate::validate::private_cidr(cidr)?;

        // Check CIDR doesn't overlap with existing VPCs in this org.
        let existing_vpcs = self.list(Some(org_name)).await?;
        let existing_cidrs: Vec<String> = existing_vpcs.iter().map(|v| v.cidr.clone()).collect();
        crate::validate::no_overlap(&new_net, &existing_cidrs)?;

        // Retry loop: on a VNI collision, re-read `max(vni)` and try
        // again. Any other error (including the composite `(org,
        // name)` duplicate) breaks out immediately.
        let mut attempt = 0u32;
        loop {
            let vni = self.next_vni().await?;
            let vpc = Vpc {
                meta: ResourceMeta::new(VpcId::generate().to_string(), name),
                cidr: cidr.to_string(),
                org_id: org.meta.id.clone().into(),
                org_name: org.meta.name.clone(),
                vni,
                project_id: project_id.clone().map(|s| s.into()),
                env_id: env_id.clone().map(|s| s.into()),
            };

            match self.insert(&vpc).await {
                Ok(()) => return Ok(vpc),
                Err(e) => {
                    let msg = e.to_string().to_lowercase();
                    let is_vni_conflict = msg.contains("vpc_vni");
                    let is_org_name_conflict = msg.contains("vpc_org_name");
                    let is_name_conflict = is_org_name_conflict
                        || (msg.contains("already exists") && !is_vni_conflict);

                    if is_name_conflict {
                        return Err(anyhow::anyhow!(
                            "vpc '{name}' already exists in org '{org_name}'"
                        ));
                    }

                    if is_vni_conflict && attempt + 1 < VNI_RETRY_LIMIT {
                        attempt += 1;
                        continue;
                    }

                    return Err(anyhow::anyhow!(classify_create_error("vpc", name, &msg)));
                }
            }
        }
    }

    /// Write the row. Returns the raw SurrealDB error on failure so
    /// the caller can classify it.
    async fn insert(&self, vpc: &Vpc) -> anyhow::Result<()> {
        let created_at_iso = epoch_to_iso8601(vpc.meta.created_at);
        let updated_at_iso = epoch_to_iso8601(vpc.meta.updated_at);
        let labels_json = serde_json::to_value(&vpc.meta.labels)
            .map_err(|e| anyhow::anyhow!("serialise labels: {e}"))?;

        let response = self
            .db
            .client()
            .query(
                "CREATE type::record($tbl, $id) SET \
                 name = $name, \
                 status = $status, \
                 labels = $labels, \
                 cidr = $cidr, \
                 vni = $vni, \
                 org = $org, \
                 org_name = $org_name, \
                 project = $project, \
                 env = $env, \
                 created_at = <datetime>$created_at, \
                 updated_at = <datetime>$updated_at",
            )
            .bind(("tbl", VPC_TABLE))
            .bind(("id", vpc.meta.id.clone()))
            .bind(("name", vpc.meta.name.clone()))
            .bind(("status", vpc.meta.status.clone()))
            .bind(("labels", labels_json))
            .bind(("cidr", vpc.cidr.clone()))
            .bind(("vni", vpc.vni as i64))
            .bind(("org", vpc.org_id.as_str().to_string()))
            .bind(("org_name", vpc.org_name.clone()))
            .bind((
                "project",
                vpc.project_id.as_ref().map(|p| p.as_str().to_string()),
            ))
            .bind(("env", vpc.env_id.as_ref().map(|e| e.as_str().to_string())))
            .bind(("created_at", created_at_iso))
            .bind(("updated_at", updated_at_iso))
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        response.check().map_err(|e| anyhow::anyhow!("{e}"))?;
        Ok(())
    }

    /// Look up a VPC by id (when the input looks like a [`VpcId`])
    /// or by name (otherwise).
    pub async fn get(
        &self,
        name_or_id: &str,
        org_name: Option<&str>,
    ) -> anyhow::Result<Option<Vpc>> {
        if VpcId::looks_like_id(name_or_id) {
            return self.get_by_id(name_or_id).await;
        }
        let org_name =
            org_name.ok_or_else(|| anyhow::anyhow!("--org required to resolve VPC by name"))?;

        let org_store = nauka_org::store::OrgStore::new(self.db.clone());
        let org = org_store
            .get(org_name)
            .await?
            .ok_or_else(|| anyhow::anyhow!("org '{org_name}' not found"))?;

        self.get_by_org_and_name(&org.meta.id, name_or_id).await
    }

    async fn get_by_id(&self, id: &str) -> anyhow::Result<Option<Vpc>> {
        let mut response = self
            .db
            .client()
            .query("SELECT * FROM type::record($tbl, $id)")
            .bind(("tbl", VPC_TABLE))
            .bind(("id", id.to_string()))
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let raw: Vec<serde_json::Value> = response.take(0).map_err(|e| anyhow::anyhow!("{e}"))?;
        decode_first(raw)
    }

    async fn get_by_org_and_name(&self, org_id: &str, name: &str) -> anyhow::Result<Option<Vpc>> {
        let mut response = self
            .db
            .client()
            .query("SELECT * FROM vpc WHERE org = $org AND name = $name LIMIT 1")
            .bind(("org", org_id.to_string()))
            .bind(("name", name.to_string()))
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let raw: Vec<serde_json::Value> = response.take(0).map_err(|e| anyhow::anyhow!("{e}"))?;
        decode_first(raw)
    }

    /// List every VPC, optionally filtered by the owning org.
    pub async fn list(&self, org_name: Option<&str>) -> anyhow::Result<Vec<Vpc>> {
        let mut response = self
            .db
            .client()
            .query("SELECT * FROM vpc")
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let raw: Vec<serde_json::Value> = response.take(0).map_err(|e| anyhow::anyhow!("{e}"))?;
        let vpcs: Vec<Vpc> = raw
            .into_iter()
            .filter_map(|v| {
                serde_json::from_value::<VpcRow>(v)
                    .ok()
                    .map(VpcRow::into_vpc)
            })
            .collect();
        match org_name {
            Some(name) => Ok(vpcs
                .into_iter()
                .filter(|v| v.org_name == name || v.org_id.as_str() == name)
                .collect()),
            None => Ok(vpcs),
        }
    }

    /// Delete a VPC. Refuses to delete if the VPC still has subnets
    /// or a NAT gateway — the caller must remove the children first.
    pub async fn delete(&self, name_or_id: &str, org_name: &str) -> anyhow::Result<()> {
        let vpc = self
            .get(name_or_id, Some(org_name))
            .await?
            .ok_or_else(|| anyhow::anyhow!("vpc '{name_or_id}' not found in org '{org_name}'"))?;

        // Check for child subnets directly on the SCHEMAFULL `subnet`
        // table — don't build a SubnetStore here, that would force a
        // circular import dance when tests link it in. Same pattern
        // OrgStore::delete uses to count projects.
        let subnet_count = self.count_subnets_in_vpc(&vpc.meta.id).await?;
        if subnet_count > 0 {
            anyhow::bail!(
                "vpc '{}' has {} subnet(s). Delete them first.",
                vpc.meta.name,
                subnet_count
            );
        }

        // Check for the NAT gateway (at most one per VPC).
        let natgw_count = self.count_natgws_in_vpc(&vpc.meta.id).await?;
        if natgw_count > 0 {
            anyhow::bail!(
                "vpc '{}' has a NAT gateway. Delete it first.",
                vpc.meta.name
            );
        }

        // Check for peerings that still reference this VPC (on either
        // side of the pair — peerings are bidirectional, and a live
        // peering to a VPC that's about to disappear would leave a
        // dangling row pointing at a dead record id).
        let peering_count = self.count_peerings_touching_vpc(&vpc.meta.id).await?;
        if peering_count > 0 {
            anyhow::bail!(
                "vpc '{}' has {} peering(s). Delete them first.",
                vpc.meta.name,
                peering_count
            );
        }

        let result = self
            .db
            .client()
            .query("DELETE type::record($tbl, $id)")
            .bind(("tbl", VPC_TABLE))
            .bind(("id", vpc.meta.id.clone()))
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        result.check().map_err(|e| anyhow::anyhow!("{e}"))?;
        Ok(())
    }

    async fn count_subnets_in_vpc(&self, vpc_id: &str) -> anyhow::Result<usize> {
        let mut response = self
            .db
            .client()
            .query("SELECT name FROM subnet WHERE vpc = $vpc")
            .bind(("vpc", vpc_id.to_string()))
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let rows: Vec<serde_json::Value> = response.take(0).map_err(|e| anyhow::anyhow!("{e}"))?;
        Ok(rows.len())
    }

    async fn count_natgws_in_vpc(&self, vpc_id: &str) -> anyhow::Result<usize> {
        let mut response = self
            .db
            .client()
            .query("SELECT name FROM natgw WHERE vpc = $vpc")
            .bind(("vpc", vpc_id.to_string()))
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let rows: Vec<serde_json::Value> = response.take(0).map_err(|e| anyhow::anyhow!("{e}"))?;
        Ok(rows.len())
    }

    async fn count_peerings_touching_vpc(&self, vpc_id: &str) -> anyhow::Result<usize> {
        let mut response = self
            .db
            .client()
            .query(
                "SELECT name FROM vpc_peering \
                 WHERE vpc = $vpc OR peer_vpc = $vpc",
            )
            .bind(("vpc", vpc_id.to_string()))
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let rows: Vec<serde_json::Value> = response.take(0).map_err(|e| anyhow::anyhow!("{e}"))?;
        Ok(rows.len())
    }
}

/// Row shape returned by `SELECT * FROM vpc`.
#[derive(Debug, Deserialize)]
struct VpcRow {
    id: serde_json::Value,
    name: String,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    labels: Option<serde_json::Value>,
    cidr: String,
    vni: i64,
    org: String,
    org_name: String,
    #[serde(default)]
    project: Option<String>,
    #[serde(default)]
    env: Option<String>,
    created_at: String,
    updated_at: String,
}

impl VpcRow {
    fn into_vpc(self) -> Vpc {
        let id = thing_to_id_string("vpc:", &self.id);
        let labels = self
            .labels
            .and_then(|v| serde_json::from_value(v).ok())
            .unwrap_or_default();
        Vpc {
            meta: ResourceMeta {
                id,
                name: self.name,
                status: self.status.unwrap_or_else(|| "active".to_string()),
                labels,
                created_at: iso8601_to_epoch(&self.created_at),
                updated_at: iso8601_to_epoch(&self.updated_at),
            },
            cidr: self.cidr,
            org_id: self.org.into(),
            org_name: self.org_name,
            vni: self.vni as u32,
            project_id: self.project.map(Into::into),
            env_id: self.env.map(Into::into),
        }
    }
}

fn decode_first(raw: Vec<serde_json::Value>) -> anyhow::Result<Option<Vpc>> {
    match raw.into_iter().next() {
        None => Ok(None),
        Some(v) => {
            let row: VpcRow = serde_json::from_value(v)
                .map_err(|e| anyhow::anyhow!("deserialise vpc row: {e}"))?;
            Ok(Some(row.into_vpc()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn temp_store() -> (tempfile::TempDir, VpcStore, nauka_org::store::OrgStore) {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = EmbeddedDb::open(&dir.path().join("vpcs.skv"))
            .await
            .expect("open EmbeddedDb at temp path");
        nauka_state::apply_cluster_schemas(&db)
            .await
            .expect("apply cluster schemas");
        let vpcs = VpcStore::new(db.clone());
        let orgs = nauka_org::store::OrgStore::new(db);
        (dir, vpcs, orgs)
    }

    #[tokio::test]
    async fn create_then_get_by_name() {
        let (_d, vpcs, orgs) = temp_store().await;
        orgs.create("acme").await.expect("create org");

        let vpc = vpcs
            .create("web", "acme", "10.0.0.0/16", None, None)
            .await
            .expect("create vpc");
        assert_eq!(vpc.meta.name, "web");
        assert!(vpc.meta.id.starts_with("vpc-"));
        assert_eq!(vpc.cidr, "10.0.0.0/16");
        assert_eq!(vpc.vni, VNI_START);

        let got = vpcs
            .get("web", Some("acme"))
            .await
            .expect("get")
            .expect("missing");
        assert_eq!(got.meta.id, vpc.meta.id);
        assert_eq!(got.vni, VNI_START);
    }

    #[tokio::test]
    async fn create_then_get_by_id() {
        let (_d, vpcs, orgs) = temp_store().await;
        orgs.create("acme").await.expect("create org");
        let vpc = vpcs
            .create("web", "acme", "10.0.0.0/16", None, None)
            .await
            .expect("create vpc");
        let got = vpcs
            .get(&vpc.meta.id, None)
            .await
            .expect("get")
            .expect("missing");
        assert_eq!(got.meta.id, vpc.meta.id);
    }

    #[tokio::test]
    async fn vni_is_monotonic() {
        let (_d, vpcs, orgs) = temp_store().await;
        orgs.create("acme").await.expect("create org");
        let a = vpcs
            .create("a", "acme", "10.0.0.0/16", None, None)
            .await
            .unwrap();
        let b = vpcs
            .create("b", "acme", "10.1.0.0/16", None, None)
            .await
            .unwrap();
        let c = vpcs
            .create("c", "acme", "10.2.0.0/16", None, None)
            .await
            .unwrap();
        assert_eq!(a.vni, VNI_START);
        assert_eq!(b.vni, VNI_START + 1);
        assert_eq!(c.vni, VNI_START + 2);
    }

    #[tokio::test]
    async fn create_duplicate_name_in_same_org_fails() {
        let (_d, vpcs, orgs) = temp_store().await;
        orgs.create("acme").await.expect("create org");
        vpcs.create("web", "acme", "10.0.0.0/16", None, None)
            .await
            .unwrap();
        let err = vpcs
            .create("web", "acme", "10.1.0.0/16", None, None)
            .await
            .expect_err("duplicate name should fail");
        assert!(err.to_string().contains("already exists"), "got: {err}");
    }

    #[tokio::test]
    async fn overlapping_cidr_fails() {
        let (_d, vpcs, orgs) = temp_store().await;
        orgs.create("acme").await.expect("create org");
        vpcs.create("web", "acme", "10.0.0.0/16", None, None)
            .await
            .unwrap();
        let err = vpcs
            .create("api", "acme", "10.0.1.0/24", None, None)
            .await
            .expect_err("overlapping cidr should fail");
        assert!(
            err.to_string().to_lowercase().contains("overlap"),
            "got: {err}"
        );
    }

    #[tokio::test]
    async fn list_filters_by_org() {
        let (_d, vpcs, orgs) = temp_store().await;
        orgs.create("acme").await.expect("create acme");
        orgs.create("globex").await.expect("create globex");
        vpcs.create("web", "acme", "10.0.0.0/16", None, None)
            .await
            .unwrap();
        vpcs.create("api", "acme", "10.1.0.0/16", None, None)
            .await
            .unwrap();
        vpcs.create("web", "globex", "10.2.0.0/16", None, None)
            .await
            .unwrap();

        let acme_vpcs = vpcs.list(Some("acme")).await.unwrap();
        assert_eq!(acme_vpcs.len(), 2);
        let all = vpcs.list(None).await.unwrap();
        assert_eq!(all.len(), 3);
    }

    #[tokio::test]
    async fn delete_removes_vpc() {
        let (_d, vpcs, orgs) = temp_store().await;
        orgs.create("acme").await.expect("create org");
        vpcs.create("web", "acme", "10.0.0.0/16", None, None)
            .await
            .unwrap();
        vpcs.delete("web", "acme").await.expect("delete");
        assert!(vpcs.get("web", Some("acme")).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn delete_missing_vpc_errors() {
        let (_d, vpcs, orgs) = temp_store().await;
        orgs.create("acme").await.expect("create org");
        let err = vpcs
            .delete("does-not-exist", "acme")
            .await
            .expect_err("missing");
        assert!(err.to_string().contains("not found"), "got: {err}");
    }

    /// VPC delete is forbidden while a child subnet still exists.
    /// We seed the `subnet` row directly via a raw SurrealQL `CREATE`
    /// to keep this test self-contained — the real SubnetStore is
    /// exercised in its own test module.
    #[tokio::test]
    async fn delete_refuses_while_child_subnet_remains() {
        let (_d, vpcs, orgs) = temp_store().await;
        orgs.create("acme").await.expect("create org");
        let vpc = vpcs
            .create("web", "acme", "10.0.0.0/16", None, None)
            .await
            .unwrap();

        vpcs.db
            .client()
            .query(
                "CREATE type::record('subnet', 'subnet-fake') SET \
                 name = 'sub1', \
                 status = 'active', \
                 labels = {}, \
                 cidr = '10.0.1.0/24', \
                 gateway = '10.0.1.1', \
                 vpc = $vpc, \
                 vpc_name = 'web', \
                 created_at = time::now(), \
                 updated_at = time::now()",
            )
            .bind(("vpc", vpc.meta.id.clone()))
            .await
            .unwrap()
            .check()
            .unwrap();

        let err = vpcs
            .delete("web", "acme")
            .await
            .expect_err("should refuse while child subnet exists");
        assert!(
            err.to_string().contains("subnet") && err.to_string().contains("Delete them first"),
            "got: {err}"
        );
    }
}
