//! VM persistence on the SurrealDB-backed cluster store.
//!
//! P2.13 (sifrah/nauka#217) migrated this module from the legacy
//! raw-KV cluster path to the native SurrealDB SDK on top of
//! [`EmbeddedDb`], completing the compute-layer migration. Every
//! method reaches the SDK via `self.db.client()` and writes/reads
//! against the SCHEMAFULL `vm` table defined in
//! `layers/compute/schemas/vm.surql` (applied by
//! `nauka_state::apply_cluster_schemas` at bootstrap per ADR 0004).
//!
//! The legacy `vm` / `vm-idx` / `_reg_v2` sidecar namespaces are
//! gone: the schema's composite unique index on `(env, name)` is
//! now the source of truth for "have we seen this (env, name) pair
//! before?", and `SELECT * FROM vm` walks every row directly.
//!
//! The composite unique index on `(subnet, private_ip)` closes the
//! allocation-collision race that the pre-P2.13 store enforced only
//! at the IPAM helper level.
//!
//! State transitions continue to use the validated-FSM pattern from
//! the pre-P2.13 version: `update_state` reads the row, checks the
//! `(from, to)` pair against the allowed transitions, then writes
//! back via `UPDATE type::record($tbl, $id) MERGE …`.

use nauka_core::id::{EnvId, OrgId, ProjectId, SubnetId, VmId, VpcId};
use nauka_core::resource::epoch_to_iso8601;
use nauka_core::resource::ResourceMeta;
use nauka_state::sdk_bridge::{iso8601_to_epoch, thing_to_id_string};
use nauka_state::EmbeddedDb;
use serde::Deserialize;

use super::types::{Vm, VmState};

/// SurrealDB table backing this store. Defined by
/// `layers/compute/schemas/vm.surql` as `DEFINE TABLE vm SCHEMAFULL`
/// and applied at bootstrap via `nauka_state::apply_cluster_schemas`.
const VM_TABLE: &str = "vm";

pub struct VmStore {
    db: EmbeddedDb,
}

impl VmStore {
    /// Build a [`VmStore`] over a SurrealDB handle.
    ///
    /// Call sites that already hold a cluster-DB wrapper pass
    /// `db.clone()`.
    pub fn new(db: EmbeddedDb) -> Self {
        Self { db }
    }

    /// Create a new VM in the given environment.
    ///
    /// Resolves the full `(org → project → env, vpc → subnet)` path,
    /// allocates a private IP via the subnet's IPAM helper, then
    /// writes the row through `CREATE type::record($tbl, $id) SET …`.
    /// The schema's composite unique index on `(env, name)` rejects
    /// duplicates.
    ///
    /// `hypervisor_id` is passed in by the caller (typically the CLI
    /// handler) so the store stays decoupled from
    /// `crate::scheduler::schedule`, which needs live fabric state
    /// and can't run in unit tests. The CLI path resolves the id via
    /// the scheduler immediately before calling `create`; tests pass
    /// a placeholder id directly.
    #[allow(clippy::too_many_arguments)]
    pub async fn create(
        &self,
        name: &str,
        org_name: &str,
        project_name: &str,
        env_name: &str,
        vpc_name: &str,
        subnet_name: &str,
        vcpus: u32,
        memory_mb: u32,
        disk_gb: u32,
        image: &str,
        region: &str,
        zone: &str,
        hypervisor_id: String,
    ) -> anyhow::Result<Vm> {
        // Resolve org → project → env. Every upstream store already
        // takes an `EmbeddedDb` directly after P2.9-P2.11.
        let org_store = nauka_org::store::OrgStore::new(self.db.clone());
        let org = org_store
            .get(org_name)
            .await?
            .ok_or_else(|| anyhow::anyhow!("org '{org_name}' not found"))?;

        let proj_store = nauka_org::project::store::ProjectStore::new(self.db.clone());
        let project = proj_store
            .get(project_name, Some(org_name))
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!("project '{project_name}' not found in org '{org_name}'")
            })?;

        let env_store = nauka_org::project::env::store::EnvStore::new(self.db.clone());
        let env = env_store
            .get(env_name, Some(project_name), Some(org_name))
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!("environment '{env_name}' not found in project '{project_name}'")
            })?;

        // Resolve vpc → subnet. Both stores take an `EmbeddedDb`
        // directly after P2.12.
        let vpc_store = nauka_network::vpc::store::VpcStore::new(self.db.clone());
        let vpc = vpc_store
            .get(vpc_name, Some(org_name))
            .await?
            .ok_or_else(|| anyhow::anyhow!("vpc '{vpc_name}' not found"))?;

        let sub_store = nauka_network::vpc::subnet::store::SubnetStore::new(self.db.clone());
        let subnet = sub_store
            .get(subnet_name, Some(vpc_name), Some(org_name))
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!("subnet '{subnet_name}' not found in vpc '{vpc_name}'")
            })?;

        // Generate VM ID first — needed for IPAM allocation.
        let vm_id = VmId::generate().to_string();

        // Allocate private IP from subnet IPAM. P2.12 migrated the
        // helper to take an `EmbeddedDb` directly.
        let private_ip = nauka_network::vpc::subnet::ipam::allocate(
            &self.db,
            &subnet.meta.id,
            &subnet.cidr,
            &subnet.gateway,
            &vm_id,
        )
        .await?;

        let vm = Vm {
            meta: ResourceMeta::new(vm_id.clone(), name),
            org_id: org.meta.id.clone().into(),
            org_name: org.meta.name.clone(),
            project_id: project.meta.id.clone().into(),
            project_name: project.meta.name.clone(),
            env_id: env.meta.id.clone().into(),
            env_name: env.meta.name.clone(),
            vpc_id: vpc.meta.id.clone().into(),
            vpc_name: vpc.meta.name.clone(),
            subnet_id: subnet.meta.id.clone().into(),
            subnet_name: subnet.meta.name.clone(),
            vcpus,
            memory_mb,
            disk_gb,
            image: image.to_string(),
            region: region.to_string(),
            zone: zone.to_string(),
            private_ip: Some(private_ip),
            hypervisor_id: Some(hypervisor_id),
            state: VmState::Pending,
        };

        // Write the row through the SDK. If the DB write fails we
        // release the IPAM allocation so the address slot doesn't
        // leak — same rollback pattern as `NatGwStore::create`.
        if let Err(e) = self.insert(&vm).await {
            let msg = e.to_string().to_lowercase();
            let _ = nauka_network::vpc::subnet::ipam::release(
                &self.db,
                vm.subnet_id.as_str(),
                &vm.meta.id,
            )
            .await;

            if msg.contains("already contains")
                || msg.contains("already exists")
                || msg.contains("duplicate")
            {
                return Err(anyhow::anyhow!(
                    "vm '{name}' already exists in environment '{env_name}'"
                ));
            }
            return Err(e);
        }

        Ok(vm)
    }

    /// Write the row via SurrealQL `CREATE`. Extracted from `create`
    /// so the rollback path there can cleanly handle the error.
    async fn insert(&self, vm: &Vm) -> anyhow::Result<()> {
        let created_at_iso = epoch_to_iso8601(vm.meta.created_at);
        let updated_at_iso = epoch_to_iso8601(vm.meta.updated_at);
        let labels_json = serde_json::to_value(&vm.meta.labels)
            .map_err(|e| anyhow::anyhow!("serialise labels: {e}"))?;

        let response = self
            .db
            .client()
            .query(
                "CREATE type::record($tbl, $id) SET \
                 name = $name, \
                 status = $status, \
                 labels = $labels, \
                 org = $org, \
                 org_name = $org_name, \
                 project = $project, \
                 project_name = $project_name, \
                 env = $env, \
                 env_name = $env_name, \
                 vpc = $vpc, \
                 vpc_name = $vpc_name, \
                 subnet = $subnet, \
                 subnet_name = $subnet_name, \
                 vcpus = $vcpus, \
                 memory_mb = $memory_mb, \
                 disk_gb = $disk_gb, \
                 image = $image, \
                 region = $region, \
                 zone = $zone, \
                 private_ip = $private_ip, \
                 hypervisor = $hypervisor, \
                 state = $state, \
                 created_at = <datetime>$created_at, \
                 updated_at = <datetime>$updated_at",
            )
            .bind(("tbl", VM_TABLE))
            .bind(("id", vm.meta.id.clone()))
            .bind(("name", vm.meta.name.clone()))
            .bind(("status", vm.meta.status.clone()))
            .bind(("labels", labels_json))
            .bind(("org", vm.org_id.as_str().to_string()))
            .bind(("org_name", vm.org_name.clone()))
            .bind(("project", vm.project_id.as_str().to_string()))
            .bind(("project_name", vm.project_name.clone()))
            .bind(("env", vm.env_id.as_str().to_string()))
            .bind(("env_name", vm.env_name.clone()))
            .bind(("vpc", vm.vpc_id.as_str().to_string()))
            .bind(("vpc_name", vm.vpc_name.clone()))
            .bind(("subnet", vm.subnet_id.as_str().to_string()))
            .bind(("subnet_name", vm.subnet_name.clone()))
            .bind(("vcpus", vm.vcpus as i64))
            .bind(("memory_mb", vm.memory_mb as i64))
            .bind(("disk_gb", vm.disk_gb as i64))
            .bind(("image", vm.image.clone()))
            .bind(("region", vm.region.clone()))
            .bind(("zone", vm.zone.clone()))
            .bind(("private_ip", vm.private_ip.clone()))
            .bind(("hypervisor", vm.hypervisor_id.clone()))
            .bind(("state", vm.state.to_string()))
            .bind(("created_at", created_at_iso))
            .bind(("updated_at", updated_at_iso))
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        response.check().map_err(|e| anyhow::anyhow!("{e}"))?;
        Ok(())
    }

    /// Look up a VM by id (when the input looks like a [`VmId`]) or
    /// by name (otherwise, requires the full `(org, project, env)`
    /// scope).
    pub async fn get(
        &self,
        name_or_id: &str,
        org_name: Option<&str>,
        project_name: Option<&str>,
        env_name: Option<&str>,
    ) -> anyhow::Result<Option<Vm>> {
        if VmId::looks_like_id(name_or_id) {
            return self.get_by_id(name_or_id).await;
        }

        let org_name =
            org_name.ok_or_else(|| anyhow::anyhow!("--org required to resolve VM by name"))?;
        let project_name = project_name
            .ok_or_else(|| anyhow::anyhow!("--project required to resolve VM by name"))?;
        let env_name =
            env_name.ok_or_else(|| anyhow::anyhow!("--env required to resolve VM by name"))?;

        // Resolve the owning env so we can query by its record id
        // via the composite `(env, name)` unique index.
        let env_store = nauka_org::project::env::store::EnvStore::new(self.db.clone());
        let env = env_store
            .get(env_name, Some(project_name), Some(org_name))
            .await?
            .ok_or_else(|| anyhow::anyhow!("environment '{env_name}' not found"))?;

        self.get_by_env_and_name(&env.meta.id, name_or_id).await
    }

    async fn get_by_id(&self, id: &str) -> anyhow::Result<Option<Vm>> {
        let mut response = self
            .db
            .client()
            .query("SELECT * FROM type::record($tbl, $id)")
            .bind(("tbl", VM_TABLE))
            .bind(("id", id.to_string()))
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let raw: Vec<serde_json::Value> = response.take(0).map_err(|e| anyhow::anyhow!("{e}"))?;
        decode_first(raw)
    }

    async fn get_by_env_and_name(&self, env_id: &str, name: &str) -> anyhow::Result<Option<Vm>> {
        let mut response = self
            .db
            .client()
            .query("SELECT * FROM vm WHERE env = $env AND name = $name LIMIT 1")
            .bind(("env", env_id.to_string()))
            .bind(("name", name.to_string()))
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let raw: Vec<serde_json::Value> = response.take(0).map_err(|e| anyhow::anyhow!("{e}"))?;
        decode_first(raw)
    }

    /// List every VM, with optional `(org, project, env)` filters
    /// applied progressively by the application layer after the
    /// `SELECT *`.
    pub async fn list(
        &self,
        org_name: Option<&str>,
        project_name: Option<&str>,
        env_name: Option<&str>,
    ) -> anyhow::Result<Vec<Vm>> {
        let mut response = self
            .db
            .client()
            .query("SELECT * FROM vm")
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let raw: Vec<serde_json::Value> = response.take(0).map_err(|e| anyhow::anyhow!("{e}"))?;
        let mut vms: Vec<Vm> = raw
            .into_iter()
            .filter_map(|v| serde_json::from_value::<VmRow>(v).ok().map(VmRow::into_vm))
            .collect();

        if let Some(org) = org_name {
            vms.retain(|v| v.org_name == org || v.org_id.as_str() == org);
        }
        if let Some(proj) = project_name {
            vms.retain(|v| v.project_name == proj || v.project_id.as_str() == proj);
        }
        if let Some(env) = env_name {
            vms.retain(|v| v.env_name == env || v.env_id.as_str() == env);
        }

        Ok(vms)
    }

    /// Apply a state transition to a VM.
    ///
    /// Validates the `(from, to)` pair against the allowed VM FSM
    /// edges, then rewrites `state` + `updated_at` atomically via
    /// `UPDATE … MERGE`.
    pub async fn update_state(
        &self,
        name_or_id: &str,
        new_state: VmState,
        org_name: Option<&str>,
        project_name: Option<&str>,
        env_name: Option<&str>,
    ) -> anyhow::Result<Vm> {
        let mut vm = self
            .get(name_or_id, org_name, project_name, env_name)
            .await?
            .ok_or_else(|| anyhow::anyhow!("vm '{name_or_id}' not found"))?;

        match (&vm.state, &new_state) {
            (VmState::Pending, VmState::Running) => {}
            (VmState::Stopped, VmState::Running) => {}
            (VmState::Running, VmState::Stopped) => {}
            (VmState::Pending, VmState::Deleted) => {}
            (VmState::Stopped, VmState::Deleted) => {}
            (from, to) => anyhow::bail!("invalid state transition: {from} -> {to}"),
        }

        vm.state = new_state;
        vm.meta.updated_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let updated_at_iso = epoch_to_iso8601(vm.meta.updated_at);
        let response = self
            .db
            .client()
            .query(
                "UPDATE type::record($tbl, $id) MERGE { \
                 state: $state, \
                 updated_at: <datetime>$updated_at \
                 }",
            )
            .bind(("tbl", VM_TABLE))
            .bind(("id", vm.meta.id.clone()))
            .bind(("state", vm.state.to_string()))
            .bind(("updated_at", updated_at_iso))
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        response.check().map_err(|e| anyhow::anyhow!("{e}"))?;
        Ok(vm)
    }

    /// Delete a VM. Refuses to delete unless in `pending` / `stopped`.
    /// Releases the IPAM allocation on success.
    pub async fn delete(
        &self,
        name_or_id: &str,
        org_name: Option<&str>,
        project_name: Option<&str>,
        env_name: Option<&str>,
    ) -> anyhow::Result<()> {
        let vm = self
            .get(name_or_id, org_name, project_name, env_name)
            .await?
            .ok_or_else(|| anyhow::anyhow!("vm '{name_or_id}' not found"))?;

        if vm.state != VmState::Stopped && vm.state != VmState::Pending {
            anyhow::bail!(
                "vm must be stopped or pending to delete (current state: {})",
                vm.state
            );
        }

        // Release IPAM allocation. The helper is idempotent — a
        // retry after a partial failure just removes the entry
        // from the blob.
        nauka_network::vpc::subnet::ipam::release(&self.db, vm.subnet_id.as_str(), &vm.meta.id)
            .await?;

        let result = self
            .db
            .client()
            .query("DELETE type::record($tbl, $id)")
            .bind(("tbl", VM_TABLE))
            .bind(("id", vm.meta.id.clone()))
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        result.check().map_err(|e| anyhow::anyhow!("{e}"))?;
        Ok(())
    }
}

/// Row shape returned by `SELECT * FROM vm`.
#[derive(Debug, Deserialize)]
struct VmRow {
    id: serde_json::Value,
    name: String,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    labels: Option<serde_json::Value>,
    org: String,
    org_name: String,
    project: String,
    project_name: String,
    env: String,
    env_name: String,
    vpc: String,
    vpc_name: String,
    subnet: String,
    subnet_name: String,
    vcpus: i64,
    memory_mb: i64,
    disk_gb: i64,
    image: String,
    region: String,
    zone: String,
    #[serde(default)]
    private_ip: Option<String>,
    #[serde(default)]
    hypervisor: Option<String>,
    state: String,
    created_at: String,
    updated_at: String,
}

impl VmRow {
    fn into_vm(self) -> Vm {
        let id = thing_to_id_string("vm:", &self.id);
        let state = match self.state.as_str() {
            "pending" => VmState::Pending,
            "creating" => VmState::Creating,
            "running" => VmState::Running,
            "stopped" => VmState::Stopped,
            "deleting" => VmState::Deleting,
            "deleted" => VmState::Deleted,
            // Default to Pending for anything else so a bad
            // migration doesn't crash the list. The schema's
            // ASSERT keeps this branch unreachable in practice.
            _ => VmState::Pending,
        };
        Vm {
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
            org_id: OrgId::from(self.org),
            org_name: self.org_name,
            project_id: ProjectId::from(self.project),
            project_name: self.project_name,
            env_id: EnvId::from(self.env),
            env_name: self.env_name,
            vpc_id: VpcId::from(self.vpc),
            vpc_name: self.vpc_name,
            subnet_id: SubnetId::from(self.subnet),
            subnet_name: self.subnet_name,
            vcpus: self.vcpus as u32,
            memory_mb: self.memory_mb as u32,
            disk_gb: self.disk_gb as u32,
            image: self.image,
            region: self.region,
            zone: self.zone,
            private_ip: self.private_ip,
            hypervisor_id: self.hypervisor,
            state,
        }
    }
}

fn decode_first(raw: Vec<serde_json::Value>) -> anyhow::Result<Option<Vm>> {
    match raw.into_iter().next() {
        None => Ok(None),
        Some(v) => {
            let row: VmRow = serde_json::from_value(v)
                .map_err(|e| anyhow::anyhow!("deserialise vm row: {e}"))?;
            Ok(Some(row.into_vm()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build an in-process `VmStore` backed by a fresh SurrealKV
    /// datastore at a temporary path, with the cluster schema
    /// bundle applied. Returns the whole tenant hierarchy stack so
    /// individual tests can seed org/project/env/vpc/subnet ahead
    /// of creating VMs.
    async fn temp_store() -> (
        tempfile::TempDir,
        VmStore,
        nauka_org::store::OrgStore,
        nauka_org::project::store::ProjectStore,
        nauka_org::project::env::store::EnvStore,
        nauka_network::vpc::store::VpcStore,
        nauka_network::vpc::subnet::store::SubnetStore,
    ) {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = EmbeddedDb::open(&dir.path().join("vms.skv"))
            .await
            .expect("open EmbeddedDb");
        nauka_state::apply_cluster_schemas(&db)
            .await
            .expect("apply cluster schemas");
        let vms = VmStore::new(db.clone());
        let orgs = nauka_org::store::OrgStore::new(db.clone());
        let projs = nauka_org::project::store::ProjectStore::new(db.clone());
        let envs = nauka_org::project::env::store::EnvStore::new(db.clone());
        let vpcs = nauka_network::vpc::store::VpcStore::new(db.clone());
        let subs = nauka_network::vpc::subnet::store::SubnetStore::new(db);
        (dir, vms, orgs, projs, envs, vpcs, subs)
    }

    async fn seed_stack(
        orgs: &nauka_org::store::OrgStore,
        projs: &nauka_org::project::store::ProjectStore,
        envs: &nauka_org::project::env::store::EnvStore,
        vpcs: &nauka_network::vpc::store::VpcStore,
        subs: &nauka_network::vpc::subnet::store::SubnetStore,
    ) {
        orgs.create("acme").await.unwrap();
        projs.create("web", "acme").await.unwrap();
        envs.create("prod", "web", "acme").await.unwrap();
        vpcs.create("vpc1", "acme", "10.0.0.0/16", None, None)
            .await
            .unwrap();
        subs.create("public", "vpc1", Some("acme"), "10.0.1.0/24")
            .await
            .unwrap();
    }

    /// Shorthand `create` wrapper for tests — threads a fake
    /// `hypervisor_id` through so we don't need to spin up a real
    /// scheduler in unit tests. The CLI path resolves a real
    /// hypervisor id via `crate::scheduler::schedule` before
    /// calling `VmStore::create`.
    #[allow(clippy::too_many_arguments)]
    async fn create_vm(
        vms: &VmStore,
        name: &str,
        org: &str,
        project: &str,
        env: &str,
        vpc: &str,
        subnet: &str,
        vcpus: u32,
        memory_mb: u32,
        disk_gb: u32,
    ) -> Vm {
        vms.create(
            name,
            org,
            project,
            env,
            vpc,
            subnet,
            vcpus,
            memory_mb,
            disk_gb,
            "ubuntu-24.04",
            "eu",
            "fsn1",
            "hv-test".to_string(),
        )
        .await
        .expect("create vm")
    }

    #[tokio::test]
    async fn create_then_get_by_name() {
        let (_d, vms, orgs, projs, envs, vpcs, subs) = temp_store().await;
        seed_stack(&orgs, &projs, &envs, &vpcs, &subs).await;

        let vm = create_vm(
            &vms, "web1", "acme", "web", "prod", "vpc1", "public", 2, 1024, 20,
        )
        .await;

        assert_eq!(vm.meta.name, "web1");
        assert!(vm.meta.id.starts_with("vm-"), "got: {}", vm.meta.id);
        assert_eq!(vm.vcpus, 2);
        assert_eq!(vm.memory_mb, 1024);
        assert!(matches!(vm.state, VmState::Pending));
        assert!(vm.private_ip.is_some());

        let got = vms
            .get("web1", Some("acme"), Some("web"), Some("prod"))
            .await
            .expect("get by name")
            .expect("missing");
        assert_eq!(got.meta.id, vm.meta.id);
        assert_eq!(got.vcpus, 2);
    }

    #[tokio::test]
    async fn create_then_get_by_id() {
        let (_d, vms, orgs, projs, envs, vpcs, subs) = temp_store().await;
        seed_stack(&orgs, &projs, &envs, &vpcs, &subs).await;
        let vm = create_vm(
            &vms, "web1", "acme", "web", "prod", "vpc1", "public", 1, 512, 10,
        )
        .await;
        let got = vms
            .get(&vm.meta.id, None, None, None)
            .await
            .expect("get by id")
            .expect("missing");
        assert_eq!(got.meta.id, vm.meta.id);
    }

    #[tokio::test]
    async fn duplicate_name_in_same_env_is_rejected() {
        let (_d, vms, orgs, projs, envs, vpcs, subs) = temp_store().await;
        seed_stack(&orgs, &projs, &envs, &vpcs, &subs).await;
        create_vm(
            &vms, "web1", "acme", "web", "prod", "vpc1", "public", 1, 512, 10,
        )
        .await;
        let err = vms
            .create(
                "web1",
                "acme",
                "web",
                "prod",
                "vpc1",
                "public",
                1,
                512,
                10,
                "ubuntu-24.04",
                "eu",
                "fsn1",
                "hv-test".to_string(),
            )
            .await
            .expect_err("duplicate name");
        assert!(err.to_string().contains("already exists"), "got: {err}");
    }

    #[tokio::test]
    async fn list_filters_progressively() {
        let (_d, vms, orgs, projs, envs, vpcs, subs) = temp_store().await;
        seed_stack(&orgs, &projs, &envs, &vpcs, &subs).await;
        envs.create("staging", "web", "acme").await.unwrap();
        create_vm(
            &vms, "a", "acme", "web", "prod", "vpc1", "public", 1, 512, 10,
        )
        .await;
        create_vm(
            &vms, "b", "acme", "web", "prod", "vpc1", "public", 1, 512, 10,
        )
        .await;
        create_vm(
            &vms, "c", "acme", "web", "staging", "vpc1", "public", 1, 512, 10,
        )
        .await;

        let all = vms.list(None, None, None).await.unwrap();
        assert_eq!(all.len(), 3);
        let web = vms.list(Some("acme"), Some("web"), None).await.unwrap();
        assert_eq!(web.len(), 3);
        let prod = vms
            .list(Some("acme"), Some("web"), Some("prod"))
            .await
            .unwrap();
        assert_eq!(prod.len(), 2);
    }

    #[tokio::test]
    async fn update_state_valid_transition() {
        let (_d, vms, orgs, projs, envs, vpcs, subs) = temp_store().await;
        seed_stack(&orgs, &projs, &envs, &vpcs, &subs).await;
        let vm = create_vm(
            &vms, "web1", "acme", "web", "prod", "vpc1", "public", 1, 512, 10,
        )
        .await;
        assert!(matches!(vm.state, VmState::Pending));

        let updated = vms
            .update_state(&vm.meta.id, VmState::Running, None, None, None)
            .await
            .expect("pending -> running");
        assert!(matches!(updated.state, VmState::Running));
        assert!(updated.meta.updated_at >= vm.meta.updated_at);

        let got = vms
            .get(&vm.meta.id, None, None, None)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(got.state, VmState::Running));
    }

    #[tokio::test]
    async fn update_state_invalid_transition_rejected() {
        let (_d, vms, orgs, projs, envs, vpcs, subs) = temp_store().await;
        seed_stack(&orgs, &projs, &envs, &vpcs, &subs).await;
        let vm = create_vm(
            &vms, "web1", "acme", "web", "prod", "vpc1", "public", 1, 512, 10,
        )
        .await;
        let err = vms
            .update_state(&vm.meta.id, VmState::Stopped, None, None, None)
            .await
            .expect_err("pending -> stopped not allowed");
        assert!(
            err.to_string().contains("invalid state transition"),
            "got: {err}"
        );
    }

    #[tokio::test]
    async fn delete_in_running_state_is_rejected() {
        let (_d, vms, orgs, projs, envs, vpcs, subs) = temp_store().await;
        seed_stack(&orgs, &projs, &envs, &vpcs, &subs).await;
        let vm = create_vm(
            &vms, "web1", "acme", "web", "prod", "vpc1", "public", 1, 512, 10,
        )
        .await;
        vms.update_state(&vm.meta.id, VmState::Running, None, None, None)
            .await
            .unwrap();

        let err = vms
            .delete("web1", Some("acme"), Some("web"), Some("prod"))
            .await
            .expect_err("delete running vm");
        assert!(
            err.to_string().contains("must be stopped or pending"),
            "got: {err}"
        );
    }

    #[tokio::test]
    async fn delete_pending_vm_releases_ipam() {
        let (_d, vms, orgs, projs, envs, vpcs, subs) = temp_store().await;
        seed_stack(&orgs, &projs, &envs, &vpcs, &subs).await;
        let vm = create_vm(
            &vms, "web1", "acme", "web", "prod", "vpc1", "public", 1, 512, 10,
        )
        .await;
        let ip = vm.private_ip.clone().unwrap();

        vms.delete("web1", Some("acme"), Some("web"), Some("prod"))
            .await
            .expect("delete");
        assert!(vms
            .get(&vm.meta.id, None, None, None)
            .await
            .unwrap()
            .is_none());

        // The IP slot should be re-allocable to another VM (same
        // slot, because no other VM has been allocated yet).
        let vm2 = create_vm(
            &vms, "web2", "acme", "web", "prod", "vpc1", "public", 1, 512, 10,
        )
        .await;
        assert_eq!(vm2.private_ip.as_deref(), Some(ip.as_str()));
    }
}
