use nauka_core::id::VmId;
use nauka_core::resource::ResourceMeta;
use nauka_hypervisor::controlplane::ClusterDb;

use super::types::{Vm, VmState};

const NS_VM: &str = "vm";
const NS_VM_IDX: &str = "vm-idx";
const REG_VMS: (&str, &str) = ("_reg", "vm-ids");

pub struct VmStore {
    db: ClusterDb,
}

impl VmStore {
    pub fn new(db: ClusterDb) -> Self {
        Self { db }
    }

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
    ) -> anyhow::Result<Vm> {
        // Resolve org -> project -> env
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

        // Resolve vpc -> subnet
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

        // Check uniqueness within env
        let idx_key = format!("{}/{}", env.meta.id, name);
        let existing: Option<String> = self.db.get(NS_VM_IDX, &idx_key).await?;
        if existing.is_some() {
            anyhow::bail!("vm '{name}' already exists in environment '{env_name}'");
        }

        let vm = Vm {
            meta: ResourceMeta::new(VmId::generate().to_string(), name),
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
            private_ip: None,
            hypervisor_id: Some(crate::scheduler::schedule(region, zone)?),
            state: VmState::Pending,
        };

        self.db.put(NS_VM, &vm.meta.id, &vm).await?;
        self.db.put(NS_VM_IDX, &idx_key, &vm.meta.id).await?;
        add_id(&self.db, &vm.meta.id).await?;

        Ok(vm)
    }

    pub async fn get(
        &self,
        name_or_id: &str,
        org_name: Option<&str>,
        project_name: Option<&str>,
        env_name: Option<&str>,
    ) -> anyhow::Result<Option<Vm>> {
        if VmId::looks_like_id(name_or_id) {
            return self.db.get(NS_VM, name_or_id).await.map_err(Into::into);
        }

        // Need full scope to resolve by name
        let org_name =
            org_name.ok_or_else(|| anyhow::anyhow!("--org required to resolve VM by name"))?;
        let project_name = project_name
            .ok_or_else(|| anyhow::anyhow!("--project required to resolve VM by name"))?;
        let env_name =
            env_name.ok_or_else(|| anyhow::anyhow!("--env required to resolve VM by name"))?;

        let env_store = nauka_org::project::env::store::EnvStore::new(self.db.clone());
        let env = env_store
            .get(env_name, Some(project_name), Some(org_name))
            .await?
            .ok_or_else(|| anyhow::anyhow!("environment '{env_name}' not found"))?;

        let idx_key = format!("{}/{}", env.meta.id, name_or_id);
        let id: Option<String> = self.db.get(NS_VM_IDX, &idx_key).await?;
        match id {
            Some(id) => self.db.get(NS_VM, &id).await.map_err(Into::into),
            None => Ok(None),
        }
    }

    pub async fn list(
        &self,
        org_name: Option<&str>,
        project_name: Option<&str>,
        env_name: Option<&str>,
    ) -> anyhow::Result<Vec<Vm>> {
        let ids = load_ids(&self.db).await?;
        let mut vms = Vec::new();
        for id in &ids {
            if let Some(vm) = self.db.get::<Vm>(NS_VM, id).await? {
                vms.push(vm);
            }
        }

        // Progressive filtering
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

        // Validate state transition
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

        self.db.put(NS_VM, &vm.meta.id, &vm).await?;
        Ok(vm)
    }

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

        let idx_key = format!("{}/{}", vm.env_id.as_str(), vm.meta.name);
        self.db.delete(NS_VM, &vm.meta.id).await?;
        self.db.delete(NS_VM_IDX, &idx_key).await?;
        remove_id(&self.db, &vm.meta.id).await?;
        Ok(())
    }
}

async fn load_ids(db: &ClusterDb) -> anyhow::Result<Vec<String>> {
    let ids: Option<Vec<String>> = db.get(REG_VMS.0, REG_VMS.1).await?;
    Ok(ids.unwrap_or_default())
}

async fn add_id(db: &ClusterDb, id: &str) -> anyhow::Result<()> {
    let mut ids = load_ids(db).await?;
    ids.push(id.to_string());
    db.put(REG_VMS.0, REG_VMS.1, &ids).await?;
    Ok(())
}

async fn remove_id(db: &ClusterDb, id: &str) -> anyhow::Result<()> {
    let mut ids = load_ids(db).await?;
    ids.retain(|i| i != id);
    db.put(REG_VMS.0, REG_VMS.1, &ids).await?;
    Ok(())
}
