use std::fmt;

use nauka_core::id::{EnvId, OrgId, ProjectId, SubnetId, VpcId};
use nauka_core::resource::{ApiResource, ResourceMeta};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum VmState {
    #[default]
    Pending,
    Creating,
    Running,
    Stopped,
    Deleting,
    Deleted,
}

impl fmt::Display for VmState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            VmState::Pending => write!(f, "pending"),
            VmState::Creating => write!(f, "creating"),
            VmState::Running => write!(f, "running"),
            VmState::Stopped => write!(f, "stopped"),
            VmState::Deleting => write!(f, "deleting"),
            VmState::Deleted => write!(f, "deleted"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Vm {
    #[serde(flatten)]
    pub meta: ResourceMeta,
    // Scope
    pub org_id: OrgId,
    pub org_name: String,
    pub project_id: ProjectId,
    pub project_name: String,
    pub env_id: EnvId,
    pub env_name: String,
    // Network
    pub vpc_id: VpcId,
    pub vpc_name: String,
    pub subnet_id: SubnetId,
    pub subnet_name: String,
    // Specs
    pub vcpus: u32,
    pub memory_mb: u32,
    pub disk_gb: u32,
    pub image: String,
    // Placement
    pub region: String,
    pub zone: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub private_ip: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hypervisor_id: Option<String>,
    // Lifecycle
    pub state: VmState,
}

impl ApiResource for Vm {
    fn meta(&self) -> &ResourceMeta {
        &self.meta
    }
    fn resource_fields(&self) -> serde_json::Value {
        let mut fields = serde_json::json!({
            "org_id": self.org_id.as_str(),
            "org_name": self.org_name,
            "project_id": self.project_id.as_str(),
            "project_name": self.project_name,
            "env_id": self.env_id.as_str(),
            "env_name": self.env_name,
            "vpc_id": self.vpc_id.as_str(),
            "vpc_name": self.vpc_name,
            "subnet_id": self.subnet_id.as_str(),
            "subnet_name": self.subnet_name,
            "vcpus": self.vcpus,
            "memory_mb": self.memory_mb,
            "disk_gb": self.disk_gb,
            "image": self.image,
            "region": self.region,
            "zone": self.zone,
            "state": self.state.to_string(),
        });
        if let Some(ref ip) = self.private_ip {
            fields["private_ip"] = serde_json::json!(ip);
        }
        if let Some(ref hv) = self.hypervisor_id {
            fields["hypervisor_id"] = serde_json::json!(hv);
        }
        fields
    }
}
