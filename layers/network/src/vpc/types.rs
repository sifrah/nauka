use nauka_core::id::{EnvId, OrgId, ProjectId};
use nauka_core::resource::{ApiResource, ResourceMeta};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Vpc {
    #[serde(flatten)]
    pub meta: ResourceMeta,
    pub cidr: String,
    pub org_id: OrgId,
    pub org_name: String,
    pub vni: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project_id: Option<ProjectId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub env_id: Option<EnvId>,
}

impl ApiResource for Vpc {
    fn meta(&self) -> &ResourceMeta {
        &self.meta
    }
    fn resource_fields(&self) -> serde_json::Value {
        let mut fields = serde_json::json!({
            "cidr": self.cidr,
            "org_id": self.org_id.as_str(),
            "org_name": self.org_name,
            "vni": self.vni,
        });
        if let Some(ref pid) = self.project_id {
            fields["project_id"] = serde_json::json!(pid.as_str());
        }
        if let Some(ref eid) = self.env_id {
            fields["env_id"] = serde_json::json!(eid.as_str());
        }
        fields
    }
}
