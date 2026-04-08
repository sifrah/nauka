use nauka_core::id::VpcId;
use nauka_core::resource::{ApiResource, ResourceMeta};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Subnet {
    #[serde(flatten)]
    pub meta: ResourceMeta,
    pub vpc_id: VpcId,
    pub vpc_name: String,
    pub cidr: String,
    pub gateway: String,
}

impl ApiResource for Subnet {
    fn meta(&self) -> &ResourceMeta {
        &self.meta
    }
    fn resource_fields(&self) -> serde_json::Value {
        serde_json::json!({
            "vpc_id": self.vpc_id.as_str(),
            "vpc_name": self.vpc_name,
            "cidr": self.cidr,
            "gateway": self.gateway,
        })
    }
}
