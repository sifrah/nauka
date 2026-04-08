use nauka_core::id::VpcId;
use nauka_core::resource::{ApiResource, ResourceMeta};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PeeringState {
    #[serde(rename = "active")]
    Active,
    #[serde(rename = "pending")]
    Pending,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VpcPeering {
    #[serde(flatten)]
    pub meta: ResourceMeta,
    pub vpc_id: VpcId,
    pub vpc_name: String,
    pub peer_vpc_id: VpcId,
    pub peer_vpc_name: String,
    pub state: PeeringState,
}

impl ApiResource for VpcPeering {
    fn meta(&self) -> &ResourceMeta {
        &self.meta
    }
    fn resource_fields(&self) -> serde_json::Value {
        serde_json::json!({
            "vpc_id": self.vpc_id.as_str(),
            "vpc_name": self.vpc_name,
            "peer_vpc_id": self.peer_vpc_id.as_str(),
            "peer_vpc_name": self.peer_vpc_name,
            "state": self.state,
        })
    }
}
