use std::net::Ipv6Addr;

use nauka_core::id::VpcId;
use nauka_core::resource::{ApiResource, ResourceMeta};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NatGwState {
    #[serde(rename = "provisioning")]
    Provisioning,
    #[serde(rename = "active")]
    Active,
    #[serde(rename = "error")]
    Error,
}

impl std::fmt::Display for NatGwState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Provisioning => write!(f, "provisioning"),
            Self::Active => write!(f, "active"),
            Self::Error => write!(f, "error"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NatGw {
    #[serde(flatten)]
    pub meta: ResourceMeta,
    pub vpc_id: VpcId,
    pub vpc_name: String,
    /// Dedicated public IPv6 address for this NAT gateway's outbound traffic.
    pub public_ipv6: Ipv6Addr,
    /// Hypervisor where this NAT gateway is provisioned.
    pub hypervisor_id: String,
    pub state: NatGwState,
}

impl ApiResource for NatGw {
    fn meta(&self) -> &ResourceMeta {
        &self.meta
    }
    fn resource_fields(&self) -> serde_json::Value {
        serde_json::json!({
            "vpc_id": self.vpc_id.as_str(),
            "vpc_name": self.vpc_name,
            "public_ipv6": self.public_ipv6.to_string(),
            "hypervisor_id": self.hypervisor_id,
            "state": self.state.to_string(),
        })
    }
}
