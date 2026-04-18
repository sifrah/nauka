//! Hypervisor resource definition — see ADR 0006.
//!
//! A node in the mesh. Replicated to every other node through Raft
//! consensus (`scope = "cluster"`), which is why writes go through
//! `nauka_state::Writer` (which routes to `RaftNode::write`) and
//! never directly to `Database::query`.

use nauka_core::resource::SurrealValue;
use nauka_core_macros::resource;
use serde::{Deserialize, Serialize};

#[resource(table = "hypervisor", scope = "cluster")]
#[derive(Serialize, Deserialize, SurrealValue, Debug, Clone)]
pub struct Hypervisor {
    #[id]
    pub public_key: String,
    #[unique]
    pub node_id: u64,
    #[unique]
    pub raft_addr: String,
    pub address: String,
    pub endpoint: Option<String>,
    pub allowed_ips: Vec<String>,
    pub keepalive: Option<u32>,
    // `created_at: Datetime`, `updated_at: Datetime`, `version: u64`
    // are injected by `#[resource]` — see ADR 0006.
}
