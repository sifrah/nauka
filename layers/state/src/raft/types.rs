use serde::{Deserialize, Serialize};
use std::fmt;

pub type NodeId = u64;

openraft::declare_raft_types!(
    pub TypeConfig:
        D = SurqlCommand,
        R = SurqlResponse,
);

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SurqlCommand {
    pub query: String,
}

impl fmt::Display for SurqlCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.query)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SurqlResponse {
    pub success: bool,
}

impl SurqlResponse {
    pub fn ok() -> Self {
        Self { success: true }
    }

    pub fn none() -> Self {
        Self { success: false }
    }
}
