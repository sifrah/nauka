//! Environment data type.

use std::collections::HashMap;

use nauka_core::id::{EnvId, OrgId, ProjectId};
use serde::{Deserialize, Serialize};

/// An environment within a project (e.g., production, staging, dev).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Environment {
    pub id: EnvId,
    pub name: String,
    pub project_id: ProjectId,
    pub project_name: String,
    pub org_id: OrgId,
    pub org_name: String,
    pub created_at: u64,
    pub updated_at: u64,
    pub status: String,
    pub labels: HashMap<String, String>,
}
