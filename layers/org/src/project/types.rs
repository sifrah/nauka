//! Project data type.

use std::collections::HashMap;

use nauka_core::id::{OrgId, ProjectId};
use serde::{Deserialize, Serialize};

/// A project within an organization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Project {
    pub id: ProjectId,
    pub name: String,
    pub org_id: OrgId,
    pub org_name: String,
    pub created_at: u64,
    pub updated_at: u64,
    pub status: String,
    pub labels: HashMap<String, String>,
}
