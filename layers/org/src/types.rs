//! Org, Project, Environment data types.

use nauka_core::id::{EnvId, OrgId, ProjectId};
use serde::{Deserialize, Serialize};

/// An organization — the top-level resource.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Org {
    pub id: OrgId,
    pub name: String,
    pub created_at: u64,
}

/// A project within an organization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Project {
    pub id: ProjectId,
    pub name: String,
    pub org_id: OrgId,
    pub org_name: String,
    pub created_at: u64,
}

/// An environment within a project.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Environment {
    pub id: EnvId,
    pub name: String,
    pub project_id: ProjectId,
    pub project_name: String,
    pub org_id: OrgId,
    pub org_name: String,
    pub created_at: u64,
}
