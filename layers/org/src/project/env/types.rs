//! Environment data type.

use nauka_core::id::{OrgId, ProjectId};
use nauka_core::resource::{ApiResource, ResourceMeta};
use serde::{Deserialize, Serialize};

/// An environment within a project (e.g., production, staging, dev).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Environment {
    #[serde(flatten)]
    pub meta: ResourceMeta,
    pub project_id: ProjectId,
    pub project_name: String,
    pub org_id: OrgId,
    pub org_name: String,
}

impl ApiResource for Environment {
    fn meta(&self) -> &ResourceMeta {
        &self.meta
    }
    fn resource_fields(&self) -> serde_json::Value {
        serde_json::json!({
            "project_id": self.project_id.as_str(),
            "project_name": self.project_name,
            "org_id": self.org_id.as_str(),
            "org_name": self.org_name,
        })
    }
}
