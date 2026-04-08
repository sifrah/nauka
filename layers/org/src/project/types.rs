//! Project data type.

use nauka_core::id::OrgId;
use nauka_core::resource::{ApiResource, ResourceMeta};
use serde::{Deserialize, Serialize};

/// A project within an organization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Project {
    #[serde(flatten)]
    pub meta: ResourceMeta,
    pub org_id: OrgId,
    pub org_name: String,
}

impl ApiResource for Project {
    fn meta(&self) -> &ResourceMeta {
        &self.meta
    }
    fn resource_fields(&self) -> serde_json::Value {
        serde_json::json!({
            "org_id": self.org_id.as_str(),
            "org_name": self.org_name,
        })
    }
}
