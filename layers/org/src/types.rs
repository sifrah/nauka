//! Org data type.

use nauka_core::resource::{ApiResource, ResourceMeta};
use serde::{Deserialize, Serialize};

/// An organization — the top-level resource.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Org {
    #[serde(flatten)]
    pub meta: ResourceMeta,
}

impl ApiResource for Org {
    fn meta(&self) -> &ResourceMeta {
        &self.meta
    }
}
