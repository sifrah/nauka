//! Org data type.

use nauka_core::id::OrgId;
use serde::{Deserialize, Serialize};

/// An organization — the top-level resource.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Org {
    pub id: OrgId,
    pub name: String,
    pub created_at: u64,
}
