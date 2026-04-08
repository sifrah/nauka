//! Organization layer — resource hierarchy for multi-tenancy.
//!
//! Three resources form the ownership tree:
//! - **Org** — top-level organization (globally unique name)
//! - **Project** — scoped within an Org
//! - **Environment** — scoped within a Project (prod, staging, dev)
//!
//! All future resources (VPC, Subnet, VM) are scoped under this hierarchy.
//! State is stored in ClusterDb (TiKV) for distributed consistency.

pub mod handlers;
pub mod store;
pub mod types;
