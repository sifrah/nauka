//! Organization layer — resource hierarchy for multi-tenancy.
//!
//! Structure mirrors the resource hierarchy:
//! - **Org** — top-level organization (globally unique name)
//!   - **Project** — scoped within an Org
//!     - **Env** — scoped within a Project (prod, staging, dev)
//!
//! CLI: `nauka org`, `nauka org project`, `nauka org project env`

pub mod handlers;
pub mod project;
mod sdk_bridge;
pub mod store;
pub mod types;

use nauka_core::resource::ResourceRegistration;

inventory::submit!(nauka_core::resource::LayerRegistration(registration));

/// Top-level registration: org with project (with env) as children.
pub fn registration() -> ResourceRegistration {
    ResourceRegistration {
        def: handlers::resource_def(),
        handler: handlers::handler(),
        children: vec![project::handlers::registration()],
    }
}
