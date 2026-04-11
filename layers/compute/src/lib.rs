//! Compute layer — virtual machine lifecycle.
//!
//! Structure:
//! - **VM** — virtual machine scoped to an Org/Project/Env, placed on a subnet
//!
//! CLI: `nauka vm`

pub mod image;
pub mod runtime;
pub mod scheduler;
pub mod vm;

use nauka_core::resource::ResourceRegistration;

inventory::submit!(nauka_core::resource::LayerRegistration(registration));

/// Top-level registration: vm resource.
pub fn registration() -> ResourceRegistration {
    vm::handlers::registration()
}
