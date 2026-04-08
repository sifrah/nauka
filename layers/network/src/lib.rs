//! Network layer — VPC, Subnet, VPC Peering.
//!
//! Structure mirrors the resource hierarchy:
//! - **VPC** — virtual private cloud scoped to an Org
//!   - **Subnet** — scoped within a VPC
//!   - **Peering** — connection between two VPCs
//!
//! CLI: `nauka vpc`, `nauka vpc subnet`, `nauka vpc peering`

pub mod validate;
pub mod vpc;

use nauka_core::resource::ResourceRegistration;

/// Top-level registration: vpc with subnet and peering as children.
pub fn registration() -> ResourceRegistration {
    vpc::handlers::registration()
}
