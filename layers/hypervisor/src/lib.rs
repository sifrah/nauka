#![allow(clippy::result_large_err)]
//! Hypervisor layer — the core unit of a Nauka cluster.
//!
//! An hypervisor is a physical or virtual server that runs workloads.
//! It contains sub-systems:
//!
//! - **fabric**: WireGuard mesh networking, peering, connectivity
//! - **controlplane**: Raft consensus, gossip, scheduler, state machine (future)
//! - **compute**: VM/container runtime (future)
//! - **storage**: ZeroFS volumes, S3 backend (future)

inventory::submit!(nauka_core::resource::LayerRegistration(
    handlers::registration,
));

pub mod controlplane;
pub mod detect;
pub mod doctor;
pub mod fabric;
pub mod handlers;
pub mod storage;
