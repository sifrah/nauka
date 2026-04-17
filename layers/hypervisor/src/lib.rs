#![deny(clippy::print_stdout, clippy::print_stderr)]

pub mod daemon;
pub mod definition;
pub mod mesh;
pub mod systemd;

pub use definition::Hypervisor;

/// Provisional: the `mesh` table is still hand-written pending P5
/// migration. Once `Mesh` itself becomes `#[resource]`, this constant
/// goes away and callers rely solely on
/// `nauka_core::{local_schemas, cluster_schemas}`.
pub const SCHEMA: &str = include_str!("mesh/definition.surql");
