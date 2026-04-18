#![deny(clippy::print_stdout, clippy::print_stderr)]

pub mod daemon;
pub mod definition;
pub mod mesh;
pub mod systemd;

pub use definition::Hypervisor;
pub use mesh::MeshRecord;
