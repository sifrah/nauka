#![deny(clippy::print_stdout, clippy::print_stderr)]

pub mod logging;
pub mod resource;

pub use logging::{
    init, install_panic_hook, instrument_op, new_trace_id, set_node_id, LogErr, LogMode,
    LogNaukaErr, NaukaError, NaukaFormat,
};
pub use resource::{
    cluster_schemas, local_schemas, Resource, ResourceDescriptor, ResourceOps, Scope,
};
