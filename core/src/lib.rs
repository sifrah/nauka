#![deny(clippy::print_stdout, clippy::print_stderr)]

pub mod logging;

pub use logging::{init, install_panic_hook, instrument_op, new_trace_id, LogErr, LogMode};
