//! Hypervisor daemon control socket — local Unix-domain IPC.
//!
//! Every deployed node runs a long-lived `nauka.service` process that
//! owns the bootstrap DB handle. Operator CLI commands (`status`,
//! `list`, `get`, `cp-status`, `drain`, …) forward their operation over
//! this Unix socket instead of opening `bootstrap.skv` directly — so
//! the daemon's flock never contends with an ad-hoc CLI.
//!
//! Protocol: length-prefixed JSON (reusing the `read_json` /
//! `write_json` helpers from `peering_server`), one request + one
//! response per connection. Not versioned — same binary on both ends.
//!
//! The CLI fallback behaviour (open `bootstrap.skv` directly when no
//! daemon is running) lives in [`client::forward_or_fallback`]. That
//! path is important for bootstrapping (`init` creates the DB before
//! installing the daemon), recovery, and test harnesses.

pub mod client;
pub mod protocol;
pub mod server;

pub use client::{forward_or_fallback, ClientError};
pub use protocol::{socket_path, ControlRequest, ControlResponse};
pub use server::run as run_control_server;
