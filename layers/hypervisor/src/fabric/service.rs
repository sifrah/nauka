//! Re-export from the wireguard backend module.
//!
//! The canonical implementation lives in `fabric::wireguard::service`.
//! This module re-exports for backward compatibility.

pub use super::wireguard::service::*;
