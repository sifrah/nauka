//! Re-export from the wireguard backend module.
//!
//! The canonical implementation lives in `fabric::wireguard::wg`.
//! This module re-exports for backward compatibility.

pub use super::wireguard::wg::*;
