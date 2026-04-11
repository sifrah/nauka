//! Auto-discovered resource registry.
//!
//! Each layer crate submits its registration via `inventory::submit!`.
//! This module collects them all — no manual wiring needed.
//! Adding a new layer crate to Cargo.toml is all it takes.

use nauka_core::resource::ResourceRegistry;

/// Build the resource registry from all layers that submitted
/// a `LayerRegistration` via `inventory::submit!`.
pub fn build_registry() -> ResourceRegistry {
    ResourceRegistry::from_layers()
}
