//! Network observer — scan for VXLAN bridges.
//!
//! Stub implementation: returns empty list.
//! Future: parse `ip link show` for nauka bridges.

/// List VPC IDs that have active bridges on this node.
pub fn list_bridges() -> Vec<String> {
    // Stub: no bridges
    vec![]
}
