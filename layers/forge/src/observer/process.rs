//! Process observer — scan for running VM processes.
//!
//! Stub implementation: returns empty list.
//! Future: scan /run/nauka/vms/ for PID files, check liveness.

/// List running VM IDs on this node.
pub fn list_vms() -> Vec<String> {
    // Stub: no VMs running
    vec![]
}
