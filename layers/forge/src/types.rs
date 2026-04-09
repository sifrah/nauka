//! Forge types — config, context, and results.

use std::net::Ipv6Addr;

use nauka_compute::runtime::RuntimeMode;
use nauka_hypervisor::controlplane::ClusterDb;

/// Shared context for all reconcilers in a cycle.
pub struct ReconcileContext {
    /// Connection to TiKV (desired state).
    pub db: ClusterDb,
    /// This node's hypervisor ID.
    pub hypervisor_id: String,
    /// This node's name.
    pub node_name: String,
    /// This node's mesh IPv6.
    pub mesh_ipv6: Ipv6Addr,
    /// Compute runtime mode (KVM or container).
    pub runtime: RuntimeMode,
    /// Cycle number (monotonically increasing).
    pub cycle: u64,
}

/// Result of one reconciler's pass.
#[derive(Debug, Default)]
pub struct ReconcileResult {
    /// Reconciler name (e.g., "vpc", "vm").
    pub reconciler: String,
    /// How many resources are desired on this node.
    pub desired: usize,
    /// How many resources actually exist on this node.
    pub actual: usize,
    /// Actions taken.
    pub created: usize,
    pub deleted: usize,
    pub updated: usize,
    pub failed: usize,
    /// Error messages from failed actions.
    pub errors: Vec<String>,
}

impl ReconcileResult {
    pub fn new(name: &str) -> Self {
        Self {
            reconciler: name.to_string(),
            ..Default::default()
        }
    }

    pub fn is_clean(&self) -> bool {
        self.created == 0 && self.deleted == 0 && self.updated == 0 && self.failed == 0
    }
}

impl std::fmt::Display for ReconcileResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_clean() {
            write!(
                f,
                "{}: {} desired, {} actual — in sync",
                self.reconciler, self.desired, self.actual
            )
        } else {
            write!(
                f,
                "{}: {} desired, {} actual — created:{} deleted:{} updated:{} failed:{}",
                self.reconciler,
                self.desired,
                self.actual,
                self.created,
                self.deleted,
                self.updated,
                self.failed
            )
        }
    }
}
