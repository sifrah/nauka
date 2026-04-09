//! Reconciler trait and orchestration.
//!
//! Each resource type (VPC, VM) implements `Reconciler`.
//! The `run_all` function executes them in dependency order.

pub mod natgw;
pub mod vm;
pub mod vpc;

use crate::types::{ReconcileContext, ReconcileResult};

/// Trait that each resource type's reconciler must implement.
#[async_trait::async_trait]
pub trait Reconciler: Send + Sync {
    /// Human-readable name for logging.
    fn name(&self) -> &str;

    /// Run one reconciliation pass. Returns a summary of actions taken.
    async fn reconcile(&self, ctx: &ReconcileContext) -> anyhow::Result<ReconcileResult>;
}

/// Run all reconcilers in dependency order.
///
/// VPCs first (bridges must exist before VMs can attach TAPs),
/// then VMs.
pub async fn run_all(ctx: &ReconcileContext) -> Vec<ReconcileResult> {
    let reconcilers: Vec<Box<dyn Reconciler>> = vec![
        Box::new(vpc::VpcReconciler),
        Box::new(natgw::NatGwReconciler),
        Box::new(vm::VmReconciler),
    ];

    let mut results = Vec::new();
    for r in &reconcilers {
        match r.reconcile(ctx).await {
            Ok(result) => {
                tracing::info!("{}", result);
                results.push(result);
            }
            Err(e) => {
                tracing::error!(reconciler = r.name(), error = %e, "reconciler failed");
                let mut result = ReconcileResult::new(r.name());
                result.failed = 1;
                result.errors.push(e.to_string());
                results.push(result);
            }
        }
    }

    results
}
