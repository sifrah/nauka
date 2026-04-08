//! VPC reconciler — ensures VXLAN bridges exist for VPCs with local VMs.

use crate::types::{ReconcileContext, ReconcileResult};

pub struct VpcReconciler;

#[async_trait::async_trait]
impl super::Reconciler for VpcReconciler {
    fn name(&self) -> &str {
        "vpc"
    }

    async fn reconcile(&self, ctx: &ReconcileContext) -> anyhow::Result<ReconcileResult> {
        let mut result = ReconcileResult::new("vpc");

        // 1. Find which VMs are on this node
        let vm_store = nauka_compute::vm::store::VmStore::new(ctx.db.clone());
        let all_vms = vm_store.list(None, None, None).await?;
        let local_vms: Vec<_> = all_vms
            .iter()
            .filter(|vm| vm.hypervisor_id.as_deref() == Some(&ctx.hypervisor_id))
            .collect();

        // 2. Collect unique VPC IDs needed on this node
        let mut needed_vpc_ids: Vec<String> = local_vms
            .iter()
            .map(|vm| vm.vpc_id.as_str().to_string())
            .collect();
        needed_vpc_ids.sort();
        needed_vpc_ids.dedup();

        result.desired = needed_vpc_ids.len();

        // 3. Check actual state (stub: nothing exists yet)
        let actual_bridges = crate::observer::network::list_bridges();
        result.actual = actual_bridges.len();

        // 4. Diff and apply
        for vpc_id in &needed_vpc_ids {
            if !actual_bridges.iter().any(|b| b == vpc_id) {
                tracing::info!(vpc_id, "would create bridge for VPC");
                result.created += 1;
            }
        }

        Ok(result)
    }
}
