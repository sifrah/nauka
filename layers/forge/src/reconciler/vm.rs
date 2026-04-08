//! VM reconciler — ensures VMs assigned to this node are running.

use nauka_compute::vm::types::VmState;

use crate::types::{ReconcileContext, ReconcileResult};

pub struct VmReconciler;

#[async_trait::async_trait]
impl super::Reconciler for VmReconciler {
    fn name(&self) -> &str {
        "vm"
    }

    async fn reconcile(&self, ctx: &ReconcileContext) -> anyhow::Result<ReconcileResult> {
        let mut result = ReconcileResult::new("vm");

        // 1. Get VMs assigned to this node that should be running
        let vm_store = nauka_compute::vm::store::VmStore::new(ctx.db.clone());
        let all_vms = vm_store.list(None, None, None).await?;
        let local_vms: Vec<_> = all_vms
            .iter()
            .filter(|vm| vm.hypervisor_id.as_deref() == Some(&ctx.hypervisor_id))
            .collect();

        let should_run: Vec<_> = local_vms
            .iter()
            .filter(|vm| vm.state == VmState::Running)
            .collect();

        result.desired = should_run.len();

        // 2. Check actual state (stub: no processes running)
        let actual_processes = crate::observer::process::list_vms();
        result.actual = actual_processes.len();

        // 3. Diff and apply
        for vm in &should_run {
            if !actual_processes.iter().any(|p| p == &vm.meta.id) {
                tracing::info!(
                    vm_id = vm.meta.id.as_str(),
                    vm_name = vm.meta.name.as_str(),
                    image = vm.image.as_str(),
                    vcpus = vm.vcpus,
                    memory_mb = vm.memory_mb,
                    "would start VM"
                );
                result.created += 1;
            }
        }

        // 4. Check for orphaned processes (running but not in desired state)
        for process_id in &actual_processes {
            if !should_run.iter().any(|vm| vm.meta.id == *process_id) {
                tracing::info!(vm_id = process_id, "would stop orphaned VM");
                result.deleted += 1;
            }
        }

        Ok(result)
    }
}
