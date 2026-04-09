//! VM reconciler — ensures TAP interfaces exist for VMs on this node.

use nauka_compute::vm::provision;
use nauka_compute::vm::types::VmState;
use nauka_network::vpc::provision as vpc_provision;

use crate::types::{ReconcileContext, ReconcileResult};

pub struct VmReconciler;

#[async_trait::async_trait]
impl super::Reconciler for VmReconciler {
    fn name(&self) -> &str {
        "vm"
    }

    async fn reconcile(&self, ctx: &ReconcileContext) -> anyhow::Result<ReconcileResult> {
        let mut result = ReconcileResult::new("vm");

        // 1. Get VMs assigned to this node
        let vm_store = nauka_compute::vm::store::VmStore::new(ctx.db.clone());
        let all_vms = vm_store.list(None, None, None).await?;
        let local_vms: Vec<_> = all_vms
            .iter()
            .filter(|vm| vm.hypervisor_id.as_deref() == Some(&ctx.hypervisor_id))
            .collect();

        // VMs that should have TAPs (pending or running — anything assigned to this node)
        let need_tap: Vec<_> = local_vms
            .iter()
            .filter(|vm| vm.state == VmState::Pending || vm.state == VmState::Running)
            .collect();

        result.desired = need_tap.len();

        // 2. Check actual TAPs
        let actual_taps = provision::list_taps();
        result.actual = actual_taps.len();

        // 3. Create missing TAPs
        for vm in &need_tap {
            let expected_tap = provision::tap_name(&vm.meta.id);
            if !actual_taps.iter().any(|t| t == &expected_tap) {
                let bridge = vpc_provision::bridge_name(vm.vpc_id.as_str());
                match provision::ensure_tap(&vm.meta.id, &bridge) {
                    Ok(_) => result.created += 1,
                    Err(e) => {
                        tracing::error!(
                            vm_id = vm.meta.id.as_str(),
                            error = %e,
                            "failed to create TAP"
                        );
                        result.failed += 1;
                        result.errors.push(format!("vm {}: {e}", vm.meta.id));
                    }
                }
            }
        }

        // 4. Remove orphaned TAPs
        let needed_tap_names: Vec<String> = need_tap
            .iter()
            .map(|vm| provision::tap_name(&vm.meta.id))
            .collect();
        for tap in &actual_taps {
            if !needed_tap_names.contains(tap) {
                tracing::info!(tap, "removing orphaned TAP");
                let _ = std::process::Command::new("ip")
                    .args(["link", "del", tap])
                    .status();
                result.deleted += 1;
            }
        }

        Ok(result)
    }
}
