//! VPC reconciler — ensures VXLAN bridges exist for VPCs with local VMs.

use nauka_network::vpc::provision;

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
            .filter(|vm| {
                vm.hypervisor_id
                    .as_ref()
                    .map(|hid| ctx.node_ids.iter().any(|nid| nid == hid))
                    .unwrap_or(false)
            })
            .collect();

        // 2. Collect unique VPC IDs needed on this node + resolve their VNIs
        let vpc_store = nauka_network::vpc::store::VpcStore::new(ctx.db.clone());
        let mut needed_vpcs: Vec<(String, u32)> = Vec::new(); // (vpc_id, vni)
        for vm in &local_vms {
            let vpc_id = vm.vpc_id.as_str().to_string();
            if !needed_vpcs.iter().any(|(id, _)| id == &vpc_id) {
                if let Some(vpc) = vpc_store.get(&vpc_id, None).await? {
                    needed_vpcs.push((vpc_id, vpc.vni));
                }
            }
        }

        result.desired = needed_vpcs.len();

        // 3. Check actual state — scan for existing bridges
        let actual_bridges = crate::observer::network::list_bridges();
        result.actual = actual_bridges.len();

        // 4. Create missing bridges
        for (vpc_id, vni) in &needed_vpcs {
            let expected_bridge = provision::bridge_name(vpc_id);
            if !actual_bridges.iter().any(|b| b == &expected_bridge) {
                match provision::ensure_bridge(vpc_id, *vni, &ctx.mesh_ipv6) {
                    Ok(()) => result.created += 1,
                    Err(e) => {
                        tracing::error!(vpc_id, error = %e, "failed to create bridge");
                        result.failed += 1;
                        result.errors.push(format!("vpc {vpc_id}: {e}"));
                    }
                }
            }
        }

        // 5. Remove orphaned bridges (exist locally but no VMs need them)
        let needed_bridge_names: Vec<String> = needed_vpcs
            .iter()
            .map(|(id, _)| provision::bridge_name(id))
            .collect();
        for bridge in &actual_bridges {
            if !needed_bridge_names.contains(bridge) {
                tracing::info!(bridge, "removing orphaned bridge");
                // Derive VXLAN name from bridge name (nkb-HASH → nkx-HASH)
                let vxlan = bridge.replace("nkb-", "nkx-");
                let _ = std::process::Command::new("ip")
                    .args(["link", "del", &vxlan])
                    .status();
                let _ = std::process::Command::new("ip")
                    .args(["link", "del", bridge])
                    .status();
                result.deleted += 1;
            }
        }

        Ok(result)
    }
}
