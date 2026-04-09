//! VPC reconciler — ensures VXLAN bridges + FDB entries for cross-node traffic.

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
        let mut needed_vpcs: Vec<(String, u32)> = Vec::new();
        for vm in &local_vms {
            let vpc_id = vm.vpc_id.as_str().to_string();
            if !needed_vpcs.iter().any(|(id, _)| id == &vpc_id) {
                if let Some(vpc) = vpc_store.get(&vpc_id, None).await? {
                    needed_vpcs.push((vpc_id, vpc.vni));
                }
            }
        }

        result.desired = needed_vpcs.len();

        // 3. Check actual state
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

        // 5. Remove orphaned bridges
        let needed_bridge_names: Vec<String> = needed_vpcs
            .iter()
            .map(|(id, _)| provision::bridge_name(id))
            .collect();
        for bridge in &actual_bridges {
            if !needed_bridge_names.contains(bridge) {
                tracing::info!(bridge, "removing orphaned bridge");
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

        // 6. FDB distribution — add entries for remote VMs in our VPCs
        for (vpc_id, _vni) in &needed_vpcs {
            // Find ALL VMs in this VPC (local + remote)
            let vpc_vms: Vec<_> = all_vms
                .iter()
                .filter(|vm| vm.vpc_id.as_str() == vpc_id)
                .collect();

            // For each remote VM, add FDB + ARP entry
            for vm in &vpc_vms {
                // Skip local VMs (they don't need FDB entries)
                let is_local = vm
                    .hypervisor_id
                    .as_ref()
                    .map(|hid| ctx.node_ids.iter().any(|nid| nid == hid))
                    .unwrap_or(false);
                if is_local {
                    continue;
                }

                let ip = match &vm.private_ip {
                    Some(ip) => ip,
                    None => continue,
                };

                let mac = match provision::mac_from_ip(ip) {
                    Some(mac) => mac,
                    None => continue,
                };

                // Resolve remote hypervisor's mesh IPv6
                let remote_ipv6 = resolve_hypervisor_ipv6(ctx, vm.hypervisor_id.as_deref());
                let remote_ipv6 = match remote_ipv6 {
                    Some(ip) => ip,
                    None => {
                        tracing::warn!(
                            vm_id = vm.meta.id.as_str(),
                            hypervisor = vm.hypervisor_id.as_deref().unwrap_or("?"),
                            "cannot resolve remote hypervisor mesh IPv6 for FDB"
                        );
                        continue;
                    }
                };

                let _ = provision::add_fdb_entry(vpc_id, &mac, &remote_ipv6);
                let _ = provision::add_arp_proxy(vpc_id, ip, &mac);
            }
        }

        Ok(result)
    }
}

/// Resolve a hypervisor identifier (HV ID or name) to its mesh IPv6.
fn resolve_hypervisor_ipv6(
    _ctx: &ReconcileContext,
    hypervisor_id: Option<&str>,
) -> Option<std::net::Ipv6Addr> {
    let hid = hypervisor_id?;

    // Load peer list from local fabric state
    let db = nauka_state::LocalDb::open("hypervisor").ok()?;
    let state = nauka_hypervisor::fabric::state::FabricState::load(&db).ok()??;

    // Check if it's self
    if hid == state.hypervisor.id.as_str() || hid == state.hypervisor.name {
        return Some(state.hypervisor.mesh_ipv6);
    }

    // Check peers (by ID or name)
    for peer in &state.peers.peers {
        if peer.id.as_str() == hid || peer.name == hid {
            return Some(peer.mesh_ipv6);
        }
    }

    None
}
