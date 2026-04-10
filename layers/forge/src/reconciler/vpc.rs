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

        // 4. Create missing bridges + verify VXLAN interfaces
        for (vpc_id, vni) in &needed_vpcs {
            let expected_bridge = provision::bridge_name(vpc_id);
            let expected_vxlan = provision::vxlan_name(vpc_id);
            let bridge_missing = !actual_bridges.iter().any(|b| b == &expected_bridge);
            let vxlan_missing = !crate::observer::network::iface_exists(&expected_vxlan);

            if bridge_missing || vxlan_missing {
                if vxlan_missing && !bridge_missing {
                    tracing::info!(
                        vpc_id,
                        vxlan = expected_vxlan.as_str(),
                        "VXLAN interface missing — recreating"
                    );
                }
                match provision::ensure_bridge(vpc_id, *vni, &ctx.mesh_ipv6) {
                    Ok(()) => result.created += 1,
                    Err(e) => {
                        tracing::error!(vpc_id, error = %e, "failed to create bridge/vxlan");
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

        // 5b. Ensure nftables host isolation rules for all active bridges
        provision::ensure_host_isolation_all(&needed_bridge_names);

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

                // Add static ARP inside local containers so they can reach this remote VM
                for local_vm in &local_vms {
                    if local_vm.vpc_id.as_str() == *vpc_id {
                        if let Some(pid) = get_container_pid(&local_vm.meta.id) {
                            let _ = add_arp_in_container(pid, ip, &mac);
                        }
                    }
                }
            }
        }

        // 7. Set gateway IP on bridges so containers can reach the gateway
        for (vpc_id, vni) in &needed_vpcs {
            let br = provision::bridge_name(vpc_id);
            // Look up the subnet gateway from any local VM in this VPC
            if let Some(local_vm) = local_vms.iter().find(|vm| vm.vpc_id.as_str() == *vpc_id) {
                let subnet_store =
                    nauka_network::vpc::subnet::store::SubnetStore::new(ctx.db.clone());
                if let Ok(Some(subnet)) = subnet_store
                    .get(local_vm.subnet_id.as_str(), None, None)
                    .await
                {
                    let gw_cidr = format!("{}/24", subnet.gateway);
                    let table = vni.to_string();

                    // Set gateway IP on the bridge
                    let _ = std::process::Command::new("ip")
                        .args(["addr", "replace", &gw_cidr, "dev", &br])
                        .stdout(std::process::Stdio::null())
                        .stderr(std::process::Stdio::null())
                        .status();

                    // Add subnet route in the VPC's routing table (for policy routing)
                    let _ = std::process::Command::new("ip")
                        .args([
                            "route",
                            "replace",
                            &subnet.cidr,
                            "dev",
                            &br,
                            "table",
                            &table,
                        ])
                        .stdout(std::process::Stdio::null())
                        .stderr(std::process::Stdio::null())
                        .status();

                    // HOST ISOLATION: remove the auto-created route from the main
                    // routing table. The kernel adds a connected route when we set
                    // the IP, but we only want the route in the VPC's table.
                    // This prevents the host from reaching VM IPs directly.
                    let _ = std::process::Command::new("ip")
                        .args(["route", "del", &subnet.cidr, "dev", &br, "table", "main"])
                        .stdout(std::process::Stdio::null())
                        .stderr(std::process::Stdio::null())
                        .status();
                }
            }
        }

        // 8. VPC Peering — route leak between VRFs
        let peering_store = nauka_network::vpc::peering::store::PeeringStore::new(ctx.db.clone());
        let mut peered_bridge_pairs: Vec<(String, String)> = Vec::new();
        for (vpc_id, _) in &needed_vpcs {
            let peerings = peering_store.list(Some(vpc_id)).await.unwrap_or_default();
            for peering in &peerings {
                if !matches!(
                    peering.state,
                    nauka_network::vpc::peering::types::PeeringState::Active
                ) {
                    continue;
                }

                let vpc_a = vpc_store.get(peering.vpc_id.as_str(), None).await?;
                let vpc_b = vpc_store.get(peering.peer_vpc_id.as_str(), None).await?;

                if let (Some(a), Some(b)) = (vpc_a, vpc_b) {
                    let br_a = provision::bridge_name(a.meta.id.as_str());
                    let br_b = provision::bridge_name(b.meta.id.as_str());

                    if crate::observer::network::bridge_exists(&br_a)
                        && crate::observer::network::bridge_exists(&br_b)
                    {
                        // Deduplicate — peering is seen from both VPCs
                        if !peered_bridge_pairs
                            .iter()
                            .any(|(a, b)| (a == &br_a && b == &br_b) || (a == &br_b && b == &br_a))
                        {
                            peered_bridge_pairs.push((br_a.clone(), br_b.clone()));
                        }
                        let _ = provision::ensure_peering_routes(
                            a.meta.id.as_str(),
                            &a.cidr,
                            a.vni,
                            b.meta.id.as_str(),
                            &b.cidr,
                            b.vni,
                        );
                    }
                }
            }
        }

        // 9. IP forwarding + nftables FORWARD chain
        //    Enable ip_forward globally (required for L3 peering between bridges)
        //    but lock down with nftables: only allow forwarding between peered bridges.
        ensure_forward_rules(&peered_bridge_pairs);

        Ok(result)
    }
}

/// Set up nftables FORWARD rules to only allow traffic between peered bridges.
///
/// - Enables ip_forward globally (required for L3 route leak between bridges)
/// - Creates a FORWARD chain with policy DROP
/// - Whitelists only the specific peered bridge pairs (bidirectional)
/// - If no peerings exist, disables ip_forward and removes the chain
fn ensure_forward_rules(peered_pairs: &[(String, String)]) {
    use std::process::Command;

    if peered_pairs.is_empty() {
        // No peerings — disable forwarding and clean up
        let _ = std::fs::write("/proc/sys/net/ipv4/ip_forward", "0");
        let _ = std::fs::write("/proc/sys/net/ipv6/conf/all/forwarding", "0");
        let _ = Command::new("nft")
            .args(["delete", "chain", "inet", "nauka", "forward"])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();
        return;
    }

    // Enable forwarding (required for peering)
    let _ = std::fs::write("/proc/sys/net/ipv4/ip_forward", "1");
    let _ = std::fs::write("/proc/sys/net/ipv6/conf/all/forwarding", "1");

    // Ensure nauka table exists
    let _ = Command::new("nft")
        .args(["add", "table", "inet", "nauka"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status();

    // Recreate forward chain with policy drop (flush if exists)
    let _ = Command::new("nft")
        .args(["delete", "chain", "inet", "nauka", "forward"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status();

    let _ = Command::new("nft")
        .args([
            "add",
            "chain",
            "inet",
            "nauka",
            "forward",
            "{ type filter hook forward priority 0; policy drop; }",
        ])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status();

    // Allow established/related connections (for return traffic)
    let _ = Command::new("nft")
        .args([
            "add",
            "rule",
            "inet",
            "nauka",
            "forward",
            "ct state established,related accept",
        ])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status();

    // Whitelist each peered bridge pair (bidirectional)
    for (br_a, br_b) in peered_pairs {
        let rule_ab = format!("iifname {br_a} oifname {br_b} accept");
        let rule_ba = format!("iifname {br_b} oifname {br_a} accept");
        let _ = Command::new("nft")
            .args(["add", "rule", "inet", "nauka", "forward", &rule_ab])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();
        let _ = Command::new("nft")
            .args(["add", "rule", "inet", "nauka", "forward", &rule_ba])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();
    }
}

/// Get the PID of a running container by VM ID.
fn get_container_pid(vm_id: &str) -> Option<u32> {
    let pid_path = std::path::PathBuf::from("/run/nauka/vms")
        .join(vm_id)
        .join("pid");
    std::fs::read_to_string(pid_path)
        .ok()
        .and_then(|s| s.trim().parse::<u32>().ok())
        .filter(|&pid| unsafe { libc::kill(pid as i32, 0) == 0 })
}

/// Add a static ARP entry inside a container's network namespace.
fn add_arp_in_container(container_pid: u32, ip: &str, mac: &str) -> anyhow::Result<()> {
    let pid = container_pid.to_string();
    let _ = std::process::Command::new("nsenter")
        .args([
            "--net",
            &format!("--target={pid}"),
            "ip",
            "neigh",
            "replace",
            ip,
            "lladdr",
            mac,
            "dev",
            "eth0",
            "nud",
            "permanent",
        ])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status();
    Ok(())
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
