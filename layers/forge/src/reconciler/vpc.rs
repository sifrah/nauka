//! VPC reconciler — ensures VXLAN bridges + FDB entries for cross-node traffic.

use nauka_network::vpc::provision;

use crate::types::{ReconcileContext, ReconcileResult};

// ═══════════════════════════════════════════════════
// Pure helper functions (no I/O, fully testable)
// ═══════════════════════════════════════════════════

/// Derive the VXLAN interface name from a bridge name by swapping the prefix.
///
/// Bridge names use `nkb-{hash}`, VXLAN names use `nkx-{hash}`.
fn vxlan_name_from_bridge(bridge: &str) -> String {
    bridge.replace("nkb-", "nkx-")
}

/// Identify orphaned bridges — bridges that exist on the system but are not
/// needed by any VM currently assigned to this node.
///
/// Returns the list of bridge names that should be removed.
fn find_orphaned_bridges<'a>(
    actual_bridges: &'a [String],
    needed_bridge_names: &[String],
) -> Vec<&'a String> {
    actual_bridges
        .iter()
        .filter(|b| !needed_bridge_names.contains(b))
        .collect()
}

/// Build nftables FORWARD rules for a list of peered bridge pairs.
///
/// Returns a Vec of rule strings in nft format (e.g. "iifname nkb-x oifname nkb-y accept").
/// Each pair generates two rules (bidirectional).
fn build_forward_rules(peered_pairs: &[(String, String)]) -> Vec<String> {
    let mut rules = Vec::new();
    for (br_a, br_b) in peered_pairs {
        rules.push(format!("iifname {br_a} oifname {br_b} accept"));
        rules.push(format!("iifname {br_b} oifname {br_a} accept"));
    }
    rules
}

/// Check whether an ip-rule entry already exists in `ip rule show` output.
///
/// Returns true if a rule matching both `direction bridge` and `lookup table`
/// is found in the output.
fn ip_rule_exists(rule_output: &str, direction: &str, bridge: &str, table: &str) -> bool {
    rule_output.contains(&format!("{direction} {bridge}"))
        && rule_output.contains(&format!("lookup {table}"))
}

/// Parse a PID from file contents (trimmed string to u32).
///
/// Returns `None` if the string is empty or not a valid u32.
fn parse_container_pid(contents: &str) -> Option<u32> {
    contents.trim().parse::<u32>().ok()
}

/// Format a gateway IP as a CIDR address with /24 prefix length.
fn gateway_cidr(gateway_ip: &str) -> String {
    format!("{gateway_ip}/24")
}

/// Deduplicate a peered bridge pair — returns true if the pair (in either
/// direction) already exists in the list.
fn peering_pair_exists(pairs: &[(String, String)], br_a: &str, br_b: &str) -> bool {
    pairs
        .iter()
        .any(|(a, b)| (a == br_a && b == br_b) || (a == br_b && b == br_a))
}

pub struct VpcReconciler;

#[async_trait::async_trait]
impl super::Reconciler for VpcReconciler {
    fn name(&self) -> &str {
        "vpc"
    }

    async fn reconcile(&self, ctx: &ReconcileContext) -> anyhow::Result<ReconcileResult> {
        let mut result = ReconcileResult::new("vpc");

        // 1. Find which VMs are on this node. P2.13 (sifrah/nauka#217)
        // migrated `VmStore` to take an `EmbeddedDb` directly.
        let vm_store = nauka_compute::vm::store::VmStore::new(ctx.db.embedded().clone());
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
        let vpc_store = nauka_network::vpc::store::VpcStore::new(ctx.db.embedded().clone());
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
        for bridge in find_orphaned_bridges(&actual_bridges, &needed_bridge_names) {
            tracing::info!(bridge, "removing orphaned bridge");
            let vxlan = vxlan_name_from_bridge(bridge);
            let _ = std::process::Command::new("ip")
                .args(["link", "del", &vxlan])
                .status();
            let _ = std::process::Command::new("ip")
                .args(["link", "del", bridge])
                .status();
            result.deleted += 1;
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
                let remote_ipv6 = resolve_hypervisor_ipv6(ctx, vm.hypervisor_id.as_deref()).await;
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

        // 6b. Ensure policy routing rules exist for each VPC bridge
        for (vpc_id, vni) in &needed_vpcs {
            let br = provision::bridge_name(vpc_id);
            let table = vni.to_string();
            ensure_ip_rule("iif", &br, &table);
            ensure_ip_rule("oif", &br, &table);
        }

        // 7. Set gateway IP on bridges so containers can reach the gateway
        for (vpc_id, vni) in &needed_vpcs {
            let br = provision::bridge_name(vpc_id);
            // Look up the subnet gateway from any local VM in this VPC
            if let Some(local_vm) = local_vms.iter().find(|vm| vm.vpc_id.as_str() == *vpc_id) {
                let subnet_store =
                    nauka_network::vpc::subnet::store::SubnetStore::new(ctx.db.embedded().clone());
                if let Ok(Some(subnet)) = subnet_store
                    .get(local_vm.subnet_id.as_str(), None, None)
                    .await
                {
                    let gw_cidr = gateway_cidr(&subnet.gateway);
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
        let peering_store =
            nauka_network::vpc::peering::store::PeeringStore::new(ctx.db.embedded().clone());
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
                        if !peering_pair_exists(&peered_bridge_pairs, &br_a, &br_b) {
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

    // Always keep a FORWARD chain with policy DROP. This ensures VPC
    // isolation even when ip_forward is enabled (NAT gateways need it).
    // Only whitelist specific peered bridge pairs.

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

    // Allow outbound traffic from VPC bridges to the public interface (NAT gateway internet)
    let _ = Command::new("nft")
        .args([
            "add",
            "rule",
            "inet",
            "nauka",
            "forward",
            "iifname \"nkb-*\" oifname != \"nkb-*\" accept",
        ])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status();

    // Whitelist each peered bridge pair (bidirectional)
    for rule in build_forward_rules(peered_pairs) {
        let _ = Command::new("nft")
            .args(["add", "rule", "inet", "nauka", "forward", &rule])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();
    }
}

/// Ensure an ip rule exists (e.g., `ip rule add iif nkb-xxx table 100`).
/// Checks first to avoid duplicates.
fn ensure_ip_rule(direction: &str, bridge: &str, table: &str) {
    use std::process::Command;

    // Check if rule already exists
    let output = Command::new("ip").args(["rule", "show"]).output().ok();

    let exists = output
        .as_ref()
        .map(|o| {
            let stdout = String::from_utf8_lossy(&o.stdout);
            ip_rule_exists(&stdout, direction, bridge, table)
        })
        .unwrap_or(false);

    if !exists {
        tracing::info!(direction, bridge, table, "restoring policy routing rule");
        let _ = Command::new("ip")
            .args(["rule", "add", direction, bridge, "table", table])
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
        .and_then(|s| parse_container_pid(&s))
        .filter(|&pid| {
            // SAFETY: `kill(pid, 0)` sends no signal — it only checks whether the
            // process exists and is reachable. The pid comes from a file we wrote
            // during VM creation, so it is always a valid positive integer. The
            // worst case (stale PID file pointing at a recycled PID) simply returns
            // 0 (process exists) or -1 (no such process), both harmless.
            unsafe { libc::kill(pid as i32, 0) == 0 }
        })
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
async fn resolve_hypervisor_ipv6(
    _ctx: &ReconcileContext,
    hypervisor_id: Option<&str>,
) -> Option<std::net::Ipv6Addr> {
    let hid = hypervisor_id?;

    // Load peer list from local fabric state
    let db = nauka_state::EmbeddedDb::open_default().await.ok()?;
    let state_opt = nauka_hypervisor::fabric::state::FabricState::load(&db)
        .await
        .ok()
        .flatten();
    let _ = db.shutdown().await;
    let state = state_opt?;

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

#[cfg(test)]
mod tests {
    use super::*;

    // ── vxlan_name_from_bridge ──────────────────────────────────────

    #[test]
    fn vxlan_name_from_bridge_swaps_prefix() {
        assert_eq!(vxlan_name_from_bridge("nkb-a1b2c3"), "nkx-a1b2c3");
    }

    #[test]
    fn vxlan_name_from_bridge_no_match_unchanged() {
        // If the bridge name doesn't contain "nkb-", it's returned as-is
        assert_eq!(vxlan_name_from_bridge("br0"), "br0");
    }

    // ── find_orphaned_bridges ───────────────────────────────────────

    #[test]
    fn orphaned_bridges_detected() {
        let actual = vec![
            "nkb-aaa111".to_string(),
            "nkb-bbb222".to_string(),
            "nkb-ccc333".to_string(),
        ];
        let needed = vec!["nkb-aaa111".to_string(), "nkb-ccc333".to_string()];

        let orphaned = find_orphaned_bridges(&actual, &needed);
        assert_eq!(orphaned.len(), 1);
        assert_eq!(orphaned[0], "nkb-bbb222");
    }

    #[test]
    fn no_orphaned_bridges_when_all_needed() {
        let actual = vec!["nkb-aaa111".to_string(), "nkb-bbb222".to_string()];
        let needed = vec!["nkb-aaa111".to_string(), "nkb-bbb222".to_string()];

        let orphaned = find_orphaned_bridges(&actual, &needed);
        assert!(orphaned.is_empty());
    }

    #[test]
    fn all_bridges_orphaned_when_no_vpcs_needed() {
        let actual = vec!["nkb-aaa111".to_string(), "nkb-bbb222".to_string()];
        let needed: Vec<String> = vec![];

        let orphaned = find_orphaned_bridges(&actual, &needed);
        assert_eq!(orphaned.len(), 2);
    }

    #[test]
    fn no_orphaned_bridges_when_none_exist() {
        let actual: Vec<String> = vec![];
        let needed = vec!["nkb-aaa111".to_string()];

        let orphaned = find_orphaned_bridges(&actual, &needed);
        assert!(orphaned.is_empty());
    }

    // ── build_forward_rules ─────────────────────────────────────────

    #[test]
    fn forward_rules_bidirectional() {
        let pairs = vec![("nkb-aaa".to_string(), "nkb-bbb".to_string())];
        let rules = build_forward_rules(&pairs);

        assert_eq!(rules.len(), 2);
        assert_eq!(rules[0], "iifname nkb-aaa oifname nkb-bbb accept");
        assert_eq!(rules[1], "iifname nkb-bbb oifname nkb-aaa accept");
    }

    #[test]
    fn forward_rules_multiple_pairs() {
        let pairs = vec![
            ("nkb-aaa".to_string(), "nkb-bbb".to_string()),
            ("nkb-ccc".to_string(), "nkb-ddd".to_string()),
        ];
        let rules = build_forward_rules(&pairs);
        assert_eq!(rules.len(), 4);
    }

    #[test]
    fn forward_rules_empty_pairs() {
        let rules = build_forward_rules(&[]);
        assert!(rules.is_empty());
    }

    // ── ip_rule_exists ──────────────────────────────────────────────

    #[test]
    fn ip_rule_found_in_output() {
        let output = "32765:\tfrom all iif nkb-a1b2c3 lookup 100\n\
                       32766:\tfrom all lookup main\n";
        assert!(ip_rule_exists(output, "iif", "nkb-a1b2c3", "100"));
    }

    #[test]
    fn ip_rule_not_found_wrong_table() {
        let output = "32765:\tfrom all iif nkb-a1b2c3 lookup 200\n";
        assert!(!ip_rule_exists(output, "iif", "nkb-a1b2c3", "100"));
    }

    #[test]
    fn ip_rule_not_found_wrong_bridge() {
        let output = "32765:\tfrom all iif nkb-ffffff lookup 100\n";
        assert!(!ip_rule_exists(output, "iif", "nkb-a1b2c3", "100"));
    }

    #[test]
    fn ip_rule_not_found_empty_output() {
        assert!(!ip_rule_exists("", "iif", "nkb-a1b2c3", "100"));
    }

    // ── parse_container_pid ─────────────────────────────────────────

    #[test]
    fn parse_pid_valid() {
        assert_eq!(parse_container_pid("12345\n"), Some(12345));
    }

    #[test]
    fn parse_pid_with_whitespace() {
        assert_eq!(parse_container_pid("  42  \n"), Some(42));
    }

    #[test]
    fn parse_pid_invalid_string() {
        assert_eq!(parse_container_pid("not-a-pid"), None);
    }

    #[test]
    fn parse_pid_empty() {
        assert_eq!(parse_container_pid(""), None);
    }

    // ── gateway_cidr ────────────────────────────────────────────────

    #[test]
    fn gateway_cidr_format() {
        assert_eq!(gateway_cidr("10.0.1.1"), "10.0.1.1/24");
    }

    // ── peering_pair_exists ─────────────────────────────────────────

    #[test]
    fn peering_pair_found_same_order() {
        let pairs = vec![("nkb-aaa".to_string(), "nkb-bbb".to_string())];
        assert!(peering_pair_exists(&pairs, "nkb-aaa", "nkb-bbb"));
    }

    #[test]
    fn peering_pair_found_reverse_order() {
        let pairs = vec![("nkb-aaa".to_string(), "nkb-bbb".to_string())];
        assert!(peering_pair_exists(&pairs, "nkb-bbb", "nkb-aaa"));
    }

    #[test]
    fn peering_pair_not_found() {
        let pairs = vec![("nkb-aaa".to_string(), "nkb-bbb".to_string())];
        assert!(!peering_pair_exists(&pairs, "nkb-aaa", "nkb-ccc"));
    }

    #[test]
    fn peering_pair_empty_list() {
        let pairs: Vec<(String, String)> = vec![];
        assert!(!peering_pair_exists(&pairs, "nkb-aaa", "nkb-bbb"));
    }
}
