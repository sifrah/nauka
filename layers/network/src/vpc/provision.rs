//! VPC provisioning — create and remove VXLAN + bridge interfaces.
//!
//! Each VPC gets two Linux interfaces:
//! - `nkx-{hash}` — VXLAN interface (encapsulates L2 frames over the mesh)
//! - `nkb-{hash}` — Linux bridge (connects local VM TAPs + VXLAN)
//!
//! The hash is derived from the VPC ID to stay within the 15-char IFNAMSIZ limit.

use std::net::Ipv6Addr;
use std::process::Command;

/// Derive a short hash (6 hex chars) from a VPC ID for interface naming.
fn iface_hash(vpc_id: &str) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    vpc_id.hash(&mut hasher);
    format!("{:06x}", hasher.finish() & 0xFFFFFF)
}

/// Bridge interface name for a VPC.
pub fn bridge_name(vpc_id: &str) -> String {
    format!("nkb-{}", iface_hash(vpc_id))
}

/// VXLAN interface name for a VPC.
pub fn vxlan_name(vpc_id: &str) -> String {
    format!("nkx-{}", iface_hash(vpc_id))
}

/// VRF name for a VPC.
pub fn vrf_name(vpc_id: &str) -> String {
    format!("nkv-{}", iface_hash(vpc_id))
}

/// Check if a network interface exists.
fn iface_exists(name: &str) -> bool {
    Command::new("ip")
        .args(["link", "show", name])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Remove all stale nauka network interfaces (nkb-*, nkx-*, nkt-*).
///
/// Called during cleanup or when stale interfaces from a previous cluster exist.
pub fn cleanup_all() {
    let output = match Command::new("ip").args(["-o", "link", "show"]).output() {
        Ok(o) if o.status.success() => o,
        _ => return,
    };

    let stdout = String::from_utf8_lossy(&output.stdout);
    for line in stdout.lines() {
        if let Some(raw) = line.split(':').nth(1).map(|s| s.trim()) {
            let name = raw.split('@').next().unwrap_or(raw);
            if name.starts_with("nkb-") || name.starts_with("nkx-") || name.starts_with("nkt-") {
                tracing::info!(iface = name, "cleaning up stale interface");
                let _ = Command::new("ip")
                    .args(["link", "del", name])
                    .stdout(std::process::Stdio::null())
                    .stderr(std::process::Stdio::null())
                    .status();
            }
        }
    }
}

/// Ensure the VXLAN + bridge for a VPC exist and are up.
///
/// Idempotent: skips creation if interfaces already exist.
pub fn ensure_bridge(vpc_id: &str, vni: u32, local_ipv6: &Ipv6Addr) -> anyhow::Result<()> {
    let vrf = vrf_name(vpc_id);
    let br = bridge_name(vpc_id);
    let vx = vxlan_name(vpc_id);

    // 1. Create VRF or set up policy-based routing for L3 isolation.
    //    VRF is preferred (cleaner), but requires the `vrf` kernel module.
    //    Policy routing (ip rule + routing tables) works on ALL kernels.
    let vrf_supported = if !iface_exists(&vrf) {
        let status = Command::new("ip")
            .args([
                "link",
                "add",
                &vrf,
                "type",
                "vrf",
                "table",
                &vni.to_string(),
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false);

        if status {
            run_ip(&["link", "set", &vrf, "up"])?;
            tracing::info!(vpc_id, vrf = vrf.as_str(), "VRF created");
            true
        } else {
            tracing::info!(vpc_id, table = vni, "using policy routing (no VRF module)");
            false
        }
    } else {
        true
    };

    // 2. Create VXLAN interface (if needed)
    if !iface_exists(&vx) {
        cleanup_stale_vxlan(vni);

        tracing::info!(vpc_id, vni, iface = vx.as_str(), "creating VXLAN interface");
        let status = Command::new("ip")
            .args([
                "link",
                "add",
                &vx,
                "type",
                "vxlan",
                "id",
                &vni.to_string(),
                "dstport",
                "4789",
                "local",
                &local_ipv6.to_string(),
                "nolearning",
            ])
            .status()
            .map_err(|e| anyhow::anyhow!("ip link add vxlan failed: {e}"))?;
        if !status.success() {
            anyhow::bail!("failed to create VXLAN interface {vx}");
        }
    }

    // 3. Create bridge (if needed)
    if !iface_exists(&br) {
        tracing::info!(vpc_id, iface = br.as_str(), "creating bridge");
        let status = Command::new("ip")
            .args(["link", "add", &br, "type", "bridge"])
            .status()
            .map_err(|e| anyhow::anyhow!("ip link add bridge failed: {e}"))?;
        if !status.success() {
            anyhow::bail!("failed to create bridge {br}");
        }
    }

    // 4. Attach bridge to VRF or set up policy routing
    if vrf_supported {
        run_ip(&["link", "set", &br, "master", &vrf])?;
    } else {
        // Policy-based routing: use ip rule to isolate this bridge's traffic
        // into its own routing table (same effect as VRF, works on all kernels)
        let table = vni.to_string();
        run_ip(&["rule", "add", "iif", &br, "table", &table])?;
        run_ip(&["rule", "add", "oif", &br, "table", &table])?;
    }

    // 5. Attach VXLAN to bridge
    run_ip(&["link", "set", &vx, "master", &br])?;

    // 6. Bring everything up
    run_ip(&["link", "set", &vx, "up"])?;
    run_ip(&["link", "set", &br, "up"])?;

    // 7. Host isolation: block host process from originating traffic to VPC bridges.
    //    Defense in depth — even if a route exists in main table, nftables blocks it.
    //    This ensures the hypervisor operator cannot access tenant VM networks.
    ensure_host_isolation(&br);

    tracing::info!(
        vpc_id,
        vrf = vrf.as_str(),
        bridge = br.as_str(),
        vxlan = vx.as_str(),
        vni,
        "VPC network ready (isolated)"
    );
    Ok(())
}

/// Block host-originated traffic to a VPC bridge via nftables.
///
/// Allows forwarded traffic (VM-to-VM via bridge) but blocks traffic
/// originating from the host itself. This prevents the hypervisor
/// from accessing tenant networks.
fn ensure_host_isolation(bridge: &str) {
    ensure_host_isolation_all(&[bridge.to_string()]);
}

/// Rebuild the nftables output chain with host isolation rules for all bridges.
///
/// Idempotent: flushes and recreates the chain with the correct rules.
/// Called by the forge reconciler on every cycle to ensure isolation is always active.
pub fn ensure_host_isolation_all(bridges: &[String]) {
    // Create the nauka table if it doesn't exist
    let _ = Command::new("nft")
        .args(["add", "table", "inet", "nauka"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status();

    // Delete and recreate output chain to avoid duplicate rules
    let _ = Command::new("nft")
        .args(["delete", "chain", "inet", "nauka", "output"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status();

    let _ = Command::new("nft")
        .args([
            "add",
            "chain",
            "inet",
            "nauka",
            "output",
            "{ type filter hook output priority 0; }",
        ])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status();

    // Allow established/related (for DNS responses, etc.)
    let _ = Command::new("nft")
        .args([
            "add",
            "rule",
            "inet",
            "nauka",
            "output",
            "oifname \"nkb-*\" ct state established,related accept",
        ])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status();

    // Allow DNS responses from host (bridge acts as DNS resolver for VMs)
    for proto in &["udp", "tcp"] {
        let rule = format!("oifname \"nkb-*\" {proto} sport 53 accept");
        let _ = Command::new("nft")
            .args(["add", "rule", "inet", "nauka", "output", &rule])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();
    }

    // Allow ICMPv6 neighbor discovery (needed for IPv6 peering)
    let _ = Command::new("nft")
        .args([
            "add", "rule", "inet", "nauka", "output",
            "oifname \"nkb-*\" icmpv6 type { nd-router-advert, nd-neighbor-solicit, nd-neighbor-advert } accept",
        ])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status();

    // Drop all other host → bridge traffic per bridge
    for bridge in bridges {
        let rule = format!("oifname {bridge} drop");
        let _ = Command::new("nft")
            .args(["add", "rule", "inet", "nauka", "output", &rule])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();
    }
}

/// Ensure VPC peering route leak between two VRFs.
///
/// Adds routes in both directions so traffic can flow between the peered VPCs.
/// Idempotent: `ip route replace` overwrites existing routes.
pub fn ensure_peering_routes(
    vpc_id: &str,
    vpc_cidr: &str,
    vpc_vni: u32,
    peer_vpc_id: &str,
    peer_vpc_cidr: &str,
    peer_vpc_vni: u32,
) -> anyhow::Result<()> {
    let vrf_a = vrf_name(vpc_id);
    let vrf_b = vrf_name(peer_vpc_id);
    let br_b = bridge_name(peer_vpc_id);
    let br_a = bridge_name(vpc_id);
    let vni_a = vpc_vni.to_string();
    let vni_b = peer_vpc_vni.to_string();

    // In VRF-A: route to peer VPC CIDR via peer bridge
    tracing::info!(
        vpc_id,
        peer_vpc_id,
        route = format!("{peer_vpc_cidr} via {br_b}").as_str(),
        "adding peering route"
    );
    // Add routes in the correct routing context (VRF or policy routing table)
    if iface_exists(&vrf_a) {
        run_ip(&[
            "route",
            "replace",
            peer_vpc_cidr,
            "dev",
            &br_b,
            "vrf",
            &vrf_a,
        ])?;
    } else {
        // Policy routing: add to the VPC's routing table (VNI as table ID)
        // The ip rules we set up in ensure_bridge direct traffic to this table
        run_ip(&[
            "route",
            "replace",
            peer_vpc_cidr,
            "dev",
            &br_b,
            "table",
            &vni_a,
        ])?;
    }

    if iface_exists(&vrf_b) {
        run_ip(&["route", "replace", vpc_cidr, "dev", &br_a, "vrf", &vrf_b])?;
    } else {
        run_ip(&["route", "replace", vpc_cidr, "dev", &br_a, "table", &vni_b])?;
    }

    Ok(())
}

/// Remove VPC peering routes.
pub fn remove_peering_routes(
    vpc_id: &str,
    vpc_cidr: &str,
    peer_vpc_id: &str,
    peer_vpc_cidr: &str,
) -> anyhow::Result<()> {
    let vrf_a = vrf_name(vpc_id);
    let vrf_b = vrf_name(peer_vpc_id);

    let _ = run_ip(&["route", "del", peer_vpc_cidr, "vrf", &vrf_a]);
    let _ = run_ip(&["route", "del", vpc_cidr, "vrf", &vrf_b]);

    Ok(())
}

/// Remove the VXLAN + bridge for a VPC.
///
/// Idempotent: skips if interfaces don't exist.
pub fn remove_bridge(vpc_id: &str) -> anyhow::Result<()> {
    let vrf = vrf_name(vpc_id);
    let br = bridge_name(vpc_id);
    let vx = vxlan_name(vpc_id);

    if iface_exists(&vx) {
        tracing::info!(vpc_id, iface = vx.as_str(), "removing VXLAN interface");
        run_ip(&["link", "del", &vx])?;
    }

    if iface_exists(&br) {
        tracing::info!(vpc_id, iface = br.as_str(), "removing bridge");
        // Clean up policy routing rules (safe to run even if they don't exist)
        let _ = run_ip(&["rule", "del", "iif", &br]);
        let _ = run_ip(&["rule", "del", "oif", &br]);
        run_ip(&["link", "del", &br])?;
    }

    if iface_exists(&vrf) {
        tracing::info!(vpc_id, iface = vrf.as_str(), "removing VRF");
        run_ip(&["link", "del", &vrf])?;
    }

    Ok(())
}

/// List VPC IDs that have active bridges on this node.
///
/// Scans `ip link show` for interfaces matching the `nkb-` prefix,
/// then reverse-maps to VPC IDs using a provided mapping.
pub fn list_active_bridges() -> Vec<String> {
    let output = match Command::new("ip").args(["-o", "link", "show"]).output() {
        Ok(o) if o.status.success() => o,
        _ => return vec![],
    };

    let stdout = String::from_utf8_lossy(&output.stdout);
    stdout
        .lines()
        .filter_map(|line| {
            // Format: "N: nkb-abc123: <...>" or "N: nkb-abc123@if1: <...>"
            let raw = line.split(':').nth(1)?.trim();
            let name = raw.split('@').next().unwrap_or(raw);
            if name.starts_with("nkb-") {
                Some(name.to_string())
            } else {
                None
            }
        })
        .collect()
}

/// Remove any VXLAN interface using a specific VNI (stale from previous cluster).
fn cleanup_stale_vxlan(vni: u32) {
    let output = match Command::new("ip")
        .args(["-d", "-o", "link", "show"])
        .output()
    {
        Ok(o) if o.status.success() => o,
        _ => return,
    };

    let vni_str = format!("vxlan id {vni} ");
    let stdout = String::from_utf8_lossy(&output.stdout);
    for line in stdout.lines() {
        if line.contains(&vni_str) {
            if let Some(name) = line
                .split(':')
                .nth(1)
                .map(|s| s.trim().split('@').next().unwrap_or("").trim())
            {
                if !name.is_empty() {
                    tracing::info!(
                        iface = name,
                        vni,
                        "removing stale VXLAN with conflicting VNI"
                    );
                    let _ = Command::new("ip").args(["link", "del", name]).status();
                }
            }
        }
    }
}

// ═══════════════════════════════════════════════════
// FDB — Forwarding Database for VXLAN cross-node traffic
// ═══════════════════════════════════════════════════

/// Derive a deterministic MAC address from an IPv4 address.
///
/// Format: `02:00:{a}.{b}.{c}.{d}` where a.b.c.d are the IP octets in hex.
/// The `02` prefix marks it as a locally administered unicast MAC.
pub fn mac_from_ip(ip: &str) -> Option<String> {
    let addr: std::net::Ipv4Addr = ip.parse().ok()?;
    let octets = addr.octets();
    Some(format!(
        "02:00:{:02x}:{:02x}:{:02x}:{:02x}",
        octets[0], octets[1], octets[2], octets[3]
    ))
}

/// Add an FDB entry for a remote VM.
///
/// Tells the VXLAN interface: "frames destined for this MAC should be
/// sent via VXLAN to this remote mesh IPv6 address."
pub fn add_fdb_entry(vpc_id: &str, mac: &str, remote_ipv6: &Ipv6Addr) -> anyhow::Result<()> {
    let vx = vxlan_name(vpc_id);

    tracing::info!(
        vpc_id,
        mac,
        remote = %remote_ipv6,
        vxlan = vx.as_str(),
        "adding FDB entry"
    );

    // bridge fdb add <mac> dev <vxlan> dst <remote_ipv6>
    let _ = Command::new("bridge")
        .args([
            "fdb",
            "replace",
            mac,
            "dev",
            &vx,
            "dst",
            &remote_ipv6.to_string(),
            "self",
            "permanent",
        ])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status();

    Ok(())
}

/// Add an ARP proxy entry for a remote VM.
///
/// The bridge answers ARP requests on behalf of the remote VM,
/// so local VMs don't need to broadcast ARP over the VXLAN.
pub fn add_arp_proxy(vpc_id: &str, ip: &str, mac: &str) -> anyhow::Result<()> {
    let br = bridge_name(vpc_id);

    let _ = Command::new("ip")
        .args([
            "neigh",
            "replace",
            ip,
            "lladdr",
            mac,
            "dev",
            &br,
            "nud",
            "permanent",
        ])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status();

    Ok(())
}

/// Remove FDB + ARP entries for a remote VM.
pub fn remove_fdb_entry(vpc_id: &str, mac: &str, ip: &str) -> anyhow::Result<()> {
    let vx = vxlan_name(vpc_id);
    let br = bridge_name(vpc_id);

    let _ = Command::new("bridge")
        .args(["fdb", "del", mac, "dev", &vx])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status();

    let _ = Command::new("ip")
        .args(["neigh", "del", ip, "dev", &br])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status();

    Ok(())
}

/// List current FDB entries for a VXLAN interface.
pub fn list_fdb_entries(vpc_id: &str) -> Vec<(String, String)> {
    let vx = vxlan_name(vpc_id);

    let output = match Command::new("bridge")
        .args(["fdb", "show", "dev", &vx])
        .output()
    {
        Ok(o) if o.status.success() => o,
        _ => return vec![],
    };

    let stdout = String::from_utf8_lossy(&output.stdout);
    stdout
        .lines()
        .filter_map(|line| {
            // Format: "02:00:0a:00:01:03 dst fd47:... self permanent"
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 4 && parts[1] == "dst" {
                let mac = parts[0].to_string();
                let dst = parts[2].to_string();
                // Skip 00:00:00:00:00:00 (default entry)
                if !mac.starts_with("00:00:00:00:00:00") {
                    return Some((mac, dst));
                }
            }
            None
        })
        .collect()
}

fn run_ip(args: &[&str]) -> anyhow::Result<()> {
    let status = Command::new("ip")
        .args(args)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map_err(|e| anyhow::anyhow!("ip command failed: {e}"))?;
    // Don't fail on non-zero — some operations (e.g., set master on already-attached) are benign
    if !status.success() {
        tracing::debug!(
            args = args.join(" ").as_str(),
            "ip command returned non-zero"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn iface_hash_deterministic() {
        let a = iface_hash("vpc-01knq123");
        let b = iface_hash("vpc-01knq123");
        assert_eq!(a, b);
        assert_eq!(a.len(), 6);
    }

    #[test]
    fn iface_hash_different_vpcs() {
        let a = iface_hash("vpc-aaa");
        let b = iface_hash("vpc-bbb");
        assert_ne!(a, b);
    }

    #[test]
    fn bridge_name_within_limit() {
        let name = bridge_name("vpc-01knqczg3xabdsv9wmvgzdsswe");
        assert!(name.len() <= 15, "name too long: {name} ({})", name.len());
        assert!(name.starts_with("nkb-"));
    }

    #[test]
    fn mac_from_ip_format() {
        assert_eq!(
            mac_from_ip("10.0.1.2"),
            Some("02:00:0a:00:01:02".to_string())
        );
        assert_eq!(
            mac_from_ip("192.168.1.100"),
            Some("02:00:c0:a8:01:64".to_string())
        );
    }

    #[test]
    fn mac_from_ip_deterministic() {
        assert_eq!(mac_from_ip("10.0.1.5"), mac_from_ip("10.0.1.5"));
    }

    #[test]
    fn mac_from_ip_invalid() {
        assert!(mac_from_ip("not-an-ip").is_none());
    }

    #[test]
    fn vxlan_name_within_limit() {
        let name = vxlan_name("vpc-01knqczg3xabdsv9wmvgzdsswe");
        assert!(name.len() <= 15, "name too long: {name} ({})", name.len());
        assert!(name.starts_with("nkx-"));
    }

    #[test]
    fn vrf_name_within_limit() {
        let name = vrf_name("vpc-01knqczg3xabdsv9wmvgzdsswe");
        assert!(name.len() <= 15, "name too long: {name} ({})", name.len());
        assert!(name.starts_with("nkv-"));
    }
}
