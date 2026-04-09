//! VM provisioning — TAP interface creation.
//!
//! Each VM gets a TAP interface attached to its VPC bridge:
//! - `nkt-{hash}` — TAP interface (will be passed to Cloud Hypervisor)
//!
//! The hash is derived from the VM ID.

use std::process::Command;

/// Derive a short hash (6 hex chars) from an ID for interface naming.
fn iface_hash(id: &str) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    id.hash(&mut hasher);
    format!("{:06x}", hasher.finish() & 0xFFFFFF)
}

/// TAP interface name for a VM.
pub fn tap_name(vm_id: &str) -> String {
    format!("nkt-{}", iface_hash(vm_id))
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

/// Ensure a TAP interface exists for a VM, attached to its VPC bridge.
///
/// Idempotent: skips creation if the TAP already exists.
pub fn ensure_tap(vm_id: &str, vpc_bridge: &str) -> anyhow::Result<String> {
    let tap = tap_name(vm_id);

    if !iface_exists(&tap) {
        tracing::info!(
            vm_id,
            tap = tap.as_str(),
            bridge = vpc_bridge,
            "creating TAP interface"
        );

        // Create TAP
        let status = Command::new("ip")
            .args(["tuntap", "add", &tap, "mode", "tap"])
            .status()
            .map_err(|e| anyhow::anyhow!("ip tuntap add failed: {e}"))?;
        if !status.success() {
            anyhow::bail!("failed to create TAP interface {tap}");
        }
    }

    // Attach to bridge (idempotent)
    let _ = Command::new("ip")
        .args(["link", "set", &tap, "master", vpc_bridge])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status();

    // Bring up
    let _ = Command::new("ip")
        .args(["link", "set", &tap, "up"])
        .status();

    tracing::info!(vm_id, tap = tap.as_str(), "TAP ready");
    Ok(tap)
}

/// Remove a TAP interface for a VM.
///
/// Idempotent: skips if the TAP doesn't exist.
pub fn remove_tap(vm_id: &str) -> anyhow::Result<()> {
    let tap = tap_name(vm_id);

    if iface_exists(&tap) {
        tracing::info!(vm_id, tap = tap.as_str(), "removing TAP interface");
        let _ = Command::new("ip").args(["link", "del", &tap]).status();
    }

    Ok(())
}

// ═══════════════════════════════════════════════════
// Veth pair — for container networking
// ═══════════════════════════════════════════════════

/// Host-side veth name for a container.
pub fn veth_host_name(vm_id: &str) -> String {
    format!("nkh-{}", iface_hash(vm_id))
}

/// Guest-side veth name (before being renamed to eth0 in the container).
pub fn veth_guest_name(vm_id: &str) -> String {
    format!("nkg-{}", iface_hash(vm_id))
}

/// Create a veth pair and attach the host side to the VPC bridge.
///
/// The guest side stays in the host netns until `move_veth_to_container`
/// moves it into the container's network namespace.
pub fn ensure_veth(vm_id: &str, vpc_bridge: &str) -> anyhow::Result<()> {
    let host = veth_host_name(vm_id);
    let guest = veth_guest_name(vm_id);

    if !iface_exists(&host) {
        tracing::info!(
            vm_id,
            host = host.as_str(),
            guest = guest.as_str(),
            "creating veth pair"
        );

        let status = Command::new("ip")
            .args(["link", "add", &host, "type", "veth", "peer", "name", &guest])
            .status()
            .map_err(|e| anyhow::anyhow!("ip link add veth failed: {e}"))?;
        if !status.success() {
            anyhow::bail!("failed to create veth pair {host}/{guest}");
        }
    }

    // Attach host side to bridge
    let _ = Command::new("ip")
        .args(["link", "set", &host, "master", vpc_bridge])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status();

    // Bring host side up
    let _ = Command::new("ip")
        .args(["link", "set", &host, "up"])
        .status();

    Ok(())
}

/// Move the guest-side veth into the container's network namespace
/// and configure IP address + default route.
pub fn setup_container_net(
    vm_id: &str,
    container_pid: u32,
    ip: &str,
    gateway: &str,
    mac: &str,
    vpc_cidr: Option<&str>,
) -> anyhow::Result<()> {
    let guest = veth_guest_name(vm_id);
    let pid = container_pid.to_string();

    tracing::info!(
        vm_id,
        pid = pid.as_str(),
        ip,
        gateway,
        "setting up container networking"
    );

    // 1. Move guest veth into container netns
    let status = Command::new("ip")
        .args(["link", "set", &guest, "netns", &pid])
        .status()
        .map_err(|e| anyhow::anyhow!("move veth to netns failed: {e}"))?;
    if !status.success() {
        anyhow::bail!("failed to move {guest} to netns of PID {pid}");
    }

    // 2. Inside the container netns: rename to eth0, set MAC, configure IP, bring up
    let nsenter = |args: &[&str]| -> anyhow::Result<()> {
        let status = Command::new("nsenter")
            .args(["--net", &format!("--target={pid}")])
            .args(args)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map_err(|e| anyhow::anyhow!("nsenter failed: {e}"))?;
        if !status.success() {
            tracing::debug!(
                args = args.join(" ").as_str(),
                "nsenter command returned non-zero"
            );
        }
        Ok(())
    };

    // Rename to eth0
    nsenter(&["ip", "link", "set", &guest, "name", "eth0"])?;

    // Set MAC address (deterministic from IP)
    nsenter(&["ip", "link", "set", "eth0", "address", mac])?;

    // Add IP address
    let cidr = format!("{ip}/24");
    nsenter(&["ip", "addr", "add", &cidr, "dev", "eth0"])?;

    // Bring up lo and eth0
    nsenter(&["ip", "link", "set", "lo", "up"])?;
    nsenter(&["ip", "link", "set", "eth0", "up"])?;

    // Default route via gateway
    nsenter(&["ip", "route", "add", "default", "via", gateway])?;

    // IPv6 for DNS64/NAT64: add ULA IPv6 + route for 64:ff9b::/96
    // The bridge gateway runs a DNS64 resolver (unbound) that synthesizes
    // AAAA records using 64:ff9b::/96. VMs need IPv6 connectivity to the
    // bridge to send traffic through Jool NAT64.
    if let Some(vpc_cidr) = vpc_cidr {
        let gw_v6 = nauka_network::vpc::natgw::provision::bridge_ipv6_gateway(vpc_cidr);
        let vm_v6 = nauka_network::vpc::natgw::provision::bridge_ipv6_vm(vpc_cidr, 0);
        let vm_v6_cidr = format!("{}/64", vm_v6);
        let gw_v6_str = gw_v6.to_string();
        nsenter(&["ip", "-6", "addr", "add", &vm_v6_cidr, "dev", "eth0"])?;
        // Route NAT64 prefix through the bridge gateway
        nsenter(&[
            "ip",
            "-6",
            "route",
            "add",
            "64:ff9b::/96",
            "via",
            &gw_v6_str,
        ])?;
        // Default IPv6 route for direct IPv6 sites (NAT66 SNAT on host)
        nsenter(&["ip", "-6", "route", "add", "default", "via", &gw_v6_str])?;
    }

    tracing::info!(vm_id, ip, "container networking ready");
    Ok(())
}

/// Remove veth pair for a container.
pub fn remove_veth(vm_id: &str) -> anyhow::Result<()> {
    let host = veth_host_name(vm_id);
    if iface_exists(&host) {
        tracing::info!(vm_id, iface = host.as_str(), "removing veth pair");
        let _ = Command::new("ip").args(["link", "del", &host]).status();
    }
    Ok(())
}

/// List VM IDs that have active veth host interfaces on this node.
pub fn list_veths() -> Vec<String> {
    let output = match Command::new("ip").args(["-o", "link", "show"]).output() {
        Ok(o) if o.status.success() => o,
        _ => return vec![],
    };

    let stdout = String::from_utf8_lossy(&output.stdout);
    stdout
        .lines()
        .filter_map(|line| {
            let name = line.split(':').nth(1)?.trim();
            if name.starts_with("nkh-") {
                Some(name.to_string())
            } else {
                None
            }
        })
        .collect()
}

/// List VM IDs that have active TAP interfaces on this node.
pub fn list_taps() -> Vec<String> {
    let output = match Command::new("ip").args(["-o", "link", "show"]).output() {
        Ok(o) if o.status.success() => o,
        _ => return vec![],
    };

    let stdout = String::from_utf8_lossy(&output.stdout);
    stdout
        .lines()
        .filter_map(|line| {
            let name = line.split(':').nth(1)?.trim();
            if name.starts_with("nkt-") {
                Some(name.to_string())
            } else {
                None
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tap_name_within_limit() {
        let name = tap_name("vm-01knqhx4v5hpxp7dwgvd43qv90");
        assert!(name.len() <= 15, "name too long: {name} ({})", name.len());
        assert!(name.starts_with("nkt-"));
    }

    #[test]
    fn tap_name_deterministic() {
        let a = tap_name("vm-01abc");
        let b = tap_name("vm-01abc");
        assert_eq!(a, b);
    }

    #[test]
    fn tap_name_different_vms() {
        let a = tap_name("vm-aaa");
        let b = tap_name("vm-bbb");
        assert_ne!(a, b);
    }
}
