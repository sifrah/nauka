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

/// Ensure the VXLAN + bridge for a VPC exist and are up.
///
/// Idempotent: skips creation if interfaces already exist.
pub fn ensure_bridge(vpc_id: &str, vni: u32, local_ipv6: &Ipv6Addr) -> anyhow::Result<()> {
    let br = bridge_name(vpc_id);
    let vx = vxlan_name(vpc_id);

    // 1. Create VXLAN interface (if needed)
    if !iface_exists(&vx) {
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

    // 2. Create bridge (if needed)
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

    // 3. Attach VXLAN to bridge
    run_ip(&["link", "set", &vx, "master", &br])?;

    // 4. Bring both up
    run_ip(&["link", "set", &vx, "up"])?;
    run_ip(&["link", "set", &br, "up"])?;

    tracing::info!(
        vpc_id,
        bridge = br.as_str(),
        vxlan = vx.as_str(),
        vni,
        "VPC bridge ready"
    );
    Ok(())
}

/// Remove the VXLAN + bridge for a VPC.
///
/// Idempotent: skips if interfaces don't exist.
pub fn remove_bridge(vpc_id: &str) -> anyhow::Result<()> {
    let br = bridge_name(vpc_id);
    let vx = vxlan_name(vpc_id);

    if iface_exists(&vx) {
        tracing::info!(vpc_id, iface = vx.as_str(), "removing VXLAN interface");
        run_ip(&["link", "del", &vx])?;
    }

    if iface_exists(&br) {
        tracing::info!(vpc_id, iface = br.as_str(), "removing bridge");
        run_ip(&["link", "del", &br])?;
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
            // Format: "N: nkb-abc123: <...>"
            let name = line.split(':').nth(1)?.trim();
            if name.starts_with("nkb-") {
                Some(name.to_string())
            } else {
                None
            }
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
    fn vxlan_name_within_limit() {
        let name = vxlan_name("vpc-01knqczg3xabdsv9wmvgzdsswe");
        assert!(name.len() <= 15, "name too long: {name} ({})", name.len());
        assert!(name.starts_with("nkx-"));
    }
}
