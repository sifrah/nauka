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
