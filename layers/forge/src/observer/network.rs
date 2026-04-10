//! Network observer — scan for active VPC bridges.
//!
//! Reads `ip link show` and finds interfaces matching the `nkb-` prefix.
//! Returns bridge interface names (e.g., "nkb-a1b2c3").

use std::process::Command;

/// List active VPC bridge interface names on this node.
pub fn list_bridges() -> Vec<String> {
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

/// Check if any network interface exists by name.
pub fn iface_exists(name: &str) -> bool {
    bridge_exists(name)
}

/// Check if a specific bridge interface exists.
pub fn bridge_exists(name: &str) -> bool {
    Command::new("ip")
        .args(["link", "show", name])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}
