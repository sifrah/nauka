//! Auto-detection of public IPv6 block and IPv4 address from the host network.

use std::process::Command;

/// Detect the public IPv6 /64 block from `ip -6 addr show scope global`.
///
/// Filters out ULA (fd00::/8), link-local (fe80::), and nauka0 interface addresses.
/// Returns the first matching /64 CIDR, e.g. `"2a01:4f8:c012:abcd::/64"`.
pub fn detect_ipv6_block() -> Option<String> {
    let output = Command::new("ip")
        .args(["-6", "addr", "show", "scope", "global"])
        .output()
        .ok()?;

    let stdout = String::from_utf8_lossy(&output.stdout);

    let mut current_iface = String::new();

    for line in stdout.lines() {
        // Interface lines: "2: eth0: <BROADCAST..."
        if !line.starts_with(' ') {
            current_iface = line
                .split(':')
                .nth(1)
                .unwrap_or("")
                .trim()
                .split('@')
                .next()
                .unwrap_or("")
                .to_string();
            continue;
        }

        // Skip nauka0 (mesh interface)
        if current_iface == "nauka0" {
            continue;
        }

        let trimmed = line.trim();
        if !trimmed.starts_with("inet6 ") {
            continue;
        }

        // Parse: "inet6 2a01:4f8:c012:abcd::1/64 scope global"
        let addr_cidr = trimmed.split_whitespace().nth(1)?;
        let parts: Vec<&str> = addr_cidr.split('/').collect();
        if parts.len() != 2 {
            continue;
        }

        let addr_str = parts[0];
        let prefix_len: u32 = parts[1].parse().ok()?;

        // We only want /64 blocks
        if prefix_len != 64 {
            continue;
        }

        let addr: std::net::Ipv6Addr = addr_str.parse().ok()?;
        let octets = addr.octets();

        // Skip ULA (fd00::/8)
        if octets[0] == 0xfd {
            continue;
        }

        // Skip link-local (fe80::/10) — shouldn't appear with scope global, but be safe
        if octets[0] == 0xfe && (octets[1] & 0xc0) == 0x80 {
            continue;
        }

        // Mask to /64: zero out the last 8 bytes
        let mut network = octets;
        for byte in &mut network[8..] {
            *byte = 0;
        }
        let network_addr = std::net::Ipv6Addr::from(network);

        return Some(format!("{network_addr}/64"));
    }

    None
}

/// Detect the public IPv4 address from `ip -4 addr show scope global`.
///
/// Filters out private ranges (10.x, 172.16-31.x, 192.168.x) and nauka0 interface.
/// Falls back to `curl -4 -sf --max-time 3 https://ifconfig.me` if no global address found.
pub fn detect_ipv4_public() -> Option<String> {
    if let Some(ip) = detect_ipv4_from_interface() {
        return Some(ip);
    }
    detect_ipv4_from_external()
}

fn detect_ipv4_from_interface() -> Option<String> {
    let output = Command::new("ip")
        .args(["-4", "addr", "show", "scope", "global"])
        .output()
        .ok()?;

    let stdout = String::from_utf8_lossy(&output.stdout);

    let mut current_iface = String::new();

    for line in stdout.lines() {
        if !line.starts_with(' ') {
            current_iface = line
                .split(':')
                .nth(1)
                .unwrap_or("")
                .trim()
                .split('@')
                .next()
                .unwrap_or("")
                .to_string();
            continue;
        }

        if current_iface == "nauka0" {
            continue;
        }

        let trimmed = line.trim();
        if !trimmed.starts_with("inet ") {
            continue;
        }

        // Parse: "inet 49.12.233.86/32 scope global"
        let addr_cidr = trimmed.split_whitespace().nth(1)?;
        let addr_str = addr_cidr.split('/').next()?;

        let addr: std::net::Ipv4Addr = addr_str.parse().ok()?;

        // Skip private ranges
        if addr.is_private() || addr.is_loopback() || addr.is_link_local() {
            continue;
        }

        return Some(addr.to_string());
    }

    None
}

fn detect_ipv4_from_external() -> Option<String> {
    let output = Command::new("curl")
        .args(["-4", "-sf", "--max-time", "3", "https://ifconfig.me"])
        .output()
        .ok()?;

    if !output.status.success() {
        return None;
    }

    let ip_str = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let _: std::net::Ipv4Addr = ip_str.parse().ok()?;
    Some(ip_str)
}

#[cfg(test)]
mod tests {
    #[test]
    fn parse_ipv6_block_from_sample_output() {
        // Simulate what detect_ipv6_block would parse
        let addr: std::net::Ipv6Addr = "2a01:4f8:c012:abcd::1".parse().unwrap();
        let octets = addr.octets();
        let mut network = octets;
        for byte in &mut network[8..] {
            *byte = 0;
        }
        let network_addr = std::net::Ipv6Addr::from(network);
        assert_eq!(format!("{network_addr}/64"), "2a01:4f8:c012:abcd::/64");
    }

    #[test]
    fn ula_is_filtered() {
        let addr: std::net::Ipv6Addr = "fd12:3456:789a::1".parse().unwrap();
        assert_eq!(addr.octets()[0], 0xfd);
    }

    #[test]
    fn private_ipv4_is_filtered() {
        let addr: std::net::Ipv4Addr = "10.0.0.1".parse().unwrap();
        assert!(addr.is_private());
        let addr: std::net::Ipv4Addr = "172.16.0.1".parse().unwrap();
        assert!(addr.is_private());
        let addr: std::net::Ipv4Addr = "192.168.1.1".parse().unwrap();
        assert!(addr.is_private());
    }

    #[test]
    fn public_ipv4_is_not_filtered() {
        let addr: std::net::Ipv4Addr = "49.12.233.86".parse().unwrap();
        assert!(!addr.is_private());
        assert!(!addr.is_loopback());
        assert!(!addr.is_link_local());
    }
}
