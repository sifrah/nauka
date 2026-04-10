//! NAT Gateway provisioning.
//!
//! Sets up outbound internet access for VMs in a VPC via a dedicated public IPv6:
//!
//! - DNS64 (unbound) on the bridge gateway synthesizes AAAA records using 64:ff9b::/96
//! - Jool NAT64 translates IPv6 (64:ff9b::) → IPv4 for IPv4-only destinations
//! - NAT66 (nftables SNAT) pins the public IPv6 as source for all VM IPv6 traffic
//! - NAT44 (MASQUERADE) as fallback for direct IPv4 outbound
//!
//! Flow: VM resolves via DNS64 → gets 64:ff9b::x.x.x.x → sends IPv6 →
//!       bridge forwards → Jool translates to IPv4 → exits with NAT GW IPv6 as source
//!       (for IPv6-native sites, NAT66 SNAT applies directly)

use std::net::Ipv6Addr;
use std::process::Command;

use super::super::provision::bridge_name;

/// Jool instance name for a NAT gateway, derived from VPC ID hash.
pub fn jool_instance_name(vpc_id: &str) -> String {
    use sha2::{Digest, Sha256};
    let hash = Sha256::digest(vpc_id.as_bytes());
    format!("nk-{}", hex::encode(&hash[..3]))
}

/// Check if the Jool kernel module is available.
pub fn jool_available() -> bool {
    Command::new("modprobe")
        .args(["--dry-run", "jool"])
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Derive a ULA IPv6 gateway address for internal use on a VPC bridge.
pub fn bridge_ipv6_gateway(vpc_cidr: &str) -> Ipv6Addr {
    use sha2::{Digest, Sha256};
    let hash = Sha256::digest(vpc_cidr.as_bytes());
    let seg2 = u16::from_be_bytes([hash[0], hash[1]]);
    let seg3 = u16::from_be_bytes([hash[2], hash[3]]);
    Ipv6Addr::new(0xfd00, 0x6e61, seg2, seg3, 0, 0, 0, 1)
}

/// Derive a ULA IPv6 for a VM inside the VPC bridge.
pub fn bridge_ipv6_vm(vpc_cidr: &str, vm_index: u16) -> Ipv6Addr {
    use sha2::{Digest, Sha256};
    let hash = Sha256::digest(vpc_cidr.as_bytes());
    let seg2 = u16::from_be_bytes([hash[0], hash[1]]);
    let seg3 = u16::from_be_bytes([hash[2], hash[3]]);
    Ipv6Addr::new(0xfd00, 0x6e61, seg2, seg3, 0, 0, 0, 2 + vm_index)
}

/// Provision NAT gateway for a VPC.
pub fn ensure_nat_gateway(
    vpc_id: &str,
    vpc_cidr: &str,
    vni: u32,
    public_ipv6: &Ipv6Addr,
    public_interface: &str,
) -> anyhow::Result<()> {
    let instance = jool_instance_name(vpc_id);
    let br = bridge_name(vpc_id);
    let table = vni.to_string();

    // ── 1. Enable forwarding ──
    let _ = run_cmd("sysctl", &["-w", "net.ipv4.ip_forward=1"]);
    let _ = run_cmd("sysctl", &["-w", "net.ipv6.conf.all.forwarding=1"]);
    let _ = run_cmd("sysctl", &["-w", "net.ipv4.conf.all.rp_filter=0"]);

    // ── 2. Default route in VPC routing table via host gateway ──
    if let Some(gw) = detect_default_gateway() {
        let _ = run_cmd(
            "ip",
            &[
                "route",
                "replace",
                "default",
                "via",
                &gw,
                "dev",
                public_interface,
                "table",
                &table,
            ],
        );
    }

    // ── 2b. ip rule: return traffic for VPC CIDR uses VPC routing table ──
    let _ = run_cmd(
        "ip",
        &[
            "rule", "add", "to", vpc_cidr, "lookup", &table, "priority", "32763",
        ],
    );

    // ── 3. NAT44 MASQUERADE for IPv4 outbound (per-VPC bridge, not wildcard) ──
    ensure_nft_nat4(&br, public_interface)?;

    // ── 4. Jool NAT64 ──
    let _ = run_cmd("modprobe", &["jool"]);
    let addr_cidr = format!("{}/128", public_ipv6);
    let _ = run_cmd(
        "ip",
        &["-6", "addr", "add", &addr_cidr, "dev", public_interface],
    );
    let _ = run_cmd(
        "jool",
        &[
            "instance",
            "add",
            &instance,
            "--netfilter",
            "--pool6",
            "64:ff9b::/96",
        ],
    );

    // ── 5. NAT66 SNAT: VM IPv6 traffic from this VPC exits with its NAT GW public IPv6 ──
    ensure_nft_nat6(&br, public_ipv6, public_interface)?;

    // ── 6. DNS64 resolver on the bridge ──
    ensure_dns64(&br, vpc_cidr)?;

    // ── 7. Fix host isolation: allow NDP + DNS + established replies ──
    fix_host_isolation()?;

    Ok(())
}

/// Remove NAT gateway provisioning for a VPC.
///
/// Cleans up: Jool instance, public IPv6 address, and nftables NAT rules.
pub fn remove_nat_gateway(
    vpc_id: &str,
    public_ipv6: &Ipv6Addr,
    public_interface: &str,
) -> anyhow::Result<()> {
    let instance = jool_instance_name(vpc_id);
    let br = bridge_name(vpc_id);

    // Remove Jool NAT64 instance
    let _ = run_cmd("jool", &["instance", "remove", &instance]);

    // Remove public IPv6 from interface
    let addr_cidr = format!("{}/128", public_ipv6);
    let _ = run_cmd(
        "ip",
        &["-6", "addr", "del", &addr_cidr, "dev", public_interface],
    );

    // Remove NAT44 masquerade rule for this bridge
    remove_nft_rules_matching("ip", "nauka_nat4", "postrouting", &br);

    // Remove NAT66 SNAT rule for this bridge
    remove_nft_rules_matching("ip6", "nauka_nat", "postrouting", &br);

    Ok(())
}

/// Remove all nftables rules in a chain that match a substring.
fn remove_nft_rules_matching(family: &str, table: &str, chain: &str, pattern: &str) {
    // List rules with handles
    let output = Command::new("nft")
        .args(["-a", "list", "chain", family, table, chain])
        .output();
    let text = match output {
        Ok(ref o) if o.status.success() => String::from_utf8_lossy(&o.stdout).to_string(),
        _ => return,
    };
    // Find handle numbers for matching rules and delete them
    for line in text.lines() {
        if line.contains(pattern) {
            if let Some(handle) = line.rsplit("# handle ").next() {
                if let Ok(_h) = handle.trim().parse::<u64>() {
                    let _ = Command::new("nft")
                        .args([
                            "delete",
                            "rule",
                            family,
                            table,
                            chain,
                            "handle",
                            handle.trim(),
                        ])
                        .stdout(std::process::Stdio::null())
                        .stderr(std::process::Stdio::null())
                        .status();
                }
            }
        }
    }
}

/// Remove NAT rules for bridges that no longer have a NAT gateway.
///
/// Compares active NAT gateway bridges against existing nftables rules and
/// removes any rules for bridges that shouldn't have NAT.
pub fn flush_stale_nat_rules(active_natgws: &[&super::types::NatGw]) {
    let active_bridges: Vec<String> = active_natgws
        .iter()
        .map(|n| bridge_name(n.vpc_id.as_str()))
        .collect();

    // Clean stale NAT44 masquerade rules
    flush_stale_rules_in_chain("ip", "nauka_nat4", "postrouting", &active_bridges);

    // Clean stale NAT66 SNAT rules
    flush_stale_rules_in_chain("ip6", "nauka_nat", "postrouting", &active_bridges);
}

fn flush_stale_rules_in_chain(family: &str, table: &str, chain: &str, active_bridges: &[String]) {
    let output = Command::new("nft")
        .args(["-a", "list", "chain", family, table, chain])
        .output();
    let text = match output {
        Ok(ref o) if o.status.success() => String::from_utf8_lossy(&o.stdout).to_string(),
        _ => return,
    };
    for line in text.lines() {
        let trimmed = line.trim();
        // Only process actual rules with iifname (skip chain header/type lines)
        if !trimmed.contains("iifname") {
            continue;
        }
        // Check if this rule references a bridge NOT in the active set
        let is_active = active_bridges
            .iter()
            .any(|br| trimmed.contains(br.as_str()));
        if !is_active {
            if let Some(handle) = trimmed.rsplit("# handle ").next() {
                if let Ok(_h) = handle.trim().parse::<u64>() {
                    let _ = Command::new("nft")
                        .args([
                            "delete",
                            "rule",
                            family,
                            table,
                            chain,
                            "handle",
                            handle.trim(),
                        ])
                        .stdout(std::process::Stdio::null())
                        .stderr(std::process::Stdio::null())
                        .status();
                }
            }
        }
    }
}

/// Detect the host's default IPv4 gateway.
fn detect_default_gateway() -> Option<String> {
    let output = Command::new("ip")
        .args(["route", "show", "default"])
        .output()
        .ok()?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let parts: Vec<&str> = stdout.split_whitespace().collect();
    for window in parts.windows(2) {
        if window[0] == "via" {
            return Some(window[1].to_string());
        }
    }
    None
}

/// Detect the IPv4 address assigned to a bridge interface.
fn detect_bridge_ipv4(bridge: &str) -> Option<String> {
    let output = Command::new("ip")
        .args(["-4", "addr", "show", "dev", bridge])
        .output()
        .ok()?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    for line in stdout.lines() {
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix("inet ") {
            if let Some(addr) = rest.split('/').next() {
                return Some(addr.to_string());
            }
        }
    }
    None
}

/// NAT44: MASQUERADE for IPv4 traffic from a specific VPC bridge.
fn ensure_nft_nat4(vpc_bridge: &str, out_iface: &str) -> anyhow::Result<()> {
    let _ = run_nft("add table ip nauka_nat4");
    let _ =
        run_nft("add chain ip nauka_nat4 postrouting { type nat hook postrouting priority 100 ; }");
    let existing = Command::new("nft")
        .args(["list", "chain", "ip", "nauka_nat4", "postrouting"])
        .output()
        .map(|o| String::from_utf8_lossy(&o.stdout).contains(vpc_bridge))
        .unwrap_or(false);
    if !existing {
        run_nft(&format!(
            "add rule ip nauka_nat4 postrouting iifname \"{}\" oifname \"{}\" masquerade",
            vpc_bridge, out_iface
        ))?;
    }
    Ok(())
}

/// NAT66: SNAT VM IPv6 traffic from a specific VPC bridge to the NAT GW's public IPv6.
///
/// Removes any stale SNAT rule for this bridge before adding the current one.
/// This handles the case where a NAT gateway is deleted and recreated with a new IPv6.
fn ensure_nft_nat6(
    vpc_bridge: &str,
    public_ipv6: &Ipv6Addr,
    out_iface: &str,
) -> anyhow::Result<()> {
    let _ = run_nft("add table ip6 nauka_nat");
    let _ =
        run_nft("add chain ip6 nauka_nat postrouting { type nat hook postrouting priority 100 ; }");

    // Check if correct rule already exists
    let ipv6_str = public_ipv6.to_string();
    let chain_output = Command::new("nft")
        .args(["list", "chain", "ip6", "nauka_nat", "postrouting"])
        .output()
        .ok();
    let chain_text = chain_output
        .as_ref()
        .map(|o| String::from_utf8_lossy(&o.stdout).to_string())
        .unwrap_or_default();

    let has_correct_rule = chain_text.contains(vpc_bridge) && chain_text.contains(&ipv6_str);
    let has_stale_rule = chain_text.contains(vpc_bridge) && !chain_text.contains(&ipv6_str);

    if has_stale_rule {
        // Flush and recreate all rules — stale SNAT for this bridge exists
        let _ = run_nft("flush chain ip6 nauka_nat postrouting");
        // Re-add rules for all bridges that aren't this one
        for line in chain_text.lines() {
            let trimmed = line.trim();
            if trimmed.contains("snat to") && !trimmed.contains(vpc_bridge) {
                let _ = run_nft(&format!("add rule ip6 nauka_nat postrouting {}", trimmed));
            }
        }
        // Add correct rule for this bridge
        run_nft(&format!(
            "add rule ip6 nauka_nat postrouting iifname \"{}\" oifname \"{}\" snat to {}",
            vpc_bridge, out_iface, public_ipv6
        ))?;
    } else if !has_correct_rule {
        run_nft(&format!(
            "add rule ip6 nauka_nat postrouting iifname \"{}\" oifname \"{}\" snat to {}",
            vpc_bridge, out_iface, public_ipv6
        ))?;
    }
    Ok(())
}

/// Fix the host isolation rules to allow NAT Gateway traffic through.
///
/// The default host isolation drops ALL output to VPC bridges. NAT Gateway
/// needs: NDP (IPv6 neighbor discovery), DNS replies, and conntrack return traffic.
fn fix_host_isolation() -> anyhow::Result<()> {
    // Check if the nauka table has the raw drop rule
    let output = Command::new("nft")
        .args(["list", "chain", "inet", "nauka", "output"])
        .output();
    let has_raw_drop = output
        .as_ref()
        .map(|o| {
            let s = String::from_utf8_lossy(&o.stdout);
            s.contains("oifname") && s.contains("drop") && !s.contains("ct state")
        })
        .unwrap_or(false);

    if has_raw_drop {
        // Replace with a smarter rule set that allows NAT GW traffic
        let _ = run_nft("flush chain inet nauka output");
        let _ = run_nft(
            "add rule inet nauka output oifname \"nkb-*\" ct state established,related accept",
        );
        let _ = run_nft("add rule inet nauka output oifname \"nkb-*\" udp sport 53 accept");
        let _ = run_nft("add rule inet nauka output oifname \"nkb-*\" tcp sport 53 accept");
        let _ = run_nft("add rule inet nauka output oifname \"nkb-*\" icmpv6 type { nd-neighbor-solicit, nd-neighbor-advert, nd-router-advert } accept");
        let _ = run_nft("add rule inet nauka output oifname \"nkb-*\" drop");
    }

    Ok(())
}

/// Install and configure unbound as a DNS64 resolver on the bridge.
fn ensure_dns64(bridge: &str, vpc_cidr: &str) -> anyhow::Result<()> {
    if !Command::new("which")
        .arg("unbound")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
    {
        let _ = run_cmd("apt-get", &["install", "-y", "-qq", "unbound"]);
    }

    let gw_ipv4 = detect_bridge_ipv4(bridge).unwrap_or_else(|| "10.0.1.1".to_string());
    let gw_ipv6 = bridge_ipv6_gateway(vpc_cidr);

    // Add ULA IPv6 to bridge
    let _ = run_cmd(
        "ip",
        &[
            "-6",
            "addr",
            "add",
            &format!("{}/64", gw_ipv6),
            "dev",
            bridge,
        ],
    );

    // Clean old nauka DNS64 configs to avoid conflicts
    if let Ok(entries) = std::fs::read_dir("/etc/unbound/unbound.conf.d") {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if name_str.starts_with("nauka-dns64-") && !name_str.contains(bridge) {
                let _ = std::fs::remove_file(entry.path());
            }
        }
    }

    let conf = format!(
        r#"server:
    interface: {gw_ipv4}
    interface: {gw_ipv6}
    access-control: 0.0.0.0/0 allow
    access-control: ::/0 allow
    do-ip6: yes
    module-config: "dns64 iterator"
    dns64-prefix: 64:ff9b::/96
    verbosity: 0

forward-zone:
    name: "."
    forward-addr: 8.8.8.8
    forward-addr: 1.1.1.1
"#,
    );

    let conf_path = format!("/etc/unbound/unbound.conf.d/nauka-dns64-{}.conf", bridge);
    std::fs::write(&conf_path, conf)?;

    let _ = run_cmd("systemctl", &["restart", "unbound"]);

    Ok(())
}

fn run_cmd(cmd: &str, args: &[&str]) -> anyhow::Result<()> {
    let output = Command::new(cmd).args(args).output()?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("{} {} failed: {}", cmd, args.join(" "), stderr.trim());
    }
    Ok(())
}

fn run_nft(rule: &str) -> anyhow::Result<()> {
    let output = Command::new("nft").arg(rule).output()?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("nft failed: {}", stderr.trim());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jool_instance_name_deterministic() {
        let a = jool_instance_name("vpc-01ABCDEF");
        let b = jool_instance_name("vpc-01ABCDEF");
        assert_eq!(a, b);
    }

    #[test]
    fn jool_instance_name_within_limits() {
        let name = jool_instance_name("vpc-01ABCDEFGHIJKLMNOP");
        assert!(name.len() <= 15, "name too long: {}", name);
    }

    #[test]
    fn jool_instance_name_different_vpcs() {
        let a = jool_instance_name("vpc-aaa");
        let b = jool_instance_name("vpc-bbb");
        assert_ne!(a, b);
    }

    #[test]
    fn bridge_ipv6_gateway_deterministic() {
        let a = bridge_ipv6_gateway("10.0.0.0/16");
        let b = bridge_ipv6_gateway("10.0.0.0/16");
        assert_eq!(a, b);
    }

    #[test]
    fn bridge_ipv6_different_cidrs() {
        let a = bridge_ipv6_gateway("10.0.0.0/16");
        let b = bridge_ipv6_gateway("10.1.0.0/16");
        assert_ne!(a, b);
    }

    #[test]
    fn bridge_ipv6_vm_different_from_gateway() {
        let gw = bridge_ipv6_gateway("10.0.0.0/16");
        let vm = bridge_ipv6_vm("10.0.0.0/16", 0);
        assert_ne!(gw, vm);
    }
}
