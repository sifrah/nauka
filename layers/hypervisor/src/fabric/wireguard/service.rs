//! Service management — install, start, stop WireGuard as a systemd service.
//!
//! Nauka installs and manages a systemd unit for WireGuard.
//! The service persists across reboots and restarts independently of nauka.

use std::path::Path;
use std::process::Command;

use nauka_core::error::NaukaError;

const SERVICE_NAME: &str = "nauka-wg";
const UNIT_PATH: &str = "/etc/systemd/system/nauka-wg.service";
const WG_CONF_DIR: &str = "/etc/wireguard";
const WG_CONF_FILE: &str = "/etc/wireguard/nauka0.conf";

/// Ensure WireGuard tools are installed. Installs automatically if missing.
///
/// Detects the package manager and installs `wireguard-tools`:
/// - apt (Debian/Ubuntu)
/// - dnf (Fedora/RHEL)
/// - yum (CentOS/older RHEL)
/// - pacman (Arch)
/// - apk (Alpine)
/// - zypper (openSUSE)
pub fn ensure_wireguard() -> Result<(), NaukaError> {
    if wg_quick_available() {
        return Ok(());
    }

    let (cmd, args): (&str, &[&str]) = if which("apt-get") {
        ("apt-get", &["install", "-y", "-qq", "wireguard-tools"])
    } else if which("dnf") {
        ("dnf", &["install", "-y", "-q", "wireguard-tools"])
    } else if which("yum") {
        ("yum", &["install", "-y", "-q", "wireguard-tools"])
    } else if which("pacman") {
        (
            "pacman",
            &["-S", "--noconfirm", "--quiet", "wireguard-tools"],
        )
    } else if which("apk") {
        ("apk", &["add", "--quiet", "wireguard-tools"])
    } else if which("zypper") {
        ("zypper", &["install", "-y", "--quiet", "wireguard-tools"])
    } else {
        return Err(NaukaError::precondition(
            "wireguard-tools not found and no supported package manager detected. \
             Install wireguard-tools manually.",
        ));
    };

    // Update package index first for apt
    if cmd == "apt-get" {
        let _ = Command::new("apt-get")
            .args(["update", "-qq"])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();
    }

    let output = Command::new(cmd)
        .args(args)
        .output()
        .map_err(|e| NaukaError::internal(format!("{cmd} failed: {e}")))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(NaukaError::internal(format!(
            "failed to install wireguard-tools: {}",
            stderr.trim()
        )));
    }

    // Verify it's now available
    if !wg_quick_available() {
        return Err(NaukaError::internal(
            "wireguard-tools installed but wg-quick still not found",
        ));
    }

    Ok(())
}

/// Check if a command exists on the system.
fn which(cmd: &str) -> bool {
    Command::new("which")
        .arg(cmd)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Generate the WireGuard config file content.
pub fn generate_wg_conf(
    private_key: &str,
    listen_port: u16,
    mesh_ipv6: &std::net::Ipv6Addr,
    peers: &[(String, String, std::net::Ipv6Addr, Option<String>)], // (pubkey, keepalive, ipv6, endpoint)
) -> String {
    let mut conf = format!(
        "[Interface]\n\
         PrivateKey = {private_key}\n\
         ListenPort = {listen_port}\n\
         Address = {mesh_ipv6}/128\n"
    );

    for (pubkey, _keepalive, ipv6, endpoint) in peers {
        conf.push_str(&format!(
            "\n[Peer]\n\
             PublicKey = {pubkey}\n\
             AllowedIPs = {ipv6}/128\n\
             PersistentKeepalive = 25\n"
        ));
        if let Some(ep) = endpoint {
            conf.push_str(&format!("Endpoint = {ep}\n"));
        }
    }

    conf
}

/// Generate the systemd unit file content.
fn generate_unit() -> String {
    format!(
        "[Unit]\n\
         Description=Nauka WireGuard Mesh\n\
         After=network-online.target\n\
         Wants=network-online.target\n\
         \n\
         [Service]\n\
         Type=oneshot\n\
         RemainAfterExit=yes\n\
         ExecStart=/usr/bin/wg-quick up {WG_CONF_FILE}\n\
         ExecStop=/usr/bin/wg-quick down {WG_CONF_FILE}\n\
         \n\
         [Install]\n\
         WantedBy=multi-user.target\n"
    )
}

/// Install the systemd service + WireGuard config.
pub fn install(
    private_key: &str,
    listen_port: u16,
    mesh_ipv6: &std::net::Ipv6Addr,
    peers: &[(String, String, std::net::Ipv6Addr, Option<String>)],
) -> Result<(), NaukaError> {
    // Create /etc/wireguard if needed
    std::fs::create_dir_all(WG_CONF_DIR).map_err(NaukaError::from)?;

    // Write WireGuard config (0o600)
    let conf = generate_wg_conf(private_key, listen_port, mesh_ipv6, peers);
    std::fs::write(WG_CONF_FILE, &conf).map_err(NaukaError::from)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(WG_CONF_FILE, std::fs::Permissions::from_mode(0o600));
    }

    // Write systemd unit
    std::fs::write(UNIT_PATH, generate_unit()).map_err(NaukaError::from)?;

    // Reload systemd
    run_systemctl(&["daemon-reload"])?;

    Ok(())
}

/// Enable and start the service.
pub fn enable_and_start() -> Result<(), NaukaError> {
    run_systemctl(&["enable", "--now", SERVICE_NAME])
}

/// Start the service.
pub fn start() -> Result<(), NaukaError> {
    run_systemctl(&["start", SERVICE_NAME])
}

/// Stop the service.
pub fn stop() -> Result<(), NaukaError> {
    run_systemctl(&["stop", SERVICE_NAME])
}

/// Check if the service is active.
pub fn is_active() -> bool {
    Command::new("systemctl")
        .args(["is-active", "--quiet", SERVICE_NAME])
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Check if the service is enabled (starts on boot).
pub fn is_enabled() -> bool {
    Command::new("systemctl")
        .args(["is-enabled", "--quiet", SERVICE_NAME])
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Uninstall the service, remove config files.
pub fn uninstall() -> Result<(), NaukaError> {
    // Stop and disable
    let _ = run_systemctl(&["disable", "--now", SERVICE_NAME]);

    // Remove files
    let _ = std::fs::remove_file(UNIT_PATH);
    let _ = std::fs::remove_file(WG_CONF_FILE);

    // Reload systemd
    let _ = run_systemctl(&["daemon-reload"]);

    Ok(())
}

/// Update the WireGuard config (e.g., when a peer joins) and apply it
/// to the running interface without tearing it down.
///
/// Writes the full wg-quick-format config (with `[Interface].Address`)
/// to `WG_CONF_FILE`, then hot-applies the peer diff via
/// `wg-quick strip | wg syncconf nauka0 /dev/stdin`. The `strip` stage
/// is load-bearing: `wg syncconf` only understands the pure wg subset
/// (PublicKey, AllowedIPs, etc.) and errors out with
/// `Line unrecognized: Address=...` if handed a wg-quick config
/// directly. Before this fix, the failed `wg syncconf` fell back to
/// `systemctl restart nauka-wg.service`, which cascaded through
/// `Requires=nauka-wg.service` on `nauka.service` / `nauka-pd` /
/// `nauka-tikv` and bounced the entire control plane for every
/// single peer add — visible during #299 Hetzner validation as the
/// hypervisor daemon deactivating the instant a join arrived.
///
/// If the hot-apply fails for any reason, we log a warning and
/// return `Ok(())`: the updated config is persisted on disk and the
/// next `wg-quick up` (service restart, reboot, or operator-driven
/// reconciliation) will pick it up. Never cascade-restart systemd —
/// the recovery path is more disruptive than the original failure.
pub fn update_config(
    private_key: &str,
    listen_port: u16,
    mesh_ipv6: &std::net::Ipv6Addr,
    peers: &[(String, String, std::net::Ipv6Addr, Option<String>)],
) -> Result<(), NaukaError> {
    let conf = generate_wg_conf(private_key, listen_port, mesh_ipv6, peers);
    std::fs::write(WG_CONF_FILE, &conf).map_err(NaukaError::from)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(WG_CONF_FILE, std::fs::Permissions::from_mode(0o600));
    }

    // Hot-apply: `wg-quick strip <conf>` → pipe → `wg syncconf nauka0 /dev/stdin`.
    // The strip stage drops `Address`, `MTU`, `PreUp`, etc., leaving
    // only the directives `wg syncconf` understands.
    match syncconf_via_strip() {
        Ok(()) => {}
        Err(e) => {
            tracing::warn!(
                error = %e,
                "wg syncconf hot-apply failed; config persisted for next wg-quick up"
            );
        }
    }

    // `wg syncconf` updates WireGuard's internal peer table but does
    // **not** touch the kernel routing table. `wg-quick up` normally
    // installs an `ip -6 route add <AllowedIPs> dev nauka0` for every
    // peer on top of the syncconf — without those routes the kernel
    // has no reason to steer a peer's mesh IPv6 via `nauka0`, so
    // traffic gets default-routed out `eth0` and never enters the
    // tunnel. Symptom during #299 Hetzner validation: a joining node
    // got its WG handshake through but `ping`/`curl` to the acceptor's
    // mesh IPv6 timed out because the return packet left via the
    // public interface.
    //
    // Install each peer's `AllowedIPs` as a `/128` route on `nauka0`.
    // Idempotent: `ip -6 route add` returns `RTNETLINK: File exists`
    // when the route is already present, which we treat as success.
    install_peer_routes(peers);

    Ok(())
}

/// Install a `/128` route on `nauka0` for every peer in the slice.
///
/// `ip -6 route add <ipv6>/128 dev nauka0` — idempotent: the
/// `File exists` failure mode is ignored. Failures to spawn `ip`
/// are logged but do not surface as errors because the config file
/// on disk will be reloaded on the next `wg-quick up`.
fn install_peer_routes(peers: &[(String, String, std::net::Ipv6Addr, Option<String>)]) {
    for (_pub, _keepalive, mesh_ipv6, _endpoint) in peers {
        let dst = format!("{mesh_ipv6}/128");
        match Command::new("ip")
            .args(["-6", "route", "add", &dst, "dev", "nauka0"])
            .output()
        {
            Ok(output) if output.status.success() => {}
            Ok(output) => {
                let stderr = String::from_utf8_lossy(&output.stderr);
                // Already-installed routes look like "File exists"
                // from ip/route2, or rtnetlink error 17. Swallow those.
                if !stderr.contains("File exists") && !stderr.contains("exists") {
                    tracing::warn!(
                        dst = %dst,
                        stderr = %stderr.trim(),
                        "ip -6 route add failed"
                    );
                }
            }
            Err(e) => {
                tracing::warn!(dst = %dst, error = %e, "ip -6 route add spawn failed");
            }
        }
    }
}

/// Run `wg-quick strip <conf>` and pipe its stdout into
/// `wg syncconf nauka0 /dev/stdin`.
///
/// Returns `Err` only when the strip step spawned successfully but
/// wrote an error, or when either subprocess exited non-zero. The
/// caller logs and swallows — we never want a syncconf failure to
/// cascade into a systemd restart.
fn syncconf_via_strip() -> Result<(), NaukaError> {
    use std::io::Write;

    let strip = Command::new("wg-quick")
        .args(["strip", WG_CONF_FILE])
        .output()
        .map_err(|e| NaukaError::internal(format!("wg-quick strip spawn: {e}")))?;

    if !strip.status.success() {
        return Err(NaukaError::internal(format!(
            "wg-quick strip: {}",
            String::from_utf8_lossy(&strip.stderr).trim()
        )));
    }

    let mut child = Command::new("wg")
        .args(["syncconf", "nauka0", "/dev/stdin"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| NaukaError::internal(format!("wg syncconf spawn: {e}")))?;

    if let Some(mut stdin) = child.stdin.take() {
        stdin
            .write_all(&strip.stdout)
            .map_err(|e| NaukaError::internal(format!("wg syncconf stdin write: {e}")))?;
        // drop stdin → EOF
    }

    let output = child
        .wait_with_output()
        .map_err(|e| NaukaError::internal(format!("wg syncconf wait: {e}")))?;

    if !output.status.success() {
        return Err(NaukaError::internal(format!(
            "wg syncconf: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }

    Ok(())
}

/// Check if wg-quick is available on the system.
pub fn wg_quick_available() -> bool {
    Command::new("which")
        .arg("wg-quick")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Check if the service unit file exists.
pub fn is_installed() -> bool {
    Path::new(UNIT_PATH).exists()
}

fn run_systemctl(args: &[&str]) -> Result<(), NaukaError> {
    let output = Command::new("systemctl")
        .args(args)
        .output()
        .map_err(|e| NaukaError::internal(format!("systemctl failed: {e}")))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(NaukaError::internal(format!(
            "systemctl {} failed: {}",
            args.join(" "),
            stderr.trim()
        )));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_wg_conf_no_peers() {
        let conf = generate_wg_conf("privkey123", 51820, &"fd01::1".parse().unwrap(), &[]);
        assert!(conf.contains("PrivateKey = privkey123"));
        assert!(conf.contains("ListenPort = 51820"));
        assert!(conf.contains("Address = fd01::1/128"));
        assert!(!conf.contains("[Peer]"));
    }

    #[test]
    fn generate_wg_conf_with_peers() {
        let peers = vec![(
            "pubkey456".to_string(),
            "25".to_string(),
            "fd01::2".parse().unwrap(),
            Some("1.2.3.4:51820".to_string()),
        )];
        let conf = generate_wg_conf("privkey123", 51820, &"fd01::1".parse().unwrap(), &peers);
        assert!(conf.contains("[Peer]"));
        assert!(conf.contains("PublicKey = pubkey456"));
        assert!(conf.contains("AllowedIPs = fd01::2/128"));
        assert!(conf.contains("Endpoint = 1.2.3.4:51820"));
        assert!(conf.contains("PersistentKeepalive = 25"));
    }

    #[test]
    fn generate_wg_conf_peer_no_endpoint() {
        let peers = vec![(
            "pubkey789".to_string(),
            "25".to_string(),
            "fd01::3".parse().unwrap(),
            None,
        )];
        let conf = generate_wg_conf("privkey", 51820, &"fd01::1".parse().unwrap(), &peers);
        assert!(conf.contains("[Peer]"));
        assert!(!conf.contains("Endpoint"));
    }

    #[test]
    fn generate_unit_has_required_sections() {
        let unit = generate_unit();
        assert!(unit.contains("[Unit]"));
        assert!(unit.contains("[Service]"));
        assert!(unit.contains("[Install]"));
        assert!(unit.contains("wg-quick up"));
        assert!(unit.contains("wg-quick down"));
        assert!(unit.contains("RemainAfterExit=yes"));
    }

    #[test]
    fn is_installed_false_by_default() {
        // On a test system without nauka installed
        // This may be true on a dev machine — just verify no panic
        let _ = is_installed();
    }

    #[test]
    fn is_active_no_panic() {
        let _ = is_active();
    }

    #[test]
    fn wg_quick_available_no_panic() {
        let _ = wg_quick_available();
    }
}
