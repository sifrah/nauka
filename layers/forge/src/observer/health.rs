//! Container health checks — verify critical services inside running containers.

use std::process::Command;

/// Check if sshd is running inside a container.
pub fn is_sshd_alive(vm_id: &str) -> bool {
    Command::new("crun")
        .args(["exec", vm_id, "/usr/bin/pgrep", "-x", "sshd"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Restart sshd inside a container.
pub fn restart_sshd(vm_id: &str) -> Result<(), String> {
    // Ensure /run/sshd exists
    let _ = Command::new("crun")
        .args(["exec", vm_id, "/bin/mkdir", "-p", "/run/sshd"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status();

    let status = Command::new("crun")
        .args([
            "exec",
            "--cap",
            "CAP_NET_BIND_SERVICE",
            vm_id,
            "/usr/sbin/sshd",
        ])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map_err(|e| format!("failed to exec sshd: {e}"))?;

    if status.success() {
        Ok(())
    } else {
        Err(format!("sshd exited with {status}"))
    }
}
