//! Install / control the `nauka-hypervisor.service` systemd unit.
//!
//! This module shells out to `systemctl` — it is NOT called from the daemon.
//! Only the `nauka hypervisor init`, `join`, and `leave` CLI commands reach
//! here, each running once as a short-lived process.

use std::process::Command;

use crate::mesh::MeshError;

pub const UNIT_NAME: &str = "nauka-hypervisor.service";
pub const UNIT_PATH: &str = "/etc/systemd/system/nauka-hypervisor.service";

/// Render the unit file and write it to disk. Executable path defaults to
/// the invoking binary's location (`/proc/self/exe`) so the service runs
/// whichever nauka binary the operator just used.
pub fn write_unit_file() -> Result<(), MeshError> {
    let exe = std::env::current_exe()
        .map_err(|e| MeshError::State(format!("current_exe: {e}")))?;
    let exe_path = exe.to_string_lossy();

    let unit = format!(
        "[Unit]\n\
         Description=Nauka hypervisor daemon\n\
         After=network-online.target\n\
         Wants=network-online.target\n\
         \n\
         [Service]\n\
         Type=simple\n\
         ExecStart={exe_path} hypervisor daemon\n\
         Restart=on-failure\n\
         RestartSec=5\n\
         # Network capabilities for the WireGuard interface + binding low ports.\n\
         AmbientCapabilities=CAP_NET_ADMIN CAP_NET_BIND_SERVICE\n\
         CapabilityBoundingSet=CAP_NET_ADMIN CAP_NET_BIND_SERVICE\n\
         # Logs go to journalctl.\n\
         StandardOutput=journal\n\
         StandardError=journal\n\
         \n\
         [Install]\n\
         WantedBy=multi-user.target\n"
    );

    std::fs::write(UNIT_PATH, unit)
        .map_err(|e| MeshError::State(format!("write {UNIT_PATH}: {e}")))?;
    Ok(())
}

pub fn daemon_reload() -> Result<(), MeshError> {
    run_systemctl(&["daemon-reload"])
}

pub fn enable_and_start() -> Result<(), MeshError> {
    run_systemctl(&["enable", "--now", UNIT_NAME])
}

pub fn stop_and_disable() -> Result<(), MeshError> {
    // If the unit file isn't there (e.g. leave on a node that never
    // successfully init'd/joined), `systemctl disable --now` fails with
    // "Unit file does not exist". That's a no-op from our perspective,
    // not an error — swallow it. Other failures (daemon refusing to
    // stop, systemctl not installed) still propagate.
    if !std::path::Path::new(UNIT_PATH).exists() {
        return Ok(());
    }
    run_systemctl(&["disable", "--now", UNIT_NAME])?;
    let _ = run_systemctl(&["reset-failed", UNIT_NAME]);
    Ok(())
}

pub fn remove_unit_file() -> Result<(), MeshError> {
    if std::path::Path::new(UNIT_PATH).exists() {
        std::fs::remove_file(UNIT_PATH)
            .map_err(|e| MeshError::State(format!("remove {UNIT_PATH}: {e}")))?;
    }
    let _ = run_systemctl(&["daemon-reload"]);
    Ok(())
}

fn run_systemctl(args: &[&str]) -> Result<(), MeshError> {
    let out = Command::new("systemctl")
        .args(args)
        .output()
        .map_err(|e| MeshError::State(format!("spawn systemctl: {e}")))?;
    if !out.status.success() {
        return Err(MeshError::State(format!(
            "systemctl {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&out.stderr).trim()
        )));
    }
    Ok(())
}
