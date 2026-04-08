//! Forge systemd service management.

use std::process::Command;

use nauka_core::error::NaukaError;

const FORGE_UNIT_PATH: &str = "/etc/systemd/system/nauka-forge.service";
const FORGE_SERVICE: &str = "nauka-forge";

fn generate_unit() -> String {
    r#"[Unit]
Description=Nauka Forge Reconciler
After=network-online.target nauka-wg.service nauka-tikv.service
Wants=network-online.target
Requires=nauka-wg.service

[Service]
Type=simple
ExecStart=/usr/local/bin/nauka forge run
Restart=on-failure
RestartSec=10
LimitNOFILE=1000000

[Install]
WantedBy=multi-user.target
"#
    .to_string()
}

/// Install the forge systemd service.
pub fn install_service() -> Result<(), NaukaError> {
    std::fs::write(FORGE_UNIT_PATH, generate_unit()).map_err(NaukaError::from)?;
    Command::new("systemctl")
        .args(["daemon-reload"])
        .output()
        .map_err(|e| NaukaError::internal(format!("systemctl failed: {e}")))?;
    Ok(())
}

/// Enable and start the forge service.
pub fn start_service() -> Result<(), NaukaError> {
    let output = Command::new("systemctl")
        .args(["enable", "--now", FORGE_SERVICE])
        .output()
        .map_err(|e| NaukaError::internal(format!("systemctl failed: {e}")))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(NaukaError::internal(format!(
            "forge start failed: {stderr}"
        )));
    }
    Ok(())
}

/// Stop the forge service.
pub fn stop_service() -> Result<(), NaukaError> {
    let _ = Command::new("systemctl")
        .args(["stop", FORGE_SERVICE])
        .output();
    Ok(())
}

/// Check if the forge service is active.
pub fn is_active() -> bool {
    Command::new("systemctl")
        .args(["is-active", "--quiet", FORGE_SERVICE])
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Uninstall the forge service.
pub fn uninstall_service() -> Result<(), NaukaError> {
    let _ = Command::new("systemctl")
        .args(["disable", "--now", FORGE_SERVICE])
        .output();
    let _ = std::fs::remove_file(FORGE_UNIT_PATH);
    let _ = Command::new("systemctl").args(["daemon-reload"]).output();
    Ok(())
}
