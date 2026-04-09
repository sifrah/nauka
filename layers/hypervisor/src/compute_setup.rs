//! Compute setup — install runtime + base images during hypervisor init/join.
//!
//! This lives in the hypervisor crate (not compute) to avoid circular deps.
//! It only uses std + shell commands, no other nauka crate deps.

use std::path::Path;
use std::process::Command;

use nauka_core::error::NaukaError;
use nauka_core::ui;

const IMAGES_DIR: &str = "/opt/nauka/images";

/// Install compute runtime + base image + forge service. Consumes 3 steps.
pub fn install(steps: &ui::Steps) -> Result<String, NaukaError> {
    let has_kvm = Path::new("/dev/kvm").exists();
    let runtime = if has_kvm { "kvm" } else { "container" };

    steps.set(&format!(
        "Installing compute ({})",
        if has_kvm {
            "KVM detected"
        } else {
            "container mode"
        }
    ));

    if !has_kvm {
        install_crun()?;
    }

    steps.inc();

    steps.set("Preparing base image");
    prepare_ubuntu_image()?;
    steps.inc();

    steps.set("Starting forge");
    install_forge_service()?;
    steps.inc();

    Ok(runtime.to_string())
}

fn install_crun() -> Result<(), NaukaError> {
    // Check if already installed
    if Command::new("crun")
        .arg("--version")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
    {
        return Ok(());
    }

    // Install via apt
    let status = Command::new("apt-get")
        .args(["install", "-y", "--no-install-recommends", "crun"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map_err(|e| NaukaError::internal(format!("apt-get install crun failed: {e}")))?;

    if !status.success() {
        return Err(NaukaError::internal(
            "crun installation failed. Install manually: apt-get install crun",
        ));
    }

    Ok(())
}

fn prepare_ubuntu_image() -> Result<(), NaukaError> {
    let image_dir = format!("{IMAGES_DIR}/ubuntu-24.04");

    if Path::new(&image_dir).join("bin/sh").exists() {
        return Ok(());
    }

    // Install debootstrap if needed
    if !Command::new("which")
        .arg("debootstrap")
        .stdout(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
    {
        let _ = Command::new("apt-get")
            .args(["install", "-y", "--no-install-recommends", "debootstrap"])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();
    }

    std::fs::create_dir_all(&image_dir)
        .map_err(|e| NaukaError::internal(format!("mkdir failed: {e}")))?;

    let status = Command::new("debootstrap")
        .args([
            "--variant=minbase",
            "noble",
            &image_dir,
            "http://archive.ubuntu.com/ubuntu",
        ])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map_err(|e| NaukaError::internal(format!("debootstrap failed: {e}")))?;

    if !status.success() {
        return Err(NaukaError::internal("debootstrap failed"));
    }

    // Install networking tools + SSH server
    let _ = Command::new("chroot")
        .args([
            &image_dir,
            "apt-get",
            "install",
            "-y",
            "--no-install-recommends",
            "iproute2",
            "iputils-ping",
            "net-tools",
            "openssh-server",
            "passwd",
        ])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status();

    // Configure SSH: permit root login, no password
    let sshd_dir = format!("{image_dir}/etc/ssh/sshd_config.d");
    let _ = std::fs::create_dir_all(&sshd_dir);
    let _ = std::fs::write(
        format!("{sshd_dir}/nauka.conf"),
        "PermitRootLogin yes\nPasswordAuthentication no\nPubkeyAuthentication yes\n",
    );

    // Create /run/sshd (required by sshd)
    let _ = std::fs::create_dir_all(format!("{image_dir}/run/sshd"));

    // Generate host keys
    let _ = Command::new("chroot")
        .args([&image_dir, "ssh-keygen", "-A"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status();

    Ok(())
}

/// Install and start the Forge systemd service.
fn install_forge_service() -> Result<(), NaukaError> {
    let unit = r#"[Unit]
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
"#;

    let unit_path = "/etc/systemd/system/nauka-forge.service";

    std::fs::write(unit_path, unit)
        .map_err(|e| NaukaError::internal(format!("write forge unit failed: {e}")))?;

    let _ = Command::new("systemctl").args(["daemon-reload"]).status();

    let _ = Command::new("systemctl")
        .args(["enable", "--now", "nauka-forge"])
        .status();

    Ok(())
}
