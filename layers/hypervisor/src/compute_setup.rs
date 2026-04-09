//! Compute setup — install runtime + base images during hypervisor init/join.
//!
//! This lives in the hypervisor crate (not compute) to avoid circular deps.
//! It only uses std + shell commands, no other nauka crate deps.

use std::path::Path;
use std::process::Command;

use nauka_core::error::NaukaError;
use nauka_core::ui;

const IMAGES_DIR: &str = "/opt/nauka/images";

/// Install compute runtime + base image. Consumes 2 steps.
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
    // KVM mode: cloud-hypervisor install is a future step

    steps.inc();

    steps.set("Preparing base image");
    prepare_ubuntu_image()?;
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

    // Install networking tools
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
        ])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status();

    Ok(())
}
