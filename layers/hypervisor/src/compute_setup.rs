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

    steps.set("Pulling base image (ubuntu-24.04)");
    pull_image("ubuntu-24.04")?;
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

/// Pull an image from the GitHub registry (sifrah/nauka-images).
/// Falls back to debootstrap if the download fails.
fn pull_image(name: &str) -> Result<(), NaukaError> {
    let image_dir = format!("{IMAGES_DIR}/{name}");

    if Path::new(&image_dir).join("bin/sh").exists() {
        return Ok(());
    }

    let arch = std::env::consts::ARCH;
    let arch_name = match arch {
        "x86_64" => "amd64",
        "aarch64" => "arm64",
        _ => arch,
    };

    let asset = format!("{name}-{arch_name}.tar.gz");
    let url = format!("https://github.com/sifrah/nauka-images/releases/download/latest/{asset}");
    let tmp_file = format!("/tmp/nauka-image-{name}.tar.gz");

    // Try downloading from GitHub
    let download_ok = Command::new("curl")
        .args(["-fsSL", "-o", &tmp_file, &url])
        .status()
        .map(|s| s.success())
        .unwrap_or(false);

    if download_ok {
        std::fs::create_dir_all(&image_dir)
            .map_err(|e| NaukaError::internal(format!("mkdir failed: {e}")))?;

        let extract_ok = Command::new("tar")
            .args(["-xzf", &tmp_file, "-C", &image_dir])
            .status()
            .map(|s| s.success())
            .unwrap_or(false);

        let _ = std::fs::remove_file(&tmp_file);

        if extract_ok {
            return Ok(());
        }
        // Extract failed — clean up and fall through to debootstrap
        let _ = std::fs::remove_dir_all(&image_dir);
    }

    // Fallback: build with debootstrap
    tracing::warn!("image download failed, falling back to debootstrap");
    fallback_debootstrap(name)
}

/// Fallback: build image locally with debootstrap.
fn fallback_debootstrap(name: &str) -> Result<(), NaukaError> {
    let image_dir = format!("{IMAGES_DIR}/{name}");

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

    let sshd_dir = format!("{image_dir}/etc/ssh/sshd_config.d");
    let _ = std::fs::create_dir_all(&sshd_dir);
    let _ = std::fs::write(
        format!("{sshd_dir}/nauka.conf"),
        "PermitRootLogin yes\nPasswordAuthentication no\nPubkeyAuthentication yes\n",
    );
    let _ = std::fs::create_dir_all(format!("{image_dir}/run/sshd"));
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
