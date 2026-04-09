//! gVisor runtime — launches VMs as sandboxed containers (no KVM needed).
//!
//! Uses `runsc` (gVisor's OCI-compatible runtime) to run containers
//! with a userspace kernel for strong isolation without hardware virtualization.
//!
//! Each VM gets:
//! - A copy of the base rootfs image
//! - An OCI config.json with network namespace
//! - A `runsc` process managed by PID file

use std::path::PathBuf;
use std::process::Command;

use super::{RunningVm, Runtime, VmRunConfig};

const VM_RUN_DIR: &str = "/run/nauka/vms";
const RUNSC_ROOT: &str = "/run/runsc";
const IMAGES_DIR: &str = "/opt/nauka/images";

pub struct GVisorRuntime;

impl Runtime for GVisorRuntime {
    fn start(&self, config: &VmRunConfig) -> anyhow::Result<u32> {
        let vm_dir = PathBuf::from(VM_RUN_DIR).join(&config.vm_id);
        let bundle_dir = vm_dir.join("bundle");
        let rootfs_dir = bundle_dir.join("rootfs");

        std::fs::create_dir_all(&rootfs_dir)?;

        // 1. Prepare rootfs — copy from base image
        let image_name = config.image.replace(':', "-");
        let base_image = PathBuf::from(IMAGES_DIR).join(&image_name);
        if !base_image.exists() {
            anyhow::bail!(
                "image '{}' not found at {}",
                config.image,
                base_image.display()
            );
        }

        // Use cp -a for fast copy (preserves permissions)
        let status = Command::new("cp")
            .args(["-a", "--reflink=auto"])
            .arg(format!("{}/.", base_image.display()))
            .arg(rootfs_dir.to_str().unwrap())
            .status()
            .map_err(|e| anyhow::anyhow!("cp rootfs failed: {e}"))?;
        if !status.success() {
            anyhow::bail!("failed to copy rootfs from {}", base_image.display());
        }

        // 2. Configure networking inside the rootfs
        //    Write /etc/network/interfaces for the container
        let interfaces = format!(
            "auto lo\niface lo inet loopback\n\nauto eth0\niface eth0 inet static\n  address {ip}\n  netmask 255.255.255.0\n  gateway {gw}\n",
            ip = config.private_ip,
            gw = config.gateway,
        );
        let etc_net = rootfs_dir.join("etc/network");
        let _ = std::fs::create_dir_all(&etc_net);
        std::fs::write(etc_net.join("interfaces"), &interfaces)?;

        // Also write resolv.conf
        std::fs::write(
            rootfs_dir.join("etc/resolv.conf"),
            "nameserver 8.8.8.8\nnameserver 8.8.4.4\n",
        )?;

        // Set hostname
        std::fs::write(rootfs_dir.join("etc/hostname"), &config.vm_name)?;

        // 3. Generate OCI config.json
        let oci_config = generate_oci_config(config);
        std::fs::write(bundle_dir.join("config.json"), oci_config)?;

        // 4. Create and start the container with runsc
        //    Delete any stale container first
        let _ = Command::new("runsc")
            .args(["--root", RUNSC_ROOT, "delete", "--force", &config.vm_id])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();

        let create = Command::new("runsc")
            .args([
                "--root",
                RUNSC_ROOT,
                "create",
                "--bundle",
                bundle_dir.to_str().unwrap(),
                &config.vm_id,
            ])
            .output()
            .map_err(|e| anyhow::anyhow!("runsc create failed: {e}"))?;

        if !create.status.success() {
            let stderr = String::from_utf8_lossy(&create.stderr);
            anyhow::bail!("runsc create failed: {stderr}");
        }

        let start = Command::new("runsc")
            .args(["--root", RUNSC_ROOT, "start", &config.vm_id])
            .output()
            .map_err(|e| anyhow::anyhow!("runsc start failed: {e}"))?;

        if !start.status.success() {
            let stderr = String::from_utf8_lossy(&start.stderr);
            anyhow::bail!("runsc start failed: {stderr}");
        }

        // 5. Get the container PID
        let state = Command::new("runsc")
            .args(["--root", RUNSC_ROOT, "state", &config.vm_id])
            .output()
            .map_err(|e| anyhow::anyhow!("runsc state failed: {e}"))?;

        let pid = if state.status.success() {
            let state_json: serde_json::Value =
                serde_json::from_slice(&state.stdout).unwrap_or_default();
            state_json["pid"].as_u64().unwrap_or(0) as u32
        } else {
            0
        };

        // 6. Write PID + runtime marker
        std::fs::write(vm_dir.join("pid"), pid.to_string())?;
        std::fs::write(vm_dir.join("runtime"), "container")?;

        tracing::info!(
            vm_id = config.vm_id.as_str(),
            vm_name = config.vm_name.as_str(),
            pid,
            ip = config.private_ip.as_str(),
            image = config.image.as_str(),
            "gVisor container started"
        );

        Ok(pid)
    }

    fn stop(&self, vm_id: &str) -> anyhow::Result<()> {
        tracing::info!(vm_id, "stopping gVisor container");

        let _ = Command::new("runsc")
            .args(["--root", RUNSC_ROOT, "kill", vm_id, "SIGKILL"])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();

        // Wait briefly then delete
        std::thread::sleep(std::time::Duration::from_secs(1));

        let _ = Command::new("runsc")
            .args(["--root", RUNSC_ROOT, "delete", "--force", vm_id])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();

        // Clean up VM directory
        let vm_dir = PathBuf::from(VM_RUN_DIR).join(vm_id);
        let _ = std::fs::remove_dir_all(&vm_dir);

        Ok(())
    }

    fn is_running(&self, vm_id: &str) -> Option<u32> {
        let output = Command::new("runsc")
            .args(["--root", RUNSC_ROOT, "state", vm_id])
            .output()
            .ok()?;

        if !output.status.success() {
            return None;
        }

        let state: serde_json::Value = serde_json::from_slice(&output.stdout).ok()?;
        if state["status"].as_str() == Some("running") {
            state["pid"].as_u64().map(|p| p as u32)
        } else {
            None
        }
    }

    fn list_running(&self) -> Vec<RunningVm> {
        let output = match Command::new("runsc")
            .args(["--root", RUNSC_ROOT, "list", "-format", "json"])
            .output()
        {
            Ok(o) if o.status.success() => o,
            _ => return vec![],
        };

        let containers: Vec<serde_json::Value> =
            serde_json::from_slice(&output.stdout).unwrap_or_default();

        containers
            .iter()
            .filter_map(|c| {
                let id = c["id"].as_str()?;
                let status = c["status"].as_str()?;
                let pid = c["pid"].as_u64()? as u32;
                if status == "running" && id.starts_with("vm-") {
                    Some(RunningVm {
                        vm_id: id.to_string(),
                        pid,
                    })
                } else {
                    None
                }
            })
            .collect()
    }
}

/// Generate an OCI runtime spec (config.json) for the container.
fn generate_oci_config(config: &VmRunConfig) -> String {
    serde_json::json!({
        "ociVersion": "1.0.2",
        "process": {
            "terminal": false,
            "user": {"uid": 0, "gid": 0},
            "args": ["sleep", "infinity"],
            "cwd": "/",
            "env": [
                "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
                format!("HOSTNAME={}", config.vm_name),
            ]
        },
        "root": {
            "path": "rootfs",
            "readonly": false
        },
        "hostname": config.vm_name,
        "linux": {
            "namespaces": [
                {"type": "pid"},
                {"type": "ipc"},
                {"type": "uts"},
                {"type": "mount"},
                {"type": "network"}
            ],
            "resources": {
                "cpu": {
                    "quota": (config.vcpus as i64) * 100000,
                    "period": 100000
                },
                "memory": {
                    "limit": (config.memory_mb as i64) * 1024 * 1024
                }
            }
        }
    })
    .to_string()
}
