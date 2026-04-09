//! Container runtime — launches VMs as OCI containers.
//!
//! Uses `crun` (or `runsc` if available and compatible) to run containers.
//! Each VM gets:
//! - A copy of the base rootfs image
//! - An OCI config.json with resource limits
//! - A container process managed via the OCI lifecycle

use std::path::PathBuf;
use std::process::Command;

use super::{RunningVm, Runtime, VmRunConfig};

const VM_RUN_DIR: &str = "/run/nauka/vms";
const IMAGES_DIR: &str = "/opt/nauka/images";

pub struct GVisorRuntime;

/// Detect which OCI runtime binary to use.
fn runtime_binary() -> &'static str {
    // Prefer crun (works everywhere) over runsc (needs special kernel support)
    if Command::new("crun")
        .arg("--version")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
    {
        return "crun";
    }
    "runsc"
}

impl Runtime for GVisorRuntime {
    fn start(&self, config: &VmRunConfig) -> anyhow::Result<u32> {
        let rt = runtime_binary();
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

        // Try overlay filesystem (instant, no copy) then fall back to cp.
        // Overlay needs upper/work dirs on a real filesystem (not tmpfs).
        // Use /var/lib/nauka/vms/ for persistence.
        let persist_dir = PathBuf::from("/var/lib/nauka/vms").join(&config.vm_id);
        let upper_dir = persist_dir.join("upper");
        let work_dir = persist_dir.join("work");
        std::fs::create_dir_all(&upper_dir)?;
        std::fs::create_dir_all(&work_dir)?;

        let overlay_opts = format!(
            "lowerdir={},upperdir={},workdir={}",
            base_image.display(),
            upper_dir.display(),
            work_dir.display()
        );

        let overlay_ok = Command::new("mount")
            .args([
                "-t",
                "overlay",
                "overlay",
                "-o",
                &overlay_opts,
                rootfs_dir.to_str().unwrap(),
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false);

        if overlay_ok {
            tracing::info!(
                vm_id = config.vm_id.as_str(),
                "using overlay filesystem (instant)"
            );
        } else {
            // Fallback: full copy
            tracing::info!(
                vm_id = config.vm_id.as_str(),
                "overlay not available, copying rootfs"
            );
            let status = Command::new("cp")
                .args(["-a", "--reflink=auto"])
                .arg(format!("{}/.", base_image.display()))
                .arg(rootfs_dir.to_str().unwrap())
                .status()
                .map_err(|e| anyhow::anyhow!("cp rootfs failed: {e}"))?;
            if !status.success() {
                anyhow::bail!("failed to copy rootfs from {}", base_image.display());
            }
        }

        // 2. Configure networking inside the rootfs
        let etc_net = rootfs_dir.join("etc/network");
        let _ = std::fs::create_dir_all(&etc_net);
        std::fs::write(
            etc_net.join("interfaces"),
            format!(
                "auto lo\niface lo inet loopback\n\nauto eth0\niface eth0 inet static\n  address {}\n  netmask 255.255.255.0\n  gateway {}\n",
                config.private_ip, config.gateway,
            ),
        )?;
        std::fs::write(
            rootfs_dir.join("etc/resolv.conf"),
            "nameserver 8.8.8.8\nnameserver 8.8.4.4\n",
        )?;
        std::fs::write(rootfs_dir.join("etc/hostname"), &config.vm_name)?;

        // 2b. Configure SSH access — inject host's authorized keys
        let ssh_dir = rootfs_dir.join("root/.ssh");
        let _ = std::fs::create_dir_all(&ssh_dir);
        // Copy host's authorized_keys into the container
        if let Ok(host_keys) = std::fs::read_to_string("/root/.ssh/authorized_keys") {
            let _ = std::fs::write(ssh_dir.join("authorized_keys"), &host_keys);
        }
        // Ensure /run/sshd exists (required by sshd)
        let _ = std::fs::create_dir_all(rootfs_dir.join("run/sshd"));

        // Write init script that starts sshd + keeps container alive
        std::fs::write(
            rootfs_dir.join("nauka-init.sh"),
            "#!/bin/sh\nmkdir -p /run/sshd\n/usr/sbin/sshd -D &\nexec sleep infinity\n",
        )?;
        let _ = Command::new("chmod")
            .args(["+x", rootfs_dir.join("nauka-init.sh").to_str().unwrap()])
            .status();

        // 3. Generate OCI config.json
        let oci_config = generate_oci_config(config);
        std::fs::write(bundle_dir.join("config.json"), oci_config)?;

        // 4. Create and start the container
        let _ = Command::new(rt)
            .args(["delete", "--force", &config.vm_id])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();

        let create_status = Command::new(rt)
            .args([
                "create",
                "--bundle",
                bundle_dir.to_str().unwrap(),
                &config.vm_id,
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map_err(|e| anyhow::anyhow!("{rt} create failed: {e}"))?;

        if !create_status.success() {
            anyhow::bail!("{rt} create failed (exit {})", create_status);
        }

        let start_status = Command::new(rt)
            .args(["start", &config.vm_id])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map_err(|e| anyhow::anyhow!("{rt} start failed: {e}"))?;

        if !start_status.success() {
            anyhow::bail!("{rt} start failed (exit {})", start_status);
        }

        // 5. Get the container PID
        let state = Command::new(rt)
            .args(["state", &config.vm_id])
            .output()
            .map_err(|e| anyhow::anyhow!("{rt} state failed: {e}"))?;

        let pid = if state.status.success() {
            let state_json: serde_json::Value =
                serde_json::from_slice(&state.stdout).unwrap_or_default();
            state_json["pid"].as_u64().unwrap_or(0) as u32
        } else {
            0
        };

        // 6. Set up container networking (veth into netns)
        if pid > 0 && !config.private_ip.is_empty() {
            let mac = nauka_network::vpc::provision::mac_from_ip(&config.private_ip)
                .unwrap_or_else(|| "02:00:00:00:00:00".to_string());
            if let Err(e) = crate::vm::provision::setup_container_net(
                &config.vm_id,
                pid,
                &config.private_ip,
                &config.gateway,
                &mac,
            ) {
                tracing::warn!(
                    vm_id = config.vm_id.as_str(),
                    error = %e,
                    "container networking setup failed (container still running)"
                );
            }
        }

        // 6b. Start sshd inside the container via nsenter
        if pid > 0 {
            let pid_str = pid.to_string();
            let _ = Command::new("nsenter")
                .args([
                    "--pid",
                    "--mount",
                    "--net",
                    &format!("--target={pid_str}"),
                    "/usr/sbin/sshd",
                ])
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status();
            tracing::info!(vm_id = config.vm_id.as_str(), "sshd started");
        }

        // 7. Write PID + runtime marker
        std::fs::write(vm_dir.join("pid"), pid.to_string())?;
        std::fs::write(vm_dir.join("runtime"), "container")?;

        tracing::info!(
            vm_id = config.vm_id.as_str(),
            vm_name = config.vm_name.as_str(),
            pid,
            ip = config.private_ip.as_str(),
            image = config.image.as_str(),
            runtime = rt,
            "container started"
        );

        Ok(pid)
    }

    fn stop(&self, vm_id: &str) -> anyhow::Result<()> {
        let rt = runtime_binary();
        tracing::info!(vm_id, runtime = rt, "stopping container");

        let _ = Command::new(rt)
            .args(["kill", vm_id, "SIGKILL"])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();

        std::thread::sleep(std::time::Duration::from_secs(1));

        let _ = Command::new(rt)
            .args(["delete", "--force", vm_id])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();

        // Unmount overlay if mounted
        let vm_dir = PathBuf::from(VM_RUN_DIR).join(vm_id);
        let rootfs = vm_dir.join("bundle/rootfs");
        let _ = Command::new("umount")
            .arg(rootfs.to_str().unwrap_or(""))
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();

        let _ = std::fs::remove_dir_all(&vm_dir);

        // Clean persistent overlay data
        let persist_dir = PathBuf::from("/var/lib/nauka/vms").join(vm_id);
        let _ = std::fs::remove_dir_all(&persist_dir);

        Ok(())
    }

    fn is_running(&self, vm_id: &str) -> Option<u32> {
        let rt = runtime_binary();
        let output = Command::new(rt).args(["state", vm_id]).output().ok()?;

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
        let rt = runtime_binary();
        let output = match Command::new(rt).args(["list", "-f", "json"]).output() {
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
            "args": ["/bin/sh", "/nauka-init.sh"],
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
        "mounts": [
            {
                "destination": "/proc",
                "type": "proc",
                "source": "proc"
            },
            {
                "destination": "/dev",
                "type": "tmpfs",
                "source": "tmpfs",
                "options": ["nosuid", "strictatime", "mode=755", "size=65536k"]
            },
            {
                "destination": "/dev/pts",
                "type": "devpts",
                "source": "devpts",
                "options": ["nosuid", "noexec", "newinstance", "ptmxmode=0666", "mode=0620"]
            },
            {
                "destination": "/dev/shm",
                "type": "tmpfs",
                "source": "shm",
                "options": ["nosuid", "noexec", "nodev", "mode=1777", "size=65536k"]
            },
            {
                "destination": "/sys",
                "type": "sysfs",
                "source": "sysfs",
                "options": ["nosuid", "noexec", "nodev", "ro"]
            },
            {
                "destination": "/tmp",
                "type": "tmpfs",
                "source": "tmpfs",
                "options": ["nosuid", "noexec", "nodev"]
            }
        ],
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
