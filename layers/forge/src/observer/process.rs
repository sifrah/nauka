//! Process observer — scan for running VM processes.
//!
//! Checks both PID liveness and container runtime state. A VM is only
//! considered running if the PID file exists, the process is alive, AND
//! the container runtime reports it as running. This prevents orphaned
//! processes (e.g. `sleep infinity` surviving after container stop) from
//! being mistaken for healthy VMs.

use std::path::PathBuf;

const VM_RUN_DIR: &str = "/run/nauka/vms";

/// List running VM IDs on this node.
pub fn list_vms() -> Vec<String> {
    let run_dir = PathBuf::from(VM_RUN_DIR);
    let entries = match std::fs::read_dir(&run_dir) {
        Ok(e) => e,
        Err(_) => return vec![],
    };

    let mut vms = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            let vm_id = entry.file_name().to_string_lossy().to_string();
            let pid_path = path.join("pid");
            if let Ok(pid_str) = std::fs::read_to_string(&pid_path) {
                if let Ok(pid) = pid_str.trim().parse::<i32>() {
                    let pid_alive = unsafe { libc::kill(pid, 0) == 0 };
                    if !pid_alive {
                        continue;
                    }

                    // PID is alive — check container runtime state.
                    // If crun says "stopped" the container is dead but the
                    // process survived as an orphan. Kill it so the
                    // reconciler can recreate the container cleanly.
                    match container_status(&vm_id) {
                        ContainerState::Running => vms.push(vm_id),
                        ContainerState::Stopped => {
                            tracing::warn!(
                                vm_id,
                                pid,
                                "container stopped but PID alive — killing orphan"
                            );
                            unsafe { libc::kill(pid, libc::SIGKILL) };
                        }
                        ContainerState::Unknown => {
                            // crun state not available (runtime dir cleaned up).
                            // Trust PID liveness — container may still be fine.
                            vms.push(vm_id);
                        }
                    }
                }
            }
        }
    }
    vms
}

enum ContainerState {
    Running,
    Stopped,
    Unknown,
}

fn container_status(vm_id: &str) -> ContainerState {
    let output = std::process::Command::new("crun")
        .args(["state", vm_id])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .output();
    match output {
        Ok(o) if o.status.success() => {
            let state: serde_json::Value = match serde_json::from_slice(&o.stdout) {
                Ok(v) => v,
                Err(_) => return ContainerState::Unknown,
            };
            match state["status"].as_str() {
                Some("running") => ContainerState::Running,
                Some("stopped") | Some("created") => ContainerState::Stopped,
                _ => ContainerState::Unknown,
            }
        }
        _ => ContainerState::Unknown,
    }
}

/// Get the PID of a running VM by its ID.
pub fn get_pid(vm_id: &str) -> Option<u32> {
    let pid_path = PathBuf::from(VM_RUN_DIR).join(vm_id).join("pid");
    let pid_str = std::fs::read_to_string(&pid_path).ok()?;
    let pid: i32 = pid_str.trim().parse().ok()?;
    if unsafe { libc::kill(pid, 0) == 0 } {
        Some(pid as u32)
    } else {
        None
    }
}
