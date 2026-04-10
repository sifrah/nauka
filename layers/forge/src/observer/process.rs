//! Process observer — scan for running VM processes.
//!
//! Scans /run/nauka/vms/ for VM directories with PID files.
//! Checks process liveness with kill(pid, 0).

use std::path::PathBuf;

const VM_RUN_DIR: &str = "/run/nauka/vms";

/// List running VM IDs on this node.
///
/// Checks both PID liveness and container runtime state. A VM is only
/// considered running if the PID file exists, the process is alive, AND
/// the container runtime reports it as running. This prevents orphaned
/// processes (e.g. `sleep infinity` surviving after container stop) from
/// being mistaken for healthy VMs.
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
                    if unsafe { libc::kill(pid, 0) == 0 } && is_container_running(&vm_id) {
                        vms.push(vm_id);
                    }
                }
            }
        }
    }
    vms
}

/// Check if the container runtime considers this VM running.
fn is_container_running(vm_id: &str) -> bool {
    for rt in &["crun", "runsc"] {
        let output = std::process::Command::new(rt)
            .args(["state", vm_id])
            .output();
        match output {
            Ok(o) if o.status.success() => {
                let state: serde_json::Value = match serde_json::from_slice(&o.stdout) {
                    Ok(v) => v,
                    Err(_) => return false,
                };
                return state["status"].as_str().unwrap_or("") == "running";
            }
            _ => continue,
        }
    }
    false
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
