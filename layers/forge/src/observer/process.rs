//! Process observer — scan for running VM processes.
//!
//! Scans /run/nauka/vms/ for VM directories with PID files.
//! Checks process liveness with kill(pid, 0).

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
                    // kill(pid, 0) checks if process exists without sending a signal
                    if unsafe { libc::kill(pid, 0) == 0 } {
                        vms.push(vm_id);
                    }
                }
            }
        }
    }
    vms
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
