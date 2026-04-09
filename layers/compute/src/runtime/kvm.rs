//! Cloud Hypervisor runtime — launches VMs with hardware KVM isolation.
//!
//! Stub implementation: writes PID files to /run/nauka/vms/{vm_id}/pid.
//! Future: spawns cloud-hypervisor process with --kernel, --disk, --net-tap.

use std::path::PathBuf;

use super::{RunningVm, Runtime, VmRunConfig};

const VM_RUN_DIR: &str = "/run/nauka/vms";

pub struct KvmRuntime;

impl Runtime for KvmRuntime {
    fn start(&self, config: &VmRunConfig) -> anyhow::Result<u32> {
        let vm_dir = PathBuf::from(VM_RUN_DIR).join(&config.vm_id);
        std::fs::create_dir_all(&vm_dir)?;

        // TODO: Actually spawn cloud-hypervisor here
        // For now, write a marker file so the observer knows the VM "exists"
        tracing::info!(
            vm_id = config.vm_id.as_str(),
            vm_name = config.vm_name.as_str(),
            vcpus = config.vcpus,
            memory_mb = config.memory_mb,
            image = config.image.as_str(),
            tap = config.tap_name.as_str(),
            ip = config.private_ip.as_str(),
            "would start cloud-hypervisor (stub)"
        );

        // Write a PID file with our own PID as placeholder
        let pid = std::process::id();
        std::fs::write(vm_dir.join("pid"), pid.to_string())?;
        std::fs::write(vm_dir.join("runtime"), "kvm")?;

        Ok(pid)
    }

    fn stop(&self, vm_id: &str) -> anyhow::Result<()> {
        let vm_dir = PathBuf::from(VM_RUN_DIR).join(vm_id);
        if vm_dir.exists() {
            tracing::info!(vm_id, "stopping VM (stub)");
            let _ = std::fs::remove_dir_all(&vm_dir);
        }
        Ok(())
    }

    fn is_running(&self, vm_id: &str) -> Option<u32> {
        let pid_path = PathBuf::from(VM_RUN_DIR).join(vm_id).join("pid");
        std::fs::read_to_string(pid_path)
            .ok()
            .and_then(|s| s.trim().parse::<u32>().ok())
            .filter(|&pid| process_alive(pid))
    }

    fn list_running(&self) -> Vec<RunningVm> {
        list_vm_dirs()
    }
}

/// Check if a process is alive.
fn process_alive(pid: u32) -> bool {
    // kill(pid, 0) checks existence without sending a signal
    unsafe { libc::kill(pid as i32, 0) == 0 }
}

/// Scan /run/nauka/vms/ for VM directories with PID files.
fn list_vm_dirs() -> Vec<RunningVm> {
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
                if let Ok(pid) = pid_str.trim().parse::<u32>() {
                    if process_alive(pid) {
                        vms.push(RunningVm { vm_id, pid });
                    }
                }
            }
        }
    }
    vms
}
