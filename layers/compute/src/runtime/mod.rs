//! Runtime abstraction — Cloud Hypervisor (KVM) or gVisor (container).
//!
//! The runtime trait abstracts how a VM is actually executed:
//! - On bare-metal with /dev/kvm → Cloud Hypervisor (hardware isolation)
//! - On VPS without KVM → gVisor/runsc (userspace kernel isolation)
//!
//! The user never sees the difference — `nauka vm create` works the same.

pub mod detect;
pub mod gvisor;
pub mod kvm;

use serde::{Deserialize, Serialize};

/// Which runtime this node uses.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum RuntimeMode {
    /// Cloud Hypervisor — bare-metal with /dev/kvm (hardware isolation)
    #[default]
    Kvm,
    /// gVisor/runsc — VPS without KVM (userspace kernel isolation)
    Container,
}

impl std::fmt::Display for RuntimeMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RuntimeMode::Kvm => write!(f, "kvm"),
            RuntimeMode::Container => write!(f, "container"),
        }
    }
}

/// Info about a running VM process.
#[derive(Debug)]
pub struct RunningVm {
    pub vm_id: String,
    pub pid: u32,
}

/// Runtime trait — how to start/stop a VM.
pub trait Runtime: Send + Sync {
    /// Start a VM. Returns the PID of the process.
    fn start(&self, config: &VmRunConfig) -> anyhow::Result<u32>;

    /// Stop a VM by its ID.
    fn stop(&self, vm_id: &str) -> anyhow::Result<()>;

    /// Check if a VM is running. Returns the PID if so.
    fn is_running(&self, vm_id: &str) -> Option<u32>;

    /// List all running VMs managed by this runtime.
    fn list_running(&self) -> Vec<RunningVm>;
}

/// Configuration passed to the runtime to start a VM.
#[derive(Debug)]
pub struct VmRunConfig {
    pub vm_id: String,
    pub vm_name: String,
    pub vcpus: u32,
    pub memory_mb: u32,
    pub disk_gb: u32,
    pub image: String,
    pub tap_name: String,
    pub private_ip: String,
    pub gateway: String,
    pub subnet_cidr: String,
    /// VPC CIDR — used for DNS64/NAT64 IPv6 addressing. None if no NAT GW.
    pub vpc_cidr: Option<String>,
}
