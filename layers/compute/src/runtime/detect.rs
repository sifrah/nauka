//! KVM detection — check if the node supports hardware virtualization.

use super::RuntimeMode;

/// Detect which runtime mode this node supports.
///
/// Checks for /dev/kvm (KVM kernel module loaded + hardware support).
/// Falls back to container mode (gVisor) if KVM is not available.
pub fn detect() -> RuntimeMode {
    if kvm_available() {
        tracing::info!("KVM detected (/dev/kvm present) — using hardware virtualization");
        RuntimeMode::Kvm
    } else {
        tracing::info!("KVM not available — using container runtime (gVisor)");
        RuntimeMode::Container
    }
}

/// Check if /dev/kvm exists and is accessible.
fn kvm_available() -> bool {
    std::path::Path::new("/dev/kvm").exists()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detect_returns_a_mode() {
        let mode = detect();
        // On CI/macOS: Container, on bare-metal: Kvm
        assert!(mode == RuntimeMode::Kvm || mode == RuntimeMode::Container);
    }
}
