//! VM reconciler — ensures VMs assigned to this node have TAPs and processes.

use nauka_compute::runtime::{
    gvisor::GVisorRuntime, kvm::KvmRuntime, Runtime, RuntimeMode, VmRunConfig,
};
use nauka_compute::vm::provision;
use nauka_compute::vm::types::VmState;
use nauka_network::vpc::provision as vpc_provision;

use crate::types::{ReconcileContext, ReconcileResult};

/// Pure logic: returns true if this VM is assigned to one of the local node IDs.
fn is_local_vm(hypervisor_id: Option<&str>, node_ids: &[String]) -> bool {
    hypervisor_id
        .map(|hid| node_ids.iter().any(|nid| nid == hid))
        .unwrap_or(false)
}

/// Pure logic: returns true if this VM's state means it should have a running
/// process on the node (i.e., it is either `Pending` or `Running`).
fn should_vm_exist(state: &VmState) -> bool {
    matches!(state, VmState::Pending | VmState::Running)
}

/// Pure logic: given the current VM state and whether the process is actually
/// running, returns the action the reconciler should take.
#[derive(Debug, PartialEq)]
pub(crate) enum VmAction {
    /// Process is running but state is `Pending` — correct to `Running`,
    /// then run health checks.
    CorrectStateAndHealthCheck,
    /// Process is running and state is already `Running` — just health check.
    HealthCheck,
    /// Process is not running — start it.
    Start,
}

/// Pure logic: decide what action to take for a VM that should exist.
pub(crate) fn decide_vm_action(state: &VmState, process_running: bool) -> VmAction {
    if process_running {
        if *state == VmState::Pending {
            VmAction::CorrectStateAndHealthCheck
        } else {
            VmAction::HealthCheck
        }
    } else {
        VmAction::Start
    }
}

/// Pure logic: returns the IDs of processes that are orphaned — they are running
/// but no longer have a corresponding VM that should exist on this node.
fn find_orphaned_processes(actual_processes: &[String], needed_ids: &[&str]) -> Vec<String> {
    actual_processes
        .iter()
        .filter(|vm_id| !needed_ids.contains(&vm_id.as_str()))
        .cloned()
        .collect()
}

/// Pure logic: returns true if the expected network interface name is present
/// in the list of actual interfaces.
fn is_interface_present(expected: &str, actual: &[String]) -> bool {
    actual.iter().any(|v| v == expected)
}

pub struct VmReconciler;

impl VmReconciler {
    /// Get the right runtime for this node.
    fn runtime(ctx: &ReconcileContext) -> Box<dyn Runtime> {
        if ctx.runtime == RuntimeMode::Kvm {
            Box::new(KvmRuntime)
        } else {
            Box::new(GVisorRuntime)
        }
    }
}

#[async_trait::async_trait]
impl super::Reconciler for VmReconciler {
    fn name(&self) -> &str {
        "vm"
    }

    async fn reconcile(&self, ctx: &ReconcileContext) -> anyhow::Result<ReconcileResult> {
        let mut result = ReconcileResult::new("vm");
        let rt = Self::runtime(ctx);

        // 1. Get VMs assigned to this node
        let vm_store = nauka_compute::vm::store::VmStore::new(ctx.db.clone());
        let all_vms = vm_store.list(None, None, None).await?;
        let local_vms: Vec<_> = all_vms
            .iter()
            .filter(|vm| is_local_vm(vm.hypervisor_id.as_deref(), &ctx.node_ids))
            .collect();

        // VMs that should be running (pending = needs starting, running = should be alive).
        // Skip orphaned VMs whose org no longer exists (cascading orphan).
        // P2.9 (sifrah/nauka#213) migrated `OrgStore` to take an
        // `EmbeddedDb` directly; we hand it the cluster-DB wrapper's
        // internal SurrealDB handle via `.embedded().clone()`.
        let org_store = nauka_org::store::OrgStore::new(ctx.db.embedded().clone());
        let mut should_exist: Vec<_> = Vec::new();
        for vm in &local_vms {
            if !should_vm_exist(&vm.state) {
                continue;
            }
            if org_store.get(vm.org_id.as_str()).await?.is_none() {
                tracing::warn!(
                    vm_id = vm.meta.id.as_str(),
                    org_id = vm.org_id.as_str(),
                    "skipping orphaned VM (org deleted)"
                );
                continue;
            }
            should_exist.push(vm);
        }

        result.desired = should_exist.len();

        // 2. Check actual state
        let actual_processes = crate::observer::process::list_vms();
        result.actual = actual_processes.len();

        // 3. Create network interface + start processes for missing VMs
        let use_veth = ctx.runtime == RuntimeMode::Container;
        for vm in &should_exist {
            let bridge = vpc_provision::bridge_name(vm.vpc_id.as_str());

            // Ensure network interface exists (veth for containers, TAP for KVM)
            if use_veth {
                let expected = provision::veth_host_name(&vm.meta.id);
                let actual = provision::list_veths();
                let veth_was_missing = !is_interface_present(&expected, &actual);
                if veth_was_missing {
                    if let Err(e) = provision::ensure_veth(&vm.meta.id, &bridge) {
                        tracing::error!(vm_id = vm.meta.id.as_str(), error = %e, "failed to create veth");
                        result.failed += 1;
                        result.errors.push(format!("veth {}: {e}", vm.meta.id));
                        continue;
                    }

                    // If container is running, inject the new veth into its netns
                    if actual_processes.contains(&vm.meta.id) {
                        if let Some(pid) = crate::observer::process::get_pid(&vm.meta.id) {
                            let ip = vm.private_ip.as_deref().unwrap_or("0.0.0.0");
                            let mac =
                                nauka_network::vpc::provision::mac_from_ip(ip).unwrap_or_default();
                            let subnet_store =
                                nauka_network::vpc::subnet::store::SubnetStore::new(ctx.db.clone());
                            let gateway = subnet_store
                                .get(vm.subnet_id.as_str(), None, None)
                                .await?
                                .map(|s| s.gateway.clone())
                                .unwrap_or_else(|| "0.0.0.0".to_string());
                            let vpc_store =
                                nauka_network::vpc::store::VpcStore::new(ctx.db.clone());
                            let vpc_cidr = vpc_store
                                .get(vm.vpc_id.as_str(), None)
                                .await?
                                .map(|v| v.cidr);
                            match provision::setup_container_net(
                                &vm.meta.id,
                                pid,
                                ip,
                                &gateway,
                                &mac,
                                vpc_cidr.as_deref(),
                            ) {
                                Ok(()) => {
                                    tracing::info!(
                                        vm_id = vm.meta.id.as_str(),
                                        "veth injected into container netns"
                                    );
                                    result.updated += 1;
                                }
                                Err(e) => {
                                    tracing::error!(vm_id = vm.meta.id.as_str(), error = %e, "failed to inject veth");
                                    result.failed += 1;
                                    result
                                        .errors
                                        .push(format!("veth inject {}: {e}", vm.meta.id));
                                }
                            }
                        }
                    }
                }
            } else {
                let expected = provision::tap_name(&vm.meta.id);
                let actual = provision::list_taps();
                if !is_interface_present(&expected, &actual) {
                    if let Err(e) = provision::ensure_tap(&vm.meta.id, &bridge) {
                        tracing::error!(vm_id = vm.meta.id.as_str(), error = %e, "failed to create TAP");
                        result.failed += 1;
                        result.errors.push(format!("tap {}: {e}", vm.meta.id));
                        continue;
                    }
                }
            }

            let action = decide_vm_action(&vm.state, actual_processes.contains(&vm.meta.id));

            // If process is already running, check health and correct state
            if matches!(
                action,
                VmAction::CorrectStateAndHealthCheck | VmAction::HealthCheck
            ) {
                if action == VmAction::CorrectStateAndHealthCheck {
                    if let Err(e) = vm_store
                        .update_state(&vm.meta.id, VmState::Running, None, None, None)
                        .await
                    {
                        tracing::error!(vm_id = vm.meta.id.as_str(), error = %e, "failed to correct VM state");
                    } else {
                        tracing::info!(
                            vm_id = vm.meta.id.as_str(),
                            "corrected state: pending -> running"
                        );
                        result.updated += 1;
                    }
                }

                // Validate OCI config.json — regenerate if corrupted
                if use_veth {
                    let config_path = format!("/run/nauka/vms/{}/bundle/config.json", vm.meta.id);
                    let config_valid = std::fs::read_to_string(&config_path)
                        .ok()
                        .and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok())
                        .and_then(|v| v["process"]["args"].as_array().cloned())
                        .is_some();

                    if !config_valid {
                        let rootfs_dir = format!("/run/nauka/vms/{}/bundle/rootfs", vm.meta.id);
                        let has_tini = std::path::Path::new(&rootfs_dir)
                            .join("usr/bin/tini")
                            .exists();
                        let subnet_store =
                            nauka_network::vpc::subnet::store::SubnetStore::new(ctx.db.clone());
                        let subnet = subnet_store.get(vm.subnet_id.as_str(), None, None).await?;
                        let (gateway, cidr) = match &subnet {
                            Some(s) => (s.gateway.clone(), s.cidr.clone()),
                            None => ("0.0.0.0".to_string(), "0.0.0.0/0".to_string()),
                        };
                        let vpc_store = nauka_network::vpc::store::VpcStore::new(ctx.db.clone());
                        let vpc_cidr = vpc_store
                            .get(vm.vpc_id.as_str(), None)
                            .await?
                            .map(|v| v.cidr);
                        let run_config = VmRunConfig {
                            vm_id: vm.meta.id.clone(),
                            vm_name: vm.meta.name.clone(),
                            vcpus: vm.vcpus,
                            memory_mb: vm.memory_mb,
                            disk_gb: vm.disk_gb,
                            image: vm.image.clone(),
                            tap_name: provision::veth_guest_name(&vm.meta.id),
                            private_ip: vm.private_ip.clone().unwrap_or_default(),
                            gateway,
                            subnet_cidr: cidr,
                            vpc_cidr,
                        };
                        let oci = nauka_compute::runtime::gvisor::generate_oci_config(
                            &run_config,
                            has_tini,
                        );
                        if let Err(e) = std::fs::write(&config_path, oci) {
                            tracing::error!(vm_id = vm.meta.id.as_str(), error = %e, "failed to regenerate config.json");
                        } else {
                            tracing::info!(
                                vm_id = vm.meta.id.as_str(),
                                "regenerated corrupted config.json"
                            );
                            result.updated += 1;
                        }
                    }
                }

                // Verify base image exists — re-pull if deleted
                if !nauka_compute::image::registry::exists(&vm.image) {
                    tracing::warn!(
                        vm_id = vm.meta.id.as_str(),
                        image = vm.image.as_str(),
                        "base image missing — pulling"
                    );
                    match nauka_compute::image::registry::pull(&vm.image).await {
                        Ok(_) => {
                            tracing::info!(
                                vm_id = vm.meta.id.as_str(),
                                image = vm.image.as_str(),
                                "base image restored"
                            );
                            result.updated += 1;
                        }
                        Err(e) => {
                            tracing::error!(vm_id = vm.meta.id.as_str(), image = vm.image.as_str(), error = %e, "failed to pull base image");
                            result.failed += 1;
                            result.errors.push(format!("image {}: {e}", vm.image));
                        }
                    }
                }

                // Health check: ensure sshd is alive inside containers
                if use_veth && !crate::observer::health::is_sshd_alive(&vm.meta.id) {
                    match crate::observer::health::restart_sshd(&vm.meta.id) {
                        Ok(()) => {
                            tracing::info!(vm_id = vm.meta.id.as_str(), "restarted sshd");
                            result.updated += 1;
                        }
                        Err(e) => {
                            tracing::error!(vm_id = vm.meta.id.as_str(), error = %e, "failed to restart sshd");
                            result.failed += 1;
                            result.errors.push(format!("sshd {}: {e}", vm.meta.id));
                        }
                    }
                }

                continue;
            }

            // Ensure process is running — clean up stale state first
            if action == VmAction::Start {
                // Kill orphaned processes and clean stale container state
                // before recreating. This handles the case where the container
                // died but `sleep infinity` or tini survived as an orphan.
                let _ = rt.stop(&vm.meta.id);

                // Remove stale veth pair — the guest side may be stuck in the
                // dead container's netns and can't be reused.
                if use_veth {
                    let _ = provision::remove_veth(&vm.meta.id);
                    if let Err(e) = provision::ensure_veth(&vm.meta.id, &bridge) {
                        tracing::error!(vm_id = vm.meta.id.as_str(), error = %e, "failed to recreate veth");
                        result.failed += 1;
                        result.errors.push(format!("veth {}: {e}", vm.meta.id));
                        continue;
                    }
                }

                let subnet_store =
                    nauka_network::vpc::subnet::store::SubnetStore::new(ctx.db.clone());
                let subnet = subnet_store.get(vm.subnet_id.as_str(), None, None).await?;

                let (gateway, cidr) = match &subnet {
                    Some(s) => (s.gateway.clone(), s.cidr.clone()),
                    None => ("0.0.0.0".to_string(), "0.0.0.0/0".to_string()),
                };

                // Resolve VPC CIDR for DNS64/NAT64 setup
                let vpc_store = nauka_network::vpc::store::VpcStore::new(ctx.db.clone());
                let vpc_cidr = vpc_store
                    .get(vm.vpc_id.as_str(), None)
                    .await?
                    .map(|v| v.cidr);

                let config = VmRunConfig {
                    vm_id: vm.meta.id.clone(),
                    vm_name: vm.meta.name.clone(),
                    vcpus: vm.vcpus,
                    memory_mb: vm.memory_mb,
                    disk_gb: vm.disk_gb,
                    image: vm.image.clone(),
                    tap_name: if use_veth {
                        provision::veth_guest_name(&vm.meta.id)
                    } else {
                        provision::tap_name(&vm.meta.id)
                    },
                    private_ip: vm.private_ip.clone().unwrap_or_default(),
                    gateway,
                    subnet_cidr: cidr,
                    vpc_cidr,
                };

                match rt.start(&config) {
                    Ok(pid) => {
                        tracing::info!(
                            vm_id = vm.meta.id.as_str(),
                            pid,
                            runtime = %ctx.runtime,
                            "VM process started"
                        );
                        if vm.state == VmState::Pending {
                            if let Err(e) = vm_store
                                .update_state(&vm.meta.id, VmState::Running, None, None, None)
                                .await
                            {
                                tracing::error!(vm_id = vm.meta.id.as_str(), error = %e, "failed to update VM state");
                            }
                        }
                        result.created += 1;
                    }
                    Err(e) => {
                        tracing::error!(vm_id = vm.meta.id.as_str(), error = %e, "failed to start VM");
                        result.failed += 1;
                        result.errors.push(format!("start {}: {e}", vm.meta.id));
                    }
                }
            }
        }

        // 4. Remove orphaned processes + network interfaces
        let needed_ids: Vec<&str> = should_exist.iter().map(|vm| vm.meta.id.as_str()).collect();
        let orphaned = find_orphaned_processes(&actual_processes, &needed_ids);
        for vm_id in &orphaned {
            tracing::info!(vm_id = vm_id.as_str(), "stopping orphaned VM");
            let _ = rt.stop(vm_id);
            if use_veth {
                let _ = provision::remove_veth(vm_id);
            } else {
                let _ = provision::remove_tap(vm_id);
            }
            result.deleted += 1;
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── is_local_vm ──────────────────────────────────────────────

    #[test]
    fn local_vm_matches_node_id() {
        let node_ids = vec!["hv-aaa".to_string(), "hv-bbb".to_string()];
        assert!(is_local_vm(Some("hv-aaa"), &node_ids));
        assert!(is_local_vm(Some("hv-bbb"), &node_ids));
    }

    #[test]
    fn local_vm_no_match() {
        let node_ids = vec!["hv-aaa".to_string()];
        assert!(!is_local_vm(Some("hv-zzz"), &node_ids));
    }

    #[test]
    fn local_vm_none_hypervisor_id() {
        let node_ids = vec!["hv-aaa".to_string()];
        assert!(!is_local_vm(None, &node_ids));
    }

    #[test]
    fn local_vm_empty_node_ids() {
        assert!(!is_local_vm(Some("hv-aaa"), &[]));
    }

    // ── should_vm_exist ──────────────────────────────────────────

    #[test]
    fn pending_and_running_should_exist() {
        assert!(should_vm_exist(&VmState::Pending));
        assert!(should_vm_exist(&VmState::Running));
    }

    #[test]
    fn other_states_should_not_exist() {
        assert!(!should_vm_exist(&VmState::Stopped));
        assert!(!should_vm_exist(&VmState::Deleting));
        assert!(!should_vm_exist(&VmState::Deleted));
        assert!(!should_vm_exist(&VmState::Creating));
    }

    // ── decide_vm_action ─────────────────────────────────────────

    #[test]
    fn running_process_with_pending_state_corrects() {
        assert_eq!(
            decide_vm_action(&VmState::Pending, true),
            VmAction::CorrectStateAndHealthCheck
        );
    }

    #[test]
    fn running_process_with_running_state_health_checks() {
        assert_eq!(
            decide_vm_action(&VmState::Running, true),
            VmAction::HealthCheck
        );
    }

    #[test]
    fn no_process_starts_vm() {
        assert_eq!(decide_vm_action(&VmState::Pending, false), VmAction::Start);
        assert_eq!(decide_vm_action(&VmState::Running, false), VmAction::Start);
    }

    // ── find_orphaned_processes ──────────────────────────────────

    #[test]
    fn orphans_detected() {
        let actual = vec![
            "vm-aaa".to_string(),
            "vm-bbb".to_string(),
            "vm-ccc".to_string(),
        ];
        let needed = vec!["vm-aaa", "vm-ccc"];
        let orphaned = find_orphaned_processes(&actual, &needed);
        assert_eq!(orphaned, vec!["vm-bbb"]);
    }

    #[test]
    fn no_orphans_when_all_needed() {
        let actual = vec!["vm-aaa".to_string(), "vm-bbb".to_string()];
        let needed = vec!["vm-aaa", "vm-bbb"];
        let orphaned = find_orphaned_processes(&actual, &needed);
        assert!(orphaned.is_empty());
    }

    #[test]
    fn all_orphans_when_nothing_needed() {
        let actual = vec!["vm-aaa".to_string(), "vm-bbb".to_string()];
        let needed: Vec<&str> = vec![];
        let orphaned = find_orphaned_processes(&actual, &needed);
        assert_eq!(orphaned.len(), 2);
    }

    #[test]
    fn empty_actual_no_orphans() {
        let actual: Vec<String> = vec![];
        let needed = vec!["vm-aaa"];
        let orphaned = find_orphaned_processes(&actual, &needed);
        assert!(orphaned.is_empty());
    }

    // ── is_interface_present ─────────────────────────────────────

    #[test]
    fn interface_found() {
        let actual = vec!["nkh-abc123".to_string(), "nkh-def456".to_string()];
        assert!(is_interface_present("nkh-abc123", &actual));
    }

    #[test]
    fn interface_not_found() {
        let actual = vec!["nkh-abc123".to_string()];
        assert!(!is_interface_present("nkh-zzz999", &actual));
    }

    #[test]
    fn interface_empty_list() {
        let actual: Vec<String> = vec![];
        assert!(!is_interface_present("nkh-abc123", &actual));
    }
}
