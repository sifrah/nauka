//! VM reconciler — ensures VMs assigned to this node have TAPs and processes.

use nauka_compute::runtime::{
    gvisor::GVisorRuntime, kvm::KvmRuntime, Runtime, RuntimeMode, VmRunConfig,
};
use nauka_compute::vm::provision;
use nauka_compute::vm::types::VmState;
use nauka_network::vpc::provision as vpc_provision;

use crate::types::{ReconcileContext, ReconcileResult};

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
            .filter(|vm| {
                vm.hypervisor_id
                    .as_ref()
                    .map(|hid| ctx.node_ids.iter().any(|nid| nid == hid))
                    .unwrap_or(false)
            })
            .collect();

        // VMs that should be running (pending = needs starting, running = should be alive).
        // Skip orphaned VMs whose org no longer exists (cascading orphan).
        let org_store = nauka_org::store::OrgStore::new(ctx.db.clone());
        let mut should_exist: Vec<_> = Vec::new();
        for vm in &local_vms {
            if vm.state != VmState::Pending && vm.state != VmState::Running {
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
                let veth_was_missing = !actual.iter().any(|v| v == &expected);
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
                if !actual.iter().any(|t| t == &expected) {
                    if let Err(e) = provision::ensure_tap(&vm.meta.id, &bridge) {
                        tracing::error!(vm_id = vm.meta.id.as_str(), error = %e, "failed to create TAP");
                        result.failed += 1;
                        result.errors.push(format!("tap {}: {e}", vm.meta.id));
                        continue;
                    }
                }
            }

            // If process is already running, check health and correct state
            if actual_processes.contains(&vm.meta.id) {
                if vm.state == VmState::Pending {
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
            if !actual_processes.contains(&vm.meta.id) {
                // Kill orphaned processes and clean stale container state
                // before recreating. This handles the case where the container
                // died but `sleep infinity` or tini survived as an orphan.
                let _ = rt.stop(&vm.meta.id);
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
        for vm_id in &actual_processes {
            if !needed_ids.contains(&vm_id.as_str()) {
                tracing::info!(vm_id, "stopping orphaned VM");
                let _ = rt.stop(vm_id);
                if use_veth {
                    let _ = provision::remove_veth(vm_id);
                } else {
                    let _ = provision::remove_tap(vm_id);
                }
                result.deleted += 1;
            }
        }

        Ok(result)
    }
}
