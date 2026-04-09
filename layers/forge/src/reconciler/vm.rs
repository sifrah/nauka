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

        // VMs that should be running (pending = needs starting, running = should be alive)
        let should_exist: Vec<_> = local_vms
            .iter()
            .filter(|vm| vm.state == VmState::Pending || vm.state == VmState::Running)
            .collect();

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
                if !actual.iter().any(|v| v == &expected) {
                    if let Err(e) = provision::ensure_veth(&vm.meta.id, &bridge) {
                        tracing::error!(vm_id = vm.meta.id.as_str(), error = %e, "failed to create veth");
                        result.failed += 1;
                        result.errors.push(format!("veth {}: {e}", vm.meta.id));
                        continue;
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

            // Ensure process is running
            if !actual_processes.contains(&vm.meta.id) {
                let subnet_store =
                    nauka_network::vpc::subnet::store::SubnetStore::new(ctx.db.clone());
                let subnet = subnet_store.get(vm.subnet_id.as_str(), None, None).await?;

                let (gateway, cidr) = match &subnet {
                    Some(s) => (s.gateway.clone(), s.cidr.clone()),
                    None => ("0.0.0.0".to_string(), "0.0.0.0/0".to_string()),
                };

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
                };

                match rt.start(&config) {
                    Ok(pid) => {
                        tracing::info!(
                            vm_id = vm.meta.id.as_str(),
                            pid,
                            runtime = %ctx.runtime,
                            "VM process started"
                        );
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
