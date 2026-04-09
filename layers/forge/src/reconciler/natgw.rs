//! NAT gateway reconciler — ensures Jool NAT64 instances + nftables rules.
//!
//! Runs after VPC reconciler (bridges must exist) and before VM reconciler.

use nauka_network::vpc::natgw::provision;
use nauka_network::vpc::natgw::store::NatGwStore;

use crate::types::{ReconcileContext, ReconcileResult};

pub struct NatGwReconciler;

#[async_trait::async_trait]
impl super::Reconciler for NatGwReconciler {
    fn name(&self) -> &str {
        "natgw"
    }

    async fn reconcile(&self, ctx: &ReconcileContext) -> anyhow::Result<ReconcileResult> {
        let mut result = ReconcileResult::new("natgw");

        let store = NatGwStore::new(ctx.db.clone());
        let all_natgws = store.list(None).await?;

        // Filter NAT gateways assigned to this node
        let local_natgws: Vec<_> = all_natgws
            .iter()
            .filter(|n| ctx.node_ids.iter().any(|nid| nid == &n.hypervisor_id))
            .collect();

        result.desired = local_natgws.len();

        // Resolve the public interface (interface with default IPv6 route)
        let public_interface = detect_public_interface().unwrap_or_else(|| "eth0".to_string());

        // Ensure each NAT GW is provisioned
        for natgw in &local_natgws {
            // Resolve VPC VNI
            let vpc_store = nauka_network::vpc::store::VpcStore::new(ctx.db.clone());
            let vpc = match vpc_store.get(natgw.vpc_id.as_str(), None).await? {
                Some(v) => v,
                None => {
                    tracing::warn!(
                        natgw = natgw.meta.name,
                        vpc_id = natgw.vpc_id.as_str(),
                        "NAT GW references unknown VPC, skipping"
                    );
                    result.failed += 1;
                    continue;
                }
            };

            match provision::ensure_nat_gateway(
                natgw.vpc_id.as_str(),
                &vpc.cidr,
                vpc.vni,
                &natgw.public_ipv6,
                &public_interface,
            ) {
                Ok(()) => {
                    result.created += 1;
                    tracing::info!(
                        natgw = natgw.meta.name,
                        vpc = natgw.vpc_name,
                        ipv6 = %natgw.public_ipv6,
                        "NAT gateway provisioned"
                    );
                }
                Err(e) => {
                    result.failed += 1;
                    result
                        .errors
                        .push(format!("natgw '{}': {}", natgw.meta.name, e));
                    tracing::error!(
                        natgw = natgw.meta.name,
                        error = %e,
                        "NAT gateway provisioning failed"
                    );
                }
            }
        }

        result.actual = result.created;

        Ok(result)
    }
}

/// Detect the public-facing network interface (the one with the default IPv6 route).
fn detect_public_interface() -> Option<String> {
    let output = std::process::Command::new("ip")
        .args(["-6", "route", "show", "default"])
        .output()
        .ok()?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    // Format: "default via <gw> dev <iface> ..."
    for part in stdout.split_whitespace().collect::<Vec<_>>().windows(2) {
        if part[0] == "dev" {
            return Some(part[1].to_string());
        }
    }
    None
}
