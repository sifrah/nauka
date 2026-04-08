//! Network layer — VPC, Subnet, VPC Peering.
//!
//! Structure mirrors the resource hierarchy:
//! - **VPC** — virtual private cloud scoped to an Org
//!   - **Subnet** — scoped within a VPC
//!   - **Peering** — connection between two VPCs
//!
//! CLI: `nauka vpc`, `nauka vpc subnet`, `nauka vpc peering`

pub mod validate;
pub mod vpc;

use nauka_core::resource::ResourceRegistration;
use nauka_hypervisor::controlplane;
use nauka_hypervisor::fabric;
use nauka_state::LocalDb;

/// Top-level registration: vpc with subnet and peering as children.
pub fn registration() -> ResourceRegistration {
    vpc::handlers::registration()
}

/// Connect to ClusterDb via PD endpoints from fabric state.
pub(crate) async fn connect_cluster_db() -> anyhow::Result<controlplane::ClusterDb> {
    let dir = nauka_core::process::nauka_dir();
    let _ = std::fs::create_dir_all(&dir);
    let db = LocalDb::open("hypervisor")?;

    let state = fabric::state::FabricState::load(&db)
        .map_err(|e| anyhow::anyhow!("{e}"))?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "cluster not initialized.\n\n\
                 Initialize a cluster first with:\n\
                 \x20 nauka hypervisor init"
            )
        })?;

    let self_endpoint = format!(
        "http://[{}]:{}",
        state.hypervisor.mesh_ipv6,
        controlplane::PD_CLIENT_PORT,
    );
    let mut endpoints = vec![self_endpoint];
    for peer in &state.peers.peers {
        endpoints.push(format!(
            "http://[{}]:{}",
            peer.mesh_ipv6,
            controlplane::PD_CLIENT_PORT,
        ));
    }
    let refs: Vec<&str> = endpoints.iter().map(|s| s.as_str()).collect();

    controlplane::ClusterDb::connect(&refs)
        .await
        .map_err(Into::into)
}
