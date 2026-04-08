//! Organization layer — resource hierarchy for multi-tenancy.
//!
//! Structure mirrors the resource hierarchy:
//! - **Org** — top-level organization (globally unique name)
//!   - **Project** — scoped within an Org
//!     - **Env** — scoped within a Project (prod, staging, dev)
//!
//! CLI: `nauka org`, `nauka org project`, `nauka org project env`

pub mod handlers;
pub mod project;
pub mod store;
pub mod types;

use nauka_core::resource::ResourceRegistration;
use nauka_hypervisor::controlplane;
use nauka_hypervisor::fabric;
use nauka_state::LocalDb;

/// Top-level registration: org with project (with env) as children.
pub fn registration() -> ResourceRegistration {
    ResourceRegistration {
        def: handlers::resource_def(),
        handler: handlers::handler(),
        children: vec![project::handlers::registration()],
    }
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

pub(crate) fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
