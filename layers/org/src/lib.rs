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

/// Convert epoch seconds to ISO 8601 UTC string (YYYY-MM-DDTHH:MM:SSZ).
pub(crate) fn to_iso8601(epoch_secs: u64) -> String {
    let secs = epoch_secs;
    let days = secs / 86400;
    let remaining = secs % 86400;
    let hours = remaining / 3600;
    let minutes = (remaining % 3600) / 60;
    let seconds = remaining % 60;

    let (year, month, day) = days_to_date(days);
    format!("{year:04}-{month:02}-{day:02}T{hours:02}:{minutes:02}:{seconds:02}Z")
}

fn days_to_date(days: u64) -> (u64, u64, u64) {
    let mut y = 1970u64;
    let mut remaining = days;
    loop {
        let diy = if y.is_multiple_of(4) && (!y.is_multiple_of(100) || y.is_multiple_of(400)) {
            366
        } else {
            365
        };
        if remaining < diy {
            break;
        }
        remaining -= diy;
        y += 1;
    }
    let leap = y.is_multiple_of(4) && (!y.is_multiple_of(100) || y.is_multiple_of(400));
    let months: [u64; 12] = if leap {
        [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    } else {
        [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    };
    let mut m = 0;
    for (i, &dim) in months.iter().enumerate() {
        if remaining < dim {
            m = i as u64 + 1;
            break;
        }
        remaining -= dim;
    }
    if m == 0 {
        m = 12;
    }
    (y, m, remaining + 1)
}
