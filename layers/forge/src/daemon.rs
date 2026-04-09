//! Forge daemon — the main reconciliation loop.

use nauka_hypervisor::controlplane;
use nauka_hypervisor::fabric;
use nauka_state::LocalDb;

use crate::reconciler;
use crate::types::ReconcileContext;

/// Default reconciliation interval in seconds.
const RECONCILE_INTERVAL_SECS: u64 = 30;

/// Run the forge daemon (blocking loop).
pub async fn run() -> anyhow::Result<()> {
    eprintln!("  Forge starting...");

    let mut cycle: u64 = 0;

    loop {
        cycle += 1;

        match run_cycle(cycle).await {
            Ok(_results) => {}
            Err(e) => {
                tracing::error!(cycle, error = %e, "reconcile cycle failed");
            }
        }

        tokio::time::sleep(std::time::Duration::from_secs(RECONCILE_INTERVAL_SECS)).await;
    }
}

/// Run a single reconciliation cycle (for `nauka forge reconcile`).
pub async fn run_once() -> anyhow::Result<String> {
    let results = run_cycle(1).await?;

    let lines: Vec<String> = results.iter().map(|r| format!("  {r}")).collect();
    Ok(format!("reconciliation complete\n{}", lines.join("\n")))
}

/// Execute one reconciliation cycle.
async fn run_cycle(cycle: u64) -> anyhow::Result<Vec<crate::types::ReconcileResult>> {
    // Connect to TiKV
    let db = controlplane::connect().await?;

    // Load this node's identity
    let local_db = LocalDb::open("hypervisor")?;
    let state = fabric::state::FabricState::load(&local_db)
        .map_err(|e| anyhow::anyhow!("{e}"))?
        .ok_or_else(|| anyhow::anyhow!("not initialized"))?;

    let runtime = if state.hypervisor.runtime == "container" {
        nauka_compute::runtime::RuntimeMode::Container
    } else {
        nauka_compute::runtime::RuntimeMode::Kvm
    };

    // Collect all IDs this node is known by:
    // - Its own HypervisorId (hv-...)
    // - Its name (used by peers to identify us)
    // - Any NodeId that peers assigned to us (node-...)
    // This ensures VMs scheduled from any node match correctly.
    let node_ids = vec![
        state.hypervisor.id.as_str().to_string(),
        state.hypervisor.name.clone(),
    ];
    // Check what IDs peers use for us by looking at peer lists on remote nodes
    // We can't do that directly, but peers reference us by NodeId.
    // The scheduler on remote nodes uses the peer's NodeId.
    // So we need to know our own NodeId as seen by peers.
    // This is stored in the peer list of OTHER nodes — we can't access it.
    // Workaround: also check by mesh_ipv6 match, or just match all VMs
    // assigned to any ID containing our name.

    let ctx = ReconcileContext {
        db,
        hypervisor_id: state.hypervisor.id.as_str().to_string(),
        node_ids,
        node_name: state.hypervisor.name.clone(),
        mesh_ipv6: state.hypervisor.mesh_ipv6,
        runtime,
        cycle,
    };

    // Ensure replication is configured correctly.
    // On every cycle, check if max-replicas matches the number of active stores (capped at 3).
    let pd_url = format!(
        "http://[{}]:{}",
        state.hypervisor.mesh_ipv6,
        controlplane::PD_CLIENT_PORT
    );
    let active_stores = controlplane::service::count_active_stores(&pd_url);
    if active_stores >= 3 {
        let _ = controlplane::service::adjust_max_replicas(&pd_url, 3);
    }

    tracing::debug!(
        cycle,
        node = ctx.node_name.as_str(),
        "reconcile cycle start"
    );

    let results = reconciler::run_all(&ctx).await;

    let total_actions: usize = results
        .iter()
        .map(|r| r.created + r.deleted + r.updated)
        .sum();
    let total_failures: usize = results.iter().map(|r| r.failed).sum();

    if total_actions > 0 || total_failures > 0 {
        tracing::info!(
            cycle,
            actions = total_actions,
            failures = total_failures,
            "reconcile cycle done"
        );
    } else {
        tracing::debug!(cycle, "reconcile cycle done — in sync");
    }

    Ok(results)
}
