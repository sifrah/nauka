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

    let ctx = ReconcileContext {
        db,
        hypervisor_id: state.hypervisor.id.as_str().to_string(),
        node_name: state.hypervisor.name.clone(),
        mesh_ipv6: state.hypervisor.mesh_ipv6,
        cycle,
    };

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
