//! Forge daemon — the main reconciliation loop.

use nauka_hypervisor::controlplane;
use nauka_hypervisor::fabric;
use nauka_state::EmbeddedDb;

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

        // Post-reconcile: health checks every 60s (every other cycle).
        // Runs outside run_cycle so health is reported even when TiKV is down.
        run_health_if_due(cycle).await;

        tokio::time::sleep(std::time::Duration::from_secs(RECONCILE_INTERVAL_SECS)).await;
    }
}

/// Run a single reconciliation cycle (for `nauka forge reconcile`).
pub async fn run_once() -> anyhow::Result<String> {
    let results = run_cycle(1).await?;

    let lines: Vec<String> = results.iter().map(|r| format!("  {r}")).collect();
    Ok(format!("reconciliation complete\n{}", lines.join("\n")))
}

/// Run health checks if this cycle is eligible.
///
/// Loads mesh_ipv6 from local state so it works even when TiKV is down.
async fn run_health_if_due(cycle: u64) {
    let local_db = match EmbeddedDb::open_default().await {
        Ok(db) => db,
        Err(_) => return,
    };
    let state = match fabric::state::FabricState::load(&local_db).await {
        Ok(Some(s)) => s,
        _ => return,
    };
    // Release the flock before the health helper does its own on-disk
    // lookups.
    let _ = local_db.shutdown().await;
    reconciler::health::run_if_due(cycle, &state.hypervisor.mesh_ipv6).await;
}

/// Execute one reconciliation cycle.
async fn run_cycle(cycle: u64) -> anyhow::Result<Vec<crate::types::ReconcileResult>> {
    // Pre-flight: recover PD/TiKV if they crashed after a data wipe.
    // This must run before connect() since PD/TiKV being down would fail the connection.
    {
        let local_db = EmbeddedDb::open_default().await?;
        let pre_state = fabric::state::FabricState::load(&local_db)
            .await
            .ok()
            .flatten();
        // Drop our handle now so `controlplane::connect()` (which opens its
        // own EmbeddedDb to read PD endpoints) doesn't contend on the flock.
        let _ = local_db.shutdown().await;

        if let Some(state) = pre_state {
            let peer_ipv6s: Vec<std::net::Ipv6Addr> =
                state.peers.peers.iter().map(|p| p.mesh_ipv6).collect();

            // Phase 1: if local PD is healthy but has dead peers, restore quorum
            if controlplane::service::recover_pd_quorum(&state.hypervisor.mesh_ipv6) {
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            }

            // Phase 2: if local PD is down, rejoin cluster via a healthy peer
            if controlplane::service::recover_stale_pd_member(
                &state.hypervisor.mesh_ipv6,
                &state.hypervisor.name,
                &peer_ipv6s,
            ) {
                // Give PD time to rejoin the cluster
                tokio::time::sleep(std::time::Duration::from_secs(10)).await;
            }

            if controlplane::service::recover_stale_store(&state.hypervisor.mesh_ipv6) {
                // Give TiKV time to start and register with PD
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            }

            // Phase 4: remove zombie PD members (unhealthy >5 min)
            reconciler::pd::cleanup_zombie_members(&state.hypervisor.mesh_ipv6);

            // Phase 5: sync TiKV PD endpoints with fabric state
            if reconciler::tikv_endpoints::sync_pd_endpoints(
                &state.hypervisor.mesh_ipv6,
                &peer_ipv6s,
            ) {
                // Give TiKV time to restart and reconnect to PD
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            }
        }
    }

    // Connect to TiKV
    let db = controlplane::connect().await?;

    // Load this node's identity
    let local_db = EmbeddedDb::open_default().await?;
    let state = fabric::state::FabricState::load(&local_db)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?
        .ok_or_else(|| anyhow::anyhow!("not initialized"))?;
    let _ = local_db.shutdown().await;

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
        // P2.14 (sifrah/nauka#218): forge holds the inner EmbeddedDb
        // directly; the ClusterDb wrapper only survives for the PD
        // health checks below, which still need TiKV-side helpers.
        db: db.embedded().clone(),
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
