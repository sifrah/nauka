//! Hypervisor daemon — long-lived process that owns the local
//! `bootstrap.skv` handle and hosts every listener/loop that used to
//! serialise on its OS flock.
//!
//! The daemon is installed by `nauka hypervisor init` / `join` as
//! `nauka.service` on the node (see [`install_service`]). Its
//! responsibilities are:
//!
//! 1. Open `EmbeddedDb` **once** on startup and keep the handle for
//!    its entire lifetime. Every in-process task (peering, announce,
//!    health, reconcile, control socket) gets a `.clone()` of the
//!    handle, so concurrent operations run against one `Datastore`
//!    with no per-request flock dance.
//! 2. Run the peering TCP listener on `wg_port + 1`, the announce
//!    TCP listener on `wg_port + 2`, the WireGuard health loop, the
//!    periodic mesh reconciliation loop, and the local Unix control
//!    socket concurrently.
//! 3. Watch a `tokio::sync::watch::channel<bool>` for SIGTERM /
//!    SIGINT / `ControlRequest::Shutdown`. On shutdown, every task
//!    exits cleanly, we drain them, and **then** call
//!    `EmbeddedDb::shutdown()` so the `LOCK` file is released before
//!    systemd restarts us.

use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use tokio::sync::watch;
use tokio::task::JoinSet;

use nauka_core::error::NaukaError;
use nauka_state::EmbeddedDb;

use super::control;
use super::state::FabricState;
use super::{announce, health, ops, peering_server};

/// systemd unit name (without the `.service` suffix).
pub const DAEMON_SERVICE: &str = "nauka";

/// Absolute path of the generated systemd unit.
pub const DAEMON_UNIT_PATH: &str = "/etc/systemd/system/nauka.service";

/// Legacy announce-listener unit — auto-removed by [`install_service`]
/// so upgrades from a pre-#299 binary don't leave two services
/// fighting over the announce port and the bootstrap flock.
const LEGACY_ANNOUNCE_UNIT_PATH: &str = "/etc/systemd/system/nauka-announce.service";
const LEGACY_ANNOUNCE_SERVICE: &str = "nauka-announce";

/// How long the main `listen` future will wait between accepts
/// before surfacing an idle timeout. In the daemon we effectively
/// never time out — ten years is well inside `tokio::time::Instant`
/// safety margins and much longer than any realistic uptime.
const NEVER_IDLE: Duration = Duration::from_secs(60 * 60 * 24 * 365 * 10);

/// Initial delay before the first mesh reconciliation pass — lets
/// startup joins settle so the reconcile pass has something to do.
const RECONCILE_WARMUP: Duration = Duration::from_secs(30);

/// Run the hypervisor daemon. Blocks until SIGTERM / SIGINT /
/// `ControlRequest::Shutdown`, then drains all child tasks and
/// returns.
pub async fn run() -> Result<(), NaukaError> {
    let db = EmbeddedDb::open_default()
        .await
        .map_err(|e| NaukaError::internal(format!("daemon: open bootstrap.skv: {e}")))?;

    let state = FabricState::load(&db)
        .await
        .map_err(|e| NaukaError::internal(e.to_string()))?
        .ok_or_else(|| {
            NaukaError::precondition(
                "daemon started but no fabric state found. \
                 Run 'nauka hypervisor init' or 'join' first.",
            )
        })?;

    let secret: nauka_core::crypto::MeshSecret = state
        .secret
        .parse()
        .map_err(|e| NaukaError::internal(format!("daemon: invalid secret: {e}")))?;
    let pin = secret.derive_pin();
    let wg_port = state.hypervisor.wg_port;
    let peering_port = wg_port + 1;
    let announce_port = wg_port + 2;

    tracing::info!(
        node = %state.hypervisor.name,
        mesh_ipv6 = %state.hypervisor.mesh_ipv6,
        peering_port,
        announce_port,
        "hypervisor daemon starting"
    );

    let peering_addr: SocketAddr = format!("[::]:{peering_port}")
        .parse()
        .map_err(|_| NaukaError::internal("daemon: invalid peering bind address"))?;
    let announce_addr: SocketAddr = format!("[::]:{announce_port}")
        .parse()
        .map_err(|_| NaukaError::internal("daemon: invalid announce bind address"))?;

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    spawn_signal_handler(shutdown_tx.clone());

    let mut tasks: JoinSet<()> = JoinSet::new();

    // Peering listener — unlimited accepts, no idle timeout.
    {
        let db = db.clone();
        let rx = shutdown_rx.clone();
        let pin = pin.clone();
        tasks.spawn(async move {
            match peering_server::listen(db, pin, peering_addr, NEVER_IDLE, 0, rx).await {
                Ok(count) => tracing::info!(accepted = count, "peering listener exited"),
                Err(e) => tracing::warn!(error = %e, "peering listener stopped with error"),
            }
        });
    }

    // Announce listener — shares the same db handle.
    {
        let db = db.clone();
        let rx = shutdown_rx.clone();
        tasks.spawn(async move {
            if let Err(e) = announce::listen(db, announce_addr, rx).await {
                tracing::warn!(error = %e, "announce listener stopped with error");
            }
        });
    }

    // Health loop — refreshes peer reachability from `wg show dump`.
    {
        let db = db.clone();
        let rx = shutdown_rx.clone();
        tasks.spawn(async move {
            health::run_loop(
                db,
                health::DEFAULT_INTERVAL_SECS,
                health::DEFAULT_STALE_THRESHOLD_SECS,
                rx,
            )
            .await;
        });
    }

    // Periodic mesh reconciliation.
    {
        let db = db.clone();
        let mut rx = shutdown_rx.clone();
        tasks.spawn(async move {
            // Skippable warmup: exit immediately if shutdown already fired.
            tokio::select! {
                biased;
                _ = rx.changed() => {
                    if *rx.borrow() { return; }
                }
                _ = tokio::time::sleep(RECONCILE_WARMUP) => {}
            }
            let interval = Duration::from_secs(ops::RECONCILE_INTERVAL_SECS);
            loop {
                ops::reconcile_mesh(&db, wg_port).await;
                tokio::select! {
                    biased;
                    _ = rx.changed() => {
                        if *rx.borrow() { break; }
                    }
                    _ = tokio::time::sleep(interval) => {}
                }
            }
        });
    }

    // Control socket — lets the CLI and `leave` talk to us without
    // touching bootstrap.skv directly.
    {
        let db = db.clone();
        let trigger = shutdown_tx.clone();
        let rx = shutdown_rx.clone();
        tasks.spawn(async move {
            if let Err(e) = control::run_control_server(db, trigger, rx).await {
                tracing::warn!(error = %e, "control socket stopped with error");
            }
        });
    }

    // Wait for shutdown. The channel fires on SIGTERM/SIGINT or when
    // any task (the control server, typically) flips it after
    // receiving `ControlRequest::Shutdown`.
    let mut wait_rx = shutdown_rx.clone();
    while !*wait_rx.borrow() {
        if wait_rx.changed().await.is_err() {
            // Sender dropped — treat as shutdown.
            break;
        }
    }

    tracing::info!("hypervisor daemon shutting down");

    // Drain spawned tasks — they all watch the same channel and will
    // exit shortly. Draining ensures every `db` clone is dropped
    // before we call `shutdown()` on the last one, which matters
    // because `EmbeddedDb::shutdown` polls the SurrealKV LOCK file
    // for release and would hang if another clone were still alive.
    while let Some(res) = tasks.join_next().await {
        if let Err(e) = res {
            if !e.is_cancelled() {
                tracing::warn!(error = %e, "daemon task join error");
            }
        }
    }

    if let Err(e) = db.shutdown().await {
        tracing::warn!(error = %e, "daemon: EmbeddedDb shutdown failed");
    }

    tracing::info!("hypervisor daemon stopped");
    Ok(())
}

/// Wire SIGTERM and SIGINT onto the shutdown watch channel. Runs as
/// a detached task — the watch sender is the only state we care
/// about, so there's nothing to join on.
fn spawn_signal_handler(tx: watch::Sender<bool>) {
    tokio::spawn(async move {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            let mut sigterm = match signal(SignalKind::terminate()) {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!(error = %e, "daemon signal handler install failed");
                    return;
                }
            };
            tokio::select! {
                _ = sigterm.recv() => tracing::info!("daemon: SIGTERM received"),
                _ = tokio::signal::ctrl_c() => tracing::info!("daemon: SIGINT received"),
            }
        }
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
        }
        let _ = tx.send(true);
    });
}

// ═══════════════════════════════════════════════════
// systemd unit generation / install / uninstall
// ═══════════════════════════════════════════════════

/// Generate the systemd unit for the hypervisor daemon.
///
/// - `RuntimeDirectory=nauka` tells systemd to create `/run/nauka`
///   (mode 0o750 via `RuntimeDirectoryMode`) before `ExecStart` runs,
///   and to clean it up when the service stops. That's where the
///   control socket lives.
/// - `Restart=on-failure` restarts the daemon if it crashes but does
///   not restart it after a clean `ControlRequest::Shutdown` (exit 0).
///   This matters during `leave`: the CLI sends Shutdown, the daemon
///   exits 0, systemd leaves it stopped, and then `uninstall_service`
///   removes the unit file.
fn generate_daemon_unit() -> String {
    r#"[Unit]
Description=Nauka Hypervisor Daemon
Documentation=https://github.com/sifrah/nauka
After=network-online.target nauka-wg.service
Wants=network-online.target
Requires=nauka-wg.service

[Service]
Type=simple
ExecStart=/usr/local/bin/nauka hypervisor daemon
Restart=on-failure
RestartSec=5
RuntimeDirectory=nauka
RuntimeDirectoryMode=0750

[Install]
WantedBy=multi-user.target
"#
    .to_string()
}

/// Install and start `nauka.service`.
///
/// Also migrates: if the legacy `nauka-announce.service` is present
/// from a pre-#299 binary, it is stopped, disabled, and removed
/// before the new unit is written so the two services don't fight
/// over the announce port (`wg_port + 2`) or the bootstrap flock.
pub fn install_service() -> Result<(), NaukaError> {
    migrate_from_announce_service();

    std::fs::write(DAEMON_UNIT_PATH, generate_daemon_unit()).map_err(NaukaError::from)?;
    run_systemctl(&["daemon-reload"])?;
    run_systemctl(&["enable", "--now", DAEMON_SERVICE])?;
    Ok(())
}

/// Remove any pre-#299 `nauka-announce.service` unit left behind by
/// an older binary. Called by [`install_service`] before writing the
/// new unit so the migration is transparent.
fn migrate_from_announce_service() {
    if !Path::new(LEGACY_ANNOUNCE_UNIT_PATH).exists() {
        return;
    }
    tracing::info!("migrating legacy nauka-announce.service -> nauka.service");
    let _ = run_systemctl(&["disable", "--now", LEGACY_ANNOUNCE_SERVICE]);
    let _ = std::fs::remove_file(LEGACY_ANNOUNCE_UNIT_PATH);
    let _ = run_systemctl(&["daemon-reload"]);
}

/// Stop + disable + remove `nauka.service`. Idempotent.
pub fn uninstall_service() -> Result<(), NaukaError> {
    let _ = run_systemctl(&["disable", "--now", DAEMON_SERVICE]);
    let _ = std::fs::remove_file(DAEMON_UNIT_PATH);
    let _ = run_systemctl(&["daemon-reload"]);
    Ok(())
}

/// `true` iff the unit file is present on disk.
pub fn is_service_installed() -> bool {
    Path::new(DAEMON_UNIT_PATH).exists()
}

/// `true` iff `systemctl is-active nauka` returns 0.
pub fn is_service_active() -> bool {
    std::process::Command::new("systemctl")
        .args(["is-active", "--quiet", DAEMON_SERVICE])
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Stop the service if it is installed. No-op otherwise.
pub fn stop_service() -> Result<(), NaukaError> {
    if !is_service_installed() {
        return Ok(());
    }
    run_systemctl(&["stop", DAEMON_SERVICE])
}

/// Start the service if it is installed. No-op otherwise.
pub fn start_service() -> Result<(), NaukaError> {
    if !is_service_installed() {
        return Ok(());
    }
    run_systemctl(&["start", DAEMON_SERVICE])
}

/// Ask the running daemon to shut itself down via the control
/// socket, then wait up to `timeout` for the socket file to
/// disappear (which happens when the daemon's main task returns
/// from `run` and `server::run` deletes the socket path on its way
/// out).
///
/// Returns:
/// - `Ok(true)` — a daemon was running, accepted the shutdown, and
///   exited within the timeout.
/// - `Ok(false)` — no daemon was running.
/// - `Err(_)` — a daemon was running but either rejected the
///   request or failed to exit within `timeout`.
pub async fn request_shutdown_and_wait(timeout: Duration) -> Result<bool, NaukaError> {
    use super::control;

    match control::send(&control::ControlRequest::Shutdown).await {
        Ok(_) => {
            let socket = control::socket_path();
            let start = std::time::Instant::now();
            while start.elapsed() < timeout {
                if !socket.exists() {
                    return Ok(true);
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(NaukaError::internal(
                "daemon did not exit within timeout after shutdown request",
            ))
        }
        Err(control::ClientError::SocketMissing)
        | Err(control::ClientError::ConnectRefused(_)) => Ok(false),
        Err(e) => Err(NaukaError::internal(format!("daemon shutdown: {e}"))),
    }
}

fn run_systemctl(args: &[&str]) -> Result<(), NaukaError> {
    let output = std::process::Command::new("systemctl")
        .args(args)
        .output()
        .map_err(|e| NaukaError::internal(format!("systemctl failed: {e}")))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(NaukaError::internal(format!(
            "systemctl {} failed: {}",
            args.join(" "),
            stderr.trim()
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_unit_contains_required_sections() {
        let unit = generate_daemon_unit();
        assert!(unit.contains("[Unit]"));
        assert!(unit.contains("[Service]"));
        assert!(unit.contains("[Install]"));
        assert!(unit.contains("ExecStart=/usr/local/bin/nauka hypervisor daemon"));
        assert!(unit.contains("RuntimeDirectory=nauka"));
        assert!(unit.contains("RuntimeDirectoryMode=0750"));
        assert!(unit.contains("Requires=nauka-wg.service"));
    }

    #[test]
    fn is_installed_returns_false_by_default() {
        // In test environments the real system unit never exists.
        if Path::new(DAEMON_UNIT_PATH).exists() {
            return;
        }
        assert!(!is_service_installed());
    }

    #[test]
    fn never_idle_is_sane() {
        // Must be well under i64::MAX seconds so tokio's Instant math
        // never overflows, but long enough that no realistic uptime
        // triggers the idle-timeout branch of `peering_server::listen`.
        assert!(NEVER_IDLE.as_secs() > 60 * 60 * 24 * 365); // > 1 year
        assert!(NEVER_IDLE.as_secs() < i64::MAX as u64);
    }
}
