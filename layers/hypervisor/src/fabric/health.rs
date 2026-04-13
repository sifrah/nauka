//! Health check — monitors peer reachability via WireGuard handshakes.
//!
//! Polls `wg show dump` at a configurable interval and updates peer status:
//! - Handshake within threshold → Active
//! - Handshake older than threshold → Unreachable
//!
//! Designed to run as a background tokio task during `peering` or future daemon mode.

use std::future::Future;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use nauka_core::error::NaukaError;
use nauka_state::EmbeddedDb;

use super::state::FabricState;
use super::wg;

/// Default health check interval.
pub const DEFAULT_INTERVAL_SECS: u64 = 30;

/// Default handshake staleness threshold (5 minutes).
/// WireGuard sends a handshake every 2 minutes when there's traffic,
/// so 5 minutes without one means the peer is likely down.
pub const DEFAULT_STALE_THRESHOLD_SECS: u64 = 300;

/// Grace period for newly added peers (5 minutes).
/// A peer that was just added hasn't had time to establish a WireGuard
/// handshake yet — don't mark it unreachable until the grace period expires.
pub const NEW_PEER_GRACE_SECS: u64 = 300;

/// Result of a single health check sweep.
#[derive(Debug, Clone)]
pub struct HealthCheckResult {
    /// Total peers checked.
    pub total: usize,
    /// Peers marked active after this check.
    pub active: usize,
    /// Peers marked unreachable after this check.
    pub unreachable: usize,
    /// Peers whose status changed.
    pub changed: usize,
}

/// Run a single health check sweep.
///
/// Reads WireGuard handshake timestamps, compares against threshold,
/// updates peer status in state, and persists if anything changed.
pub async fn check_once(
    db: &EmbeddedDb,
    stale_threshold_secs: u64,
) -> Result<HealthCheckResult, NaukaError> {
    let mut state = match FabricState::load(db)
        .await
        .map_err(|e| NaukaError::internal(e.to_string()))?
    {
        Some(s) => s,
        None => return Err(NaukaError::precondition("not initialized")),
    };

    if state.peers.is_empty() {
        return Ok(HealthCheckResult {
            total: 0,
            active: 0,
            unreachable: 0,
            changed: 0,
        });
    }

    // Get handshake info from WireGuard
    let handshakes = match wg::get_peer_handshakes() {
        Ok(h) => h,
        Err(_) => {
            // WG interface not up — can't check
            return Ok(HealthCheckResult {
                total: state.peers.len(),
                active: 0,
                unreachable: state.peers.len(),
                changed: 0,
            });
        }
    };

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let mut changed = 0;

    for peer in &mut state.peers.peers {
        let was_active = peer.is_active();

        // Grace period: don't mark a newly added peer as unreachable
        // if it hasn't had time to establish a WireGuard handshake yet.
        let is_new_peer = peer.last_handshake == 0
            && peer.added_at > 0
            && now.saturating_sub(peer.added_at) < NEW_PEER_GRACE_SECS;

        if let Some(wg_peer) = handshakes
            .iter()
            .find(|h| h.public_key == peer.wg_public_key)
        {
            peer.last_handshake = wg_peer.latest_handshake;

            if wg_peer.latest_handshake > 0
                && now.saturating_sub(wg_peer.latest_handshake) <= stale_threshold_secs
            {
                peer.status = super::peer::PeerStatus::Active;
            } else if is_new_peer {
                // Keep Active during grace period — handshake not yet expected
                peer.status = super::peer::PeerStatus::Active;
            } else {
                peer.status = super::peer::PeerStatus::Unreachable;
            }
        } else if is_new_peer {
            // Not in WireGuard yet, but just added — give it time
            peer.status = super::peer::PeerStatus::Active;
        } else {
            // Peer not in WireGuard at all — unreachable
            peer.status = super::peer::PeerStatus::Unreachable;
        }

        if was_active != peer.is_active() {
            changed += 1;
            if peer.is_active() {
                tracing::info!(peer = %peer.name, "peer now active");
            } else {
                tracing::warn!(peer = %peer.name, "peer now unreachable");
            }
        }
    }

    let active = state.peers.active_count();
    let unreachable = state.peers.unreachable_count();
    let total = state.peers.len();

    // Only persist if something changed
    if changed > 0 {
        state
            .save(db)
            .await
            .map_err(|e| NaukaError::internal(e.to_string()))?;
    }

    Ok(HealthCheckResult {
        total,
        active,
        unreachable,
        changed,
    })
}

/// Run the health check loop. Blocks until cancelled.
///
/// Opens a fresh DB connection each sweep to avoid holding the lock.
pub async fn run_loop<F, Fut>(db_opener: F, interval_secs: u64, stale_threshold_secs: u64)
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<EmbeddedDb, NaukaError>>,
{
    let interval = Duration::from_secs(interval_secs);

    loop {
        tokio::time::sleep(interval).await;

        let db = match db_opener().await {
            Ok(db) => db,
            Err(e) => {
                tracing::warn!(error = %e, "health check: failed to open DB");
                continue;
            }
        };

        match check_once(&db, stale_threshold_secs).await {
            Ok(result) => {
                if result.changed > 0 {
                    tracing::info!(
                        total = result.total,
                        active = result.active,
                        unreachable = result.unreachable,
                        changed = result.changed,
                        "health check complete"
                    );
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "health check failed");
            }
        }

        // Explicit shutdown to release the SurrealKV flock before the next
        // iteration opens a new handle.
        let _ = db.shutdown().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn health_check_result_defaults() {
        let r = HealthCheckResult {
            total: 3,
            active: 2,
            unreachable: 1,
            changed: 1,
        };
        assert_eq!(r.total, 3);
        assert_eq!(r.active, 2);
        assert_eq!(r.changed, 1);
    }

    #[test]
    fn constants_sensible() {
        assert_eq!(DEFAULT_INTERVAL_SECS, 30);
        assert_eq!(DEFAULT_STALE_THRESHOLD_SECS, 300);
        // Verify threshold > interval at compile time
        const _: () = assert!(DEFAULT_STALE_THRESHOLD_SECS > DEFAULT_INTERVAL_SECS);
    }

    async fn temp_embedded() -> (tempfile::TempDir, EmbeddedDb) {
        let dir = tempfile::tempdir().unwrap();
        let db = EmbeddedDb::open(&dir.path().join("test.skv"))
            .await
            .unwrap();
        (dir, db)
    }

    #[tokio::test]
    async fn check_once_no_state() {
        let (_d, db) = temp_embedded().await;
        let result = check_once(&db, 300).await;
        assert!(result.is_err()); // not initialized
        db.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn check_once_no_peers() {
        let (_d, db) = temp_embedded().await;

        // Create state with no peers
        let (mesh, secret) = super::super::mesh::create_mesh();
        let hv =
            super::super::mesh::create_hypervisor(&super::super::mesh::CreateHypervisorConfig {
                name: "node-1",
                region: "eu",
                zone: "fsn1",
                port: 51820,
                endpoint: None,
                fabric_interface: "",
                mesh_prefix: &mesh.prefix,
                ipv6_block: None,
                ipv4_public: None,
            })
            .unwrap();
        let state = FabricState {
            mesh,
            hypervisor: hv,
            secret: secret.to_string(),
            peers: super::super::peer::PeerList::new(),
            network_mode: super::super::backend::NetworkMode::default(),
            node_state: super::super::state::NodeState::default(),
            max_pd_members: 3,
        };
        state.save(&db).await.unwrap();

        let result = check_once(&db, 300).await.unwrap();
        assert_eq!(result.total, 0);
        assert_eq!(result.active, 0);
        assert_eq!(result.changed, 0);

        db.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn check_once_with_peers_no_wg() {
        let (_d, db) = temp_embedded().await;

        let (mesh, secret) = super::super::mesh::create_mesh();
        let hv =
            super::super::mesh::create_hypervisor(&super::super::mesh::CreateHypervisorConfig {
                name: "node-1",
                region: "eu",
                zone: "fsn1",
                port: 51820,
                endpoint: None,
                fabric_interface: "",
                mesh_prefix: &mesh.prefix,
                ipv6_block: None,
                ipv4_public: None,
            })
            .unwrap();
        let mut peers = super::super::peer::PeerList::new();
        peers
            .add(super::super::peer::Peer::new(
                "node-2".into(),
                "eu".into(),
                "nbg1".into(),
                "key-2".into(),
                51820,
                None,
                "fd01::2".parse().unwrap(),
            ))
            .unwrap();

        let state = FabricState {
            mesh,
            hypervisor: hv,
            secret: secret.to_string(),
            peers,
            network_mode: super::super::backend::NetworkMode::default(),
            node_state: super::super::state::NodeState::default(),
            max_pd_members: 3,
        };
        state.save(&db).await.unwrap();

        // WG not running → get_peer_handshakes fails → all unreachable, changed=0
        let result = check_once(&db, 300).await.unwrap();
        assert_eq!(result.total, 1);
        assert_eq!(result.unreachable, 1);

        db.shutdown().await.unwrap();
    }
}
