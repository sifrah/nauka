//! Peer announcement — broadcasts new peers to all existing mesh members.
//!
//! When a new node joins, the accepting node sends a PeerAnnounce message
//! to every existing peer so they can add the new node to their WireGuard config.
//!
//! Protocol: TCP connect → send PeerAnnounce → close.
//! Same length-prefixed JSON as the peering protocol.
//!
//! Announcements are best-effort: if a peer is unreachable, we skip it
//! and rely on the next health check or manual sync.

use std::time::Duration;

use tokio::sync::watch;

use nauka_core::error::NaukaError;
use nauka_state::EmbeddedDb;

use super::peer::Peer;
use super::peering::{PeerAnnounce, PeerInfo, PeerRemove, PromoteToPd, StateChange};
use super::peering_server::{read_json, write_json};
use super::service;

/// Envelope for announce protocol messages.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type")]
pub enum AnnounceMessage {
    #[serde(rename = "announce")]
    Announce(PeerAnnounce),
    #[serde(rename = "remove")]
    Remove(PeerRemove),
    #[serde(rename = "state_change")]
    StateChange(StateChange),
    #[serde(rename = "promote_to_pd")]
    PromoteToPd(PromoteToPd),
}

/// Default announce port offset from WireGuard port.
pub const ANNOUNCE_PORT_OFFSET: u16 = 2;

/// Default timeout for each announce connection.
const ANNOUNCE_TIMEOUT_SECS: u64 = 5;

/// Broadcast a new peer to all existing peers (in parallel).
///
/// Connects to each peer's announce port (mesh_ipv6:wg_port+2) and sends
/// a PeerAnnounce message. Best-effort: failures are logged but don't
/// stop the broadcast. Skips peers marked as unreachable.
///
/// Returns (successes, failures).
pub async fn broadcast_new_peer(
    new_peer: &PeerInfo,
    announced_by: &str,
    existing_peers: &[Peer],
    _wg_port: u16,
) -> (usize, usize) {
    let msg = AnnounceMessage::Announce(PeerAnnounce {
        peer: new_peer.clone(),
        announced_by: announced_by.to_string(),
    });

    // Skip unreachable peers — no point waiting for a timeout
    let reachable: Vec<_> = existing_peers
        .iter()
        .filter(|p| p.status != super::peer::PeerStatus::Unreachable)
        .collect();

    // Broadcast in parallel — don't let one dead peer block the rest
    let mut handles = Vec::with_capacity(reachable.len());
    for peer in &reachable {
        let addr = format!(
            "[{}]:{}",
            peer.mesh_ipv6,
            peer.wg_port + ANNOUNCE_PORT_OFFSET
        );
        let peer_name = peer.name.clone();
        let new_peer_name = new_peer.name.clone();
        let msg = msg.clone();
        handles.push(tokio::spawn(async move {
            match send_message(&addr, &msg).await {
                Ok(()) => {
                    tracing::info!(
                        peer = %peer_name,
                        new_peer = %new_peer_name,
                        "announced new peer"
                    );
                    true
                }
                Err(e) => {
                    tracing::warn!(
                        peer = %peer_name,
                        error = %e,
                        "failed to announce new peer"
                    );
                    false
                }
            }
        }));
    }

    let mut successes = 0;
    let mut failures = 0;
    for handle in handles {
        match handle.await {
            Ok(true) => successes += 1,
            _ => failures += 1,
        }
    }

    (successes, failures)
}

/// Send a PD promotion request to a TiKV-only peer (used during scale-up).
///
/// `addr` is "[ipv6]:announce_port" of the receiving peer. Best-effort:
/// caller must verify the promotion took effect (e.g. by polling PD members).
pub async fn send_promote_to_pd(
    addr: &str,
    promote: &PromoteToPd,
) -> Result<(), NaukaError> {
    let msg = AnnounceMessage::PromoteToPd(promote.clone());
    send_message(addr, &msg).await
}

/// Send an announce message to one peer (over TLS).
async fn send_message(addr: &str, msg: &AnnounceMessage) -> Result<(), NaukaError> {
    let timeout = Duration::from_secs(ANNOUNCE_TIMEOUT_SECS);

    let tcp_stream = tokio::time::timeout(timeout, tokio::net::TcpStream::connect(addr))
        .await
        .map_err(|_| NaukaError::timeout("announce connection", ANNOUNCE_TIMEOUT_SECS))?
        .map_err(|e| NaukaError::network(format!("announce connect failed: {e}")))?;

    // TLS handshake (TOFU — same model as peering)
    let tls_config = super::tls::client_config();
    let connector = tokio_rustls::TlsConnector::from(tls_config);
    let server_name = rustls::pki_types::ServerName::try_from("nauka-peering")
        .map_err(|e| NaukaError::internal(format!("TLS server name: {e}")))?;

    let mut stream = connector
        .connect(server_name, tcp_stream)
        .await
        .map_err(|e| NaukaError::network(format!("announce TLS handshake failed: {e}")))?;

    write_json(&mut stream, msg).await?;

    Ok(())
}

/// Broadcast a peer removal to all existing peers in parallel (best-effort).
/// Called during `leave` before tearing down the network.
pub async fn broadcast_peer_remove(
    name: &str,
    wg_public_key: &str,
    existing_peers: &[Peer],
    _wg_port: u16,
) -> (usize, usize) {
    let msg = AnnounceMessage::Remove(PeerRemove {
        name: name.to_string(),
        wg_public_key: wg_public_key.to_string(),
    });

    let mut handles = Vec::with_capacity(existing_peers.len());
    for peer in existing_peers {
        let addr = format!(
            "[{}]:{}",
            peer.mesh_ipv6,
            peer.wg_port + ANNOUNCE_PORT_OFFSET
        );
        let peer_name = peer.name.clone();
        let leaving_name = name.to_string();
        let msg = msg.clone();
        handles.push(tokio::spawn(async move {
            match send_message(&addr, &msg).await {
                Ok(()) => {
                    tracing::info!(peer = %peer_name, leaving = %leaving_name, "sent peer removal");
                    true
                }
                Err(e) => {
                    tracing::warn!(peer = %peer_name, error = %e, "failed to send peer removal");
                    false
                }
            }
        }));
    }

    let mut successes = 0;
    let mut failures = 0;
    for handle in handles {
        match handle.await {
            Ok(true) => successes += 1,
            _ => failures += 1,
        }
    }

    (successes, failures)
}

/// Broadcast a state change (drain/enable) to all peers in parallel (best-effort).
/// Returns (successes, failures).
pub async fn broadcast_state_change(
    change: &StateChange,
    existing_peers: &[Peer],
) -> (usize, usize) {
    let msg = AnnounceMessage::StateChange(change.clone());

    let reachable: Vec<_> = existing_peers
        .iter()
        .filter(|p| p.status != super::peer::PeerStatus::Unreachable)
        .collect();

    let mut handles = Vec::with_capacity(reachable.len());
    for peer in &reachable {
        let addr = format!(
            "[{}]:{}",
            peer.mesh_ipv6,
            peer.wg_port + ANNOUNCE_PORT_OFFSET
        );
        let peer_name = peer.name.clone();
        let node_name = change.name.clone();
        let new_state = change.node_state;
        let msg = msg.clone();
        handles.push(tokio::spawn(async move {
            match send_message(&addr, &msg).await {
                Ok(()) => {
                    tracing::info!(
                        peer = %peer_name,
                        node = %node_name,
                        state = %new_state,
                        "sent state change"
                    );
                    true
                }
                Err(e) => {
                    tracing::warn!(
                        peer = %peer_name,
                        error = %e,
                        "failed to send state change"
                    );
                    false
                }
            }
        }));
    }

    let mut successes = 0;
    let mut failures = 0;
    for handle in handles {
        match handle.await {
            Ok(true) => successes += 1,
            _ => failures += 1,
        }
    }

    (successes, failures)
}

/// Listen for peer announcements and apply them (over TLS).
///
/// Runs on wg_port + 2. When an announcement arrives:
/// 1. Add the new peer to our PeerList
/// 2. Update WireGuard config
/// 3. Persist state
///
/// The caller supplies a long-lived [`EmbeddedDb`] handle; each incoming
/// message is serviced in a freshly spawned task with a clone of that
/// handle, so concurrent announces run in parallel against the same
/// underlying `Datastore` with no flock contention.
///
/// Exits when `shutdown` transitions to `true`.
pub async fn listen(
    db: EmbeddedDb,
    bind_addr: std::net::SocketAddr,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), NaukaError> {
    let listener = tokio::net::TcpListener::bind(bind_addr)
        .await
        .map_err(|e| NaukaError::internal(format!("failed to bind announce port: {e}")))?;

    let tls_config = super::tls::server_config()?;
    let tls_acceptor = tokio_rustls::TlsAcceptor::from(tls_config);

    tracing::info!(addr = %bind_addr, "announce listener started (TLS)");

    loop {
        if *shutdown.borrow() {
            break;
        }

        tokio::select! {
            biased;
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    tracing::info!("announce listener shutting down");
                    break;
                }
            }
            accept_result = listener.accept() => {
                match accept_result {
                    Ok((tcp_stream, peer_addr)) => {
                        let tls_acceptor = tls_acceptor.clone();
                        let db = db.clone();
                        tokio::spawn(async move {
                            let mut stream = match tls_acceptor.accept(tcp_stream).await {
                                Ok(s) => s,
                                Err(e) => {
                                    tracing::warn!(peer = %peer_addr, error = %e, "announce TLS handshake failed");
                                    return;
                                }
                            };

                            match handle_message(&mut stream, &db).await {
                                Ok(msg) => {
                                    tracing::info!(from = %peer_addr, result = %msg, "announce handled");
                                }
                                Err(e) => {
                                    tracing::warn!(from = %peer_addr, error = %e, "announce failed");
                                }
                            }
                        });
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "announce accept error");
                    }
                }
            }
        }
    }

    Ok(())
}

/// Handle an incoming announce message (add or remove).
async fn handle_message(
    stream: &mut (impl tokio::io::AsyncReadExt + tokio::io::AsyncWriteExt + Unpin),
    db: &EmbeddedDb,
) -> Result<String, NaukaError> {
    let msg: AnnounceMessage = read_json(stream).await?;

    match msg {
        AnnounceMessage::Announce(announce) => handle_peer_announce(announce, db).await,
        AnnounceMessage::Remove(remove) => handle_peer_remove(remove, db).await,
        AnnounceMessage::StateChange(change) => handle_state_change(change, db).await,
        AnnounceMessage::PromoteToPd(promote) => handle_promote_to_pd(promote, db).await,
    }
}

/// Handle a PromoteToPd — install PD locally and join the existing cluster.
///
/// Validates that `promote.target_name` matches our own name (defence against
/// a misdirected or replayed message), checks that PD isn't already running
/// (idempotency), then delegates to `controlplane::ops::promote_self_to_pd`
/// which rewrites the systemd units and starts PD in `--join` mode.
///
/// The promotion work is sync and blocks on `systemctl` + a PD readiness
/// probe. It runs on a `spawn_blocking` task so the announce listener's
/// async runtime isn't stalled.
async fn handle_promote_to_pd(
    promote: PromoteToPd,
    db: &EmbeddedDb,
) -> Result<String, NaukaError> {
    super::peering_server::validate_peer_field(&promote.target_name, "target_name")?;

    let state = super::state::FabricState::load(db)
        .await
        .map_err(|e| NaukaError::internal(e.to_string()))?
        .ok_or_else(|| NaukaError::precondition("not initialized"))?;

    if state.hypervisor.name != promote.target_name {
        return Err(NaukaError::precondition(format!(
            "promote_to_pd targets '{}' but we are '{}'",
            promote.target_name, state.hypervisor.name
        )));
    }

    if crate::controlplane::service::pd_is_active() {
        tracing::info!(
            node = %state.hypervisor.name,
            "promote_to_pd: PD already active, skipping"
        );
        return Ok(format!("{} already PD", state.hypervisor.name));
    }

    let node_name = state.hypervisor.name.clone();
    let mesh_ipv6 = state.hypervisor.mesh_ipv6;
    let primary_pd = promote.primary_pd_url.clone();
    let pd_endpoints = promote.pd_endpoints.clone();
    let requested_by = promote.requested_by.clone();

    tracing::info!(
        node = %node_name,
        by = %requested_by,
        "promote_to_pd: starting local PD promotion"
    );

    tokio::task::spawn_blocking(move || {
        crate::controlplane::ops::promote_self_to_pd(
            &node_name,
            &mesh_ipv6,
            &primary_pd,
            &pd_endpoints,
        )
    })
    .await
    .map_err(|e| NaukaError::internal(format!("promote task join: {e}")))??;

    Ok(format!("{} promoted to PD", state.hypervisor.name))
}

/// Handle a PeerRemove — remove a leaving peer.
async fn handle_peer_remove(remove: PeerRemove, db: &EmbeddedDb) -> Result<String, NaukaError> {
    super::peering_server::validate_peer_field(&remove.name, "name")?;

    let mut state = super::state::FabricState::load(db)
        .await
        .map_err(|e| NaukaError::internal(e.to_string()))?
        .ok_or_else(|| NaukaError::precondition("not initialized"))?;

    if let Some(removed) = state.peers.remove(&remove.name) {
        tracing::info!(peer = %remove.name, "removed leaving peer");

        state
            .save(db)
            .await
            .map_err(|e| NaukaError::internal(e.to_string()))?;

        // Update WireGuard config
        let peers_for_wg: Vec<_> = state
            .peers
            .peers
            .iter()
            .map(|p| {
                (
                    p.wg_public_key.clone(),
                    "25".to_string(),
                    p.mesh_ipv6,
                    p.endpoint.clone(),
                )
            })
            .collect();

        let _ = service::update_config(
            &state.hypervisor.wg_private_key,
            state.hypervisor.wg_port,
            &state.hypervisor.mesh_ipv6,
            &peers_for_wg,
        );

        Ok(format!("removed {}", removed.name))
    } else {
        Ok(format!("{} not found, skipped", remove.name))
    }
}

/// Handle a StateChange — update a peer's scheduling state.
async fn handle_state_change(change: StateChange, db: &EmbeddedDb) -> Result<String, NaukaError> {
    super::peering_server::validate_peer_field(&change.name, "name")?;

    let mut state = super::state::FabricState::load(db)
        .await
        .map_err(|e| NaukaError::internal(e.to_string()))?
        .ok_or_else(|| NaukaError::precondition("not initialized"))?;

    // Find the peer by name or public key and update its node_state
    let found = state
        .peers
        .peers
        .iter_mut()
        .find(|p| p.name == change.name || p.wg_public_key == change.wg_public_key);

    if let Some(peer) = found {
        peer.node_state = change.node_state;
        tracing::info!(
            peer = %change.name,
            state = %change.node_state,
            "updated peer scheduling state"
        );

        state
            .save(db)
            .await
            .map_err(|e| NaukaError::internal(e.to_string()))?;

        Ok(format!("{} → {}", change.name, change.node_state))
    } else {
        Ok(format!("{} not found, skipped", change.name))
    }
}

/// Handle a PeerAnnounce — add or replace a peer.
async fn handle_peer_announce(
    announce: PeerAnnounce,
    db: &EmbeddedDb,
) -> Result<String, NaukaError> {
    // Validate peer data from untrusted source
    super::peering_server::validate_peer_field(&announce.peer.name, "name")?;
    super::peering_server::validate_peer_field(&announce.peer.region, "region")?;
    super::peering_server::validate_peer_field(&announce.peer.zone, "zone")?;
    super::peering_server::validate_peer_field(&announce.peer.wg_public_key, "wg_public_key")?;

    let mut state = super::state::FabricState::load(db)
        .await
        .map_err(|e| NaukaError::internal(e.to_string()))?
        .ok_or_else(|| NaukaError::precondition("not initialized"))?;

    let peer_name = announce.peer.name.clone();

    // Skip if exact same key already known
    if state
        .peers
        .find_by_key(&announce.peer.wg_public_key)
        .is_some()
    {
        tracing::debug!(peer = %peer_name, "already known (same key), skipping");
        return Ok(peer_name);
    }

    // Remove stale entry if same name but different key (leave/rejoin)
    if state.peers.find_by_name(&peer_name).is_some() {
        tracing::info!(peer = %peer_name, "replacing stale peer entry (rejoin announce)");
        state.peers.remove(&peer_name);
    }

    // Add the new peer
    let new_peer = Peer::new(
        announce.peer.name,
        announce.peer.region,
        announce.peer.zone,
        announce.peer.wg_public_key,
        announce.peer.wg_port,
        announce.peer.endpoint,
        announce.peer.mesh_ipv6,
    );
    if let Err(e) = state.peers.add(new_peer) {
        tracing::debug!(error = %e, "announce: peer add failed");
        return Ok(peer_name);
    }

    // Persist immediately
    state
        .save(db)
        .await
        .map_err(|e| NaukaError::internal(e.to_string()))?;

    // Update WireGuard config
    let peers_for_wg: Vec<_> = state
        .peers
        .peers
        .iter()
        .map(|p| {
            (
                p.wg_public_key.clone(),
                "25".to_string(),
                p.mesh_ipv6,
                p.endpoint.clone(),
            )
        })
        .collect();

    service::update_config(
        &state.hypervisor.wg_private_key,
        state.hypervisor.wg_port,
        &state.hypervisor.mesh_ipv6,
        &peers_for_wg,
    )?;

    Ok(peer_name)
}

// ═══════════════════════════════════════════════════
// Announce listener systemd service
// ═══════════════════════════════════════════════════

const ANNOUNCE_SERVICE: &str = "nauka-announce";
const ANNOUNCE_UNIT_PATH: &str = "/etc/systemd/system/nauka-announce.service";

/// Generate the systemd unit for the persistent announce listener.
fn generate_announce_unit(port: u16) -> String {
    let announce_port = port + ANNOUNCE_PORT_OFFSET;
    format!(
        r#"[Unit]
Description=Nauka Peer Announce Listener
After=network-online.target nauka-wg.service
Wants=network-online.target
Requires=nauka-wg.service

[Service]
Type=simple
ExecStart=/usr/local/bin/nauka hypervisor announce-listen --port {announce_port}
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
"#
    )
}

/// Install and start the announce listener service.
pub fn install_service(wg_port: u16) -> Result<(), NaukaError> {
    std::fs::write(ANNOUNCE_UNIT_PATH, generate_announce_unit(wg_port))
        .map_err(NaukaError::from)?;
    run_systemctl(&["daemon-reload"])?;
    run_systemctl(&["enable", "--now", ANNOUNCE_SERVICE])?;
    Ok(())
}

/// Start the announce listener service.
pub fn start_service() -> Result<(), NaukaError> {
    if !is_service_installed() {
        return Ok(());
    }
    run_systemctl(&["start", ANNOUNCE_SERVICE])
}

/// Stop the announce listener service.
pub fn stop_service() -> Result<(), NaukaError> {
    if !is_service_installed() {
        return Ok(());
    }
    let _ = run_systemctl(&["stop", ANNOUNCE_SERVICE]);
    Ok(())
}

/// Uninstall the announce listener service.
pub fn uninstall_service() -> Result<(), NaukaError> {
    let _ = run_systemctl(&["disable", "--now", ANNOUNCE_SERVICE]);
    let _ = std::fs::remove_file(ANNOUNCE_UNIT_PATH);
    let _ = run_systemctl(&["daemon-reload"]);
    Ok(())
}

/// Check if the service is installed.
pub fn is_service_installed() -> bool {
    std::path::Path::new(ANNOUNCE_UNIT_PATH).exists()
}

/// Check if the service is active.
pub fn is_service_active() -> bool {
    std::process::Command::new("systemctl")
        .args(["is-active", "--quiet", ANNOUNCE_SERVICE])
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
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
    fn announce_port_offset() {
        assert_eq!(ANNOUNCE_PORT_OFFSET, 2);
        // WG port 51820 → announce port 51822
        let wg_port: u16 = 51820;
        assert_eq!(wg_port + ANNOUNCE_PORT_OFFSET, 51822);
    }

    #[tokio::test]
    async fn broadcast_to_empty_list() {
        let peer_info = PeerInfo {
            name: "new-node".into(),
            region: "eu".into(),
            zone: "fsn1".into(),
            wg_public_key: "key".into(),
            wg_port: 51820,
            endpoint: None,
            mesh_ipv6: "fd01::99".parse().unwrap(),
        };
        let (ok, fail) = broadcast_new_peer(&peer_info, "node-1", &[], 51820).await;
        assert_eq!(ok, 0);
        assert_eq!(fail, 0);
    }

    #[tokio::test]
    async fn announce_roundtrip() {
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let msg: AnnounceMessage = read_json(&mut stream).await.unwrap();
            match msg {
                AnnounceMessage::Announce(a) => {
                    assert_eq!(a.peer.name, "new-node");
                    assert_eq!(a.announced_by, "node-1");
                }
                _ => panic!("expected Announce"),
            }
        });

        let msg = AnnounceMessage::Announce(PeerAnnounce {
            peer: PeerInfo {
                name: "new-node".into(),
                region: "eu".into(),
                zone: "fsn1".into(),
                wg_public_key: "key".into(),
                wg_port: 51820,
                endpoint: None,
                mesh_ipv6: "fd01::99".parse().unwrap(),
            },
            announced_by: "node-1".into(),
        });

        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        write_json(&mut stream, &msg).await.unwrap();
        drop(stream);

        server.await.unwrap();
    }

    #[tokio::test]
    async fn send_message_unreachable() {
        let msg = AnnounceMessage::Announce(PeerAnnounce {
            peer: PeerInfo {
                name: "n".into(),
                region: "eu".into(),
                zone: "fsn1".into(),
                wg_public_key: "k".into(),
                wg_port: 51820,
                endpoint: None,
                mesh_ipv6: "fd01::1".parse().unwrap(),
            },
            announced_by: "test".into(),
        });
        // Connect to a port nothing is listening on
        let result = send_message("127.0.0.1:1", &msg).await;
        assert!(result.is_err());
    }
}
