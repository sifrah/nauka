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

use nauka_core::error::NaukaError;

use super::peer::Peer;
use super::peering::{PeerAnnounce, PeerInfo, PeerRemove};
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
}

/// Default announce port offset from WireGuard port.
pub const ANNOUNCE_PORT_OFFSET: u16 = 2;

/// Default timeout for each announce connection.
const ANNOUNCE_TIMEOUT_SECS: u64 = 5;

/// Broadcast a new peer to all existing peers.
///
/// Connects to each peer's announce port (mesh_ipv6:wg_port+2) and sends
/// a PeerAnnounce message. Best-effort: failures are logged but don't
/// stop the broadcast.
///
/// Returns (successes, failures).
pub async fn broadcast_new_peer(
    new_peer: &PeerInfo,
    announced_by: &str,
    existing_peers: &[Peer],
    wg_port: u16,
) -> (usize, usize) {
    let msg = AnnounceMessage::Announce(PeerAnnounce {
        peer: new_peer.clone(),
        announced_by: announced_by.to_string(),
    });

    let mut successes = 0;
    let mut failures = 0;

    for peer in existing_peers {
        let addr = format!("[{}]:{}", peer.mesh_ipv6, wg_port + ANNOUNCE_PORT_OFFSET);

        match send_message(&addr, &msg).await {
            Ok(()) => {
                tracing::info!(
                    peer = %peer.name,
                    new_peer = %new_peer.name,
                    "announced new peer"
                );
                successes += 1;
            }
            Err(e) => {
                tracing::warn!(
                    peer = %peer.name,
                    error = %e,
                    "failed to announce new peer"
                );
                failures += 1;
            }
        }
    }

    (successes, failures)
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

/// Broadcast a peer removal to all existing peers (best-effort).
/// Called during `leave` before tearing down the network.
pub async fn broadcast_peer_remove(
    name: &str,
    wg_public_key: &str,
    existing_peers: &[Peer],
    wg_port: u16,
) -> (usize, usize) {
    let msg = AnnounceMessage::Remove(PeerRemove {
        name: name.to_string(),
        wg_public_key: wg_public_key.to_string(),
    });

    let mut successes = 0;
    let mut failures = 0;

    for peer in existing_peers {
        let addr = format!("[{}]:{}", peer.mesh_ipv6, wg_port + ANNOUNCE_PORT_OFFSET);
        match send_message(&addr, &msg).await {
            Ok(()) => {
                tracing::info!(peer = %peer.name, leaving = %name, "sent peer removal");
                successes += 1;
            }
            Err(e) => {
                tracing::warn!(peer = %peer.name, error = %e, "failed to send peer removal");
                failures += 1;
            }
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
/// Opens DB per-request to avoid lock contention.
pub async fn listen(
    db_opener: impl Fn() -> Result<nauka_state::LayerDb, NaukaError>,
    bind_addr: std::net::SocketAddr,
) -> Result<(), NaukaError> {
    let listener = tokio::net::TcpListener::bind(bind_addr)
        .await
        .map_err(|e| NaukaError::internal(format!("failed to bind announce port: {e}")))?;

    let tls_config = super::tls::server_config()?;
    let tls_acceptor = tokio_rustls::TlsAcceptor::from(tls_config);

    tracing::info!(addr = %bind_addr, "announce listener started (TLS)");

    loop {
        match listener.accept().await {
            Ok((tcp_stream, peer_addr)) => {
                // TLS handshake
                let mut stream = match tls_acceptor.accept(tcp_stream).await {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::warn!(peer = %peer_addr, error = %e, "announce TLS handshake failed");
                        continue;
                    }
                };

                let db = match db_opener() {
                    Ok(db) => db,
                    Err(e) => {
                        tracing::warn!(error = %e, "announce: failed to open DB");
                        continue;
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
            }
            Err(e) => {
                tracing::warn!(error = %e, "announce accept error");
            }
        }
    }
}

/// Handle an incoming announce message (add or remove).
async fn handle_message(
    stream: &mut (impl tokio::io::AsyncReadExt + tokio::io::AsyncWriteExt + Unpin),
    db: &nauka_state::LayerDb,
) -> Result<String, NaukaError> {
    let msg: AnnounceMessage = read_json(stream).await?;

    match msg {
        AnnounceMessage::Announce(announce) => handle_peer_announce(announce, db).await,
        AnnounceMessage::Remove(remove) => handle_peer_remove(remove, db),
    }
}

/// Handle a PeerRemove — remove a leaving peer.
fn handle_peer_remove(remove: PeerRemove, db: &nauka_state::LayerDb) -> Result<String, NaukaError> {
    super::peering_server::validate_peer_field(&remove.name, "name")?;

    let mut state = super::state::FabricState::load(db)
        .map_err(|e| NaukaError::internal(e.to_string()))?
        .ok_or_else(|| NaukaError::precondition("not initialized"))?;

    if let Some(removed) = state.peers.remove(&remove.name) {
        tracing::info!(peer = %remove.name, "removed leaving peer");

        state
            .save(db)
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

/// Handle a PeerAnnounce — add or replace a peer.
async fn handle_peer_announce(
    announce: PeerAnnounce,
    db: &nauka_state::LayerDb,
) -> Result<String, NaukaError> {
    // Validate peer data from untrusted source
    super::peering_server::validate_peer_field(&announce.peer.name, "name")?;
    super::peering_server::validate_peer_field(&announce.peer.region, "region")?;
    super::peering_server::validate_peer_field(&announce.peer.zone, "zone")?;
    super::peering_server::validate_peer_field(&announce.peer.wg_public_key, "wg_public_key")?;

    let mut state = super::state::FabricState::load(db)
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
