//! Peering TCP server — accepts join requests from new nodes.
//!
//! The listener borrows a long-lived [`EmbeddedDb`] handle from its
//! caller (the hypervisor daemon, or `listen_for_peers` during
//! `init --peering`) and clones it into a per-connection `tokio::spawn`
//! for every incoming join. SurrealDB itself is thread-safe, so
//! concurrent joins run truly in parallel against the same underlying
//! `Datastore` — no OS-level flock serialisation.

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use base64::Engine as _;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio_rustls::TlsAcceptor;

use nauka_core::error::NaukaError;
use nauka_state::EmbeddedDb;

/// Max PIN failures per IP before blocking.
const MAX_PIN_FAILURES: u32 = 5;
/// Duration to block an IP after too many failures.
const PIN_BLOCK_DURATION: Duration = Duration::from_secs(600); // 10 minutes

/// Simple per-IP rate limiter for PIN brute-force protection.
struct PinRateLimiter {
    failures: HashMap<IpAddr, (u32, Instant)>,
}

impl PinRateLimiter {
    fn new() -> Self {
        Self {
            failures: HashMap::new(),
        }
    }

    fn is_blocked(&self, ip: &IpAddr) -> bool {
        if let Some((count, since)) = self.failures.get(ip) {
            if *count >= MAX_PIN_FAILURES && since.elapsed() < PIN_BLOCK_DURATION {
                return true;
            }
        }
        false
    }

    fn record_failure(&mut self, ip: IpAddr) {
        let entry = self.failures.entry(ip).or_insert((0, Instant::now()));
        if entry.1.elapsed() >= PIN_BLOCK_DURATION {
            *entry = (1, Instant::now());
        } else {
            entry.0 += 1;
        }
    }

    fn record_success(&mut self, ip: &IpAddr) {
        self.failures.remove(ip);
    }
}

use super::peer::Peer;
use super::peering::{JoinRequest, JoinResponse, PeerInfo};
use super::service;
use super::state::FabricState;

/// Run the peering listener on `bind_addr`.
///
/// Convenience wrapper that binds a `TcpListener` and forwards to
/// [`serve`]. Integration tests that need to pick the bound port
/// before driving clients can skip the bind and call `serve` directly.
pub async fn listen(
    db: EmbeddedDb,
    pin: String,
    bind_addr: SocketAddr,
    timeout: Duration,
    max_joins: usize,
    shutdown: watch::Receiver<bool>,
) -> Result<usize, NaukaError> {
    let listener = TcpListener::bind(bind_addr)
        .await
        .map_err(|e| NaukaError::internal(format!("failed to bind peering port: {e}")))?;
    serve(listener, db, pin, timeout, max_joins, shutdown).await
}

/// Run the peering accept loop against a pre-bound `TcpListener`.
///
/// Accepts TLS-wrapped join requests and services each one in a freshly
/// spawned task. The supplied `db` handle is cloned into every spawn so
/// concurrent joins share the same underlying `Surreal<Db>` (and
/// therefore never contend on the SurrealKV `LOCK`).
///
/// Termination rules:
/// - If `max_joins > 0`, the accept loop stops once `max_joins`
///   handlers have returned `Ok(_)`. Spawned handlers that are still
///   running are left to finish on their own.
/// - If no connection is accepted for `timeout`, the accept loop exits.
/// - If `shutdown` transitions to `true`, the accept loop exits
///   immediately.
///
/// Returns the number of successfully accepted joins.
pub async fn serve(
    listener: TcpListener,
    db: EmbeddedDb,
    pin: String,
    timeout: Duration,
    max_joins: usize,
    mut shutdown: watch::Receiver<bool>,
) -> Result<usize, NaukaError> {
    let tls_config = super::tls::server_config()?;
    let tls_acceptor = TlsAcceptor::from(tls_config);

    let bind_addr = listener
        .local_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "<unknown>".to_string());
    tracing::info!(addr = %bind_addr, "peering listener started (TLS)");

    let rate_limiter = Arc::new(Mutex::new(PinRateLimiter::new()));
    let accepted = Arc::new(AtomicUsize::new(0));
    let pin = Arc::new(pin);

    loop {
        if max_joins > 0 && accepted.load(Ordering::Acquire) >= max_joins {
            break;
        }
        if *shutdown.borrow() {
            break;
        }

        tokio::select! {
            biased;
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    tracing::info!("peering listener shutting down");
                    break;
                }
            }
            accept_result = tokio::time::timeout(timeout, listener.accept()) => {
                match accept_result {
                    Ok(Ok((tcp_stream, peer_addr))) => {
                        // Cheap per-IP rate-limit check before paying
                        // for the TLS handshake.
                        {
                            let rl = rate_limiter.lock().unwrap_or_else(|e| e.into_inner());
                            if rl.is_blocked(&peer_addr.ip()) {
                                tracing::warn!(peer = %peer_addr, "blocked (too many failed PIN attempts)");
                                continue;
                            }
                        }

                        let db = db.clone();
                        let pin = pin.clone();
                        let tls_acceptor = tls_acceptor.clone();
                        let rate_limiter = rate_limiter.clone();
                        let accepted = accepted.clone();

                        tokio::spawn(async move {
                            let mut stream = match tls_acceptor.accept(tcp_stream).await {
                                Ok(s) => s,
                                Err(e) => {
                                    tracing::warn!(peer = %peer_addr, error = %e, "TLS handshake failed");
                                    return;
                                }
                            };

                            tracing::info!(peer = %peer_addr, "incoming join request (TLS)");

                            match handle_join(&mut stream, &db, pin.as_str(), peer_addr).await {
                                Ok(peer_name) => {
                                    rate_limiter
                                        .lock()
                                        .unwrap_or_else(|e| e.into_inner())
                                        .record_success(&peer_addr.ip());
                                    accepted.fetch_add(1, Ordering::AcqRel);
                                    tracing::info!(peer = %peer_addr, name = %peer_name, "join accepted");
                                }
                                Err(e) => {
                                    rate_limiter
                                        .lock()
                                        .unwrap_or_else(|e| e.into_inner())
                                        .record_failure(peer_addr.ip());
                                    tracing::warn!(peer = %peer_addr, error = %e, "join rejected");
                                }
                            }
                        });
                    }
                    Ok(Err(e)) => {
                        tracing::warn!(error = %e, "accept error");
                    }
                    Err(_) => {
                        tracing::info!("peering listener idle timeout");
                        break;
                    }
                }
            }
        }
    }

    Ok(accepted.load(Ordering::Acquire))
}

/// Handle a single join request on a stream.
async fn handle_join(
    stream: &mut (impl AsyncReadExt + AsyncWriteExt + Unpin),
    db: &EmbeddedDb,
    expected_pin: &str,
    peer_addr: SocketAddr,
) -> Result<String, NaukaError> {
    let mut req = read_json::<JoinRequest>(stream).await?;

    // Log trace ID from the joining node for distributed correlation
    if let Some(ref tid) = req.trace_id {
        tracing::info!(trace_id = %tid, peer = %peer_addr, joiner = %req.name, "received join request with trace context");
    }

    // Validate peer-provided fields against injection (newlines, control chars)
    validate_peer_field(&req.name, "name")?;
    validate_peer_field(&req.region, "region")?;
    validate_peer_field(&req.zone, "zone")?;
    validate_peer_field(&req.wg_public_key, "wg_public_key")?;
    if let Some(ref ep) = req.endpoint {
        validate_peer_field(ep, "endpoint")?;
    }

    // If the joiner didn't specify an endpoint, use their TCP source IP + WG port.
    // Convert IPv4-mapped IPv6 (::ffff:x.x.x.x) back to plain IPv4 for WireGuard.
    if req.endpoint.is_none() {
        let ip = match peer_addr.ip() {
            std::net::IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
                Some(v4) => std::net::IpAddr::V4(v4),
                None => std::net::IpAddr::V6(v6),
            },
            other => other,
        };
        req.endpoint = Some(format!("{ip}:{}", req.wg_port));
    }

    if !super::peering::validate_pin(expected_pin, req.pin.as_deref()) {
        let resp = JoinResponse::rejected("invalid PIN");
        write_json(stream, &resp).await?;
        return Err(NaukaError::permission_denied("invalid PIN"));
    }

    // Derive the new peer's mesh IPv6 *before* taking the write lock
    // so the base64 decode error (if any) surfaces without ever
    // touching the shared state.
    let pub_bytes = base64::engine::general_purpose::STANDARD
        .decode(&req.wg_public_key)
        .map_err(|e| NaukaError::validation(format!("invalid WireGuard key: {e}")))?;

    // ─── Critical section: serialise load+modify+save against
    // every other concurrent state mutator in this process.
    // Without this, two `handle_join` tasks running in parallel
    // both load a snapshot with the same `peers` vec, each add
    // their new peer locally, each save back — and the last
    // writer wins, silently dropping every earlier join. The
    // pre-#299 OS flock hid this by serialising at the process
    // level; with one shared `EmbeddedDb` handle the race is
    // live, which is exactly what the `parallel_join` regression
    // test exercises.
    let write_guard = super::state::write_lock().lock().await;

    let mut state = FabricState::load(db)
        .await
        .map_err(|e| NaukaError::internal(e.to_string()))?
        .ok_or_else(|| NaukaError::precondition("not initialized"))?;

    let self_info = PeerInfo {
        name: state.hypervisor.name.clone(),
        region: state.hypervisor.region.clone(),
        zone: state.hypervisor.zone.clone(),
        wg_public_key: state.hypervisor.wg_public_key.clone(),
        wg_port: state.hypervisor.wg_port,
        endpoint: state.hypervisor.endpoint.clone(),
        mesh_ipv6: state.hypervisor.mesh_ipv6,
    };

    // Filter out any stale entry for the joining node (leave/rejoin scenario)
    let existing_peers: Vec<PeerInfo> = state
        .peers
        .peers
        .iter()
        .filter(|p| p.name != req.name)
        .map(|p| PeerInfo {
            name: p.name.clone(),
            region: p.region.clone(),
            zone: p.zone.clone(),
            wg_public_key: p.wg_public_key.clone(),
            wg_port: p.wg_port,
            endpoint: p.endpoint.clone(),
            mesh_ipv6: p.mesh_ipv6,
        })
        .collect();

    let resp = JoinResponse::accepted(
        &state.secret,
        state.mesh.prefix,
        state.mesh.id.as_str(),
        existing_peers,
        self_info,
        state.max_pd_members,
    );

    let peer_ipv6 = nauka_core::addressing::derive_node_address(&state.mesh.prefix, &pub_bytes);

    // Build announce info before consuming req fields
    let new_peer_info = PeerInfo {
        name: req.name.clone(),
        region: req.region.clone(),
        zone: req.zone.clone(),
        wg_public_key: req.wg_public_key.clone(),
        wg_port: req.wg_port,
        endpoint: req.endpoint.clone(),
        mesh_ipv6: peer_ipv6,
    };

    // Skip if exact same key already exists (duplicate join)
    if state.peers.find_by_key(&req.wg_public_key).is_some() {
        tracing::info!(peer = %req.name, "duplicate join, same key already known");
        drop(write_guard);
        write_json(stream, &resp).await?;
        return Ok(req.name);
    }

    // Remove stale entry if same name but different key (leave/rejoin)
    if state.peers.find_by_name(&req.name).is_some() {
        tracing::info!(peer = %req.name, "replacing stale peer entry (rejoin)");
        state.peers.remove(&req.name);
    }

    // Add peer + save + update WG
    let new_peer = Peer::new(
        req.name.clone(),
        req.region.clone(),
        req.zone.clone(),
        req.wg_public_key.clone(),
        req.wg_port,
        req.endpoint.clone(),
        peer_ipv6,
    );
    state
        .peers
        .add(new_peer)
        .map_err(|e| NaukaError::internal(format!("peer add failed: {e}")))?;

    // Save state FIRST — state is the source of truth. If the
    // process crashes after this point, WireGuard will be
    // reconciled from state on the next `wg-quick up` / service
    // restart.
    state
        .save(db)
        .await
        .map_err(|e| NaukaError::internal(e.to_string()))?;

    // Update WireGuard config. If this fails, state is still
    // correct and WG will catch up on next restart — log a
    // warning but don't fail the join.
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

    if let Err(e) = service::update_config(
        &state.hypervisor.wg_private_key,
        state.hypervisor.wg_port,
        &state.hypervisor.mesh_ipv6,
        &peers_for_wg,
    ) {
        tracing::warn!(error = %e, "WireGuard config update failed (will reconcile on restart)");
    }

    let announcer_name = state.hypervisor.name.clone();
    let wg_port = state.hypervisor.wg_port;

    // Collect peers to announce to (all except the new one)
    let new_key = new_peer_info.wg_public_key.clone();
    let announce_targets: Vec<_> = state
        .peers
        .peers
        .iter()
        .filter(|p| p.wg_public_key != new_key)
        .cloned()
        .collect();

    // Release the write lock before the async response write + the
    // spawned broadcast. The next joiner can proceed immediately.
    drop(write_guard);

    write_json(stream, &resp).await?;

    if !announce_targets.is_empty() {
        tokio::spawn(async move {
            let (ok, fail) = super::announce::broadcast_new_peer(
                &new_peer_info,
                &announcer_name,
                &announce_targets,
                wg_port,
            )
            .await;
            if ok > 0 || fail > 0 {
                tracing::info!(successes = ok, failures = fail, "peer announce broadcast");
            }
        });
    }

    Ok(req.name)
}

/// Read a length-prefixed JSON message from a stream.
pub async fn read_json<T: serde::de::DeserializeOwned>(
    stream: &mut (impl AsyncReadExt + Unpin),
) -> Result<T, NaukaError> {
    let mut len_buf = [0u8; 4];
    stream
        .read_exact(&mut len_buf)
        .await
        .map_err(|e| NaukaError::network(format!("read length failed: {e}")))?;
    let len = u32::from_be_bytes(len_buf) as usize;

    if len > 1_048_576 {
        return Err(NaukaError::validation("message too large"));
    }

    let mut buf = vec![0u8; len];
    stream
        .read_exact(&mut buf)
        .await
        .map_err(|e| NaukaError::network(format!("read body failed: {e}")))?;

    serde_json::from_slice(&buf).map_err(|e| NaukaError::validation(format!("invalid JSON: {e}")))
}

/// Write a length-prefixed JSON message to a stream.
pub async fn write_json<T: serde::Serialize>(
    stream: &mut (impl AsyncWriteExt + Unpin),
    msg: &T,
) -> Result<(), NaukaError> {
    let data = serde_json::to_vec(msg)
        .map_err(|e| NaukaError::internal(format!("serialize failed: {e}")))?;
    let len = (data.len() as u32).to_be_bytes();

    stream
        .write_all(&len)
        .await
        .map_err(|e| NaukaError::network(format!("write failed: {e}")))?;
    stream
        .write_all(&data)
        .await
        .map_err(|e| NaukaError::network(format!("write failed: {e}")))?;
    stream
        .flush()
        .await
        .map_err(|e| NaukaError::network(format!("flush failed: {e}")))?;

    Ok(())
}

/// Validate a peer-provided field against injection attacks.
/// Rejects newlines, control characters, and excessive length.
pub fn validate_peer_field(value: &str, field_name: &str) -> Result<(), NaukaError> {
    if value.len() > 256 {
        return Err(NaukaError::validation(format!(
            "{field_name} too long (max 256 chars)"
        )));
    }
    if value.chars().any(|c| c.is_control()) {
        return Err(NaukaError::validation(format!(
            "{field_name} contains control characters"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn json_roundtrip_over_tcp() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let req: JoinRequest = read_json(&mut stream).await.unwrap();
            assert_eq!(req.name, "test-node");

            let resp = JoinResponse::rejected("test rejection");
            write_json(&mut stream, &resp).await.unwrap();
        });

        let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
        let req = JoinRequest {
            name: "test-node".into(),
            region: "eu".into(),
            zone: "fsn1".into(),
            wg_public_key: "key".into(),
            wg_port: 51820,
            endpoint: None,
            pin: Some("1234".into()),
            trace_id: None,
        };
        write_json(&mut client, &req).await.unwrap();

        let resp: JoinResponse = read_json(&mut client).await.unwrap();
        assert!(!resp.accepted);
        assert_eq!(resp.reason.as_deref(), Some("test rejection"));

        server.await.unwrap();
    }
}
