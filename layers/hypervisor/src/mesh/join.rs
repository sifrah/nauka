use defguard_wireguard_rs::key::Key;
use defguard_wireguard_rs::{Kernel, WGApi, WireguardInterfaceApi};
use nauka_state::RaftNode;
use serde::{Deserialize, Serialize};
use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpStream};
use std::str::FromStr;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};

use super::{KeyPair, Mesh, MeshError, MeshId, MeshPeer};

pub const DEFAULT_JOIN_PORT: u16 = 51821;
pub const DEFAULT_PEERING_TIMEOUT: u64 = 60;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PeerInfo {
    pub public_key: String,
    pub endpoint: String,
    pub mesh_address: String,
}

#[derive(Serialize, Deserialize)]
struct JoinRequest {
    pin: String,
    public_key: String,
    listen_port: u16,
}

#[derive(Serialize, Deserialize)]
struct JoinResponse {
    mesh_id: String,
    public_key: String,
    listen_port: u16,
    mesh_address: String,
    raft_addr: String,
    peers: Vec<PeerInfo>,
    #[serde(default)]
    ca_cert: Option<String>,
    #[serde(default)]
    tls_cert: Option<String>,
    #[serde(default)]
    tls_key: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct RemoveRequest {
    remove_public_key: String,
}

pub fn generate_pin() -> String {
    let entropy = Key::generate();
    let bytes = entropy.as_array();
    let num = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) % 1_000_000;
    format!("{num:06}")
}

fn add_peer_to_wg(interface_name: &str, info: &PeerInfo) {
    let Ok(api) = WGApi::<Kernel>::new(interface_name.to_string()) else {
        return;
    };
    let Ok(key) = Key::from_str(&info.public_key) else {
        return;
    };
    let mut peer = defguard_wireguard_rs::peer::Peer::new(key);
    let _ = peer.set_endpoint(&info.endpoint);
    peer.persistent_keepalive_interval = Some(25);
    if let Ok(addr) = info.mesh_address.parse() {
        peer.allowed_ips.push(addr);
    }
    if api.configure_peer(&peer).is_ok() {
        let _ = api.configure_peer_routing(&[peer]);
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn mesh_listener(
    raft: Option<Arc<RaftNode>>,
    mesh_id: MeshId,
    keypair: KeyPair,
    mesh_address: String,
    interface_name: String,
    wg_port: u16,
    peering_pin: Option<String>,
    join_port: u16,
    ca_cert: Option<String>,
    ca_key: Option<String>,
) {
    let listener = match tokio::net::TcpListener::bind(("0.0.0.0", join_port)).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("  mesh listener bind failed: {e}");
            return;
        }
    };

    let raft_addr = format!("[{}]:4001", mesh_address.split('/').next().unwrap_or(""));
    let known_peers: Arc<tokio::sync::Mutex<Vec<PeerInfo>>> =
        Arc::new(tokio::sync::Mutex::new(Vec::new()));

    loop {
        let (stream, peer_addr) = match listener.accept().await {
            Ok(v) => v,
            Err(_) => continue,
        };

        let raft = raft.clone();
        let mesh_id = mesh_id.clone();
        let keypair = keypair.clone();
        let mesh_address = mesh_address.clone();
        let iface = interface_name.clone();
        let pin = peering_pin.clone();
        let peers = known_peers.clone();
        let ra = raft_addr.clone();
        let ca_c = ca_cert.clone();
        let ca_k = ca_key.clone();

        tokio::spawn(async move {
            let _ = handle_connection(
                stream, peer_addr, raft, &mesh_id, &keypair, &mesh_address, &iface, wg_port,
                pin.as_deref(), &peers, &ra, ca_c.as_deref(), ca_k.as_deref(),
            )
            .await;
        });
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_connection(
    stream: tokio::net::TcpStream,
    peer_addr: SocketAddr,
    raft: Option<Arc<RaftNode>>,
    mesh_id: &MeshId,
    keypair: &KeyPair,
    mesh_address: &str,
    interface_name: &str,
    wg_port: u16,
    peering_pin: Option<&str>,
    known_peers: &tokio::sync::Mutex<Vec<PeerInfo>>,
    raft_addr: &str,
    ca_cert: Option<&str>,
    ca_key: Option<&str>,
) -> Result<(), MeshError> {
    let (reader, mut writer) = stream.into_split();
    let mut lines = tokio::io::BufReader::new(reader).lines();

    let line = lines
        .next_line()
        .await
        .map_err(|e| MeshError::Join(e.to_string()))?
        .ok_or_else(|| MeshError::Join("empty".into()))?;

    let v: serde_json::Value =
        serde_json::from_str(&line).map_err(|e| MeshError::Join(e.to_string()))?;

    if v.get("pin").is_some() {
        // --- Join request ---
        let pin = peering_pin.ok_or_else(|| MeshError::Join("peering not enabled".into()))?;
        let raft = raft.as_ref().ok_or_else(|| MeshError::Join("no raft".into()))?;

        let req: JoinRequest =
            serde_json::from_value(v).map_err(|e| MeshError::Join(e.to_string()))?;
        if req.pin != pin {
            let _ = writer.write_all(b"{\"error\":\"invalid pin\"}\n").await;
            return Err(MeshError::Join("invalid pin".into()));
        }

        let joiner_pk = Key::from_str(&req.public_key).map_err(|_| MeshError::InvalidKey)?;
        let joiner_address = mesh_id.node_address(&joiner_pk);
        let peer_ip = peer_addr.ip().to_string();

        let peers_snapshot = known_peers.lock().await.clone();

        // Sign a TLS cert for the joiner if we have the CA
        let (joiner_tls_cert, joiner_tls_key) =
            if let (Some(ca_c), Some(ca_k)) = (ca_cert, ca_key) {
                match super::certs::sign_node_cert(ca_c, ca_k) {
                    Ok((cert, key)) => (Some(cert), Some(key)),
                    Err(e) => {
                        eprintln!("  ! sign node cert: {e}");
                        (None, None)
                    }
                }
            } else {
                (None, None)
            };

        let resp = JoinResponse {
            mesh_id: mesh_id.to_string(),
            public_key: keypair.public_key().to_string(),
            listen_port: wg_port,
            mesh_address: mesh_address.to_string(),
            raft_addr: raft_addr.to_string(),
            peers: peers_snapshot,
            ca_cert: ca_cert.map(|s| s.to_string()),
            tls_cert: joiner_tls_cert,
            tls_key: joiner_tls_key,
        };
        let resp_json = serde_json::to_string(&resp).expect("serialize");
        writer
            .write_all(format!("{resp_json}\n").as_bytes())
            .await
            .map_err(|e| MeshError::Join(e.to_string()))?;

        // Add to local WG immediately
        let new_peer = PeerInfo {
            public_key: req.public_key.clone(),
            endpoint: format!("{peer_ip}:{}", req.listen_port),
            mesh_address: joiner_address.to_string(),
        };
        add_peer_to_wg(interface_name, &new_peer);

        // Register joiner as hypervisor via Raft — replicates to all nodes
        let joiner_node_id = nauka_state::node_id_from_key(&req.public_key);
        let joiner_raft_addr = format!("[{}]:4001", joiner_address.address);
        let surql = format!(
            "CREATE hypervisor SET \
             public_key = '{}', node_id = {}, address = '{}', \
             endpoint = '{}:{}', allowed_ips = ['{}'], keepalive = 25, \
             raft_addr = '{joiner_raft_addr}'",
            req.public_key, joiner_node_id as i64, joiner_address,
            peer_ip, req.listen_port, joiner_address
        );
        if let Err(e) = raft.write(surql).await {
            eprintln!("  ! raft write failed: {e}");
        }

        // Add joiner to Raft cluster as learner — retry in background
        let raft_clone = Arc::clone(raft);
        tokio::spawn(async move {
            for attempt in 1..=15 {
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                match raft_clone.add_learner(joiner_node_id, &joiner_raft_addr).await {
                    Ok(_) => {
                        eprintln!("  + raft learner: {joiner_raft_addr} (attempt {attempt})");
                        match raft_clone.promote_voter(joiner_node_id).await {
                            Ok(_) => eprintln!("  + raft voter: {joiner_raft_addr}"),
                            Err(e) => eprintln!("  ! raft voter promotion failed: {e}"),
                        }
                        return;
                    }
                    Err(_) if attempt < 15 => continue,
                    Err(e) => eprintln!("  ! raft learner failed: {e}"),
                }
            }
        });

        known_peers.lock().await.push(new_peer);
        println!("  + peer joined: {} ({})", joiner_address, peer_ip);
    } else if v.get("remove_public_key").is_some() {
        // --- Peer removal request ---
        let raft = raft.as_ref().ok_or_else(|| MeshError::Join("no raft".into()))?;
        let req: RemoveRequest =
            serde_json::from_value(v).map_err(|e| MeshError::Join(e.to_string()))?;

        // Parse as a WireGuard key to reject anything that isn't a pubkey
        // (including SurQL-breaking characters) before touching the DB.
        let canonical_pk = match Key::from_str(&req.remove_public_key) {
            Ok(k) => k.to_string(),
            Err(_) => {
                let _ = writer
                    .write_all(b"{\"error\":\"invalid public key\"}\n")
                    .await;
                return Err(MeshError::InvalidKey);
            }
        };

        let surql = format!(
            "DELETE hypervisor WHERE public_key = '{canonical_pk}'"
        );
        if let Err(e) = raft.write(surql).await {
            eprintln!("  ! raft remove failed: {e}");
            let _ = writer.write_all(b"{\"error\":\"raft write failed\"}\n").await;
        } else {
            let _ = writer.write_all(b"{\"ok\":true}\n").await;
            println!("  - peer removed: {canonical_pk}");
        }
    }

    Ok(())
}

// --- Sync join client (bootstrap, before daemon starts) ---

pub use super::certs::TlsCerts;

pub fn join_mesh(
    host: &str,
    pin: &str,
    interface_name: String,
    listen_port: u16,
    join_port: u16,
) -> Result<(Mesh, Vec<PeerInfo>, Option<TlsCerts>), MeshError> {
    let keypair = KeyPair::generate();

    let addr = format!("{host}:{join_port}");
    let mut stream =
        TcpStream::connect(&addr).map_err(|e| MeshError::Join(format!("connect {addr}: {e}")))?;

    let req = JoinRequest {
        pin: pin.to_string(),
        public_key: keypair.public_key().to_string(),
        listen_port,
    };
    writeln!(stream, "{}", serde_json::to_string(&req).expect("serialize"))
        .map_err(|e| MeshError::Join(e.to_string()))?;

    let reader = BufReader::new(stream.try_clone().map_err(|e| MeshError::Join(e.to_string()))?);
    let mut lines = reader.lines();
    let line = lines
        .next()
        .ok_or_else(|| MeshError::Join("no response".into()))?
        .map_err(|e| MeshError::Join(e.to_string()))?;

    let v: serde_json::Value =
        serde_json::from_str(&line).map_err(|e| MeshError::Join(format!("bad response: {e}")))?;
    if let Some(error) = v.get("error").and_then(|e| e.as_str()) {
        return Err(MeshError::Join(error.to_string()));
    }
    let resp: JoinResponse =
        serde_json::from_value(v).map_err(|e| MeshError::Join(format!("bad response: {e}")))?;

    let mesh_id: MeshId = resp.mesh_id.parse()?;
    let mut mesh = Mesh::new(interface_name, listen_port, Some(mesh_id), Some(keypair), None)?;
    mesh.up()?;

    let server_info = PeerInfo {
        public_key: resp.public_key.clone(),
        endpoint: format!("{host}:{}", resp.listen_port),
        mesh_address: resp.mesh_address.clone(),
    };

    let server_peer = MeshPeer {
        public_key: resp.public_key,
        endpoint: Some(format!("{host}:{}", resp.listen_port)),
        allowed_ips: vec![resp.mesh_address],
        persistent_keepalive: Some(25),
    };
    mesh.add_peer(&server_peer)?;

    for p in &resp.peers {
        let peer = MeshPeer {
            public_key: p.public_key.clone(),
            endpoint: Some(p.endpoint.clone()),
            allowed_ips: vec![p.mesh_address.clone()],
            persistent_keepalive: Some(25),
        };
        mesh.add_peer(&peer)?;
    }

    let mut all_peers = vec![server_info];
    all_peers.extend(resp.peers);

    let tls_certs = match (resp.ca_cert, resp.tls_cert, resp.tls_key) {
        (Some(ca), Some(cert), Some(key)) => Some(TlsCerts {
            ca_cert: ca,
            tls_cert: cert,
            tls_key: key,
        }),
        _ => None,
    };

    Ok((mesh, all_peers, tls_certs))
}

/// Send a remove-peer command to the local daemon
pub fn request_peer_removal(join_port: u16, public_key: &str) -> Result<(), MeshError> {
    let addr = format!("127.0.0.1:{join_port}");
    let mut stream =
        TcpStream::connect(&addr).map_err(|e| MeshError::Join(format!("connect daemon: {e}")))?;

    let req = RemoveRequest {
        remove_public_key: public_key.to_string(),
    };
    writeln!(stream, "{}", serde_json::to_string(&req).expect("serialize"))
        .map_err(|e| MeshError::Join(e.to_string()))?;

    let reader = BufReader::new(stream);
    let mut lines = reader.lines();
    if let Some(Ok(line)) = lines.next() {
        let v: serde_json::Value = serde_json::from_str(&line).unwrap_or_default();
        if let Some(err) = v.get("error").and_then(|e| e.as_str()) {
            return Err(MeshError::Join(err.to_string()));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // The validation in handle_connection (remove path) relies on
    // `Key::from_str` to reject anything that isn't a canonical WireGuard
    // public key before the value enters a SurQL literal. These tests lock
    // in that assumption — if the key crate ever starts accepting looser
    // inputs, the `DELETE hypervisor WHERE public_key = '...'` path would
    // become injectable again.
    const VALID_WG_PUBKEY: &str = "PQgVf+YiO+S7LTaOqtGSEUxXpmEb5hPEb+g5mTwQdC0=";

    #[test]
    fn key_from_str_rejects_surql_injection() {
        let attacks = [
            "'; DROP TABLE hypervisor; --",
            "' OR 1=1; --",
            "abc'; DELETE hypervisor; '",
            "\"injected\"",
            "", // empty
            "not base64 at all",
        ];
        for a in attacks {
            assert!(
                Key::from_str(a).is_err(),
                "Key::from_str should reject {a:?} but accepted it"
            );
        }
    }

    #[test]
    fn key_from_str_accepts_canonical_pubkey_and_roundtrips() {
        let k = Key::from_str(VALID_WG_PUBKEY).expect("valid pubkey should parse");
        assert_eq!(k.to_string(), VALID_WG_PUBKEY);
    }

    #[test]
    fn key_from_str_rejects_wrong_length_base64() {
        // Valid base64 but wrong size for a 32-byte WG key.
        assert!(Key::from_str("dGVzdA==").is_err()); // 4 bytes
        assert!(Key::from_str("AA==").is_err()); // 1 byte
    }
}
