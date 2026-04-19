use defguard_wireguard_rs::key::Key;
use defguard_wireguard_rs::{Kernel, WGApi, WireguardInterfaceApi};
use nauka_core::resource::Datetime;
use nauka_core::LogNaukaErr;
use nauka_state::{Database, RaftNode, Writer};

use crate::definition::Hypervisor;
use serde::{Deserialize, Serialize};
use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpStream};
use std::str::FromStr;
use std::sync::Arc;
use surrealdb::types::SurrealValue;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use tracing::Instrument;

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

#[derive(Serialize, Deserialize)]
struct RaftWriteRequest {
    query: String,
}

#[derive(Serialize, Deserialize)]
struct IamSignupRequest {
    email: String,
    password: String,
    display_name: String,
}

#[derive(Serialize, Deserialize)]
struct IamSigninRequest {
    email: String,
    password: String,
}

#[derive(Serialize, Deserialize)]
struct IamOrgCreateRequest {
    jwt: String,
    slug: String,
    display_name: String,
}

#[derive(Serialize, Deserialize)]
struct IamProjectCreateRequest {
    jwt: String,
    org: String,
    slug: String,
    display_name: String,
}

#[derive(Serialize, Deserialize)]
struct IamEnvCreateRequest {
    jwt: String,
    project: String,
    slug: String,
    display_name: String,
}

#[derive(Serialize, Deserialize)]
struct IamRoleBindRequest {
    jwt: String,
    principal: String,
    role: String,
    org: String,
    #[serde(default)]
    reason: String,
}

#[derive(Serialize, Deserialize)]
struct IamRoleUnbindRequest {
    jwt: String,
    principal: String,
    role: String,
    org: String,
}

#[derive(Serialize, Deserialize)]
struct IamSaCreateRequest {
    jwt: String,
    org: String,
    slug: String,
    display_name: String,
}

#[derive(Serialize, Deserialize)]
struct IamTokenCreateRequest {
    jwt: String,
    service_account: String,
    name: String,
}

#[derive(Serialize, Deserialize)]
struct IamTokenRevokeRequest {
    jwt: String,
    token_id: String,
}

#[derive(Serialize, Deserialize)]
struct IamPasswordResetRequestReq {
    email: String,
}

#[derive(Serialize, Deserialize)]
struct IamPasswordResetReq {
    token_id: String,
    new_password: String,
}

#[derive(Serialize, Deserialize)]
struct IamUserSetActiveRequest {
    jwt: String,
    email: String,
    active: bool,
    reason: String,
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
    db: Arc<Database>,
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
            tracing::error!(
                event = "mesh.listener.bind_failed",
                join_port,
                error = %e,
                "mesh listener bind failed"
            );
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
        let db = db.clone();
        let mesh_id = mesh_id.clone();
        let keypair = keypair.clone();
        let mesh_address = mesh_address.clone();
        let iface = interface_name.clone();
        let pin = peering_pin.clone();
        let peers = known_peers.clone();
        let ra = raft_addr.clone();
        let ca_c = ca_cert.clone();
        let ca_k = ca_key.clone();

        // One trace_id per incoming connection, so every event produced
        // while handling this TCP stream (including nested instrument_op
        // scopes like peer.join) is greppable as one unit in journalctl.
        let trace_id = nauka_core::new_trace_id();
        let span = tracing::info_span!(
            "peer_conn",
            trace_id = %trace_id,
            peer = %peer_addr,
        );
        tokio::spawn(
            async move {
                let _ = handle_connection(
                    stream,
                    peer_addr,
                    raft,
                    &db,
                    &mesh_id,
                    &keypair,
                    &mesh_address,
                    &iface,
                    wg_port,
                    pin.as_deref(),
                    &peers,
                    &ra,
                    ca_c.as_deref(),
                    ca_k.as_deref(),
                )
                .await;
            }
            .instrument(span),
        );
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_connection(
    stream: tokio::net::TcpStream,
    peer_addr: SocketAddr,
    raft: Option<Arc<RaftNode>>,
    db: &Database,
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

    if v.get("whoami").is_some() {
        // --- Observability: tell the caller what public IP we see them on.
        // Used by restarted nodes to detect their current public address
        // without depending on external STUN-like services.
        let resp = format!("{{\"observed_ip\":\"{}\"}}\n", peer_addr.ip());
        let _ = writer.write_all(resp.as_bytes()).await;
        return Ok(());
    } else if v.get("pin").is_some() {
        // --- Join request ---
        return nauka_core::instrument_op("peer.join", async {
            let pin = match peering_pin {
                Some(p) => p,
                None => {
                    let _ = writer
                        .write_all(b"{\"error\":\"peering not enabled on this node\"}\n")
                        .await;
                    nauka_core::bail_log!(
                        MeshError::Join("peering not enabled".into()),
                        event = "peer.join.peering_not_enabled",
                        peer = %peer_addr,
                        "peering not enabled on this node"
                    );
                }
            };
            let raft = raft
                .as_ref()
                .ok_or_else(|| MeshError::Join("no raft".into()))?;

            let req: JoinRequest =
                serde_json::from_value(v).map_err(|e| MeshError::Join(e.to_string()))?;
            if req.pin != pin {
                let _ = writer.write_all(b"{\"error\":\"invalid pin\"}\n").await;
                nauka_core::bail_log!(
                    MeshError::Join("invalid pin".into()),
                    event = "peer.join.pin_mismatch",
                    peer = %peer_addr,
                    "invalid pin"
                );
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
                            tracing::warn!(
                                event = "peer.join.sign_cert_failed",
                                error = %e,
                                "sign node cert failed"
                            );
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

            // Register joiner as hypervisor via Raft — replicates to all
            // nodes. Timestamps computed on the leader so each state
            // machine applies the same byte-identical value (see ADR 0006
            // rule #7; Raft apply determinism).
            let joiner_node_id = nauka_state::node_id_from_key(&req.public_key);
            let joiner_raft_addr = format!("[{}]:4001", joiner_address.address);
            let now = Datetime::now();
            let joiner_addr_str = joiner_address.to_string();
            let hv = Hypervisor {
                public_key: req.public_key.clone(),
                node_id: joiner_node_id,
                raft_addr: joiner_raft_addr.clone(),
                address: joiner_addr_str.clone(),
                endpoint: Some(format!("{peer_ip}:{}", req.listen_port)),
                allowed_ips: vec![joiner_addr_str],
                keepalive: Some(25),
                created_at: now,
                updated_at: now,
                version: 0,
            };
            let _ = Writer::new(db).with_raft(raft).create(&hv).await.warn();

            // Add joiner to Raft cluster as learner — retry in background
            let raft_clone = Arc::clone(raft);
            tokio::spawn(async move {
                for attempt in 1..=15 {
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                    match raft_clone
                        .add_learner(joiner_node_id, &joiner_raft_addr)
                        .await
                    {
                        Ok(_) => {
                            tracing::info!(
                                event = "raft.learner.added",
                                raft_addr = %joiner_raft_addr,
                                attempt,
                                "raft learner added"
                            );
                            match raft_clone.promote_voter(joiner_node_id).await {
                                Ok(_) => tracing::info!(
                                    event = "raft.voter.promoted",
                                    raft_addr = %joiner_raft_addr,
                                    "raft voter promoted"
                                ),
                                Err(e) => tracing::warn!(
                                    event = "raft.voter.promote_failed",
                                    raft_addr = %joiner_raft_addr,
                                    error = %e,
                                    "raft voter promotion failed"
                                ),
                            }
                            return;
                        }
                        Err(_) if attempt < 15 => continue,
                        Err(e) => tracing::warn!(
                            event = "raft.learner.add_failed",
                            raft_addr = %joiner_raft_addr,
                            error = %e,
                            "raft learner add failed"
                        ),
                    }
                }
            });

            known_peers.lock().await.push(new_peer);
            tracing::info!(
                event = "peer.join",
                joiner_address = %joiner_address,
                peer_ip = %peer_ip,
                "peer joined"
            );
            Ok(())
        })
        .await;
    } else if v.get("remove_public_key").is_some() {
        // --- Peer removal request ---
        let raft = raft
            .as_ref()
            .ok_or_else(|| MeshError::Join("no raft".into()))?;
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

        let delete_result = Writer::new(db)
            .with_raft(raft)
            .delete::<Hypervisor>(&canonical_pk)
            .await
            .warn();
        if delete_result.is_err() {
            let _ = writer
                .write_all(b"{\"error\":\"raft write failed\"}\n")
                .await;
        } else {
            let _ = writer.write_all(b"{\"ok\":true}\n").await;
            tracing::info!(
                event = "peer.remove",
                public_key = %canonical_pk,
                "peer removed"
            );
        }
    } else if v.get("raft_write").is_some() {
        // Debug escape hatch: write arbitrary SurQL through Raft. Restricted
        // to loopback so an attacker on the public peering port can't
        // corrupt cluster state. Used by operators (and test-issue-315.sh)
        // to simulate scenarios like stale endpoints.
        if !peer_addr.ip().is_loopback() {
            let _ = writer
                .write_all(b"{\"error\":\"raft_write requires loopback\"}\n")
                .await;
            nauka_core::bail_log!(
                MeshError::Join("non-loopback raft_write".into()),
                event = "debug.raft_write.non_loopback_rejected",
                peer = %peer_addr,
                "raft_write requires loopback"
            );
        }
        let raft = raft
            .as_ref()
            .ok_or_else(|| MeshError::Join("no raft".into()))?;
        let req: RaftWriteRequest =
            serde_json::from_value(v).map_err(|e| MeshError::Join(e.to_string()))?;
        match raft.write(req.query.clone()).await {
            Ok(_) => {
                let _ = writer.write_all(b"{\"ok\":true}\n").await;
                tracing::info!(
                    event = "debug.raft_write.ok",
                    query = %req.query,
                    "raft_write ok"
                );
            }
            Err(e) => {
                let body = serde_json::json!({ "error": e.to_string() }).to_string();
                let _ = writer.write_all(format!("{body}\n").as_bytes()).await;
                tracing::warn!(
                    event = "debug.raft_write.fail",
                    query = %req.query,
                    error = %e,
                    "raft_write failed"
                );
            }
        }
    } else if v.get("status").is_some() {
        // Read-only status: local state snapshot + replicated hypervisor
        // list. Loopback-only since it reveals internal topology.
        if !peer_addr.ip().is_loopback() {
            let _ = writer
                .write_all(b"{\"error\":\"status requires loopback\"}\n")
                .await;
            return Err(MeshError::Join("non-loopback status".into()));
        }
        let hypervisors: Vec<serde_json::Value> = db
            .query_take::<HypervisorListRow>(
                "SELECT public_key, address, endpoint FROM hypervisor ORDER BY public_key",
            )
            .await
            .map(|rows| {
                rows.into_iter()
                    .map(|r| {
                        serde_json::json!({
                            "public_key": r.public_key,
                            "address": r.address,
                            "endpoint": r.endpoint,
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        let resp = serde_json::json!({
            "mesh_id": mesh_id.to_string(),
            "public_key": keypair.public_key().to_string(),
            "address": mesh_address,
            "peering_open": peering_pin.is_some(),
            "hypervisors": hypervisors,
        });
        let _ = writer.write_all(format!("{resp}\n").as_bytes()).await;
    } else if v.get("leave").is_some() {
        // Graceful leave: raft.write DELETE for self so other nodes drop
        // us from their tables and WG peers. Loopback-only — a remote
        // attacker shouldn't be able to remove us from the cluster.
        if !peer_addr.ip().is_loopback() {
            let _ = writer
                .write_all(b"{\"error\":\"leave requires loopback\"}\n")
                .await;
            nauka_core::bail_log!(
                MeshError::Join("non-loopback leave".into()),
                event = "hypervisor.leave.non_loopback_rejected",
                peer = %peer_addr,
                "leave requires loopback"
            );
        }
        let raft = raft
            .as_ref()
            .ok_or_else(|| MeshError::Join("no raft".into()))?;
        let own_pk = keypair.public_key().to_string();
        match Writer::new(db)
            .with_raft(raft)
            .delete::<Hypervisor>(&own_pk)
            .await
            .warn()
        {
            Ok(_) => {
                let _ = writer.write_all(b"{\"ok\":true}\n").await;
                tracing::info!(
                    event = "hypervisor.leave.broadcast",
                    public_key = %own_pk,
                    "leave: broadcast DELETE for self"
                );
            }
            Err(e) => {
                let body = serde_json::json!({ "error": e.to_string() }).to_string();
                let _ = writer.write_all(format!("{body}\n").as_bytes()).await;
            }
        }
    } else if v.get("iam_signup").is_some() {
        // Create a user. Loopback-only: signup is an operator action
        // today (no REST surface yet). The daemon hashes the password
        // with Argon2id in Rust, then replicates the literal hash via
        // Raft so every follower applies the same PHC string. Without
        // this, `crypto::argon2::generate` running on the leader
        // would produce a different hash on each replica and the
        // state machine would diverge.
        if !peer_addr.ip().is_loopback() {
            let _ = writer
                .write_all(b"{\"error\":\"iam_signup requires loopback\"}\n")
                .await;
            nauka_core::bail_log!(
                MeshError::Join("non-loopback iam_signup".into()),
                event = "iam.signup.non_loopback_rejected",
                peer = %peer_addr,
                "iam_signup requires loopback"
            );
        }
        let raft = raft
            .as_ref()
            .ok_or_else(|| MeshError::Join("no raft".into()))?;
        let req: IamSignupRequest =
            serde_json::from_value(v).map_err(|e| MeshError::Join(e.to_string()))?;
        let peer_ip_str = peer_addr.ip().to_string();
        match nauka_iam::signup(
            db,
            raft,
            &req.email,
            &req.password,
            &req.display_name,
            &peer_ip_str,
        )
        .await
        {
            Ok(jwt) => {
                let body = serde_json::json!({ "ok": true, "jwt": jwt.into_string() }).to_string();
                let _ = writer.write_all(format!("{body}\n").as_bytes()).await;
                tracing::info!(
                    event = "iam.signup.ok",
                    email = %req.email,
                    "iam signup ok"
                );
            }
            Err(e) => {
                let body = serde_json::json!({ "error": e.to_string() }).to_string();
                let _ = writer.write_all(format!("{body}\n").as_bytes()).await;
                tracing::warn!(
                    event = "iam.signup.fail",
                    email = %req.email,
                    error = %e,
                    "iam signup failed"
                );
            }
        }
    } else if v.get("iam_org_create").is_some() {
        // IAM-2: create an Org on behalf of the JWT-authenticated
        // caller. Loopback only — the CLI is the sole consumer; a
        // REST surface with proper bearer auth arrives with
        // ResourceDef (#342).
        if !peer_addr.ip().is_loopback() {
            let _ = writer
                .write_all(b"{\"error\":\"iam_org_create requires loopback\"}\n")
                .await;
            return Err(MeshError::Join("non-loopback iam_org_create".into()));
        }
        let raft = raft
            .as_ref()
            .ok_or_else(|| MeshError::Join("no raft".into()))?;
        let req: IamOrgCreateRequest =
            serde_json::from_value(v).map_err(|e| MeshError::Join(e.to_string()))?;
        match nauka_iam::create_org(db, raft, &req.jwt, &req.slug, &req.display_name).await {
            Ok(org) => {
                let body = serde_json::json!({
                    "ok": true,
                    "org": {
                        "slug": org.slug,
                        "display_name": org.display_name,
                        "owner": org.owner.id(),
                    }
                })
                .to_string();
                let _ = writer.write_all(format!("{body}\n").as_bytes()).await;
                tracing::info!(event = "iam.org.create.ok", slug = %req.slug);
            }
            Err(e) => {
                let body = serde_json::json!({ "error": e.to_string() }).to_string();
                let _ = writer.write_all(format!("{body}\n").as_bytes()).await;
                tracing::warn!(event = "iam.org.create.fail", slug = %req.slug, error = %e);
            }
        }
    } else if v.get("iam_project_create").is_some() {
        if !peer_addr.ip().is_loopback() {
            let _ = writer
                .write_all(b"{\"error\":\"iam_project_create requires loopback\"}\n")
                .await;
            return Err(MeshError::Join("non-loopback iam_project_create".into()));
        }
        let raft = raft
            .as_ref()
            .ok_or_else(|| MeshError::Join("no raft".into()))?;
        let req: IamProjectCreateRequest =
            serde_json::from_value(v).map_err(|e| MeshError::Join(e.to_string()))?;
        match nauka_iam::create_project(db, raft, &req.jwt, &req.org, &req.slug, &req.display_name)
            .await
        {
            Ok(p) => {
                let body = serde_json::json!({
                    "ok": true,
                    "project": {
                        "uid": p.uid,
                        "slug": p.slug,
                        "org": p.org.id(),
                        "display_name": p.display_name,
                    }
                })
                .to_string();
                let _ = writer.write_all(format!("{body}\n").as_bytes()).await;
                tracing::info!(event = "iam.project.create.ok", slug = %req.slug);
            }
            Err(e) => {
                let body = serde_json::json!({ "error": e.to_string() }).to_string();
                let _ = writer.write_all(format!("{body}\n").as_bytes()).await;
                tracing::warn!(event = "iam.project.create.fail", slug = %req.slug, error = %e);
            }
        }
    } else if v.get("iam_env_create").is_some() {
        if !peer_addr.ip().is_loopback() {
            let _ = writer
                .write_all(b"{\"error\":\"iam_env_create requires loopback\"}\n")
                .await;
            return Err(MeshError::Join("non-loopback iam_env_create".into()));
        }
        let raft = raft
            .as_ref()
            .ok_or_else(|| MeshError::Join("no raft".into()))?;
        let req: IamEnvCreateRequest =
            serde_json::from_value(v).map_err(|e| MeshError::Join(e.to_string()))?;
        match nauka_iam::create_env(
            db,
            raft,
            &req.jwt,
            &req.project,
            &req.slug,
            &req.display_name,
        )
        .await
        {
            Ok(e) => {
                let body = serde_json::json!({
                    "ok": true,
                    "env": {
                        "uid": e.uid,
                        "slug": e.slug,
                        "project": e.project.id(),
                        "display_name": e.display_name,
                    }
                })
                .to_string();
                let _ = writer.write_all(format!("{body}\n").as_bytes()).await;
                tracing::info!(event = "iam.env.create.ok", slug = %req.slug);
            }
            Err(e) => {
                let body = serde_json::json!({ "error": e.to_string() }).to_string();
                let _ = writer.write_all(format!("{body}\n").as_bytes()).await;
                tracing::warn!(event = "iam.env.create.fail", slug = %req.slug, error = %e);
            }
        }
    } else if v.get("iam_role_bind").is_some() {
        if !peer_addr.ip().is_loopback() {
            let _ = writer
                .write_all(b"{\"error\":\"iam_role_bind requires loopback\"}\n")
                .await;
            return Err(MeshError::Join("non-loopback iam_role_bind".into()));
        }
        let raft = raft
            .as_ref()
            .ok_or_else(|| MeshError::Join("no raft".into()))?;
        let req: IamRoleBindRequest =
            serde_json::from_value(v).map_err(|e| MeshError::Join(e.to_string()))?;
        match nauka_iam::bind_role(
            db,
            raft,
            &req.jwt,
            &req.principal,
            &req.role,
            &req.org,
            &req.reason,
        )
        .await
        {
            Ok(b) => {
                let body = serde_json::json!({
                    "ok": true,
                    "binding": {
                        "uid": b.uid,
                        "principal": b.principal.id(),
                        "role": b.role.id(),
                        "org": b.org.id(),
                        "reason": b.reason,
                    }
                })
                .to_string();
                let _ = writer.write_all(format!("{body}\n").as_bytes()).await;
                tracing::info!(
                    event = "iam.role.bind.ok",
                    principal = %req.principal,
                    role = %req.role,
                    org = %req.org
                );
            }
            Err(e) => {
                let body = serde_json::json!({ "error": e.to_string() }).to_string();
                let _ = writer.write_all(format!("{body}\n").as_bytes()).await;
                tracing::warn!(
                    event = "iam.role.bind.fail",
                    principal = %req.principal,
                    role = %req.role,
                    org = %req.org,
                    error = %e
                );
            }
        }
    } else if v.get("iam_role_unbind").is_some() {
        if !peer_addr.ip().is_loopback() {
            let _ = writer
                .write_all(b"{\"error\":\"iam_role_unbind requires loopback\"}\n")
                .await;
            return Err(MeshError::Join("non-loopback iam_role_unbind".into()));
        }
        let raft = raft
            .as_ref()
            .ok_or_else(|| MeshError::Join("no raft".into()))?;
        let req: IamRoleUnbindRequest =
            serde_json::from_value(v).map_err(|e| MeshError::Join(e.to_string()))?;
        match nauka_iam::unbind_role(db, raft, &req.jwt, &req.principal, &req.role, &req.org).await
        {
            Ok(()) => {
                let _ = writer.write_all(b"{\"ok\":true}\n").await;
                tracing::info!(event = "iam.role.unbind.ok", principal = %req.principal, role = %req.role, org = %req.org);
            }
            Err(e) => {
                let body = serde_json::json!({ "error": e.to_string() }).to_string();
                let _ = writer.write_all(format!("{body}\n").as_bytes()).await;
            }
        }
    } else if v.get("iam_sa_create").is_some() {
        if !peer_addr.ip().is_loopback() {
            let _ = writer
                .write_all(b"{\"error\":\"iam_sa_create requires loopback\"}\n")
                .await;
            return Err(MeshError::Join("non-loopback iam_sa_create".into()));
        }
        let raft = raft
            .as_ref()
            .ok_or_else(|| MeshError::Join("no raft".into()))?;
        let req: IamSaCreateRequest =
            serde_json::from_value(v).map_err(|e| MeshError::Join(e.to_string()))?;
        match nauka_iam::create_service_account(
            db,
            raft,
            &req.jwt,
            &req.org,
            &req.slug,
            &req.display_name,
        )
        .await
        {
            Ok(sa) => {
                let body = serde_json::json!({
                    "ok": true,
                    "service_account": {
                        "slug": sa.slug,
                        "org": sa.org.id(),
                        "display_name": sa.display_name,
                    }
                })
                .to_string();
                let _ = writer.write_all(format!("{body}\n").as_bytes()).await;
                tracing::info!(event = "iam.sa.create.ok", slug = %req.slug);
            }
            Err(e) => {
                let body = serde_json::json!({ "error": e.to_string() }).to_string();
                let _ = writer.write_all(format!("{body}\n").as_bytes()).await;
                tracing::warn!(event = "iam.sa.create.fail", slug = %req.slug, error = %e);
            }
        }
    } else if v.get("iam_token_create").is_some() {
        if !peer_addr.ip().is_loopback() {
            let _ = writer
                .write_all(b"{\"error\":\"iam_token_create requires loopback\"}\n")
                .await;
            return Err(MeshError::Join("non-loopback iam_token_create".into()));
        }
        let raft = raft
            .as_ref()
            .ok_or_else(|| MeshError::Join("no raft".into()))?;
        let req: IamTokenCreateRequest =
            serde_json::from_value(v).map_err(|e| MeshError::Join(e.to_string()))?;
        match nauka_iam::create_api_token(db, raft, &req.jwt, &req.service_account, &req.name).await
        {
            Ok(minted) => {
                let body = serde_json::json!({
                    "ok": true,
                    "token_id": minted.record.token_id,
                    "name": minted.record.name,
                    "plaintext": minted.plaintext,
                })
                .to_string();
                let _ = writer.write_all(format!("{body}\n").as_bytes()).await;
                tracing::info!(event = "iam.token.create.ok", name = %req.name);
            }
            Err(e) => {
                let body = serde_json::json!({ "error": e.to_string() }).to_string();
                let _ = writer.write_all(format!("{body}\n").as_bytes()).await;
                tracing::warn!(event = "iam.token.create.fail", name = %req.name, error = %e);
            }
        }
    } else if v.get("iam_token_revoke").is_some() {
        if !peer_addr.ip().is_loopback() {
            let _ = writer
                .write_all(b"{\"error\":\"iam_token_revoke requires loopback\"}\n")
                .await;
            return Err(MeshError::Join("non-loopback iam_token_revoke".into()));
        }
        let raft = raft
            .as_ref()
            .ok_or_else(|| MeshError::Join("no raft".into()))?;
        let req: IamTokenRevokeRequest =
            serde_json::from_value(v).map_err(|e| MeshError::Join(e.to_string()))?;
        match nauka_iam::revoke_api_token(db, raft, &req.jwt, &req.token_id).await {
            Ok(()) => {
                let _ = writer.write_all(b"{\"ok\":true}\n").await;
                tracing::info!(event = "iam.token.revoke.ok", token_id = %req.token_id);
            }
            Err(e) => {
                let body = serde_json::json!({ "error": e.to_string() }).to_string();
                let _ = writer.write_all(format!("{body}\n").as_bytes()).await;
            }
        }
    } else if v.get("iam_password_reset_request").is_some() {
        if !peer_addr.ip().is_loopback() {
            let _ = writer
                .write_all(b"{\"error\":\"iam_password_reset_request requires loopback\"}\n")
                .await;
            return Err(MeshError::Join(
                "non-loopback iam_password_reset_request".into(),
            ));
        }
        let raft = raft
            .as_ref()
            .ok_or_else(|| MeshError::Join("no raft".into()))?;
        let req: IamPasswordResetRequestReq =
            serde_json::from_value(v).map_err(|e| MeshError::Join(e.to_string()))?;
        match nauka_iam::request_password_reset(db, raft, &req.email).await {
            Ok(Some(token_id)) => {
                // Deliberately NOT echoing the token in the IPC
                // response — the CLI gets an opaque `ok`, while the
                // daemon journal carries the plaintext token so an
                // admin can fish it out until IAM-7b wires up
                // email delivery. That keeps the reset-request IPC
                // indistinguishable between known / unknown emails.
                tracing::info!(
                    event = "iam.password.reset_request.minted",
                    email = %req.email,
                    token_id = %token_id,
                    "password reset token minted — plaintext logged for admin retrieval"
                );
                let _ = writer.write_all(b"{\"ok\":true}\n").await;
            }
            Ok(None) => {
                // Silent no-op: the client cannot tell this from
                // the minted case. Preserves the no-enumeration
                // property the epic called out.
                tracing::info!(
                    event = "iam.password.reset_request.no_user",
                    email = %req.email
                );
                let _ = writer.write_all(b"{\"ok\":true}\n").await;
            }
            Err(e) => {
                let body = serde_json::json!({ "error": e.to_string() }).to_string();
                let _ = writer.write_all(format!("{body}\n").as_bytes()).await;
                tracing::warn!(
                    event = "iam.password.reset_request.fail",
                    error = %e
                );
            }
        }
    } else if v.get("iam_password_reset").is_some() {
        if !peer_addr.ip().is_loopback() {
            let _ = writer
                .write_all(b"{\"error\":\"iam_password_reset requires loopback\"}\n")
                .await;
            return Err(MeshError::Join("non-loopback iam_password_reset".into()));
        }
        let raft = raft
            .as_ref()
            .ok_or_else(|| MeshError::Join("no raft".into()))?;
        let req: IamPasswordResetReq =
            serde_json::from_value(v).map_err(|e| MeshError::Join(e.to_string()))?;
        match nauka_iam::consume_password_reset(db, raft, &req.token_id, &req.new_password).await {
            Ok(()) => {
                let _ = writer.write_all(b"{\"ok\":true}\n").await;
                tracing::info!(event = "iam.password.reset.ok");
            }
            Err(e) => {
                let body = serde_json::json!({ "error": e.to_string() }).to_string();
                let _ = writer.write_all(format!("{body}\n").as_bytes()).await;
                tracing::warn!(event = "iam.password.reset.fail", error = %e);
            }
        }
    } else if v.get("iam_user_set_active").is_some() {
        if !peer_addr.ip().is_loopback() {
            let _ = writer
                .write_all(b"{\"error\":\"iam_user_set_active requires loopback\"}\n")
                .await;
            return Err(MeshError::Join("non-loopback iam_user_set_active".into()));
        }
        let raft = raft
            .as_ref()
            .ok_or_else(|| MeshError::Join("no raft".into()))?;
        let req: IamUserSetActiveRequest =
            serde_json::from_value(v).map_err(|e| MeshError::Join(e.to_string()))?;
        match nauka_iam::set_user_active(db, raft, &req.jwt, &req.email, req.active, &req.reason)
            .await
        {
            Ok(()) => {
                let _ = writer.write_all(b"{\"ok\":true}\n").await;
                tracing::info!(
                    event = "iam.user.set_active.ok",
                    email = %req.email,
                    active = req.active
                );
            }
            Err(e) => {
                let body = serde_json::json!({ "error": e.to_string() }).to_string();
                let _ = writer.write_all(format!("{body}\n").as_bytes()).await;
                tracing::warn!(
                    event = "iam.user.set_active.fail",
                    email = %req.email,
                    error = %e
                );
            }
        }
    } else if v.get("iam_signin").is_some() {
        // Authenticate an existing user. Loopback-only in IAM-1; a
        // REST surface with bearer-token auth arrives with the
        // ResourceDef work (#342 + IAM-3).
        if !peer_addr.ip().is_loopback() {
            let _ = writer
                .write_all(b"{\"error\":\"iam_signin requires loopback\"}\n")
                .await;
            nauka_core::bail_log!(
                MeshError::Join("non-loopback iam_signin".into()),
                event = "iam.signin.non_loopback_rejected",
                peer = %peer_addr,
                "iam_signin requires loopback"
            );
        }
        let req: IamSigninRequest =
            serde_json::from_value(v).map_err(|e| MeshError::Join(e.to_string()))?;
        let raft = raft
            .as_ref()
            .ok_or_else(|| MeshError::Join("no raft".into()))?;
        let peer_ip_str = peer_addr.ip().to_string();
        match nauka_iam::signin(db, raft, &req.email, &req.password, &peer_ip_str).await {
            Ok(jwt) => {
                let body = serde_json::json!({ "ok": true, "jwt": jwt.into_string() }).to_string();
                let _ = writer.write_all(format!("{body}\n").as_bytes()).await;
                tracing::info!(
                    event = "iam.signin.ok",
                    email = %req.email,
                    "iam signin ok"
                );
            }
            Err(e) => {
                // Collapse to a fixed "invalid credentials" response
                // from the client's point of view even though we log
                // the real reason — that distinction must never be
                // exposed externally (enumeration oracle).
                let body = serde_json::json!({ "error": e.to_string() }).to_string();
                let _ = writer.write_all(format!("{body}\n").as_bytes()).await;
                tracing::warn!(
                    event = "iam.signin.fail",
                    email = %req.email,
                    error = %e,
                    "iam signin failed"
                );
            }
        }
    }

    Ok(())
}

#[derive(Deserialize, SurrealValue)]
struct HypervisorListRow {
    public_key: String,
    address: String,
    endpoint: Option<String>,
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
    writeln!(
        stream,
        "{}",
        serde_json::to_string(&req).expect("serialize")
    )
    .map_err(|e| MeshError::Join(e.to_string()))?;

    let reader = BufReader::new(
        stream
            .try_clone()
            .map_err(|e| MeshError::Join(e.to_string()))?,
    );
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
    let mut mesh = Mesh::new(
        interface_name,
        listen_port,
        Some(mesh_id),
        Some(keypair),
        None,
    )?;
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

/// Query the local daemon for its view of the cluster. Returns raw JSON
/// so the CLI can pretty-print. Connects over loopback only.
pub fn request_status(join_port: u16) -> Result<serde_json::Value, MeshError> {
    let addr = format!("127.0.0.1:{join_port}");
    let mut stream =
        TcpStream::connect(&addr).map_err(|e| MeshError::Join(format!("connect daemon: {e}")))?;
    writeln!(stream, "{}", serde_json::json!({ "status": true }))
        .map_err(|e| MeshError::Join(e.to_string()))?;
    let reader = BufReader::new(stream);
    let mut lines = reader.lines();
    let line = lines
        .next()
        .ok_or_else(|| MeshError::Join("no status response".into()))?
        .map_err(|e| MeshError::Join(e.to_string()))?;
    let v: serde_json::Value =
        serde_json::from_str(&line).map_err(|e| MeshError::Join(format!("status parse: {e}")))?;
    if let Some(err) = v.get("error").and_then(|e| e.as_str()) {
        return Err(MeshError::Join(err.to_string()));
    }
    Ok(v)
}

/// Tell the local daemon to raft.write a DELETE for this node's own
/// hypervisor record. The daemon keeps running — the CLI is expected to
/// stop the systemd service next. Bounded read timeout so a daemon that's
/// stuck (no leader, election loop) doesn't block teardown forever.
pub fn request_leave(join_port: u16) -> Result<(), MeshError> {
    let addr = format!("127.0.0.1:{join_port}");
    let stream =
        TcpStream::connect(&addr).map_err(|e| MeshError::Join(format!("connect daemon: {e}")))?;
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(5)))
        .map_err(|e| MeshError::Join(format!("set read timeout: {e}")))?;
    let mut s = stream
        .try_clone()
        .map_err(|e| MeshError::Join(e.to_string()))?;
    writeln!(s, "{}", serde_json::json!({ "leave": true }))
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

/// Debug escape hatch: send an arbitrary SurQL write to the local daemon,
/// which forwards it to the Raft leader. Connects over loopback only; any
/// non-loopback peer trying the same thing is rejected by the listener.
pub fn request_raft_write(join_port: u16, query: &str) -> Result<(), MeshError> {
    let addr = format!("127.0.0.1:{join_port}");
    let mut stream =
        TcpStream::connect(&addr).map_err(|e| MeshError::Join(format!("connect daemon: {e}")))?;

    let req = serde_json::json!({ "raft_write": true, "query": query });
    writeln!(stream, "{req}").map_err(|e| MeshError::Join(e.to_string()))?;

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

/// Ask a remote peer what IP it observes us on. Used by restart flow to
/// learn our current public endpoint without an external STUN-like service.
pub async fn whoami(peer_ip: &str, join_port: u16) -> Result<std::net::IpAddr, MeshError> {
    use tokio::io::AsyncBufReadExt as _;
    use tokio::net::TcpStream;
    let addr = format!("{peer_ip}:{join_port}");
    let stream = TcpStream::connect(&addr)
        .await
        .map_err(|e| MeshError::Join(format!("whoami connect {addr}: {e}")))?;
    let (reader, mut writer) = stream.into_split();
    tokio::io::AsyncWriteExt::write_all(&mut writer, b"{\"whoami\":true}\n")
        .await
        .map_err(|e| MeshError::Join(format!("whoami write: {e}")))?;
    let mut lines = tokio::io::BufReader::new(reader).lines();
    let line = lines
        .next_line()
        .await
        .map_err(|e| MeshError::Join(format!("whoami read: {e}")))?
        .ok_or_else(|| MeshError::Join("whoami empty response".into()))?;
    let v: serde_json::Value =
        serde_json::from_str(&line).map_err(|e| MeshError::Join(format!("whoami parse: {e}")))?;
    let ip_str = v
        .get("observed_ip")
        .and_then(|x| x.as_str())
        .ok_or_else(|| MeshError::Join("whoami: missing observed_ip".into()))?;
    ip_str
        .parse()
        .map_err(|_| MeshError::Join(format!("whoami: invalid ip {ip_str}")))
}

/// Ask the local daemon to create a new user. The daemon hashes the
/// password in Rust and replicates the record via Raft. Loopback
/// only; returns the minted JWT on success.
pub fn request_iam_signup(
    join_port: u16,
    email: &str,
    password: &str,
    display_name: &str,
) -> Result<String, MeshError> {
    let addr = format!("127.0.0.1:{join_port}");
    let mut stream =
        TcpStream::connect(&addr).map_err(|e| MeshError::Join(format!("connect daemon: {e}")))?;
    let req = serde_json::json!({
        "iam_signup": true,
        "email": email,
        "password": password,
        "display_name": display_name,
    });
    writeln!(stream, "{req}").map_err(|e| MeshError::Join(e.to_string()))?;
    read_jwt_response(stream)
}

/// Ask the local daemon to authenticate an existing user. Returns
/// the minted JWT on success, an error otherwise.
pub fn request_iam_signin(
    join_port: u16,
    email: &str,
    password: &str,
) -> Result<String, MeshError> {
    let addr = format!("127.0.0.1:{join_port}");
    let mut stream =
        TcpStream::connect(&addr).map_err(|e| MeshError::Join(format!("connect daemon: {e}")))?;
    let req = serde_json::json!({
        "iam_signin": true,
        "email": email,
        "password": password,
    });
    writeln!(stream, "{req}").map_err(|e| MeshError::Join(e.to_string()))?;
    read_jwt_response(stream)
}

/// Send a JSON request to the local daemon and return the raw
/// response value. Used by the IAM org/project/env RPCs — each one
/// returns structured data (not a JWT), so they don't go through
/// `read_jwt_response`.
pub fn request_json(
    join_port: u16,
    req: serde_json::Value,
) -> Result<serde_json::Value, MeshError> {
    let addr = format!("127.0.0.1:{join_port}");
    let mut stream =
        TcpStream::connect(&addr).map_err(|e| MeshError::Join(format!("connect daemon: {e}")))?;
    writeln!(stream, "{req}").map_err(|e| MeshError::Join(e.to_string()))?;
    let reader = BufReader::new(stream);
    let mut lines = reader.lines();
    let line = lines
        .next()
        .ok_or_else(|| MeshError::Join("no response".into()))?
        .map_err(|e| MeshError::Join(e.to_string()))?;
    let v: serde_json::Value =
        serde_json::from_str(&line).map_err(|e| MeshError::Join(e.to_string()))?;
    if let Some(err) = v.get("error").and_then(|e| e.as_str()) {
        return Err(MeshError::Join(err.to_string()));
    }
    Ok(v)
}

fn read_jwt_response(stream: TcpStream) -> Result<String, MeshError> {
    let reader = BufReader::new(stream);
    let mut lines = reader.lines();
    let line = lines
        .next()
        .ok_or_else(|| MeshError::Join("no response".into()))?
        .map_err(|e| MeshError::Join(e.to_string()))?;
    let v: serde_json::Value =
        serde_json::from_str(&line).map_err(|e| MeshError::Join(e.to_string()))?;
    if let Some(err) = v.get("error").and_then(|e| e.as_str()) {
        return Err(MeshError::Join(err.to_string()));
    }
    let jwt = v
        .get("jwt")
        .and_then(|x| x.as_str())
        .ok_or_else(|| MeshError::Join("response missing `jwt` field".into()))?;
    Ok(jwt.to_string())
}

/// Send a remove-peer command to the local daemon
pub fn request_peer_removal(join_port: u16, public_key: &str) -> Result<(), MeshError> {
    let addr = format!("127.0.0.1:{join_port}");
    let mut stream =
        TcpStream::connect(&addr).map_err(|e| MeshError::Join(format!("connect daemon: {e}")))?;

    let req = RemoveRequest {
        remove_public_key: public_key.to_string(),
    };
    writeln!(
        stream,
        "{}",
        serde_json::to_string(&req).expect("serialize")
    )
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
