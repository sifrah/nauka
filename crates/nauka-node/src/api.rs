//! Public HTTP API of a node: any node of the cluster is a complete entry
//! point.
//!
//! - `POST /api/upload?name=…`: takes in the file, encodes it Reed-Solomon
//!   stripe by stripe and dispatches every shard to its HRW owner, then
//!   records the manifest in the Raft registry.
//! - `GET /f/{hash}`: rebuilds the file, streaming, from the cluster
//!   (k shards are enough, wherever they live), integrity verified.
//! - `GET /api/files`: the replicated registry.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use anyhow::{anyhow, Context, Result};
use axum::body::Body;
use axum::extract::{Path, Query, Request, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use nauka_erasure::{decode_stripe, encode_stripe, ErasureConfig, FileManifest, StripeMeta};
use nauka_store::ShardStore;
use nauka_transport::PeerClient;
use tokio::io::AsyncReadExt;

pub struct ApiState {
    pub store: Arc<ShardStore>,
    pub app: Arc<nauka_raft::RaftApp>,
    /// Advertised address of THIS node (its placement identity).
    pub self_id: String,
    /// Public location of THIS node, resolved from the same city database
    /// as the geo-DNS. `None` until that database is ready.
    pub node_location: RwLock<Option<NodeLocation>>,
    pub config: ErasureConfig,
    /// Directory used to buffer in-flight uploads.
    pub tmp_dir: PathBuf,
    /// Liveness map fed by the background pinger: uploads only route
    /// shards at members currently answering.
    pub health: Arc<nauka_cluster::health::PeerHealth>,
    /// This node's monthly egress ledger (bytes served to clients),
    /// published into the replicated state by the maintenance ticker.
    pub egress: Arc<crate::egress::EgressMeter>,
    /// Opt-in stripe cache (`NAUKA_CACHE_SIZE`): decoded stripes that
    /// crossed the cluster once are served from local disk after.
    pub cache: Option<Arc<crate::cache::StripeCache>>,
    /// Global admission budget for upload RAM windows — sized once at
    /// startup so concurrent uploads share a fixed fraction of the machine
    /// instead of racing for "what is left".
    pub ingest_pool: Arc<crate::ingest::RamPool>,
    /// Bytes of locally-acked uploads not yet dispersed. Bounds the
    /// local-ack window: past a cap, uploads pay full dispersal again.
    pub staged_bytes: Arc<std::sync::atomic::AtomicU64>,
    /// Per-space egress served by THIS node, not yet published to the
    /// replicated ledger (space → (month, bytes)). The maintenance pass
    /// folds it into `AppState::space_egress` under this node's row.
    pub space_egress_local:
        Arc<std::sync::Mutex<std::collections::BTreeMap<String, (String, u64)>>>,
    /// Queue of files to pre-warm into the stripe cache (None when the
    /// cache is disabled). Best-effort by design: a full queue drops the
    /// signal, never blocks the request that emitted it.
    pub warm_tx: Option<tokio::sync::mpsc::Sender<String>>,
    /// Partial-read counters feeding the hot-file warming signal
    /// (file hash → count within the current window).
    pub hot_reads: std::sync::Mutex<HashMap<String, (u32, std::time::Instant)>>,
    /// Connections in flight per signed link (keyed by signature), for
    /// links carrying a `conc=` cap. This map is the node's OWN truth;
    /// it is also what the gossip loop pushes to the neighborhood once
    /// a second while non-empty.
    pub link_conc: Arc<std::sync::Mutex<HashMap<String, u32>>>,
    /// The neighborhood's view: per sending peer, its last pushed
    /// per-link counts and when they arrived. Entries older than
    /// [`REMOTE_CONC_TTL`] are ignored (a dead peer must not hold a
    /// link's budget hostage). Admission sums local + fresh remote, so
    /// a client fanning out across the DNS answer still hits the cap —
    /// within a second of gossip lag rather than exactly, by design.
    #[allow(clippy::type_complexity)]
    pub link_conc_remote:
        Arc<std::sync::Mutex<HashMap<String, (std::time::Instant, HashMap<String, u32>)>>>,
}

/// Minimal public identity of the node selected by geo-DNS. Deliberately
/// excludes its address and every cluster detail: products only need a
/// human-readable city and a flag.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct NodeLocation {
    pub city: String,
    pub country_code: String,
}

impl NodeLocation {
    pub(crate) fn new(city: &str, country_code: &str) -> Option<Self> {
        let city = city.trim();
        let country_code = country_code.trim().to_ascii_uppercase();
        if city.is_empty()
            || country_code.len() != 2
            || !country_code.bytes().all(|byte| byte.is_ascii_uppercase())
        {
            return None;
        }
        Some(Self {
            city: city.to_string(),
            country_code,
        })
    }
}

/// Remote conc gossip older than this is stale and ignored.
pub const REMOTE_CONC_TTL: std::time::Duration = std::time::Duration::from_secs(5);

/// The landing pad of [`nauka_transport::server::ConcView`] gossip,
/// sharing the remote map with [`ApiState`].
pub struct ConcAbsorber(
    #[allow(clippy::type_complexity)]
    pub  Arc<std::sync::Mutex<HashMap<String, (std::time::Instant, HashMap<String, u32>)>>>,
);

impl nauka_transport::server::ConcView for ConcAbsorber {
    fn absorb(&self, from: &str, counts: Vec<(String, u32)>) {
        let mut m = self.0.lock().unwrap();
        if counts.is_empty() {
            // "Nothing in flight anymore": drop the row now instead of
            // letting it age out — frees the budget a second earlier.
            m.remove(from);
        } else {
            m.insert(
                from.to_string(),
                (std::time::Instant::now(), counts.into_iter().collect()),
            );
        }
    }
}

impl ApiState {
    /// Weighted view of the cluster used for placement — the very same one
    /// the scrubbers use (capacities declared in the Raft state).
    fn view(&self) -> Vec<(String, u64)> {
        let mut nodes = self
            .app
            .weighted_view(nauka_cluster::placement::DEFAULT_CAPACITY);
        if nodes.is_empty() {
            nodes.push((
                self.self_id.clone(),
                nauka_cluster::placement::DEFAULT_CAPACITY,
            ));
        }
        nodes
    }

    /// Placement view for NEW writes: the members currently answering. A
    /// node marked down keeps its membership but takes no new shards; the
    /// scrubber completes the redundancy when it returns (or elsewhere,
    /// since it also works on the live view).
    fn view_alive(&self) -> Vec<(String, u64)> {
        self.health.filter_view(self.view())
    }

    /// Whether a registry write can plausibly commit right now. Two cheap
    /// local signals:
    ///   - no leader at all (an isolated follower, an election in flight)
    ///     — instant; and
    ///   - this node cannot see a quorum of voters on the data plane.
    ///     openraft keeps a partitioned leader believing it still leads,
    ///     so `leader_known()` alone would let the write sit for the whole
    ///     write timeout; the health map (fed by the peer pinger) catches
    ///     the lost quorum and lets the caller refuse immediately instead.
    ///
    /// Optimistic by construction: an unprobed peer counts as alive, so
    /// this only says "no" once the pinger has actually observed peers as
    /// gone — it never manufactures a false refusal on a healthy cluster.
    pub(crate) fn can_commit_write(&self) -> bool {
        if !self.app.leader_known() {
            return false;
        }
        let members = self.app.members();
        if members.is_empty() {
            return true; // not in consensus mode; nothing to gate on
        }
        // This node is never pinged, so it counts as alive by construction.
        let alive = members
            .values()
            .filter(|addr| self.health.is_alive(addr))
            .count();
        alive * 2 > members.len()
    }
}

pub async fn serve_http(listen: SocketAddr, state: Arc<ApiState>) -> Result<()> {
    tokio::fs::create_dir_all(&state.tmp_dir).await?;
    let router = router(state);
    let listener = tokio::net::TcpListener::bind(listen).await?;
    tracing::info!("HTTP API on http://{listen}");
    axum::serve(
        listener,
        router.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .await?;
    Ok(())
}

/// The full public API router — served plain on :8080 and, when a
/// domain is configured, over TLS by the HTTPS front (same routes,
/// same handlers, one truth).
pub fn router(state: Arc<ApiState>) -> Router {
    // Nauka is the storage engine: it serves its HTTP API and nothing
    // more. A user-facing web interface belongs to a product built on top,
    // not in the engine.
    Router::new()
        // PUT as well as POST: `curl -T file` — the streaming upload every
        // doc example recommends — sends PUT, and answering it with a 405
        // was the first thing a reader following the docs would hit.
        .route("/api/upload", post(upload).put(upload))
        .route("/api/files", get(files))
        .route("/api/status", get(status))
        .route("/api/location", get(location))
        .route("/api/removal-check", get(removal_check))
        .route("/api/shard-inventory", get(shard_inventory))
        .route("/api/orgs", get(orgs_view))
        .route(
            "/f/{hash}",
            get(download).head(download_head).delete(delete_file),
        )
        .route("/f/{hash}/refs", axum::routing::post(ref_add))
        .with_state(state)
        // The HTTP door is a public API and browsers are clients: a
        // web product built on the engine has its users' browsers PUT
        // uploads (signed X-Nauka-* headers → CORS preflight) and
        // fetch() signed links directly at the nodes. Permissive by
        // design — auth is signatures, never cookies, so there is no
        // ambient credential for a foreign origin to ride.
        .layer(tower_http::cors::CorsLayer::permissive())
}

async fn location(State(state): State<Arc<ApiState>>) -> Response {
    let value = state
        .node_location
        .read()
        .ok()
        .and_then(|location| location.clone());
    match value {
        Some(location) => ([(header::CACHE_CONTROL, "no-store")], Json(location)).into_response(),
        None => StatusCode::SERVICE_UNAVAILABLE.into_response(),
    }
}

#[derive(serde::Serialize)]
struct NodeStatus {
    addr: String,
    /// Raft id of the member at this address, None for an address present
    /// in the placement view but not (yet) in the consensus membership.
    /// This is what `node remove <id>` takes — exposed here so the id can
    /// be read over plain HTTP (`nauka status`) without a cluster identity.
    id: Option<u64>,
    capacity_bytes: u64,
    is_leader: bool,
    is_self: bool,
    /// Draining (`nauka node disable`): still a member, excluded from
    /// placement while its shards migrate away.
    #[serde(default)]
    disabled: bool,
    /// Liveness as the local pinger sees it: false once the peer missed
    /// `MISS_THRESHOLD` probes in a row (~15 s). This is THIS node's view,
    /// not a cluster-wide verdict — a member is still a full member while
    /// down, it just takes no new shards. Optimistic like the map it comes
    /// from: a peer nobody has probed yet reads alive.
    is_alive: bool,
}

#[derive(serde::Serialize)]
struct ClusterStatusResponse {
    self_addr: String,
    /// This node's Raft id. Exposed so `node add` can learn a freshly
    /// provisioned node's id over plain HTTP — no cluster identity needed
    /// for the query — instead of shelling `node-info` on the target.
    self_node_id: u64,
    leader: Option<String>,
    nodes: Vec<NodeStatus>,
    files: usize,
    total_bytes: u64,
    /// Shard bytes THIS node holds on disk — each node only knows its own
    /// store, so a whole-cluster view (`nauka top`) asks every member.
    self_used_bytes: u64,
    /// Shard files behind `self_used_bytes`.
    self_shard_count: u64,
    /// Cumulative machine-level network counters (bytes received/sent on
    /// every non-loopback interface, from /proc/net/dev). Machine-level
    /// on purpose: shards, Raft, HTTP and healing all count — the number
    /// an operator's bandwidth bill sees. Absent off Linux.
    #[serde(skip_serializing_if = "Option::is_none")]
    self_net_rx_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    self_net_tx_bytes: Option<u64>,
}

/// Sum of (rx, tx) bytes across non-loopback interfaces. Linux only —
/// which every systemd deployment is; a macOS dev node just omits it.
fn net_counters() -> Option<(u64, u64)> {
    let dev = std::fs::read_to_string("/proc/net/dev").ok()?;
    let (mut rx, mut tx) = (0u64, 0u64);
    for line in dev.lines().skip(2) {
        let Some((iface, rest)) = line.split_once(':') else {
            continue;
        };
        if iface.trim() == "lo" {
            continue;
        }
        let f: Vec<&str> = rest.split_whitespace().collect();
        if f.len() >= 9 {
            rx += f[0].parse::<u64>().unwrap_or(0);
            tx += f[8].parse::<u64>().unwrap_or(0);
        }
    }
    Some((rx, tx))
}

async fn status(State(state): State<Arc<ApiState>>) -> Json<ClusterStatusResponse> {
    let members = state.app.members();
    let metrics = state.app.raft.metrics().borrow().clone();
    let leader_addr = metrics
        .current_leader
        .and_then(|id| members.get(&id).cloned());
    let app_state = state.app.app_state();
    // The FULL membership, annotated with liveness — not `view_alive()`:
    // a down member must still be listed, marked down, rather than vanish
    // from the cluster page. One snapshot for the whole list, so every row
    // reports the same probe round. One row PER MEMBER, id attached — two
    // members can share an address (a replaced machine whose stale
    // identity lingers), and rows keyed by address would collapse them
    // into an indistinguishable duplicate.
    let liveness = state.health.snapshot();
    let capacities = app_state.node_capacities.clone();
    let mut nodes: Vec<NodeStatus> = members
        .iter()
        .map(|(id, addr)| NodeStatus {
            is_leader: leader_addr.as_deref() == Some(addr.as_str()),
            is_self: *addr == state.self_id,
            disabled: app_state.disabled.contains(addr),
            // Nobody pings themselves, so self is never in the map; an
            // unprobed peer reads alive, same rule as `is_alive`.
            is_alive: liveness.get(addr).copied().unwrap_or(true),
            id: Some(*id),
            addr: addr.clone(),
            capacity_bytes: capacities
                .get(addr)
                .copied()
                .unwrap_or(nauka_cluster::placement::DEFAULT_CAPACITY),
        })
        .collect();
    // Not yet in consensus (a node still waiting to be added): show self.
    if nodes.is_empty() {
        nodes.push(NodeStatus {
            is_leader: false,
            is_self: true,
            is_alive: true,
            disabled: false,
            id: Some(state.app.id),
            addr: state.self_id.clone(),
            capacity_bytes: nauka_cluster::placement::DEFAULT_CAPACITY,
        });
    }
    nodes.sort_by(|a, b| a.addr.cmp(&b.addr).then(a.id.cmp(&b.id)));
    let (self_used_bytes, self_shard_count) = state.store.disk_usage();
    let (self_net_rx_bytes, self_net_tx_bytes) = match net_counters() {
        Some((rx, tx)) => (Some(rx), Some(tx)),
        None => (None, None),
    };
    Json(ClusterStatusResponse {
        self_addr: state.self_id.clone(),
        self_node_id: state.app.id,
        leader: leader_addr,
        nodes,
        files: app_state.manifests.len(),
        total_bytes: app_state.manifests.values().map(|m| m.file_size).sum(),
        self_used_bytes,
        self_shard_count,
        self_net_rx_bytes,
        self_net_tx_bytes,
    })
}

#[derive(serde::Deserialize)]
struct RemovalCheckParams {
    /// Advertised address of the node about to be removed or drained.
    target: String,
}

#[derive(serde::Serialize)]
struct AtRiskFile {
    hash: String,
    name: Option<String>,
    /// The worst stripe of this file: how many of its shards would survive
    /// on reliable nodes (must be ≥ k to stay recoverable).
    shards_left: usize,
}

#[derive(serde::Serialize)]
struct RemovalCheckResponse {
    target: String,
    /// The reconstruction threshold — every stripe needs this many shards.
    k: usize,
    /// Reliable nodes that would remain: alive, not draining, not the target.
    reliable_nodes: usize,
    /// True when every file keeps at least k shards on those nodes.
    safe: bool,
    /// Total files that would drop below recoverable BECAUSE of this
    /// removal (they are fine today, counting every reachable copy).
    at_risk: usize,
    /// Files below k shards no matter what — already unrecoverable before
    /// this removal, even counting the target's own disk. They do not
    /// block the removal (their fate is sealed either way); they are
    /// surfaced so the operator learns about them HERE, not later.
    already_lost: usize,
    /// The worst case seen across all stripes (min shards left).
    worst_shards_left: usize,
    /// A sample of at-risk files (capped), for the operator to see.
    sample: Vec<AtRiskFile>,
    /// Why it is unsafe, in one human line (empty when safe).
    reason: String,
}

/// GET /api/shard-inventory — the shard hashes physically present in this
/// node's store, one flat JSON array. The removal pre-flight sums these
/// across the surviving nodes: placement says where shards SHOULD be, and
/// during an active rebalance reality lags it (a pre-flight built on
/// placement blessed a removal while a file was already below k — found
/// the hard way on 2026-08-12).
async fn shard_inventory(State(state): State<Arc<ApiState>>) -> Json<Vec<String>> {
    Json(state.store.list_shards().unwrap_or_default())
}

/// GET /api/orgs — the replicated organisation/space registry, as the CLI
/// (`nauka org list`) reads it. Names and policies only, never keys.
/// NOTE: public for now like the rest of the read API; AUTH-4's
/// private-by-default switch will decide what this endpoint exposes to
/// whom.
async fn orgs_view(State(state): State<Arc<ApiState>>) -> Json<serde_json::Value> {
    let s = state.app.app_state();
    // Keys go out hex-encoded (they are PUBLIC verification material —
    // the private halves never exist server-side).
    let keys: std::collections::BTreeMap<&String, Vec<serde_json::Value>> = s
        .space_keys
        .iter()
        .map(|(space, keys)| {
            (
                space,
                keys.iter()
                    .map(|k| {
                        serde_json::json!({
                            "public_key": hex::encode(k.public_key),
                            "role": match k.role {
                                nauka_raft::types::SpaceKeyRole::Signer => "signer",
                                nauka_raft::types::SpaceKeyRole::Admin => "admin",
                            },
                            "name": k.name,
                        })
                    })
                    .collect(),
            )
        })
        .collect();
    let month = crate::egress::month_key(crate::spaceauth::unix_now());
    let usage: std::collections::BTreeMap<&String, serde_json::Value> = s
        .spaces
        .keys()
        .map(|name| {
            (
                name,
                serde_json::json!({
                    "storage_bytes": space_storage_bytes(&s, name),
                    "egress_month_bytes": space_egress_month(&state, &s, name, &month),
                }),
            )
        })
        .collect();
    Json(serde_json::json!({
        "orgs": s.orgs,
        "spaces": s.spaces,
        "space_keys": keys,
        "usage": usage,
    }))
}

/// GET /api/removal-check?target=<addr> — would removing (or fully
/// draining) `target` leave any file unrecoverable? For every stripe it
/// counts how many shards are PHYSICALLY present on RELIABLE nodes
/// (alive, not already draining, not the target), by summing the actual
/// shard inventories of those nodes. Placement is deliberately not
/// consulted: it says where shards should be, and during an active
/// rebalance reality lags it — a placement-based pre-flight once blessed
/// a removal while a file already sat below k on disk. A stripe is safe
/// while ≥ k of its 6 shards exist on surviving disks; the file is at
/// risk the moment any of its stripes drops below k. Plain HTTP, no
/// identity — the pre-flight `node remove` and `node disable` run before
/// they touch anything.
async fn removal_check(
    State(state): State<Arc<ApiState>>,
    Query(p): Query<RemovalCheckParams>,
) -> Json<RemovalCheckResponse> {
    let k = state.config.data_shards;
    let members = state.app.members();
    let app_state = state.app.app_state();
    let disabled = &app_state.disabled;
    let liveness = state.health.snapshot();

    // A node still counted on after the removal: not the target, not
    // already draining, and currently answering the liveness pinger. A
    // down node cannot be relied on to hold the surviving copy.
    let reliable = |addr: &str| {
        addr != p.target && !disabled.contains(addr) && liveness.get(addr).copied().unwrap_or(true)
    };
    let member_addrs: std::collections::BTreeSet<String> = members.values().cloned().collect();
    let reliable_nodes = member_addrs.iter().filter(|a| reliable(a)).count();

    // The physical truth: which shard hashes exist on which disks, asked
    // to every member (5 s each, one round). `surviving` counts only the
    // reliable nodes; `anywhere` also counts the target and the draining
    // or unreachable-but-answering ones — the difference separates "this
    // removal would lose it" from "it was already gone".
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()
        .expect("static client config");
    let mut surviving: std::collections::BTreeSet<String> = Default::default();
    let mut anywhere: std::collections::BTreeSet<String> = Default::default();
    let mut unanswered = 0usize;
    for addr in &member_addrs {
        let inventory: Option<Vec<String>> = if *addr == state.self_id {
            state.store.list_shards().ok()
        } else {
            let ip = addr.split(':').next().unwrap_or_default();
            match client
                .get(format!("http://{ip}:8080/api/shard-inventory"))
                .send()
                .await
            {
                Ok(r) => r.json().await.ok(),
                Err(_) => None,
            }
        };
        match inventory {
            Some(hashes) => {
                if reliable(addr) {
                    surviving.extend(hashes.iter().cloned());
                }
                anywhere.extend(hashes);
            }
            // An unreachable inventory contributes nothing to `surviving`:
            // shards we cannot see are shards we must not count on. The
            // check fails towards "unsafe", never towards data loss.
            None => unanswered += 1,
        }
    }

    let mut at_risk: Vec<AtRiskFile> = Vec::new();
    let mut already_lost = 0usize;
    let mut worst = k.max(1);
    for (fh, m) in &app_state.manifests {
        let mut surviving_min = usize::MAX;
        let mut anywhere_min = usize::MAX;
        for stripe in m.stripes.iter() {
            let on_survivors = stripe
                .shard_hashes
                .iter()
                .filter(|h| surviving.contains(*h))
                .count();
            let on_any = stripe
                .shard_hashes
                .iter()
                .filter(|h| anywhere.contains(*h))
                .count();
            surviving_min = surviving_min.min(on_survivors);
            anywhere_min = anywhere_min.min(on_any);
        }
        if surviving_min == usize::MAX {
            continue; // a file with no stripes; nothing to lose
        }
        if anywhere_min < k {
            // Below k even counting every reachable disk, target included:
            // this removal changes nothing for it. Reported, not blocking.
            already_lost += 1;
            continue;
        }
        worst = worst.min(surviving_min);
        if surviving_min < k {
            if at_risk.len() < 20 {
                at_risk.push(AtRiskFile {
                    hash: fh.clone(),
                    name: m.name.clone(),
                    shards_left: surviving_min,
                });
            } else {
                at_risk.push(AtRiskFile {
                    hash: String::new(),
                    name: None,
                    shards_left: surviving_min,
                }); // counted, not sampled
            }
        }
    }
    let at_risk_count = at_risk.len();
    let sample: Vec<AtRiskFile> = at_risk.into_iter().filter(|f| !f.hash.is_empty()).collect();
    let safe = at_risk_count == 0;
    let mut reason = if safe {
        String::new()
    } else if reliable_nodes < k {
        format!(
            "only {reliable_nodes} reliable node(s) would remain — 4+2 needs at least \
             {k} to keep any file recoverable"
        )
    } else {
        format!(
            "{at_risk_count} file(s) would drop below {k} shards on the remaining disks — \
             bring every node back online first, or drain this node so its copies move \
             before it leaves"
        )
    };
    if !safe && unanswered > 0 {
        reason.push_str(&format!(
            " ({unanswered} node(s) did not answer the inventory call; \
             their shards are not counted)"
        ));
    }

    Json(RemovalCheckResponse {
        target: p.target,
        k,
        reliable_nodes,
        safe,
        at_risk: at_risk_count,
        already_lost,
        worst_shards_left: worst,
        sample,
        reason,
    })
}

/// Logical bytes a space accounts for: the sum of the sizes of the
/// files it references. Dedup is physical, quotas are logical — two
/// spaces referencing the same file each count it in full.
fn space_storage_bytes(s: &nauka_raft::types::AppState, space: &str) -> u64 {
    s.file_refs
        .iter()
        .filter(|(_, spaces)| spaces.contains(space))
        .filter_map(|(hash, _)| s.manifests.get(hash).map(|m| m.file_size))
        .sum()
}

/// A space's egress for `month`, replicated rows plus this node's
/// unpublished local delta.
fn space_egress_month(
    state: &ApiState,
    s: &nauka_raft::types::AppState,
    space: &str,
    month: &str,
) -> u64 {
    let replicated: u64 = s
        .space_egress
        .get(space)
        .map(|rows| {
            rows.values()
                .filter(|e| e.month == month)
                .map(|e| e.served_bytes)
                .sum()
        })
        .unwrap_or(0);
    let local = state
        .space_egress_local
        .lock()
        .ok()
        .and_then(|m| m.get(space).filter(|(mo, _)| mo == month).map(|(_, b)| *b))
        .unwrap_or(0);
    replicated + local
}

/// Records `bytes` of egress against the space whose grant served the
/// read. Folded into the replicated ledger by the maintenance pass.
fn record_space_egress(state: &ApiState, space: &str, bytes: u64) {
    let month = crate::egress::month_key(crate::spaceauth::unix_now());
    if let Ok(mut m) = state.space_egress_local.lock() {
        let entry = m
            .entry(space.to_string())
            .or_insert_with(|| (month.clone(), 0));
        if entry.0 != month {
            *entry = (month, 0);
        }
        entry.1 += bytes;
    }
}

/// Refuses a write that would push a space (or its organisation) past
/// its storage quota. `incoming` is the file's logical size; counts
/// only when the space does not already reference the hash.
fn check_storage_quota(
    s: &nauka_raft::types::AppState,
    space: &str,
    file_hash: &str,
    incoming: u64,
) -> Result<(), String> {
    if s.file_refs
        .get(file_hash)
        .is_some_and(|r| r.contains(space))
    {
        return Ok(()); // already referenced: no logical growth
    }
    let record = match s.spaces.get(space) {
        Some(r) => r,
        None => return Ok(()), // upstream checks handle unknown spaces
    };
    if let Some(q) = record.quota_bytes {
        let used = space_storage_bytes(s, space);
        if used.saturating_add(incoming) > q {
            return Err(format!(
                "storage quota exceeded on {space}: {used} B used of {q} B, this file                  adds {incoming} B — raise the quota (`nauka space set {space} --quota …`)                  or release files"
            ));
        }
    }
    if let Some(org) = s.orgs.get(&record.org) {
        if let Some(q) = org.quota_bytes {
            let used: u64 = s
                .spaces
                .iter()
                .filter(|(_, r)| r.org == record.org)
                .map(|(name, _)| space_storage_bytes(s, name))
                .sum();
            if used.saturating_add(incoming) > q {
                return Err(format!(
                    "organisation {} is at its storage quota ({used} B of {q} B)",
                    record.org
                ));
            }
        }
    }
    Ok(())
}

#[derive(serde::Deserialize)]
struct RefAddParams {
    /// Target space receiving the reference.
    to: String,
}

/// POST /f/{hash}/refs?to=<org/space> — reference an EXISTING file from
/// another space, without re-uploading a byte. The "make it public"
/// gesture: publish a private file by referencing it from a public-read
/// space; revoke by signed-DELETEing that reference.
///
/// The signature covers the full path INCLUDING `?to=` (a captured
/// request cannot be replayed towards a different target), and the
/// chain of custody is enforced: the signing space must already
/// reference the file, and the target must belong to the SAME
/// organisation — no space can annex another tenant's content. The one
/// exception is adoption: an unowned pre-tenant file can be claimed by
/// the signing space itself (`to` = the signer), which is the migration
/// path out of the legacy era.
async fn ref_add(
    State(state): State<Arc<ApiState>>,
    Path(hash): Path<String>,
    Query(p): Query<RefAddParams>,
    headers: axum::http::HeaderMap,
) -> Result<Response, ApiError> {
    let signed_path = format!("/f/{hash}/refs?to={}", p.to);
    let auth =
        verify_space_write(&state, &headers, "POST", &signed_path, None)?.ok_or_else(|| {
            ApiError(
                StatusCode::UNAUTHORIZED,
                anyhow!("adding a reference requires an admin signature (X-Nauka-* headers)"),
            )
        })?;
    let s = state.app.app_state();
    if !s.manifests.contains_key(&hash) {
        return Ok((StatusCode::NOT_FOUND, "unknown file").into_response());
    }
    let target = s.spaces.get(&p.to).ok_or_else(|| {
        ApiError(
            StatusCode::NOT_FOUND,
            anyhow!("no space named {} on this cluster", p.to),
        )
    })?;
    let signer_org = s
        .spaces
        .get(&auth.space)
        .map(|r| r.org.clone())
        .unwrap_or_default();
    if target.org != signer_org {
        return Ok((
            StatusCode::FORBIDDEN,
            format!(
                "{} belongs to another organisation — references never cross orgs",
                p.to
            ),
        )
            .into_response());
    }
    if target.suspended {
        return Ok((
            StatusCode::FORBIDDEN,
            format!("space {} is suspended", p.to),
        )
            .into_response());
    }
    let target_public = target.public_read;
    let refs = s.file_refs.get(&hash);
    let owned = refs.is_some_and(|r| !r.is_empty());
    if owned {
        if !refs.is_some_and(|r| r.contains(&auth.space)) {
            return Ok((
                StatusCode::FORBIDDEN,
                format!(
                    "{} does not reference this file — only a space that holds it may \
                     share it",
                    auth.space
                ),
            )
                .into_response());
        }
    } else if p.to != auth.space {
        return Ok((
            StatusCode::FORBIDDEN,
            "an unowned file can only be ADOPTED by the signing space itself \
             (?to=<your own space>)"
                .to_string(),
        )
            .into_response());
    }
    let incoming = s.manifests.get(&hash).map(|m| m.file_size).unwrap_or(0);
    if let Err(msg) = check_storage_quota(&s, &p.to, &hash, incoming) {
        return Ok((StatusCode::FORBIDDEN, msg).into_response());
    }
    drop(s);
    let resp = state
        .app
        .write(nauka_raft::types::AppCommand::AddFileRef {
            file_hash: hash.clone(),
            space: p.to.clone(),
        })
        .await
        .context("recording the reference")?;
    if !resp.ok {
        return Ok((
            StatusCode::CONFLICT,
            resp.info.unwrap_or_else(|| "the cluster refused".into()),
        )
            .into_response());
    }
    // Publishing to a public-read space says "this is about to be
    // served": the node that took the publish warms itself in the
    // background, best-effort.
    if target_public {
        if let Some(tx) = &state.warm_tx {
            let _ = tx.try_send(hash.clone());
        }
    }
    Ok(Json(serde_json::json!({ "hash": hash, "space": p.to })).into_response())
}

/// The `?space=&exp=&sig=` triplet of a signed read link, plus the
/// optional signed ceilings: `rate` (bytes/s per connection) and
/// `conc` (simultaneous connections on this node), plus `ct` — the
/// content type the issuer wants served inline.
#[derive(serde::Deserialize)]
struct ReadLinkParams {
    space: Option<String>,
    exp: Option<u64>,
    sig: Option<String>,
    rate: Option<u64>,
    conc: Option<u32>,
    ct: Option<String>,
}

/// The read gate. Ownership decides the rules:
/// - a file referenced by an ACTIVE public-read space is served bare;
/// - any other owned file requires a valid signed link — space in the
///   query references the file, space and org active, not expired, and
///   the Ed25519 signature checks out under ANY of the space's keys
///   (`signer` keys exist exactly for this);
/// - an unowned pre-tenant file keeps the open behavior until the final
///   flip, once anonymous uploads are retired.
///
/// Every check is local: replicated registry, no network.
fn authorize_read(
    state: &ApiState,
    hash: &str,
    p: &ReadLinkParams,
) -> Result<ReadGrant, Box<Response>> {
    let s = state.app.app_state();
    let Some(refs) = s.file_refs.get(hash).filter(|r| !r.is_empty()) else {
        // The 0.6 flip: an unowned file is served to NOBODY (except the
        // node's own loopback, handled by the caller). Pre-flip files
        // become readable again the moment a space adopts them.
        return Err(Box::new(
            (
                StatusCode::FORBIDDEN,
                "this file belongs to no space. Adopt it \
                 (`nauka space publish <org>/<space> <hash> --key nsk_…`) and read it \
                 through that space",
            )
                .into_response(),
        ));
    };
    let active = |space: &str| {
        s.spaces
            .get(space)
            .is_some_and(|r| !r.suspended && s.orgs.get(&r.org).is_some_and(|o| !o.suspended))
    };
    let public_grants: Vec<&str> = refs
        .iter()
        .filter(|sp| active(sp) && s.spaces[sp.as_str()].public_read)
        .map(|sp| sp.as_str())
        .collect();
    if !public_grants.is_empty() {
        // Served bare through whichever public space is most generous:
        // a space with no rate_default imposes nothing; otherwise the
        // highest configured ceiling wins.
        let rate = if public_grants
            .iter()
            .any(|sp| s.spaces[*sp].rate_default.is_none())
        {
            None
        } else {
            public_grants
                .iter()
                .filter_map(|sp| s.spaces[*sp].rate_default)
                .max()
        };
        // The most generous public space is also the one that pays.
        let billed = public_grants
            .iter()
            .max_by_key(|sp| {
                s.spaces[**sp]
                    .rate_default
                    .map(|r| (1, r))
                    .unwrap_or((2, 0))
            })
            .map(|sp| sp.to_string());
        return Ok(ReadGrant {
            rate,
            conc: None,
            content_type: None,
            billed_space: billed,
        });
    }
    let deny = |msg: String| Err(Box::new((StatusCode::FORBIDDEN, msg).into_response()));
    let (Some(space), Some(exp), Some(sig)) = (&p.space, p.exp, &p.sig) else {
        return deny(
            "this file is private — it takes a signed link \
             (?space=<org/space>&exp=<unix>&sig=<hex>, see `nauka space link`) or a \
             public-read space referencing it"
                .into(),
        );
    };
    if !refs.contains(space) {
        return deny(format!("{space} does not reference this file"));
    }
    if !active(space) {
        return deny(format!("space {space} is suspended"));
    }
    if exp <= crate::spaceauth::unix_now() {
        return deny("link expired — ask the issuer for a fresh one".into());
    }
    // The type is checked BEFORE the signature is even considered: a
    // space that signs something outside the table gets a plain refusal
    // rather than a quiet downgrade to `attachment`, so the mistake
    // surfaces in the issuer's tests instead of in production.
    let content_type = match &p.ct {
        Some(ct) => match crate::spaceauth::inline_content_type(ct) {
            Some(served) => Some(served),
            None => {
                return deny(format!(
                    "{ct} is not servable inline — drop the ct parameter and the file \
                     downloads as an attachment"
                ))
            }
        },
        None => None,
    };
    let canonical =
        crate::spaceauth::canonical_link(hash, space, exp, p.rate, p.conc, p.ct.as_deref());
    let signed_by_space = s.space_keys.get(space).is_some_and(|keys| {
        keys.iter()
            .any(|k| crate::spaceauth::verify(&k.public_key, &canonical, sig))
    });
    if !signed_by_space {
        return deny(
            "invalid link signature (hash, space, exp, rate, conc and ct must match what was \
             signed)"
                .into(),
        );
    }
    // The issuer's decision, cryptographically bound: no rate in the
    // link means the issuer chose not to throttle, no conc means
    // unlimited parallel connections.
    Ok(ReadGrant {
        rate: p.rate,
        conc: p.conc,
        content_type,
        billed_space: Some(space.clone()),
    })
}

/// What a granted read carries: the applicable speed ceiling, the
/// signed cap on simultaneous connections, and the space whose egress
/// ledger the bytes land on.
struct ReadGrant {
    rate: Option<u64>,
    conc: Option<u32>,
    /// The signed inline type, already resolved to the exact header
    /// value through [`crate::spaceauth::inline_content_type`]. `None`
    /// means the historical behavior: an octet-stream attachment.
    content_type: Option<&'static str>,
    billed_space: Option<String>,
}

/// Applies a grant's presentation decision to a response: the signed
/// inline type when there is one, the octet-stream attachment
/// otherwise. `nosniff` rides along with every inline answer — the
/// whole point of the allowlist is that the type served is the type
/// signed, and sniffing is exactly what would undo that.
fn present(
    mut b: axum::http::response::Builder,
    content_type: Option<&'static str>,
    name: Option<&String>,
) -> axum::http::response::Builder {
    match content_type {
        Some(ct) => {
            b = b
                .header(header::CONTENT_TYPE, ct)
                .header("X-Content-Type-Options", "nosniff");
            if let Some(name) = name {
                let safe = name.replace(['"', '\r', '\n'], "_");
                b = b.header(
                    header::CONTENT_DISPOSITION,
                    format!("inline; filename=\"{safe}\""),
                );
            }
        }
        None => {
            b = b.header(header::CONTENT_TYPE, "application/octet-stream");
            if let Some(name) = name {
                let safe = name.replace(['"', '\r', '\n'], "_");
                b = b.header(
                    header::CONTENT_DISPOSITION,
                    format!("attachment; filename=\"{safe}\""),
                );
            }
        }
    }
    b
}

/// One occupied slot of a link's signed concurrency budget, counted
/// per node and keyed by the link's signature (one signed link = one
/// budget, however many people it was shared with). The slot frees
/// when the response stream drops — normal completion and mid-transfer
/// disconnects alike — so it can never outlive its connection.
struct ConcGuard {
    map: Arc<std::sync::Mutex<HashMap<String, u32>>>,
    key: String,
}

impl ConcGuard {
    /// Takes a slot, or refuses when the budget is full. The budget is
    /// local in-flight PLUS what the neighborhood gossiped within
    /// [`REMOTE_CONC_TTL`]: the slot itself is only ever local — remote
    /// counts are somebody else's slots, observed, never mutated.
    fn acquire(state: &ApiState, key: &str, cap: u32) -> Option<ConcGuard> {
        let remote: u32 = {
            let m = state.link_conc_remote.lock().unwrap();
            m.values()
                .filter(|(at, _)| at.elapsed() < REMOTE_CONC_TTL)
                .filter_map(|(_, counts)| counts.get(key))
                .sum()
        };
        let map = &state.link_conc;
        let mut m = map.lock().unwrap();
        let count = m.entry(key.to_string()).or_insert(0);
        if (*count).saturating_add(remote) >= cap {
            let stale = *count == 0;
            if stale {
                m.remove(key);
            }
            return None;
        }
        *count += 1;
        Some(ConcGuard {
            map: map.clone(),
            key: key.to_string(),
        })
    }
}

impl Drop for ConcGuard {
    fn drop(&mut self) {
        let mut m = self.map.lock().unwrap();
        if let Some(count) = m.get_mut(&self.key) {
            if *count <= 1 {
                // Last slot released: drop the entry entirely, the map
                // only ever holds links with connections in flight.
                m.remove(&self.key);
            } else {
                *count -= 1;
            }
        }
    }
}

/// HEAD /f/{hash}: size without a body (the download page relies on it).
async fn download_head(
    State(state): State<Arc<ApiState>>,
    Path(hash): Path<String>,
    Query(link): Query<ReadLinkParams>,
    axum::extract::ConnectInfo(peer): axum::extract::ConnectInfo<std::net::SocketAddr>,
) -> Response {
    if let Some(resp) = unavailable(&state, &hash) {
        return resp;
    }
    // A media element probes with HEAD before it plays: this answer has
    // to carry the same type the GET will, or the player gives up here.
    let mut content_type = None;
    if !peer.ip().is_loopback() {
        match authorize_read(&state, &hash, &link) {
            Ok(grant) => content_type = grant.content_type,
            Err(resp) => return *resp,
        }
    }
    let manifest = match state.store.get_manifest(&hash) {
        Ok(m) => m,
        Err(_) => match state.app.app_state().manifests.get(&hash) {
            Some(m) => m.clone(),
            None => return StatusCode::NOT_FOUND.into_response(),
        },
    };
    present(Response::builder(), content_type, manifest.name.as_ref())
        .header(header::ACCEPT_RANGES, "bytes")
        .header(header::CONTENT_LENGTH, manifest.file_size)
        .body(Body::empty())
        .unwrap()
}

/// A banned file is never served (410), and neither is an expired one.
fn unavailable(state: &ApiState, hash: &str) -> Option<Response> {
    let app_state = state.app.app_state();
    if let Some(reason) = app_state.banned.get(hash) {
        return Some((StatusCode::GONE, format!("content removed: {reason}")).into_response());
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    match app_state.manifests.get(hash) {
        Some(m) if m.expires_at.is_some_and(|e| e <= now) => {
            Some((StatusCode::GONE, "file expired").into_response())
        }
        // Absent from the registry but present locally: normally a
        // deletion (the registry drops the entry, the GC purges the shards
        // later). But it is ALSO what an upload looks like on a follower
        // whose state machine has not applied the registration yet: the
        // manifest is written locally before the Raft entry comes back
        // round. Reading a file one had just uploaded therefore answered
        // "410 file deleted", reproducibly, for a few seconds.
        //
        // A manifest written moments ago is a fresh upload, not a
        // deletion. The cost of the grace is that a file deleted within
        // seconds of its upload may still be served by a lagging node
        // until the window closes.
        None if state.store.get_manifest(hash).is_ok() => match state.store.manifest_age(hash) {
            Some(age) if age < REGISTRY_LAG_GRACE => None,
            _ => Some((StatusCode::GONE, "file deleted").into_response()),
        },
        _ => None,
    }
}

/// How long a locally known manifest missing from the replicated registry
/// is read as "not replicated here yet" rather than "deleted".
const REGISTRY_LAG_GRACE: std::time::Duration = std::time::Duration::from_secs(30);

/// DELETE /f/{hash}: removes the file from the replicated registry. The
/// shards are purged by each node's GC on the following pass.
async fn delete_file(
    State(state): State<Arc<ApiState>>,
    Path(hash): Path<String>,
    axum::extract::ConnectInfo(peer): axum::extract::ConnectInfo<std::net::SocketAddr>,
    headers: axum::http::HeaderMap,
) -> Result<Response, ApiError> {
    if !state.app.app_state().manifests.contains_key(&hash) {
        return Ok((StatusCode::NOT_FOUND, "unknown file").into_response());
    }
    // Ownership decides the rules. A file referenced by spaces belongs
    // to them: deletion is a SIGNED release of one space's reference
    // (the file itself only dies with its last reference). An unowned
    // legacy file keeps the open pre-tenant behavior.
    let auth = verify_space_write(&state, &headers, "DELETE", &format!("/f/{hash}"), None)?;
    let refs: Vec<String> = state
        .app
        .app_state()
        .file_refs
        .get(&hash)
        .map(|r| r.iter().cloned().collect())
        .unwrap_or_default();
    match auth {
        Some(auth) => {
            if !refs.contains(&auth.space) {
                return Ok((
                    StatusCode::FORBIDDEN,
                    format!("{} does not reference this file", auth.space),
                )
                    .into_response());
            }
            let resp = state
                .app
                .write(nauka_raft::types::AppCommand::RemoveFileRef {
                    file_hash: hash.clone(),
                    space: auth.space.clone(),
                })
                .await
                .context("releasing the reference")?;
            if !resp.ok {
                return Ok((
                    StatusCode::CONFLICT,
                    resp.info.unwrap_or_else(|| "the cluster refused".into()),
                )
                    .into_response());
            }
            Ok((StatusCode::NO_CONTENT, ()).into_response())
        }
        None => {
            if !refs.is_empty() {
                return Ok((
                    StatusCode::FORBIDDEN,
                    format!(
                        "this file belongs to {} — deletion must be signed by an \
                         admin key of a referencing space (`nauka space sign --method \
                         DELETE --path /f/{hash}`)",
                        refs.join(", ")
                    ),
                )
                    .into_response());
            }
            // Unowned leftovers from before the 0.6 flip: only the node's
            // operator (loopback) may clear them unsigned.
            if !peer.ip().is_loopback() {
                return Ok((
                    StatusCode::UNAUTHORIZED,
                    "unsigned deletion is operator-only (loopback). Adopt the file into \
                     a space and sign the DELETE, or run this on a node",
                )
                    .into_response());
            }
            let resp = state
                .app
                .write(nauka_raft::types::AppCommand::UnregisterManifest {
                    file_hash: hash.clone(),
                })
                .await
                .context("deleting from the registry")?;
            if !resp.ok {
                return Ok((StatusCode::NOT_FOUND, "unknown file").into_response());
            }
            Ok((StatusCode::NO_CONTENT, ()).into_response())
        }
    }
}

/// Uniform HTTP error. Anything that is not deliberately classified is an
/// internal error, as before; the status is carried so the paths that can
/// tell "the cluster cannot take this write right now" from "this failed"
/// answer something the client can act on.
struct ApiError(StatusCode, anyhow::Error);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, format!("{:#}", self.1)).into_response()
    }
}

impl<E: Into<anyhow::Error>> From<E> for ApiError {
    fn from(e: E) -> Self {
        Self(StatusCode::INTERNAL_SERVER_ERROR, e.into())
    }
}

impl From<DispatchError> for ApiError {
    fn from(e: DispatchError) -> Self {
        // A cluster that cannot commit right now is a transient condition
        // the client should retry — 503, not 500.
        let status = match e {
            DispatchError::Unavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
            DispatchError::Failed(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        Self(status, e.into_anyhow())
    }
}

/// Why a file dispatch failed.
///
/// `dispatch_file` is the single object-write path behind both front doors
/// (the S3 endpoint and the native HTTP API), so it must not decide the
/// wire status itself — it reports *what* went wrong and each front door
/// renders it in its own protocol. The distinction that matters is
/// availability (retryable) versus a genuine failure of this upload.
pub(crate) enum DispatchError {
    /// The registry could not commit: no leader, no quorum, or the commit
    /// timed out. Retryable. Carries the closed-set label the S3 write
    /// rejection counter uses.
    Unavailable(&'static str),
    /// Anything else: this upload really did fail.
    Failed(anyhow::Error),
}

/// What both front doors tell a client whose write cannot commit.
pub(crate) const WRITE_UNAVAILABLE_MSG: &str =
    "the cluster cannot commit this write right now (no quorum); retry shortly";

impl DispatchError {
    fn into_anyhow(self) -> anyhow::Error {
        match self {
            Self::Unavailable(_) => anyhow!(WRITE_UNAVAILABLE_MSG),
            Self::Failed(e) => e,
        }
    }
}

impl<E: Into<anyhow::Error>> From<E> for DispatchError {
    fn from(e: E) -> Self {
        Self::Failed(e.into())
    }
}

#[derive(serde::Deserialize)]
struct UploadParams {
    name: Option<String>,
    /// Time to live in seconds: past that, the file is purged from the cluster.
    ttl: Option<u64>,
}

#[derive(serde::Serialize)]
struct UploadResponse {
    hash: String,
    size: u64,
    name: Option<String>,
    stripes: usize,
    data_shards: usize,
    parity_shards: usize,
    link: String,
    /// Shards that could not be delivered to their owner (degraded write,
    /// completed later by the scrubber). 0 on a healthy cluster.
    degraded_shards: usize,
    /// The space now referencing this file (signed uploads only).
    #[serde(skip_serializing_if = "Option::is_none")]
    space: Option<String>,
}

/// A write authenticated for a space (`X-Nauka-Space` was present and
/// its signature checked out).
struct SpaceWriteAuth {
    space: String,
    /// BLAKE3 the client bound into its signature, if any — verified
    /// against the actual upload once it is fully hashed.
    claimed_hash: Option<String>,
    /// Taille liée par les grants d'upload v2. `None` pour les anciennes
    /// signatures d'administration qui ne sont jamais remises à un tiers.
    claimed_size: Option<u64>,
}

fn auth_header<'h>(headers: &'h axum::http::HeaderMap, name: &str) -> Option<&'h str> {
    headers.get(name).and_then(|v| v.to_str().ok())
}

/// Checks the Ed25519 write signature carried by a request, against the
/// replicated space registry. `Ok(None)` = no `X-Nauka-Space` header: a
/// legacy anonymous write, still accepted until the private-by-default
/// switch flips. `Ok(Some(_))` = a valid ADMIN signature for the space.
/// Everything else is a 401/403 with the remedy in the message.
fn verify_space_write(
    state: &ApiState,
    headers: &axum::http::HeaderMap,
    method: &str,
    path: &str,
    query: Option<&str>,
) -> Result<Option<SpaceWriteAuth>, ApiError> {
    let Some(space) = auth_header(headers, "x-nauka-space") else {
        return Ok(None);
    };
    let deny = |code: StatusCode, msg: String| ApiError(code, anyhow!(msg));
    let key_hex = auth_header(headers, "x-nauka-key").ok_or_else(|| {
        deny(
            StatusCode::UNAUTHORIZED,
            "signed write: missing x-nauka-key (generate the headers with `nauka space sign`)"
                .into(),
        )
    })?;
    let timestamp: u64 = auth_header(headers, "x-nauka-timestamp")
        .and_then(|t| t.parse().ok())
        .ok_or_else(|| {
            deny(
                StatusCode::UNAUTHORIZED,
                "signed write: missing or non-numeric x-nauka-timestamp".into(),
            )
        })?;
    let signature = auth_header(headers, "x-nauka-signature").ok_or_else(|| {
        deny(
            StatusCode::UNAUTHORIZED,
            "signed write: missing x-nauka-signature".into(),
        )
    })?;
    let claimed_hash = auth_header(headers, "x-nauka-content-hash").map(str::to_string);
    let signature_version = auth_header(headers, "x-nauka-signature-version").unwrap_or("1");
    let claimed_size = auth_header(headers, "x-nauka-content-length")
        .map(|value| {
            value.parse::<u64>().map_err(|_| {
                deny(
                    StatusCode::UNAUTHORIZED,
                    "signed upload: x-nauka-content-length must be an integer".into(),
                )
            })
        })
        .transpose()?;

    let s = state.app.app_state();
    let record = s.spaces.get(space).ok_or_else(|| {
        deny(
            StatusCode::UNAUTHORIZED,
            format!("no space named {space} on this cluster"),
        )
    })?;
    let org_suspended = s.orgs.get(&record.org).is_none_or(|o| o.suspended);
    if record.suspended || org_suspended {
        return Err(deny(
            StatusCode::FORBIDDEN,
            format!("space {space} is suspended"),
        ));
    }
    let key = s
        .space_keys
        .get(space)
        .and_then(|keys| {
            keys.iter()
                .find(|k| hex::encode(k.public_key) == key_hex.to_lowercase())
        })
        .ok_or_else(|| {
            deny(
                StatusCode::UNAUTHORIZED,
                format!("this key is not registered on {space}"),
            )
        })?;
    if key.role != nauka_raft::types::SpaceKeyRole::Admin {
        return Err(deny(
            StatusCode::FORBIDDEN,
            format!(
                "key {:?} is a signer key — writes require an admin key of {space}",
                key.name
            ),
        ));
    }
    if !crate::spaceauth::timestamp_fresh(timestamp, crate::spaceauth::unix_now()) {
        return Err(deny(
            StatusCode::UNAUTHORIZED,
            format!(
                "signature timestamp outside the ±{}s window — re-sign and retry",
                crate::spaceauth::MAX_CLOCK_SKEW
            ),
        ));
    }
    let canonical = match signature_version {
        "1" => crate::spaceauth::canonical_write(
            method,
            path,
            space,
            timestamp,
            claimed_hash.as_deref(),
        ),
        "2" if method == "PUT" && path == "/api/upload" => {
            let hash = claimed_hash.as_deref().ok_or_else(|| {
                deny(
                    StatusCode::UNAUTHORIZED,
                    "delegated upload: x-nauka-content-hash is required".into(),
                )
            })?;
            let size = claimed_size.ok_or_else(|| {
                deny(
                    StatusCode::UNAUTHORIZED,
                    "delegated upload: x-nauka-content-length is required".into(),
                )
            })?;
            crate::spaceauth::canonical_upload(
                path,
                query.unwrap_or_default(),
                space,
                timestamp,
                hash,
                size,
            )
        }
        "2" => {
            return Err(deny(
                StatusCode::UNAUTHORIZED,
                "upload signature v2 is valid only for PUT /api/upload".into(),
            ))
        }
        _ => {
            return Err(deny(
                StatusCode::UNAUTHORIZED,
                "unsupported x-nauka-signature-version".into(),
            ))
        }
    };
    if !crate::spaceauth::verify(&key.public_key, &canonical, signature) {
        return Err(deny(
            StatusCode::UNAUTHORIZED,
            "invalid signature (method, path, query, space, timestamp, content-hash and \
             content-length must match what was signed)"
                .into(),
        ));
    }
    Ok(Some(SpaceWriteAuth {
        space: space.to_string(),
        claimed_hash,
        claimed_size,
    }))
}

async fn upload(
    State(state): State<Arc<ApiState>>,
    Query(params): Query<UploadParams>,
    request: Request,
) -> Result<Json<UploadResponse>, ApiError> {
    // Space authentication first, before a single body byte is spooled:
    // an unauthorized writer costs one signature check, not disk. Since
    // the 0.6 flip, EVERY upload belongs to a space — the anonymous era
    // is over, and the error is the onboarding.
    let signed_query = request.uri().query().unwrap_or_default().to_string();
    let write_auth = verify_space_write(
        &state,
        request.headers(),
        request.method().as_str(),
        "/api/upload",
        Some(&signed_query),
    )?
    .ok_or_else(|| {
        ApiError(
            StatusCode::UNAUTHORIZED,
            anyhow!(
                "uploads belong to a space. Create one and sign the request:\n  \
                 nauka org create <org>\n  \
                 nauka space create <org>/<space>\n  \
                 nauka space key add <org>/<space> --role admin\n  \
                 nauka space sign <org>/<space> --key nsk_…   # prints these headers"
            ),
        )
    })?;
    // A multipart form is a client mistake this endpoint must not absorb:
    // the framing would be stored verbatim as the object, boundary and
    // headers included. Refuse it with the remedy instead of storing junk.
    let is_multipart = request
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|ct| ct.to_str().ok())
        .is_some_and(|ct| ct.trim_start().starts_with("multipart/"));
    if is_multipart {
        return Err(ApiError(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            anyhow!("multipart forms are not accepted — send the raw file bytes (curl --data-binary @file)"),
        ));
    }
    // The body streams into the elastic buffer while the encoder drains it
    // concurrently — encoding starts on the first complete stripe, not
    // after the last byte. Placement still waits for the file hash (it is
    // keyed on it), but that is the dispatcher's phase 2, not ours.
    let expires_at = params.ttl.map(|ttl| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
            + ttl
    });
    let spool_path = state.tmp_dir.join(format!("ingest-{}", uuid_ish()));
    // The spool only engages for a ZERO RAM grant (pool dry under heavy
    // concurrency): with a window, the ring at capacity backpressures the
    // producer instead — spilling there re-created the old 2.5× write
    // amplification. With no window, the spool is what keeps push() from
    // waiting forever on capacity that will never exist (measured: the
    // bound-0 version deadlocked concurrent uploads in the conformance
    // suite until the client timed out).
    let spool_bound = crate::ingest::fs_available(&state.tmp_dir) / 2;
    let (mut tx, rx) =
        crate::ingest::channel(&state.ingest_pool, INGEST_RAM_WANT, spool_path, spool_bound);
    let dispatch = tokio::spawn(dispatch_stream(
        state.clone(),
        rx,
        params.name.clone(),
        expires_at,
    ));
    let mut size: u64 = 0;
    let mut body = request.into_body().into_data_stream();
    use tokio_stream::StreamExt;
    while let Some(chunk) = body.next().await {
        let chunk = match chunk.context("reading the request body") {
            Ok(c) => c,
            Err(e) => {
                // Dropping the writer unfinished aborts the dispatcher: a
                // truncated stream must never become an object.
                drop(tx);
                let _ = dispatch.await;
                return Err(e.into());
            }
        };
        size += chunk.len() as u64;
        if write_auth
            .claimed_size
            .is_some_and(|claimed| size > claimed)
        {
            // Ne pas attendre la fin pour constater le mensonge : sans
            // cette garde, un porteur pouvait signer 1 octet puis remplir
            // le spool avec un corps chunked sans limite avant le 403.
            drop(tx);
            let _ = dispatch.await;
            return Err(ApiError(
                StatusCode::FORBIDDEN,
                anyhow!("request body exceeds the content length bound by its signature"),
            ));
        }
        if let Err(e) = tx.push(chunk).await {
            drop(tx);
            let _ = dispatch.await;
            return Err(e.into());
        }
    }
    tx.finish();
    // An empty body is a client mistake (a typoed curl, a missing file),
    // not a server failure — 4xx, not the 500 the encoder's "empty file"
    // error would surface as.
    if size == 0 {
        let _ = dispatch.await;
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            anyhow!("empty upload — the request body must be the file's bytes"),
        ));
    }
    let (manifest, degraded_shards) = dispatch
        .await
        .map_err(|e| ApiError::from(anyhow!("dispatch task: {e}")))??;
    debug_assert_eq!(size, manifest.file_size);

    // When the signature bound a content hash, the uploaded bytes must
    // BE that content — otherwise a captured signature could push
    // arbitrary bytes into the space within the timestamp window. The
    // file registered before we could hash it, so a lie is unregistered
    // again (the GC reclaims the shards).
    // Discarding a rejected upload must never take down a live file: with
    // global dedup, the rejected body can hash to something ANOTHER space
    // legitimately references (or that pre-existed). Only an unreferenced
    // hash is unregistered.
    let discard_safe = |file_hash: &str| !state.app.app_state().file_refs.contains_key(file_hash);
    {
        let auth = &write_auth;
        if let Some(claimed) = &auth.claimed_hash {
            if !claimed.eq_ignore_ascii_case(&manifest.file_hash) {
                if discard_safe(&manifest.file_hash) {
                    let _ = state
                        .app
                        .write(nauka_raft::types::AppCommand::UnregisterManifest {
                            file_hash: manifest.file_hash.clone(),
                        })
                        .await;
                }
                return Err(ApiError(
                    StatusCode::FORBIDDEN,
                    anyhow!(
                        "content hash mismatch for space {}: the signature binds {}, the \
                         body hashes to {} — upload discarded",
                        auth.space,
                        claimed,
                        manifest.file_hash
                    ),
                ));
            }
        }
        if let Some(claimed) = auth.claimed_size {
            if claimed != manifest.file_size {
                if discard_safe(&manifest.file_hash) {
                    let _ = state
                        .app
                        .write(nauka_raft::types::AppCommand::UnregisterManifest {
                            file_hash: manifest.file_hash.clone(),
                        })
                        .await;
                }
                return Err(ApiError(
                    StatusCode::FORBIDDEN,
                    anyhow!(
                        "content length mismatch for space {}: the signature binds {}, the body \
                         contains {} bytes — upload discarded",
                        auth.space,
                        claimed,
                        manifest.file_size
                    ),
                ));
            }
        }
        // Storage quota, checked at the earliest point where the size is
        // known and before the reference lands. A refusal discards the
        // upload (ref-guarded, like every rejection path here).
        if let Err(msg) = check_storage_quota(
            &state.app.app_state(),
            &auth.space,
            &manifest.file_hash,
            manifest.file_size,
        ) {
            if discard_safe(&manifest.file_hash) {
                let _ = state
                    .app
                    .write(nauka_raft::types::AppCommand::UnregisterManifest {
                        file_hash: manifest.file_hash.clone(),
                    })
                    .await;
            }
            return Err(ApiError(StatusCode::FORBIDDEN, anyhow!(msg)));
        }
        // The signed upload's whole point: the space now REFERENCES the
        // file. Same content already referenced elsewhere = same hash,
        // zero new shards — the reference is the only thing written.
        let resp = state
            .app
            .write(nauka_raft::types::AppCommand::AddFileRef {
                file_hash: manifest.file_hash.clone(),
                space: auth.space.clone(),
            })
            .await
            .context("recording the space's reference")?;
        if !resp.ok {
            // The space vanished between the signature check and here
            // (deleted mid-flight). Without a reference the upload must
            // not survive as an orphan the space cannot manage.
            if discard_safe(&manifest.file_hash) {
                let _ = state
                    .app
                    .write(nauka_raft::types::AppCommand::UnregisterManifest {
                        file_hash: manifest.file_hash.clone(),
                    })
                    .await;
            }
            return Err(ApiError(
                StatusCode::CONFLICT,
                anyhow!(
                    "the reference was refused ({}) — upload discarded",
                    resp.info.unwrap_or_default()
                ),
            ));
        }
    }

    Ok(Json(UploadResponse {
        hash: manifest.file_hash.clone(),
        size: manifest.file_size,
        name: manifest.name.clone(),
        stripes: manifest.stripes.len(),
        data_shards: manifest.config.data_shards,
        parity_shards: manifest.config.parity_shards,
        link: format!("/f/{}", manifest.file_hash),
        degraded_shards,
        space: Some(write_auth.space.clone()),
    }))
}

/// Where the bytes of an upload come from: a live client stream through
/// the elastic buffer, or a file already sitting on disk (form uploads,
/// multipart assembly, SSE-C ciphertext). The file variant reads directly
/// — pumping an on-disk source through the buffer would be a pointless
/// disk-to-disk copy.
pub(crate) enum StripeSource {
    Stream(crate::ingest::IngestReader),
    File(tokio::fs::File),
}

impl StripeSource {
    /// The next stripe's worth of data, short only at EOF, empty at EOF.
    async fn next_stripe(&mut self, len: usize) -> Result<bytes::Bytes> {
        match self {
            Self::Stream(reader) => reader.next_exact(len).await,
            Self::File(f) => {
                let mut buf = vec![0u8; len];
                let mut filled = 0;
                while filled < len {
                    let n = f.read(&mut buf[filled..]).await?;
                    if n == 0 {
                        break;
                    }
                    filled += n;
                }
                buf.truncate(filled);
                Ok(bytes::Bytes::from(buf))
            }
        }
    }
}

/// Streaming entry: encode from a live client, overlapped with reception.
/// Owned `Arc` because the caller runs this as a task concurrent with the
/// pushes feeding the reader.
pub(crate) async fn dispatch_stream(
    state: Arc<ApiState>,
    reader: crate::ingest::IngestReader,
    name: Option<String>,
    expires_at: Option<u64>,
) -> std::result::Result<(FileManifest, usize), DispatchError> {
    dispatch_core(&state, StripeSource::Stream(reader), name, expires_at).await
}

/// File entry: encode a source that is already on disk.
pub(crate) async fn dispatch_file(
    state: &Arc<ApiState>,
    tmp_path: &std::path::Path,
    name: Option<String>,
    expires_at: Option<u64>,
) -> std::result::Result<(FileManifest, usize), DispatchError> {
    let f = tokio::fs::File::open(tmp_path)
        .await
        .map_err(|e| DispatchError::Failed(anyhow!("opening the staged upload: {e}")))?;
    dispatch_core(state, StripeSource::File(f), name, expires_at).await
}

/// The shared engine, one streaming pass: placement is keyed on each
/// STRIPE's content hash — known the moment the stripe is encoded — so
/// shards go onto per-peer send queues while the next stripe is still
/// arriving. Only the manifest waits for the final file hash, and it is
/// registered last: a truncated upload never becomes an object.
async fn dispatch_core(
    state: &Arc<ApiState>,
    mut source: StripeSource,
    name: Option<String>,
    expires_at: Option<u64>,
) -> std::result::Result<(FileManifest, usize), DispatchError> {
    // Fail fast when the manifest provably cannot be recorded. Checked
    // before a single stripe is encoded: an upload that ends in an
    // uncommittable registry write is wasted work on every node it
    // touches, and the client would otherwise wait out the write timeout
    // (~4s) for an answer we already know.
    if !state.can_commit_write() {
        return Err(DispatchError::Unavailable(NO_QUORUM));
    }
    let mut cfg = state.config;

    // The first read decides the shard density. `next_stripe` is short
    // only at EOF, so a short FIRST stripe means the whole file is
    // already in hand — encode it with shards sized to the content
    // instead of padding them to the fixed stripe size (a 80 KiB PDF
    // used to cost 6 MiB on disk). Multi-stripe files keep the fixed
    // size: `shard_size` is per-manifest, and their last-stripe padding
    // is bounded by one stripe per FILE, which large files amortize.
    let first = source.next_stripe(cfg.stripe_data_len()).await?;
    if !first.is_empty() && first.len() < cfg.stripe_data_len() {
        cfg = if first.len() <= small_file_threshold() {
            // Below the threshold, striping is all overhead: replicate.
            cfg.replicated_for(first.len())
        } else {
            cfg.densified_for(first.len())
        };
    }
    let mut pending = Some(first);

    // One streaming pass: placement is keyed on stripe content, so the
    // owners of a stripe are known the moment it is encoded — its shards
    // go straight onto per-peer send queues while the next stripe is still
    // arriving. The old two-phase shape (park everything locally, ship
    // after the hash) died here; only the manifest still waits for the
    // final hash.
    //
    // Placement view snapshotted once for the whole upload, like before:
    // a dead node must cost a little redundancy (healed later), never the
    // whole upload.
    let view = state.view_alive();
    let view_refs: Vec<(&str, u64)> = view.iter().map(|(n, w)| (n.as_str(), *w)).collect();
    let coords = state.app.coords();

    // One bounded queue and one sender task per peer. A peer that FAILS
    // trips its breaker and its shards are parked in the local store (the
    // healer completes them — degraded, not lost). A peer that is merely
    // busy backpressures the encoder through `send().await`: the upload
    // then advances at dispatch speed, which is the encoded-ack contract.
    let mut queues: HashMap<String, tokio::sync::mpsc::Sender<(usize, bytes::Bytes)>> =
        HashMap::new();
    let mut senders: tokio::task::JoinSet<HashMap<usize, usize>> = tokio::task::JoinSet::new();
    let parked = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut hasher = blake3::Hasher::new();
    let mut stripes_meta: Vec<StripeMeta> = Vec::new();
    let mut local_placed: Vec<usize> = Vec::new();
    let mut size: u64 = 0;
    loop {
        let stripe = match pending.take() {
            Some(first) => first,
            None => source.next_stripe(cfg.stripe_data_len()).await?,
        };
        if stripe.is_empty() {
            break;
        }
        hasher.update(&stripe);
        size += stripe.len() as u64;
        let si = stripes_meta.len();
        let shards = encode_stripe(&stripe, &cfg)?;
        let stripe_key = shards[0].hash.clone();
        let owners = nauka_cluster::placement::stripe_owners_geo(
            &stripe_key,
            si,
            shards.len(),
            &view_refs,
            &coords,
        );
        let mut placed_here = 0usize;
        for shard in &shards {
            let owner = owners[shard.index];
            if owner == state.self_id {
                state.store.put_shard(&shard.data)?;
                placed_here += 1;
                continue;
            }
            let q = match queues.get(owner) {
                Some(q) => q.clone(),
                None => {
                    let (q_tx, q_rx) = tokio::sync::mpsc::channel(PEER_QUEUE_SHARDS);
                    queues.insert(owner.to_string(), q_tx.clone());
                    senders.spawn(peer_sender(
                        state.store.clone(),
                        owner.to_string(),
                        q_rx,
                        parked.clone(),
                    ));
                    q_tx
                }
            };
            if q.send((si, bytes::Bytes::from(shard.data.clone())))
                .await
                .is_err()
            {
                // Sender task gone (breaker tripped and it drained out):
                // park like any other undeliverable shard.
                state.store.put_shard(&shard.data)?;
                parked.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
        local_placed.push(placed_here);
        stripes_meta.push(StripeMeta {
            data_len: stripe.len(),
            shard_hashes: shards.iter().map(|s| s.hash.clone()).collect(),
        });
    }
    if size == 0 {
        return Err(anyhow!("empty file").into());
    }
    let file_hash = hasher.finalize().to_hex().to_string();

    // The tail is only the queues draining what reception outran; close
    // them and collect the per-stripe delivery counts.
    drop(queues);
    let mut acked: HashMap<usize, usize> = HashMap::new();
    while let Some(res) = senders.join_next().await {
        for (si, n) in res.map_err(|e| anyhow!("peer sender task: {e}"))? {
            *acked.entry(si).or_insert(0) += n;
        }
    }
    let undelivered = parked.load(std::sync::atomic::Ordering::Relaxed);
    // Below k placed shards a stripe is not reconstructible from the
    // CLUSTER — its parked shards exist here, but a single node is not the
    // durability the client was promised. Same abort rule as always,
    // evaluated at the end because acks are asynchronous now.
    for (si, meta) in stripes_meta.iter().enumerate() {
        let placed = local_placed[si] + acked.get(&si).copied().unwrap_or(0);
        if placed < cfg.data_shards {
            return Err(anyhow!(
                "stripe {si}: only {placed} of {} shards could be placed \
                 ({} required) — upload aborted",
                meta.shard_hashes.len(),
                cfg.data_shards
            )
            .into());
        }
    }
    if undelivered > 0 {
        tracing::warn!(
            file = %file_hash,
            undelivered,
            "degraded upload: redundancy will be completed by the scrubber"
        );
        // One count per degraded upload, plus the shard shortfall itself:
        // the first says how often writes land under-replicated, the second
        // how much repair debt each one leaves for the scrubber.
        metrics::counter!("nauka_writes_degraded_total").increment(1);
        metrics::counter!("nauka_write_shards_undelivered_total").increment(undelivered as u64);
    }

    // A re-upload of existing content without ?name= must not ERASE the
    // stored name — the name slot is per-hash, and the second uploader
    // rarely means "unname it". Resolved HERE, before the Raft proposal,
    // so every replica applies identical bytes; resolving inside the
    // state machine would let mixed binary versions diverge.
    let name = name.or_else(|| {
        state
            .app
            .app_state()
            .manifests
            .get(&file_hash)
            .and_then(|m| m.name.clone())
    });
    let manifest = FileManifest {
        file_hash,
        file_size: size,
        name,
        expires_at,
        config: cfg,
        stripes: stripes_meta,
    };
    // Available locally right away, then replicated by the registry.
    state.store.put_manifest(&manifest)?;
    let resp = match state
        .app
        .write(nauka_raft::types::AppCommand::RegisterManifest(
            manifest.clone(),
        ))
        .await
    {
        Ok(resp) => resp,
        // The registry did not commit in time: quorum lost mid-flight, or
        // the leader went away between the check above and now. An
        // availability failure, not an internal bug.
        Err(_) => return Err(DispatchError::Unavailable(COMMIT_TIMEOUT)),
    };
    if !resp.ok {
        // A command the state machine deliberately rejected is a real
        // error, not a retryable one.
        return Err(anyhow!("the registry refused the manifest (banned content?)").into());
    }
    Ok((manifest, undelivered))
}

/// Rejection reasons, the closed label set of the write-rejection counter.
pub(crate) const NO_QUORUM: &str = "no_quorum";
pub(crate) const COMMIT_TIMEOUT: &str = "commit_timeout";

/// RAM window an upload asks the pool for: a dozen stripes. Enough to
/// absorb encode-and-store hiccups without the spool; small enough that a
/// dry pool is a degradation, not a cliff.
pub(crate) const INGEST_RAM_WANT: u64 = 48 << 20;

/// Prefix of a locally-acked upload waiting to be dispersed. The content
/// hash follows, which is the whole recovery index: a restart lists these
/// and asks the registry which ones still lack a manifest.
pub(crate) const STAGED_PREFIX: &str = "staged-";

/// How many undispersed bytes this node tolerates before local-ack
/// uploads fall back to `encoded`.
///
/// The mode's premise is a SHORT window of single-node residency. Let the
/// backlog grow without bound — a stalled cluster, peers gone — and the
/// premise quietly becomes false while clients keep being told 200. The
/// cap is what keeps the promise honest; past it, uploads simply pay the
/// full dispersal again.
#[cfg(feature = "s3")]
const STAGED_BACKLOG_MAX: u64 = 4 << 30;

pub(crate) fn staged_path(state: &Arc<ApiState>, hash: &str) -> PathBuf {
    state.tmp_dir.join(format!("{STAGED_PREFIX}{hash}.bin"))
}

/// Length of the staged copy this node holds for `hash`, if any.
pub(crate) async fn staged_len(state: &Arc<ApiState>, hash: &str) -> Option<u64> {
    tokio::fs::metadata(staged_path(state, hash))
        .await
        .ok()
        .map(|m| m.len())
}

/// Bytes `[start, end]` of a locally-staged upload.
///
/// A locally-acked object is readable from the moment it is acked, because
/// the bytes are right here: local disk, no erasure decode, no cluster
/// round-trip — strictly faster than the dispersed read that replaces it.
/// Without this, the drain window would be a window of 404s on objects the
/// registry already acknowledges, which is the one thing the ack promised
/// would not happen.
pub(crate) async fn staged_range(
    state: &Arc<ApiState>,
    hash: &str,
    start: u64,
    end: u64,
) -> Option<Vec<u8>> {
    use tokio::io::{AsyncReadExt, AsyncSeekExt};
    let path = staged_path(state, hash);
    let len = tokio::fs::metadata(&path).await.ok()?.len();
    if start >= len {
        return None;
    }
    let end = end.min(len.saturating_sub(1));
    let want = end.saturating_sub(start).saturating_add(1) as usize;
    let mut f = tokio::fs::File::open(&path).await.ok()?;
    f.seek(std::io::SeekFrom::Start(start)).await.ok()?;
    let mut buf = vec![0u8; want];
    f.read_exact(&mut buf).await.ok()?;
    metrics::counter!("nauka_staged_reads_total").increment(1);
    Some(buf)
}

/// Serves a staged upload over the native door, ranges included.
///
/// Deliberately plain: no erasure decode, no cluster fetch, just the file
/// this node fsynced before it answered 200.
async fn serve_staged(
    state: &Arc<ApiState>,
    hash: &str,
    len: u64,
    headers: &axum::http::HeaderMap,
) -> Result<Response, ApiError> {
    let range = parse_range(
        headers.get(header::RANGE).and_then(|v| v.to_str().ok()),
        len,
    );
    if headers.contains_key(header::RANGE) && range.is_none() {
        return Ok((
            StatusCode::RANGE_NOT_SATISFIABLE,
            [(header::CONTENT_RANGE, format!("bytes */{len}"))],
        )
            .into_response());
    }
    let (start, end) = range.unwrap_or((0, len.saturating_sub(1)));
    let Some(bytes) = staged_range(state, hash, start, end).await else {
        return Ok((StatusCode::NOT_FOUND, "unknown file").into_response());
    };
    state.egress.add(bytes.len() as u64);
    let mut resp = (
        if range.is_some() {
            StatusCode::PARTIAL_CONTENT
        } else {
            StatusCode::OK
        },
        bytes,
    )
        .into_response();
    let h = resp.headers_mut();
    h.insert(header::ACCEPT_RANGES, "bytes".parse().expect("static"));
    if range.is_some() {
        if let Ok(v) = format!("bytes {start}-{end}/{len}").parse() {
            h.insert(header::CONTENT_RANGE, v);
        }
    }
    Ok(resp)
}

/// Whether a new local-ack upload may be admitted.
///
/// Only the S3 door consumes this today: the local-ack opt-in rides a
/// bucket tag. Re-exposing the mode on the native door is a planned
/// follow-up; the machinery below it — staged files, recovery sweep,
/// drain — is door-agnostic and stays live.
#[cfg(feature = "s3")]
pub(crate) fn staged_window_open(state: &Arc<ApiState>) -> bool {
    state
        .staged_bytes
        .load(std::sync::atomic::Ordering::Relaxed)
        < STAGED_BACKLOG_MAX
}

/// Disperse a staged upload in the background, then drop the staged copy.
///
/// Failure here is not silent data loss: the staged file stays on disk and
/// its content hash is already in the registry, so the next restart's
/// recovery sweep picks it up again.
pub(crate) fn spawn_staged_drain(
    state: Arc<ApiState>,
    path: PathBuf,
    size: u64,
    name: Option<String>,
) {
    let now = state
        .staged_bytes
        .fetch_add(size, std::sync::atomic::Ordering::Relaxed)
        + size;
    // Published here rather than only on the maintenance tick: the window
    // is often shorter than the tick interval, so a 30 s gauge reads 0
    // through the whole of it — telling an operator nothing is staged at
    // exactly the moment something is.
    metrics::gauge!("nauka_staged_bytes").set(now as f64);
    metrics::counter!("nauka_local_ack_uploads_total").increment(1);
    tokio::spawn(async move {
        match dispatch_file(&state, &path, name, None).await {
            Ok((manifest, degraded)) => {
                tracing::info!(
                    file = %manifest.file_hash, degraded,
                    "locally-acked upload dispersed"
                );
                let _ = tokio::fs::remove_file(&path).await;
            }
            Err(e) => {
                // Left on disk on purpose: the recovery sweep is the
                // retry, and the object is unreadable-but-known until it
                // succeeds — never silently absent.
                tracing::error!(
                    path = %path.display(),
                    "dispersing a locally-acked upload failed, left staged: {}",
                    match &e { DispatchError::Unavailable(r) => (*r).to_string(),
                               DispatchError::Failed(e) => format!("{e:#}") }
                );
                metrics::counter!("nauka_local_ack_drain_failures_total").increment(1);
            }
        }
        let left = state
            .staged_bytes
            .fetch_sub(size, std::sync::atomic::Ordering::Relaxed)
            .saturating_sub(size);
        metrics::gauge!("nauka_staged_bytes").set(left as f64);
    });
}

/// Finish the dispersal of uploads this node acked but had not dispersed
/// when it stopped.
///
/// The staged file's name IS its content hash, and the registry already
/// carries that hash: an entry whose manifest is missing is exactly an
/// interrupted drain, and one whose manifest exists is a drain that
/// finished before the file could be removed. No extra bookkeeping, no
/// new garbage collector.
pub(crate) async fn recover_staged_uploads(state: Arc<ApiState>) {
    let mut dir = match tokio::fs::read_dir(&state.tmp_dir).await {
        Ok(d) => d,
        Err(_) => return,
    };
    let mut resumed = 0usize;
    while let Ok(Some(entry)) = dir.next_entry().await {
        let path = entry.path();
        let Some(hash) = path
            .file_name()
            .and_then(|n| n.to_str())
            .and_then(|n| n.strip_prefix(STAGED_PREFIX))
            .and_then(|n| n.strip_suffix(".bin"))
        else {
            continue;
        };
        if state.store.get_manifest(hash).is_ok() {
            let _ = tokio::fs::remove_file(&path).await;
            continue;
        }
        let size = entry.metadata().await.map(|m| m.len()).unwrap_or(0);
        tracing::warn!(file = %hash, "resuming a locally-acked upload left undispersed");
        spawn_staged_drain(state.clone(), path, size, None);
        resumed += 1;
    }
    if resumed > 0 {
        tracing::warn!(resumed, "resumed locally-acked uploads after restart");
    }
}

/// Sends a shard to a peer, reconnecting as needed (idempotent: storage is
/// content-addressed, so a resend duplicates nothing).
/// Per-peer send-queue depth, in shards (1 MiB each). Deep enough to keep
/// the wire busy across stripe boundaries; shallow enough that the
/// encoder feels a genuinely stalled peer quickly.
const PEER_QUEUE_SHARDS: usize = 64;
/// Consecutive failures after which a peer's sender gives up and parks
/// everything else addressed to it.
const PEER_BREAKER: u32 = 2;

/// Drains one peer's queue for one upload. Owns its connection, retries a
/// failed shard once (reconnecting), and after `PEER_BREAKER` consecutive
/// failures parks every remaining shard in the local store — the healer's
/// job from there, at the price of redundancy, never of the upload.
/// Returns how many shards were ACKED per stripe index.
async fn peer_sender(
    store: Arc<ShardStore>,
    owner: String,
    mut rx: tokio::sync::mpsc::Receiver<(usize, bytes::Bytes)>,
    parked: Arc<std::sync::atomic::AtomicUsize>,
) -> HashMap<usize, usize> {
    use futures::stream::{FuturesUnordered, StreamExt};
    let mut acked: HashMap<usize, usize> = HashMap::new();
    let mut consecutive = 0u32;
    let addr: Option<SocketAddr> = owner.parse().ok();
    let mut client: Option<PeerClient> = match addr {
        Some(a) => connect_with_timeout(a).await,
        None => None,
    };
    // Several puts in flight on the one connection — QUIC gives each its
    // own stream, so this pipelines the peer's disk writes behind the
    // wire instead of paying a full round trip per shard (measured: the
    // one-at-a-time version spent the whole upload waiting, ~80 ms per
    // shard against ~12 ms of actual work).
    const PEER_INFLIGHT: usize = 4;
    let mut inflight: FuturesUnordered<_> = FuturesUnordered::new();
    let push = |c: &PeerClient, si: usize, data: bytes::Bytes| {
        let c = c.clone();
        async move {
            let ok = matches!(
                tokio::time::timeout(SHARD_TIMEOUT, c.put_shard(data.to_vec())).await,
                Ok(Ok(_))
            );
            (si, data, ok)
        }
    };
    let settle =
        |res: (usize, bytes::Bytes, bool), acked: &mut HashMap<usize, usize>, cons: &mut u32| {
            let (si, data, ok) = res;
            if ok {
                *cons = 0;
                *acked.entry(si).or_insert(0) += 1;
            } else {
                // One immediate park rather than an in-order retry dance:
                // the shard stays durable locally and the healer finishes
                // the job. Retry-by-reconnect happens naturally on the
                // next shard once the breaker logic below resets `client`.
                metrics::counter!("nauka_shard_send_retries_total").increment(1);
                *cons += 1;
                if store.put_shard(&data).is_ok() {
                    parked.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
        };
    while let Some((si, data)) = rx.recv().await {
        if consecutive >= PEER_BREAKER || client.is_none() {
            if consecutive < PEER_BREAKER {
                client = match addr {
                    Some(a) => connect_with_timeout(a).await,
                    None => None,
                };
            }
            if client.is_none() {
                // A failed CONNECT must feed the breaker like a failed put,
                // or a frozen peer prices every shard at a 3 s connect
                // timeout — 512 shards made that a 25-minute stall, seen
                // as the conformance suite hanging and clients timing out.
                consecutive += 1;
                if store.put_shard(&data).is_ok() {
                    parked.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                continue;
            }
        }
        let c = client.as_ref().expect("checked above");
        inflight.push(push(c, si, data));
        if inflight.len() >= PEER_INFLIGHT {
            if let Some(res) = inflight.next().await {
                if !res.2 {
                    client = None;
                }
                settle(res, &mut acked, &mut consecutive);
            }
        }
    }
    while let Some(res) = inflight.next().await {
        settle(res, &mut acked, &mut consecutive);
    }
    acked
}

/// Requested byte range, resolved to (start, inclusive end).
fn parse_range(header: Option<&str>, size: u64) -> Option<(u64, u64)> {
    let spec = header?.strip_prefix("bytes=")?.trim();
    // A single range is supported (enough for media playback).
    let (start, end) = spec.split_once('-')?;
    let (start, end) = match (start.trim(), end.trim()) {
        ("", "") => return None,
        // bytes=-N: the last N bytes.
        ("", n) => {
            let n: u64 = n.parse().ok()?;
            (size.saturating_sub(n.min(size)), size.saturating_sub(1))
        }
        (s, "") => (s.parse().ok()?, size.saturating_sub(1)),
        (s, e) => (
            s.parse().ok()?,
            e.parse::<u64>().ok()?.min(size.saturating_sub(1)),
        ),
    };
    (start <= end && start < size).then_some((start, end))
}

/// Rebuilds one stripe: the k data shards in parallel, parity only if one
/// is missing — on a healthy cluster not a single parity byte crosses the
/// wire.
/// Fetches enough shards of one stripe to decode it, choosing WHICH k of
/// the k+m to ask for by egress budget: shards this node holds first
/// (free), then the ones whose predicted holder has the most monthly
/// budget left. Any k slots decode identically — this is the flow-side
/// twin of the capacity weight in placement. The prediction reuses the
/// exact placement function writes used; when it is wrong (membership
/// drifted), `fetch` scans every member anyway, so a miss costs a probe,
/// never correctness. Slots still missing after the preferred round are
/// completed from the rest.
pub(crate) async fn fetch_stripe_slots(
    fetcher: &Arc<Fetcher>,
    stripe: &StripeMeta,
    stripe_idx: usize,
    m: &FileManifest,
) -> (Vec<Option<Vec<u8>>>, bool) {
    let k = m.config.data_shards;
    let total = stripe.shard_hashes.len();
    let view_refs: Vec<(&str, u64)> = fetcher.view.iter().map(|(n, w)| (n.as_str(), *w)).collect();
    let state = fetcher.state.app.app_state();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let month = crate::egress::month_key(now);
    let holders = nauka_cluster::placement::stripe_owners_geo(
        nauka_cluster::placement::stripe_key_of(&m.stripes[stripe_idx]),
        stripe_idx,
        total,
        &view_refs,
        &state.node_coords,
    );
    let ratios: Vec<(bool, f64)> = holders
        .iter()
        .map(|holder| {
            if *holder == fetcher.state.self_id {
                (true, f64::INFINITY)
            } else {
                (
                    false,
                    crate::egress::remaining_ratio(state.node_egress.get(*holder), &month),
                )
            }
        })
        .collect();
    let order = crate::egress::rank_slots(&ratios);

    let mut slots: Vec<Option<Vec<u8>>> = vec![None; total];
    let mut remote_used = false;
    let cut = k.min(total);
    // Hedged race instead of two joined rounds: the k best slots start
    // immediately; parity joins the race when the hedge timer fires or a
    // fetch FAILS — whichever comes first — and the first k valid shards
    // win. MDS makes the extra fetches free redundancy, and a peer that
    // is merely slow (the old join waited for it) now just loses the
    // race. Dropping the set cancels whatever is still in flight.
    {
        use futures::stream::{FuturesUnordered, StreamExt};
        let spawn_fetch = |i: usize| {
            let f = fetcher.clone();
            let h = stripe.shard_hashes[i].clone();
            async move {
                let t0 = std::time::Instant::now();
                let out = f.clone().fetch(h).await;
                if out.is_some() {
                    f.note_fetch_latency(t0.elapsed());
                }
                (i, out)
            }
        };
        let mut inflight: FuturesUnordered<_> =
            order[..cut].iter().map(|&i| spawn_fetch(i)).collect();
        let mut hedged = false;
        let hedge = tokio::time::sleep(fetcher.hedge_delay());
        tokio::pin!(hedge);
        let mut have = 0usize;
        while have < k {
            tokio::select! {
                biased;
                res = inflight.next(), if !inflight.is_empty() => {
                    let Some((i, out)) = res else { break };
                    match out {
                        Some((d, remote)) => {
                            remote_used |= remote;
                            if slots[i].is_none() {
                                slots[i] = Some(d);
                                have += 1;
                            }
                        }
                        None if !hedged => {
                            // A failed fetch is the loudest reason to hedge.
                            hedged = true;
                            metrics::counter!("nauka_read_hedges_total").increment(1);
                            for &j in &order[cut..] {
                                inflight.push(spawn_fetch(j));
                            }
                        }
                        None => {}
                    }
                }
                _ = &mut hedge, if !hedged => {
                    hedged = true;
                    metrics::counter!("nauka_read_hedges_total").increment(1);
                    for &j in &order[cut..] {
                        inflight.push(spawn_fetch(j));
                    }
                }
                else => break,
            }
        }
    }
    (slots, remote_used)
}

/// Peers whose Vivaldi POSITION is further than this are not neighbors.
/// Deliberately `drift_from` (Euclidean plus the height DELTA), not
/// `distance` (which ADDS both heights): a young same-datacenter pair
/// carries two inflated heights and reads ~29 ms under `distance` while
/// sitting at the same position — right on any sane threshold, so the
/// neighbor flickered in and out. Position drift reads ~0 for them and
/// stays huge across continents, which is exactly the question asked.
const NEIGHBOR_MAX_MS: f64 = 30.0;
/// Ceiling on one neighbor-cache lookup; past it, reconstruct as usual.
const NEIGHBOR_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(250);

/// Cooperative regional cache: ask the closest neighbor for the decoded
/// stripe before paying k shard fetches from far owners. One neighbor,
/// one short timeout, cache-only on the far side (a miss never triggers
/// a reconstruction over there) — and the bytes are VERIFIED by
/// re-encoding them against the manifest's shard hashes: the transport
/// authenticates the peer, never the content.
async fn neighbor_cached_stripe(
    fetcher: &Arc<Fetcher>,
    stripe: &StripeMeta,
    stripe_idx: usize,
    m: &FileManifest,
) -> Option<Vec<u8>> {
    let addr = fetcher.neighbor().await?;
    let client = fetcher.client_for(&addr).await?;
    let data = tokio::time::timeout(
        NEIGHBOR_TIMEOUT,
        client.get_cached_stripe(&m.file_hash, stripe_idx),
    )
    .await
    .ok()?
    .ok()??;
    let shards = nauka_erasure::encode_stripe(&data, &m.config).ok()?;
    let genuine = shards.len() == stripe.shard_hashes.len()
        && shards
            .iter()
            .zip(&stripe.shard_hashes)
            .all(|(s, h)| s.hash == *h);
    if !genuine {
        metrics::counter!("nauka_coop_cache_rejected_total").increment(1);
        return None;
    }
    metrics::counter!("nauka_coop_cache_hits_total").increment(1);
    Some(data)
}

pub(crate) async fn reconstruct_stripe(
    fetcher: &Arc<Fetcher>,
    stripe: &StripeMeta,
    stripe_idx: usize,
    m: &FileManifest,
) -> Result<Vec<u8>> {
    // The cache first: a decoded stripe under a content-addressed key can
    // never be stale, only absent.
    if let Some(cache) = &fetcher.state.cache {
        if let Some(data) = cache.get(&m.file_hash, stripe_idx) {
            return Ok(data);
        }
    }
    // Then the neighbor's cache: one local transfer beats k far ones.
    if let Some(data) = neighbor_cached_stripe(fetcher, stripe, stripe_idx, m).await {
        if let Some(cache) = &fetcher.state.cache {
            cache.put(&m.file_hash, stripe_idx, &data);
        }
        return Ok(data);
    }
    let (slots, remote_used) = fetch_stripe_slots(fetcher, stripe, stripe_idx, m).await;
    let data = decode_stripe(slots, stripe, &m.config)?;
    // Only worth keeping when the bytes crossed the cluster: stripes that
    // decode from local shards are already free.
    if remote_used {
        if let Some(cache) = &fetcher.state.cache {
            cache.put(&m.file_hash, stripe_idx, &data);
        }
    }
    Ok(data)
}

async fn download(
    State(state): State<Arc<ApiState>>,
    Path(hash): Path<String>,
    Query(link): Query<ReadLinkParams>,
    axum::extract::ConnectInfo(peer): axum::extract::ConnectInfo<std::net::SocketAddr>,
    headers: axum::http::HeaderMap,
) -> Result<Response, ApiError> {
    if let Some(resp) = unavailable(&state, &hash) {
        return Ok(resp);
    }
    // The node's operator reads anything locally — whoever holds a shell
    // on the machine holds the disk anyway, and `nauka verify` needs the
    // real read path. Loopback bypasses the gate for READS only; writes
    // stay strict everywhere.
    let grant = if peer.ip().is_loopback() {
        ReadGrant {
            rate: None,
            conc: None,
            content_type: None,
            billed_space: None,
        }
    } else {
        match authorize_read(&state, &hash, &link) {
            Ok(g) => g,
            Err(resp) => return Ok(*resp),
        }
    };
    // The signed connection cap: take a slot for the whole life of the
    // response stream, or refuse with 429 while the budget is full. The
    // signature is the budget's key — the cap belongs to the LINK, not
    // to whoever is holding it.
    let conc_guard = match (grant.conc, &link.sig) {
        (Some(cap), Some(sig)) => match ConcGuard::acquire(&state, sig, cap) {
            Some(g) => Some(g),
            None => {
                metrics::counter!("nauka_link_conc_rejects_total").increment(1);
                return Ok((
                    StatusCode::TOO_MANY_REQUESTS,
                    [(header::RETRY_AFTER, "1")],
                    format!(
                        "this link is limited to {cap} simultaneous connection(s) — \
                         retry when one finishes"
                    ),
                )
                    .into_response());
            }
        },
        _ => None,
    };
    // Egress quota: past the space's monthly cap, reads slow to a crawl
    // instead of dying — a throttled link hurts less than a dead one on
    // someone's page. The X-Nauka-Throttled header says why.
    let mut rate = grant.rate;
    let mut throttled_by_quota = false;
    if let Some(billed) = &grant.billed_space {
        let s = state.app.app_state();
        if let Some(q) = s.spaces.get(billed).and_then(|r| r.egress_quota_bytes) {
            let month = crate::egress::month_key(crate::spaceauth::unix_now());
            if space_egress_month(&state, &s, billed, &month) >= q {
                rate = Some(rate.map_or(EGRESS_CRAWL, |r| r.min(EGRESS_CRAWL)));
                throttled_by_quota = true;
            }
        }
    }
    // Manifest: local store (materialized), else the replicated registry.
    let manifest = match state.store.get_manifest(&hash) {
        Ok(m) => m,
        Err(_) => match state.app.app_state().manifests.get(&hash) {
            Some(m) => m.clone(),
            // Same fallback as the S3 door: a locally-acked upload still
            // dispersing is readable from its staged copy on this disk.
            None => match staged_len(&state, &hash).await {
                Some(len) => return serve_staged(&state, &hash, len, &headers).await,
                None => return Ok((StatusCode::NOT_FOUND, "unknown file").into_response()),
            },
        },
    };

    // Partial request (media playback, resumed download): only the stripes
    // covering the range are fetched from the cluster.
    let range = parse_range(
        headers.get(header::RANGE).and_then(|v| v.to_str().ok()),
        manifest.file_size,
    );
    if headers.contains_key(header::RANGE) && range.is_none() {
        return Ok((
            StatusCode::RANGE_NOT_SATISFIABLE,
            [(
                header::CONTENT_RANGE,
                format!("bytes */{}", manifest.file_size),
            )],
        )
            .into_response());
    }
    if let Some((start, end)) = range {
        if let Some(billed) = &grant.billed_space {
            record_space_egress(&state, billed, end - start + 1);
        }
        note_hot_read(&state, &manifest.file_hash);
        return serve_range(
            state,
            manifest,
            start,
            end,
            rate,
            throttled_by_quota,
            conc_guard,
            grant.content_type,
        )
        .await;
    }

    // Streaming reconstruction: one stripe at a time towards the client.
    // Per stripe: the k DATA shards are fetched in parallel; parity is only
    // requested if one of them is missing — on a healthy cluster, not a
    // single parity byte crosses the wire.
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<bytes::Bytes, std::io::Error>>(4);
    let fetcher = Arc::new(Fetcher::new(state.clone()));
    let expected_hash = manifest.file_hash.clone();
    let m = Arc::new(manifest.clone());

    // Reconstruct the FIRST stripe before committing to a status. Once the
    // 200 and its Content-Length are on the wire the only way to signal
    // failure is to truncate the body — correct clients detect it (curl
    // exits 18) but the status still says OK. Checking upfront turns the
    // common "too many shards are gone" case into an honest 503. A stripe
    // that fails later still truncates: nothing better exists mid-stream.
    let first = match m.stripes.first() {
        Some(stripe) => Some(reconstruct_stripe(&fetcher, stripe, 0, &m).await),
        None => None,
    };
    if let Some(Err(e)) = &first {
        return Ok((
            StatusCode::SERVICE_UNAVAILABLE,
            format!("file currently unrecoverable: {e:#}"),
        )
            .into_response());
    }
    let first = first.and_then(|r| r.ok());

    tokio::spawn(async move {
        use futures::StreamExt as _;
        let mut hasher = blake3::Hasher::new();
        // Stripe 0 was already reconstructed by the pre-flight above.
        if let Some(data) = first {
            hasher.update(&data);
            if tx.send(Ok(bytes::Bytes::from(data))).await.is_err() {
                return;
            }
        }
        // Read-ahead pipeline: up to READ_AHEAD_STRIPES reconstructions
        // in flight (budget-aware slot selection + stripe cache, shared
        // with every other read path), consumed strictly in order.
        let mut pipeline =
            futures::stream::iter(
                (1..m.stripes.len()).map(|stripe_idx| {
                    let fetcher = fetcher.clone();
                    let m = m.clone();
                    async move {
                        reconstruct_stripe(&fetcher, &m.stripes[stripe_idx], stripe_idx, &m).await
                    }
                }),
            )
            .buffered(READ_AHEAD_STRIPES);
        while let Some(res) = pipeline.next().await {
            let data = match res {
                Ok(d) => d,
                Err(e) => {
                    let _ = tx
                        .send(Err(std::io::Error::other(format!(
                            "unrecoverable stripe: {e}"
                        ))))
                        .await;
                    return;
                }
            };
            hasher.update(&data);
            if tx.send(Ok(bytes::Bytes::from(data))).await.is_err() {
                return; // client gone
            }
        }
        drop(pipeline);
        if hasher.finalize().to_hex().to_string() != expected_hash {
            let _ = tx
                .send(Err(std::io::Error::other("integrity violated")))
                .await;
        }
    });

    let rx = match rate {
        Some(r) if r > 0 => paced(rx, r),
        _ => rx,
    };
    let body = body_holding_slot(rx, conc_guard);
    // Egress is counted when the response is committed to — a client that
    // disconnects mid-download still spent its slice of budget.
    state.egress.add(manifest.file_size);
    if let Some(billed) = &grant.billed_space {
        record_space_egress(&state, billed, manifest.file_size);
    }
    let mut response = present(
        Response::builder(),
        grant.content_type,
        manifest.name.as_ref(),
    )
    .header(header::ACCEPT_RANGES, "bytes")
    .header(header::CONTENT_LENGTH, manifest.file_size);
    if throttled_by_quota {
        response = response.header("X-Nauka-Throttled", "egress-quota");
    }
    Ok(response.body(body).map_err(anyhow::Error::from)?)
}

/// Serves a byte range: only the stripes that intersect it are fetched and
/// decoded (the rest of the file is never touched).
/// Pushes this node's in-flight conc counts to its DNS neighborhood
/// once a second while any exist — plus one final EMPTY push when the
/// last connection ends, so the peers release the shared budget now
/// instead of waiting out the staleness TTL. Fire-and-forget: a peer
/// missing a push keeps admitting on slightly stale numbers for a
/// second, which is the accepted precision of the whole mechanism.
pub async fn conc_gossip_loop(state: Arc<ApiState>, geo: Option<Arc<crate::dns::GeoDns>>) {
    let mut was_empty = true;
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        let counts: Vec<(String, u32)> = {
            let m = state.link_conc.lock().unwrap();
            m.iter().map(|(k, v)| (k.clone(), *v)).collect()
        };
        if counts.is_empty() && was_empty {
            continue;
        }
        was_empty = counts.is_empty();
        let targets: Vec<String> = geo
            .as_ref()
            .and_then(|g| g.neighborhood_of_self())
            .unwrap_or_else(|| {
                // No geography (DNS off, database not fetched yet):
                // correctness over thrift, push to every living member.
                let liveness = state.health.snapshot();
                state
                    .app
                    .members()
                    .values()
                    .filter(|a| **a != state.self_id)
                    .filter(|a| liveness.get(*a).copied().unwrap_or(true))
                    .cloned()
                    .collect()
            });
        for addr in targets {
            let counts = counts.clone();
            let from = state.self_id.clone();
            tokio::spawn(async move {
                let Ok(sock) = addr.parse() else { return };
                if let Ok(client) = nauka_transport::PeerClient::connect(sock).await {
                    let _ = client.link_conc_counts(&from, counts).await;
                }
            });
        }
    }
}

/// Builds the response body, tying an optional concurrency slot to the
/// stream's lifetime: the slot frees exactly when the body is dropped,
/// whether the download completed or the client vanished mid-transfer.
fn body_holding_slot(
    rx: tokio::sync::mpsc::Receiver<Result<bytes::Bytes, std::io::Error>>,
    guard: Option<ConcGuard>,
) -> Body {
    use futures::StreamExt as _;
    Body::from_stream(
        tokio_stream::wrappers::ReceiverStream::new(rx).map(move |item| {
            let _slot_held = &guard;
            item
        }),
    )
}

/// Wraps a byte stream in a pacing stage: cumulative bytes never run
/// ahead of `rate` bytes/s. Chunks are re-cut to 64 KiB so the flow is
/// smooth rather than stripe-sized bursts, and the bounded channel
/// backpressures the producer — with the reconstruction pipeline behind
/// it, throttling the client throttles the internal stripe fetches too,
/// instead of buffering the file at full speed.
fn paced(
    mut rx: tokio::sync::mpsc::Receiver<Result<bytes::Bytes, std::io::Error>>,
    rate: u64,
) -> tokio::sync::mpsc::Receiver<Result<bytes::Bytes, std::io::Error>> {
    let (tx, out) = tokio::sync::mpsc::channel(2);
    tokio::spawn(async move {
        let started = tokio::time::Instant::now();
        let mut sent: u64 = 0;
        while let Some(item) = rx.recv().await {
            match item {
                Err(e) => {
                    let _ = tx.send(Err(e)).await;
                    return;
                }
                Ok(chunk) => {
                    let mut chunk = chunk;
                    while !chunk.is_empty() {
                        let piece = chunk.split_to(chunk.len().min(64 * 1024));
                        sent += piece.len() as u64;
                        let due =
                            started + std::time::Duration::from_secs_f64(sent as f64 / rate as f64);
                        let now = tokio::time::Instant::now();
                        if due > now {
                            tokio::time::sleep(due - now).await;
                        }
                        if tx.send(Ok(piece)).await.is_err() {
                            return; // client gone
                        }
                    }
                }
            }
        }
    });
    out
}

/// Reads that repeat on the same file within this window count towards
/// the hot-file warming signal.
const HOT_WINDOW: std::time::Duration = std::time::Duration::from_secs(900);
/// Partial reads of one file before it is warmed whole.
const HOT_THRESHOLD: u32 = 3;
/// Warming reconstructs this many stripes concurrently — deliberately
/// below READ_AHEAD_STRIPES: background comfort must not compete with a
/// paying read.
const WARM_CONCURRENCY: usize = 2;

/// The background warmer: drains the queue and pulls whole files into
/// the local stripe cache, at low concurrency, skipping what is already
/// cached (reconstruct_stripe checks the cache first) and anything too
/// big to fit without evicting half the LRU.
pub(crate) async fn warmer_loop(state: Arc<ApiState>, mut rx: tokio::sync::mpsc::Receiver<String>) {
    while let Some(file_hash) = rx.recv().await {
        let Some(manifest) = state.app.app_state().manifests.get(&file_hash).cloned() else {
            continue;
        };
        let Some(cache) = &state.cache else { continue };
        // A single file may not monopolize the cache: past a quarter of
        // the budget, warming it whole would evict more value than it
        // adds.
        if manifest.file_size > cache.budget() / 4 {
            continue;
        }
        use futures::StreamExt as _;
        let fetcher = Arc::new(Fetcher::new(state.clone()));
        let m = Arc::new(manifest);
        let warmed = futures::stream::iter((0..m.stripes.len()).map(|i| {
            let fetcher = fetcher.clone();
            let m = m.clone();
            async move {
                reconstruct_stripe(&fetcher, &m.stripes[i], i, &m)
                    .await
                    .is_ok()
            }
        }))
        .buffered(WARM_CONCURRENCY)
        .filter(|ok| std::future::ready(*ok))
        .count()
        .await;
        metrics::counter!("nauka_warm_files_total").increment(1);
        metrics::counter!("nauka_warm_stripes_total").increment(warmed as u64);
    }
}

/// Bumps the partial-read counter for `hash`; at the threshold, queues a
/// full background warm and resets. Full GETs warm the cache by reading;
/// only ranges need the signal.
fn note_hot_read(state: &ApiState, hash: &str) {
    let Some(tx) = &state.warm_tx else { return };
    let now = std::time::Instant::now();
    let mut hot = match state.hot_reads.lock() {
        Ok(h) => h,
        Err(_) => return,
    };
    // Sweep the map opportunistically: stale windows and, if it somehow
    // still grows, everything — a heuristic table never justifies
    // unbounded memory.
    if hot.len() > 4096 {
        hot.clear();
    }
    let entry = hot.entry(hash.to_string()).or_insert((0, now));
    if now.duration_since(entry.1) > HOT_WINDOW {
        *entry = (0, now);
    }
    entry.0 += 1;
    if entry.0 >= HOT_THRESHOLD {
        let _ = tx.try_send(hash.to_string());
        hot.remove(hash);
    }
}

/// One stripe's byte window for a range read, by the cheapest honest
/// route: local cache slice → neighbor's cached stripe → PARTIAL fetch
/// of only the covering data shards (the layout is contiguous, so a
/// small seek lives in one or two shards — no reason to move k MiB for
/// 100 KiB) → full reconstruction. Every partial shard is BLAKE3-checked
/// against the manifest before a byte is believed.
async fn read_stripe_window(
    fetcher: &Arc<Fetcher>,
    m: &FileManifest,
    stripe_idx: usize,
    off: u64,
    start: u64,
    end: u64,
) -> Result<Vec<u8>> {
    let stripe = &m.stripes[stripe_idx];
    let stripe_len = stripe.data_len as u64;
    let from = start.saturating_sub(off) as usize;
    let to = ((end - off + 1).min(stripe_len)) as usize;
    if from >= to {
        return Ok(Vec::new());
    }
    if let Some(cache) = &fetcher.state.cache {
        if let Some(d) = cache.get(&m.file_hash, stripe_idx) {
            if to <= d.len() {
                return Ok(d[from..to].to_vec());
            }
        }
    }
    if let Some(d) = neighbor_cached_stripe(fetcher, stripe, stripe_idx, m).await {
        if let Some(cache) = &fetcher.state.cache {
            cache.put(&m.file_hash, stripe_idx, &d);
        }
        if to <= d.len() {
            return Ok(d[from..to].to_vec());
        }
    }
    let ssz = m.config.shard_size;
    let s0 = from / ssz;
    let s1 = (to - 1) / ssz;
    if ssz > 0 && s1 < m.config.data_shards && s1 - s0 + 1 < m.config.data_shards {
        let mut span: Vec<u8> = Vec::with_capacity((s1 - s0 + 1) * ssz);
        let mut complete = true;
        for i in s0..=s1 {
            match fetcher.clone().fetch(stripe.shard_hashes[i].clone()).await {
                Some((d, _)) if nauka_erasure::hash_bytes(&d) == stripe.shard_hashes[i] => {
                    span.extend_from_slice(&d)
                }
                _ => {
                    complete = false;
                    break;
                }
            }
        }
        if complete {
            metrics::counter!("nauka_partial_range_reads_total").increment(1);
            let a = from - s0 * ssz;
            return Ok(span[a..a + (to - from)].to_vec());
        }
        // A covering shard is missing: the stripe is degraded, pay the
        // full reconstruction (which can use parity).
    }
    let d = reconstruct_stripe(fetcher, stripe, stripe_idx, m).await?;
    Ok(d[from..to.min(d.len())].to_vec())
}

#[allow(clippy::too_many_arguments)]
async fn serve_range(
    state: Arc<ApiState>,
    manifest: FileManifest,
    start: u64,
    end: u64,
    rate: Option<u64>,
    throttled_by_quota: bool,
    conc_guard: Option<ConcGuard>,
    content_type: Option<&'static str>,
) -> Result<Response, ApiError> {
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<bytes::Bytes, std::io::Error>>(4);
    let fetcher = Arc::new(Fetcher::new(state.clone()));
    let m = Arc::new(manifest.clone());
    tokio::spawn(async move {
        use futures::StreamExt as _;
        // Offsets are loop-carried over variable stripe lengths: plan the
        // covered stripes first, pipeline the reconstructions after.
        let mut plan: Vec<(usize, u64)> = Vec::new();
        let mut offset: u64 = 0;
        for (stripe_idx, stripe) in m.stripes.iter().enumerate() {
            let stripe_end = offset + stripe.data_len as u64; // exclusive
            if stripe_end > start && offset <= end {
                plan.push((stripe_idx, offset));
            }
            offset = stripe_end;
            if offset > end {
                break;
            }
        }
        // Same read-ahead as the full download: media players issue long
        // range reads, and a seek far from the shards deserves the same
        // pipelining as a plain GET.
        let mut pipeline = futures::stream::iter(plan.into_iter().map(|(stripe_idx, off)| {
            let fetcher = fetcher.clone();
            let m = m.clone();
            async move { read_stripe_window(&fetcher, &m, stripe_idx, off, start, end).await }
        }))
        .buffered(READ_AHEAD_STRIPES);
        while let Some(res) = pipeline.next().await {
            let window = match res {
                Ok(w) => w,
                Err(e) => {
                    let _ = tx
                        .send(Err(std::io::Error::other(format!(
                            "unrecoverable stripe: {e}"
                        ))))
                        .await;
                    return;
                }
            };
            if !window.is_empty() && tx.send(Ok(bytes::Bytes::from(window))).await.is_err() {
                return; // client gone
            }
        }
    });

    let rx = match rate {
        Some(r) if r > 0 => paced(rx, r),
        _ => rx,
    };
    let body = body_holding_slot(rx, conc_guard);
    state.egress.add(end - start + 1);
    let mut builder = present(Response::builder(), content_type, manifest.name.as_ref());
    if throttled_by_quota {
        builder = builder.header("X-Nauka-Throttled", "egress-quota");
    }
    let _ = &builder;
    Ok(builder
        .status(StatusCode::PARTIAL_CONTENT)
        .header(header::ACCEPT_RANGES, "bytes")
        .header(header::CONTENT_LENGTH, end - start + 1)
        .header(
            header::CONTENT_RANGE,
            format!("bytes {start}-{end}/{}", manifest.file_size),
        )
        .body(body)
        .map_err(anyhow::Error::from)?)
}

/// Shard fetcher shared across one download request: a connection cache
/// (failures are memoized — a dead node is contacted only once per
/// request) usable from parallel fetches.
pub(crate) struct Fetcher {
    state: Arc<ApiState>,
    view: Vec<(String, u64)>,
    clients: tokio::sync::Mutex<HashMap<String, Option<PeerClient>>>,
    /// Closest settled Vivaldi neighbor (cooperative cache), resolved
    /// once per fetcher. None = nobody close enough, or coords not
    /// settled yet — then the cooperative path stays off for this read.
    neighbor: tokio::sync::OnceCell<Option<String>>,
    /// EWMA of successful shard-fetch latencies, microseconds. Feeds the
    /// hedge timer: what "abnormally slow" means is learned per
    /// download, not configured.
    fetch_ewma_us: std::sync::atomic::AtomicU64,
}

impl Fetcher {
    pub(crate) fn new(state: Arc<ApiState>) -> Self {
        let view = state.view();
        Self {
            state,
            view,
            clients: tokio::sync::Mutex::new(HashMap::new()),
            neighbor: tokio::sync::OnceCell::new(),
            fetch_ewma_us: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Records one successful shard fetch into the latency EWMA
    /// (7/8 old + 1/8 new — jumpy enough to follow a route change,
    /// stable enough to ignore one outlier).
    fn note_fetch_latency(&self, elapsed: std::time::Duration) {
        use std::sync::atomic::Ordering;
        let new = elapsed.as_micros() as u64;
        let old = self.fetch_ewma_us.load(Ordering::Relaxed);
        let next = if old == 0 {
            new
        } else {
            old - old / 8 + new / 8
        };
        self.fetch_ewma_us.store(next, Ordering::Relaxed);
    }

    /// How long a shard fetch may lag its peers before parity is raced
    /// against it: three times the learned typical latency, clamped to
    /// [100 ms, 2 s]. Before any sample: 500 ms.
    fn hedge_delay(&self) -> std::time::Duration {
        let ewma = self
            .fetch_ewma_us
            .load(std::sync::atomic::Ordering::Relaxed);
        if ewma == 0 {
            return std::time::Duration::from_millis(500);
        }
        std::time::Duration::from_micros((ewma * 3).clamp(100_000, 2_000_000))
    }

    /// The nearest peer by Vivaldi estimate, if it is close enough to be
    /// called a neighbor (same metro/region). Deliberately NOT gated on
    /// `is_settled`: inside one datacenter a coordinate never settles
    /// (sub-ms RTTs keep the error above the threshold — the telemetry
    /// tests guard the same lesson), and that is precisely where
    /// neighbors live. The residual risk is a fresh multi-region cluster
    /// whose nodes all still sit at the origin and briefly look adjacent:
    /// a false neighbor then costs one extra round-trip per stripe at
    /// worst (the lookup is cache-only on the far side), and the window
    /// closes by itself as the first cross-region pings pull the
    /// coordinates apart.
    async fn neighbor(&self) -> Option<String> {
        self.neighbor
            .get_or_init(|| async {
                let coords = self.state.app.coords();
                let me = coords.get(&self.state.self_id)?;
                self.state
                    .app
                    .members()
                    .values()
                    .filter(|addr| **addr != self.state.self_id)
                    .filter_map(|addr| {
                        let c = coords.get(addr)?;
                        let d = me.drift_from(c);
                        (d <= NEIGHBOR_MAX_MS).then_some((addr.clone(), d))
                    })
                    .min_by(|a, b| a.1.total_cmp(&b.1))
                    .map(|(addr, _)| addr)
            })
            .await
            .clone()
    }

    /// A client towards `node`, created on first need. `None` = already
    /// known to be unreachable.
    async fn client_for(&self, node: &str) -> Option<PeerClient> {
        if let Some(cached) = self.clients.lock().await.get(node) {
            return cached.clone();
        }
        // Connect outside the lock (3 s max); on a race, only one of the
        // two connections is kept — with no consequence.
        let connected = match node.parse::<SocketAddr>() {
            Ok(addr) => connect_with_timeout(addr).await,
            Err(_) => None,
        };
        self.clients
            .lock()
            .await
            .entry(node.to_string())
            .or_insert(connected)
            .clone()
    }

    /// Writes a peer off for the rest of THIS request.
    ///
    /// A verdict of a different kind from `PeerHealth`'s: that one is
    /// cross-request and needs three consecutive misses before it commits,
    /// this one is immediate and forgotten when the download ends. A node
    /// whose writeoff counter climbs while `nauka_peer_up` stays at 1 is
    /// the interesting case — a peer healthy enough to answer pings and too
    /// slow to serve a 1 MiB shard inside `SHARD_TIMEOUT`.
    async fn mark_dead(&self, node: &str) {
        metrics::counter!("nauka_read_peer_writeoffs_total").increment(1);
        self.clients.lock().await.insert(node.to_string(), None);
    }

    /// Looks for a shard: locally first, then on every reachable member.
    /// The flag says whether the bytes crossed the network — what decides
    /// if the decoded stripe is worth caching.
    pub(crate) async fn fetch(self: Arc<Self>, hash: String) -> Option<(Vec<u8>, bool)> {
        if let Ok(data) = self.state.store.get_shard(&hash) {
            record_shard_fetch(SHARD_LOCAL);
            return Some((data, false));
        }
        for (node, _) in self.view.iter().filter(|(n, _)| *n != self.state.self_id) {
            let Some(client) = self.client_for(node).await else {
                continue;
            };
            match tokio::time::timeout(SHARD_TIMEOUT, client.get_shard(&hash)).await {
                Ok(Ok(Some(data))) => {
                    record_shard_fetch(SHARD_REMOTE);
                    return Some((data, true));
                }
                Ok(Ok(None)) => {}
                // Error or timeout: the connection is suspect, we write it
                // off for the rest of the request.
                _ => self.mark_dead(node).await,
            }
        }
        record_shard_fetch(SHARD_MISSING);
        None
    }
}

/// The shard was already on this node's disk — no network, no erasure
/// arithmetic beyond the decode itself.
const SHARD_LOCAL: &str = "local";
/// The shard came from a peer. The ratio of this to `local` is what says
/// whether reads are being served where the data actually lives.
const SHARD_REMOTE: &str = "remote";
/// Nobody had it. Not an error on its own — Reed-Solomon only needs k of
/// the k+m shards — but a rising rate is redundancy being eaten away.
const SHARD_MISSING: &str = "missing";

fn record_shard_fetch(source: &'static str) {
    metrics::counter!("nauka_read_shard_fetches_total", "source" => source).increment(1);
}

/// Register the HELP/TYPE text of the read-path metrics.
pub(crate) fn describe_metrics() {
    metrics::describe_counter!(
        "nauka_read_shard_fetches_total",
        "Shard lookups on the download path, by where the bytes came from: the local store, a peer, or nowhere at all."
    );
    metrics::describe_counter!(
        "nauka_read_peer_writeoffs_total",
        "Peers written off for the remainder of a download after a failed or timed-out shard transfer. Per-request and independent of the cluster-wide liveness map."
    );
}

/// Read speed once a space is past its monthly egress quota: slow
/// enough to stop the bleeding, alive enough not to break pages.
const EGRESS_CRAWL: u64 = 64 * 1024;

/// Files at or under this many bytes are REPLICATED (1+m copies, one
/// round-trip reads) instead of striped — striping a 4 KiB file into
/// micro-shards costs k round-trips and padding for nothing. Override
/// with NAUKA_SMALL_THRESHOLD (bytes, 0 disables replication).
fn small_file_threshold() -> usize {
    static CACHED: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *CACHED.get_or_init(|| {
        std::env::var("NAUKA_SMALL_THRESHOLD")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(128 * 1024)
    })
}

/// Stripes reconstructed in parallel ahead of the client on the read
/// path. One stripe at a time serialized a WAN round-trip per 4 MiB —
/// measured at 1.5 MB/s from a node 200 ms away from the shards, with
/// the client's own link idle. The pipeline multiplies cold-read
/// throughput by its depth on high-RTT paths; the price is bounded
/// memory (depth × one stripe) per in-flight download. Results are
/// consumed IN ORDER: the client and the end-to-end hash both need it.
const READ_AHEAD_STRIPES: usize = 6;

/// Delay past which a peer is considered unreachable.
const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);
/// Timeout of a single shard transfer.
const SHARD_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(20);

async fn connect_with_timeout(addr: SocketAddr) -> Option<PeerClient> {
    match tokio::time::timeout(CONNECT_TIMEOUT, PeerClient::connect(addr)).await {
        Ok(Ok(c)) => Some(c),
        _ => None,
    }
}

#[derive(serde::Serialize)]
struct FileEntry {
    hash: String,
    size: u64,
    name: Option<String>,
    link: String,
    /// Spaces referencing this file; empty = pre-tenant legacy (unowned).
    spaces: Vec<String>,
}

async fn files(State(state): State<Arc<ApiState>>) -> Json<Vec<FileEntry>> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let s = state.app.app_state();
    let entries = s
        .manifests
        .values()
        .filter(|m| m.expires_at.is_none_or(|e| e > now))
        .map(|m| FileEntry {
            hash: m.file_hash.clone(),
            size: m.file_size,
            name: m.name.clone(),
            link: format!("/f/{}", m.file_hash),
            spaces: s
                .file_refs
                .get(&m.file_hash)
                .map(|r| r.iter().cloned().collect())
                .unwrap_or_default(),
        })
        .collect();
    Json(entries)
}

/// Unique temporary file identifier (no need for real cryptography here,
/// just to avoid collisions between uploads).
fn uuid_ish() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{nanos:x}-{:x}", std::process::id())
}

#[cfg(test)]
mod tests {
    use super::{parse_range, ApiError, DispatchError, NodeLocation, StatusCode, NO_QUORUM};

    #[test]
    fn node_location_only_accepts_a_city_and_iso_country_code() {
        assert_eq!(
            NodeLocation::new(" Helsinki ", "fi"),
            Some(NodeLocation {
                city: "Helsinki".to_string(),
                country_code: "FI".to_string(),
            })
        );
        assert_eq!(NodeLocation::new("", "FI"), None);
        assert_eq!(NodeLocation::new("Helsinki", "FIN"), None);
        assert_eq!(NodeLocation::new("Helsinki", "F1"), None);
    }

    #[test]
    fn an_uncommittable_write_is_retryable_on_the_native_api() {
        // S3 clients retry a 503 and never a 500; so does anything sane
        // driving /api/upload. A cluster that momentarily has no quorum
        // must not look like a bug in the node.
        let e: ApiError = DispatchError::Unavailable(NO_QUORUM).into();
        assert_eq!(e.0, StatusCode::SERVICE_UNAVAILABLE);
        let e: ApiError = DispatchError::Failed(anyhow::anyhow!("disk on fire")).into();
        assert_eq!(e.0, StatusCode::INTERNAL_SERVER_ERROR);
        // An ordinary error keeps the old behaviour.
        let e: ApiError = std::io::Error::other("nope").into();
        assert_eq!(e.0, StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn ranges_are_parsed_and_clamped() {
        let size = 1000;
        assert_eq!(parse_range(Some("bytes=0-99"), size), Some((0, 99)));
        assert_eq!(parse_range(Some("bytes=100-"), size), Some((100, 999)));
        // Suffix: the last N bytes.
        assert_eq!(parse_range(Some("bytes=-50"), size), Some((950, 999)));
        // End past the file: clamped.
        assert_eq!(parse_range(Some("bytes=900-99999"), size), Some((900, 999)));
        assert_eq!(parse_range(Some("bytes=0-0"), size), Some((0, 0)));
        // Invalid.
        assert_eq!(parse_range(Some("bytes=1000-1100"), size), None);
        assert_eq!(parse_range(Some("bytes=500-100"), size), None);
        assert_eq!(parse_range(Some("bytes=-"), size), None);
        assert_eq!(parse_range(Some("bits=0-10"), size), None);
        assert_eq!(parse_range(None, size), None);
    }
}
