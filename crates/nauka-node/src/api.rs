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
use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};
use axum::body::Body;
use axum::extract::{Path, Query, Request, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use nauka_erasure::{decode_stripe, encode_stripe, ErasureConfig, FileManifest, StripeMeta};
use nauka_store::ShardStore;
use nauka_transport::PeerClient;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub struct ApiState {
    pub store: Arc<ShardStore>,
    pub app: Arc<nauka_raft::RaftApp>,
    /// Advertised address of THIS node (its placement identity).
    pub self_id: String,
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

pub async fn serve_http(
    listen: SocketAddr,
    state: Arc<ApiState>,
    webui_dir: Option<PathBuf>,
) -> Result<()> {
    tokio::fs::create_dir_all(&state.tmp_dir).await?;
    let mut router = Router::new()
        .route("/api/upload", post(upload))
        .route("/api/files", get(files))
        .route("/api/status", get(status))
        .route(
            "/f/{hash}",
            get(download).head(download_head).delete(delete_file),
        )
        .with_state(state);
    // Web UI (SPA). Served from the binary by default; --webui points at a
    // dist directory instead, for front-end development.
    match webui_dir {
        Some(dir) => {
            let index = dir.join("index.html");
            router = router.fallback_service(
                tower_http::services::ServeDir::new(&dir)
                    .fallback(tower_http::services::ServeFile::new(index)),
            );
            tracing::info!("web UI served from {}", dir.display());
        }
        None if crate::webui::is_embedded() => {
            router = router.fallback(crate::webui::serve);
            tracing::info!("web UI served from the binary");
        }
        None => tracing::warn!("no web UI in this build — API only"),
    }
    let listener = tokio::net::TcpListener::bind(listen).await?;
    tracing::info!("HTTP API on http://{listen}");
    axum::serve(listener, router).await?;
    Ok(())
}

#[derive(serde::Serialize)]
struct NodeStatus {
    addr: String,
    capacity_bytes: u64,
    is_leader: bool,
    is_self: bool,
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
    leader: Option<String>,
    nodes: Vec<NodeStatus>,
    files: usize,
    total_bytes: u64,
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
    // reports the same probe round.
    let liveness = state.health.snapshot();
    let nodes = state
        .view()
        .into_iter()
        .map(|(addr, capacity_bytes)| NodeStatus {
            is_leader: leader_addr.as_deref() == Some(addr.as_str()),
            is_self: addr == state.self_id,
            // Nobody pings themselves, so self is never in the map; an
            // unprobed peer reads alive, same rule as `is_alive`.
            is_alive: liveness.get(&addr).copied().unwrap_or(true),
            addr,
            capacity_bytes,
        })
        .collect();
    Json(ClusterStatusResponse {
        self_addr: state.self_id.clone(),
        leader: leader_addr,
        nodes,
        files: app_state.manifests.len(),
        total_bytes: app_state.manifests.values().map(|m| m.file_size).sum(),
    })
}

/// HEAD /f/{hash}: size without a body (the download page relies on it).
async fn download_head(State(state): State<Arc<ApiState>>, Path(hash): Path<String>) -> Response {
    if let Some(resp) = unavailable(&state, &hash) {
        return resp;
    }
    let manifest = match state.store.get_manifest(&hash) {
        Ok(m) => m,
        Err(_) => match state.app.app_state().manifests.get(&hash) {
            Some(m) => m.clone(),
            None => return StatusCode::NOT_FOUND.into_response(),
        },
    };
    Response::builder()
        .header(header::CONTENT_TYPE, "application/octet-stream")
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
) -> Result<Response, ApiError> {
    if !state.app.app_state().manifests.contains_key(&hash) {
        return Ok((StatusCode::NOT_FOUND, "unknown file").into_response());
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
}

async fn upload(
    State(state): State<Arc<ApiState>>,
    Query(params): Query<UploadParams>,
    request: Request,
) -> Result<Json<UploadResponse>, ApiError> {
    // 1. Buffer the body to disk, hashing as it streams in: placement is
    //    keyed on the file hash, which is only known at the very end.
    let tmp_path = state.tmp_dir.join(format!("upload-{}", uuid_ish()));
    let mut tmp = tokio::fs::File::create(&tmp_path).await?;
    let mut hasher = blake3::Hasher::new();
    let mut size: u64 = 0;
    let mut body = request.into_body().into_data_stream();
    use tokio_stream::StreamExt;
    while let Some(chunk) = body.next().await {
        let chunk = chunk.context("reading the request body")?;
        hasher.update(&chunk);
        tmp.write_all(&chunk).await?;
        size += chunk.len() as u64;
    }
    tmp.flush().await?;
    drop(tmp);
    let expires_at = params.ttl.map(|ttl| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
            + ttl
    });
    let result = dispatch_file(&state, &tmp_path, size, hasher, params.name, expires_at).await;
    let _ = tokio::fs::remove_file(&tmp_path).await;
    let (manifest, degraded_shards) = result?;

    Ok(Json(UploadResponse {
        hash: manifest.file_hash.clone(),
        size: manifest.file_size,
        name: manifest.name.clone(),
        stripes: manifest.stripes.len(),
        data_shards: manifest.config.data_shards,
        parity_shards: manifest.config.parity_shards,
        link: format!("/f/{}", manifest.file_hash),
        degraded_shards,
    }))
}

/// Encodes the temporary file stripe by stripe and pushes every shard to
/// its owner (this node included), then records the manifest.
pub(crate) async fn dispatch_file(
    state: &Arc<ApiState>,
    tmp_path: &std::path::Path,
    size: u64,
    hasher: blake3::Hasher,
    name: Option<String>,
    expires_at: Option<u64>,
) -> std::result::Result<(FileManifest, usize), DispatchError> {
    if size == 0 {
        return Err(anyhow!("empty file").into());
    }
    // Fail fast when the manifest provably cannot be recorded. Checked
    // before a single shard is encoded or placed: an upload that ends in
    // an uncommittable registry write is wasted work on every node it
    // touches, and the client would otherwise wait out the write timeout
    // (~4s) for an answer we already know.
    if !state.can_commit_write() {
        return Err(DispatchError::Unavailable(NO_QUORUM));
    }
    let file_hash = hasher.finalize().to_hex().to_string();
    // Place on the members currently answering: a dead node must cost a
    // little redundancy (healed later), never the whole upload.
    let view = state.view_alive();
    let view_refs: Vec<(&str, u64)> = view.iter().map(|(n, w)| (n.as_str(), *w)).collect();
    let coords = state.app.coords();
    let cfg = state.config;

    let mut clients: HashMap<String, PeerClient> = HashMap::new();
    let mut f = tokio::fs::File::open(tmp_path).await?;
    let mut stripe_buf = vec![0u8; cfg.stripe_data_len()];
    let mut stripes_meta: Vec<StripeMeta> = Vec::new();
    // Degraded-write bookkeeping. A stripe is durable once its k data
    // shards' worth of pieces are placed; losing up to m deliveries is
    // redundancy the scrubber rebuilds, not a failed upload. A destination
    // that fails twice is skipped for the rest of the file (circuit
    // breaker) so a freshly dead node costs seconds, not
    // stripes × retries × timeout.
    let mut undelivered: usize = 0;
    let mut dest_failures: HashMap<String, u32> = HashMap::new();
    const BREAKER_THRESHOLD: u32 = 2;
    loop {
        let mut filled = 0;
        while filled < stripe_buf.len() {
            let n = f.read(&mut stripe_buf[filled..]).await?;
            if n == 0 {
                break;
            }
            filled += n;
        }
        if filled == 0 {
            break;
        }
        let si = stripes_meta.len();
        let shards = encode_stripe(&stripe_buf[..filled], &cfg)?;
        let owners = nauka_cluster::placement::stripe_owners_geo(
            &file_hash,
            si,
            shards.len(),
            &view_refs,
            &coords,
        );
        let mut placed = 0usize;
        for shard in &shards {
            let owner = owners[shard.index];
            if owner == state.self_id {
                state.store.put_shard(&shard.data)?;
                placed += 1;
                continue;
            }
            if dest_failures
                .get(owner)
                .is_some_and(|f| *f >= BREAKER_THRESHOLD)
            {
                undelivered += 1;
                continue;
            }
            match send_shard(&mut clients, owner, &shard.data).await {
                Ok(()) => placed += 1,
                Err(_) => {
                    *dest_failures.entry(owner.to_string()).or_insert(0) += 1;
                    undelivered += 1;
                }
            }
        }
        // Below k placed shards the stripe is not reconstructible anywhere:
        // that is a failed upload, not a degraded one.
        if placed < cfg.data_shards {
            return Err(anyhow!(
                "stripe {si}: only {placed} of {} shards could be placed \
                 ({} required) — upload aborted",
                shards.len(),
                cfg.data_shards
            )
            .into());
        }
        stripes_meta.push(StripeMeta {
            data_len: filled,
            shard_hashes: shards.iter().map(|s| s.hash.clone()).collect(),
        });
    }
    if undelivered > 0 {
        tracing::warn!(
            file = %file_hash,
            undelivered,
            "degraded upload: redundancy will be completed by the scrubber"
        );
    }

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

/// Sends a shard to a peer, reconnecting as needed (idempotent: storage is
/// content-addressed, so a resend duplicates nothing).
async fn send_shard(
    clients: &mut HashMap<String, PeerClient>,
    owner: &str,
    data: &[u8],
) -> Result<()> {
    let addr: SocketAddr = owner.parse()?;
    for attempt in 0..3u32 {
        if attempt > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(200 * attempt as u64)).await;
            clients.remove(owner);
        }
        if !clients.contains_key(owner) {
            match connect_with_timeout(addr).await {
                Some(c) => {
                    clients.insert(owner.to_string(), c);
                }
                None => continue,
            }
        }
        if let Ok(Ok(_)) =
            tokio::time::timeout(SHARD_TIMEOUT, clients[owner].put_shard(data.to_vec())).await
        {
            return Ok(());
        }
    }
    bail!("shard not delivered to {owner}")
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
        &m.file_hash,
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
    for round in [&order[..cut], &order[cut..]] {
        if slots.iter().filter(|s| s.is_some()).count() >= k {
            break;
        }
        let fetches = round.iter().map(|&i| {
            let f = fetcher.clone();
            let h = stripe.shard_hashes[i].clone();
            async move { f.fetch(h).await.map(|d| (i, d)) }
        });
        for (i, (d, remote)) in futures_join_all(fetches).await.into_iter().flatten() {
            remote_used |= remote;
            slots[i] = Some(d);
        }
    }
    (slots, remote_used)
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
    headers: axum::http::HeaderMap,
) -> Result<Response, ApiError> {
    if let Some(resp) = unavailable(&state, &hash) {
        return Ok(resp);
    }
    // Manifest: local store (materialized), else the replicated registry.
    let manifest = match state.store.get_manifest(&hash) {
        Ok(m) => m,
        Err(_) => match state.app.app_state().manifests.get(&hash) {
            Some(m) => m.clone(),
            None => return Ok((StatusCode::NOT_FOUND, "unknown file").into_response()),
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
        return serve_range(state, manifest, start, end).await;
    }

    // Streaming reconstruction: one stripe at a time towards the client.
    // Per stripe: the k DATA shards are fetched in parallel; parity is only
    // requested if one of them is missing — on a healthy cluster, not a
    // single parity byte crosses the wire.
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<bytes::Bytes, std::io::Error>>(4);
    let fetcher = Arc::new(Fetcher::new(state.clone()));
    let expected_hash = manifest.file_hash.clone();
    let m = manifest.clone();

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
        let mut hasher = blake3::Hasher::new();
        let mut prefetched = first;
        for (stripe_idx, stripe) in m.stripes.iter().enumerate() {
            if let Some(data) = prefetched.take() {
                hasher.update(&data);
                if tx.send(Ok(bytes::Bytes::from(data))).await.is_err() {
                    return;
                }
                continue;
            }
            // Budget-aware slot selection + stripe cache, shared with
            // every other read path.
            let data = match reconstruct_stripe(&fetcher, stripe, stripe_idx, &m).await {
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
        if hasher.finalize().to_hex().to_string() != expected_hash {
            let _ = tx
                .send(Err(std::io::Error::other("integrity violated")))
                .await;
        }
    });

    let body = Body::from_stream(tokio_stream::wrappers::ReceiverStream::new(rx));
    // Egress is counted when the response is committed to — a client that
    // disconnects mid-download still spent its slice of budget.
    state.egress.add(manifest.file_size);
    let mut response = Response::builder()
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .header(header::ACCEPT_RANGES, "bytes")
        .header(header::CONTENT_LENGTH, manifest.file_size);
    if let Some(name) = &manifest.name {
        let safe = name.replace(['"', '\r', '\n'], "_");
        response = response.header(
            header::CONTENT_DISPOSITION,
            format!("attachment; filename=\"{safe}\""),
        );
    }
    Ok(response.body(body).map_err(anyhow::Error::from)?)
}

/// Serves a byte range: only the stripes that intersect it are fetched and
/// decoded (the rest of the file is never touched).
async fn serve_range(
    state: Arc<ApiState>,
    manifest: FileManifest,
    start: u64,
    end: u64,
) -> Result<Response, ApiError> {
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<bytes::Bytes, std::io::Error>>(4);
    let fetcher = Arc::new(Fetcher::new(state.clone()));
    let m = manifest.clone();
    tokio::spawn(async move {
        let mut offset: u64 = 0; // start of the current stripe in the file
        for (stripe_idx, stripe) in m.stripes.iter().enumerate() {
            let stripe_len = stripe.data_len as u64;
            let stripe_end = offset + stripe_len; // exclusive
                                                  // Stripe entirely before/after the range: nothing to do.
            if stripe_end <= start {
                offset = stripe_end;
                continue;
            }
            if offset > end {
                break;
            }
            let data = match reconstruct_stripe(&fetcher, stripe, stripe_idx, &m).await {
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
            // Cut out the useful portion of this stripe.
            let from = start.saturating_sub(offset) as usize;
            let to = ((end - offset + 1).min(stripe_len)) as usize;
            if from < to
                && to <= data.len()
                && tx
                    .send(Ok(bytes::Bytes::from(data[from..to].to_vec())))
                    .await
                    .is_err()
            {
                return; // client gone
            }
            offset = stripe_end;
        }
    });

    let body = Body::from_stream(tokio_stream::wrappers::ReceiverStream::new(rx));
    state.egress.add(end - start + 1);
    Ok(Response::builder()
        .status(StatusCode::PARTIAL_CONTENT)
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .header(header::ACCEPT_RANGES, "bytes")
        .header(header::CONTENT_LENGTH, end - start + 1)
        .header(
            header::CONTENT_RANGE,
            format!("bytes {start}-{end}/{}", manifest.file_size),
        )
        .body(body)
        .map_err(anyhow::Error::from)?)
}

/// Hand-rolled `join_all` (order preserved) — saves one more dependency.
async fn futures_join_all<F, T>(futures: impl Iterator<Item = F>) -> Vec<Option<T>>
where
    F: std::future::Future<Output = Option<T>> + Send + 'static,
    T: Send + 'static,
{
    let handles: Vec<_> = futures.map(tokio::spawn).collect();
    let mut out = Vec::with_capacity(handles.len());
    for h in handles {
        out.push(h.await.ok().flatten());
    }
    out
}

/// Shard fetcher shared across one download request: a connection cache
/// (failures are memoized — a dead node is contacted only once per
/// request) usable from parallel fetches.
pub(crate) struct Fetcher {
    state: Arc<ApiState>,
    view: Vec<(String, u64)>,
    clients: tokio::sync::Mutex<HashMap<String, Option<PeerClient>>>,
}

impl Fetcher {
    pub(crate) fn new(state: Arc<ApiState>) -> Self {
        let view = state.view();
        Self {
            state,
            view,
            clients: tokio::sync::Mutex::new(HashMap::new()),
        }
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
}

async fn files(State(state): State<Arc<ApiState>>) -> Json<Vec<FileEntry>> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let entries = state
        .app
        .app_state()
        .manifests
        .values()
        .filter(|m| m.expires_at.is_none_or(|e| e > now))
        .map(|m| FileEntry {
            hash: m.file_hash.clone(),
            size: m.file_size,
            name: m.name.clone(),
            link: format!("/f/{}", m.file_hash),
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
    use super::{parse_range, ApiError, DispatchError, StatusCode, NO_QUORUM};

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
