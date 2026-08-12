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
    // Nauka is the storage engine: it serves its HTTP API and nothing
    // more. A user-facing web interface belongs to a product built on top,
    // not in the engine.
    let router = Router::new()
        // PUT as well as POST: `curl -T file` — the streaming upload every
        // doc example recommends — sends PUT, and answering it with a 405
        // was the first thing a reader following the docs would hit.
        .route("/api/upload", post(upload).put(upload))
        .route("/api/files", get(files))
        .route("/api/status", get(status))
        .route(
            "/f/{hash}",
            get(download).head(download_head).delete(delete_file),
        )
        .with_state(state);
    let listener = tokio::net::TcpListener::bind(listen).await?;
    tracing::info!("HTTP API on http://{listen}");
    axum::serve(listener, router).await?;
    Ok(())
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
        cfg = cfg.densified_for(first.len());
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
