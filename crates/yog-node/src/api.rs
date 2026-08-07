//! API HTTP publique d'un nœud : n'importe quel nœud du cluster est un
//! point d'entrée complet.
//!
//! - `POST /api/upload?name=…` : reçoit le fichier, l'encode en Reed-Solomon
//!   stripe par stripe et dispatche chaque shard chez son propriétaire HRW,
//!   puis enregistre le manifest dans le registre Raft.
//! - `GET /f/{hash}` : reconstruit le fichier en streaming depuis le cluster
//!   (k shards suffisent, où qu'ils soient), intégrité vérifiée.
//! - `GET /api/files` : le registre répliqué.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use axum::body::Body;
use axum::extract::{Path, Query, Request, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use yog_erasure::{decode_stripe, encode_stripe, ErasureConfig, FileManifest, StripeMeta};
use yog_store::ShardStore;
use yog_transport::PeerClient;

pub struct ApiState {
    pub store: Arc<ShardStore>,
    pub app: Arc<yog_raft::RaftApp>,
    /// Adresse annoncée de CE nœud (son identité de placement).
    pub self_id: String,
    pub config: ErasureConfig,
    /// Répertoire pour bufferiser les uploads en cours.
    pub tmp_dir: PathBuf,
}

impl ApiState {
    /// Vue pondérée du cluster pour le placement — la même que celle des
    /// scrubbers (capacités déclarées dans l'état Raft).
    fn view(&self) -> Vec<(String, u64)> {
        let mut nodes = self
            .app
            .weighted_view(yog_cluster::placement::DEFAULT_CAPACITY);
        if nodes.is_empty() {
            nodes.push((self.self_id.clone(), yog_cluster::placement::DEFAULT_CAPACITY));
        }
        nodes
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
        .route("/f/{hash}", get(download).head(download_head))
        .with_state(state);
    // Interface web (SPA) : fichiers statiques, et index.html pour les
    // routes applicatives (/files, /dashboard, /d/<hash>).
    if let Some(dir) = webui_dir {
        let index = dir.join("index.html");
        router = router.fallback_service(
            tower_http::services::ServeDir::new(&dir)
                .fallback(tower_http::services::ServeFile::new(index)),
        );
        tracing::info!("webui servie depuis {}", dir.display());
    }
    let listener = tokio::net::TcpListener::bind(listen).await?;
    tracing::info!("API HTTP sur http://{listen}");
    axum::serve(listener, router).await?;
    Ok(())
}

#[derive(serde::Serialize)]
struct NodeStatus {
    addr: String,
    capacity_bytes: u64,
    is_leader: bool,
    is_self: bool,
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
    let leader_addr = metrics.current_leader.and_then(|id| members.get(&id).cloned());
    let app_state = state.app.app_state();
    let nodes = state
        .view()
        .into_iter()
        .map(|(addr, capacity_bytes)| NodeStatus {
            is_leader: leader_addr.as_deref() == Some(addr.as_str()),
            is_self: addr == state.self_id,
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

/// HEAD /f/{hash} : taille sans corps (la page de téléchargement s'en sert).
async fn download_head(
    State(state): State<Arc<ApiState>>,
    Path(hash): Path<String>,
) -> Response {
    let manifest = match state.store.get_manifest(&hash) {
        Ok(m) => m,
        Err(_) => match state.app.app_state().manifests.get(&hash) {
            Some(m) => m.clone(),
            None => return StatusCode::NOT_FOUND.into_response(),
        },
    };
    Response::builder()
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .header(header::CONTENT_LENGTH, manifest.file_size)
        .body(Body::empty())
        .unwrap()
}

/// Erreur HTTP uniforme.
struct ApiError(anyhow::Error);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (StatusCode::INTERNAL_SERVER_ERROR, format!("{:#}", self.0)).into_response()
    }
}

impl<E: Into<anyhow::Error>> From<E> for ApiError {
    fn from(e: E) -> Self {
        Self(e.into())
    }
}

#[derive(serde::Deserialize)]
struct UploadParams {
    name: Option<String>,
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
}

async fn upload(
    State(state): State<Arc<ApiState>>,
    Query(params): Query<UploadParams>,
    request: Request,
) -> Result<Json<UploadResponse>, ApiError> {
    // 1. Bufferise le corps sur disque en hashant au fil de l'eau : le
    //    placement est keyé sur le hash du fichier, connu seulement à la fin.
    let tmp_path = state.tmp_dir.join(format!("upload-{}", uuid_ish()));
    let mut tmp = tokio::fs::File::create(&tmp_path).await?;
    let mut hasher = blake3::Hasher::new();
    let mut size: u64 = 0;
    let mut body = request.into_body().into_data_stream();
    use tokio_stream::StreamExt;
    while let Some(chunk) = body.next().await {
        let chunk = chunk.context("lecture du corps de la requête")?;
        hasher.update(&chunk);
        tmp.write_all(&chunk).await?;
        size += chunk.len() as u64;
    }
    tmp.flush().await?;
    drop(tmp);
    let result = dispatch_file(&state, &tmp_path, size, hasher, params.name).await;
    let _ = tokio::fs::remove_file(&tmp_path).await;
    let manifest = result?;

    Ok(Json(UploadResponse {
        hash: manifest.file_hash.clone(),
        size: manifest.file_size,
        name: manifest.name.clone(),
        stripes: manifest.stripes.len(),
        data_shards: manifest.config.data_shards,
        parity_shards: manifest.config.parity_shards,
        link: format!("/f/{}", manifest.file_hash),
    }))
}

/// Encode le fichier temporaire stripe par stripe et pousse chaque shard
/// chez son propriétaire (ce nœud inclus), puis enregistre le manifest.
async fn dispatch_file(
    state: &Arc<ApiState>,
    tmp_path: &std::path::Path,
    size: u64,
    hasher: blake3::Hasher,
    name: Option<String>,
) -> Result<FileManifest> {
    if size == 0 {
        bail!("fichier vide");
    }
    let file_hash = hasher.finalize().to_hex().to_string();
    let view = state.view();
    let view_refs: Vec<(&str, u64)> = view.iter().map(|(n, w)| (n.as_str(), *w)).collect();
    let cfg = state.config;

    let mut clients: HashMap<String, PeerClient> = HashMap::new();
    let mut f = tokio::fs::File::open(tmp_path).await?;
    let mut stripe_buf = vec![0u8; cfg.stripe_data_len()];
    let mut stripes_meta: Vec<StripeMeta> = Vec::new();
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
        for shard in &shards {
            let owner =
                yog_cluster::placement::shard_owner(&file_hash, si, shard.index, &view_refs);
            if owner == state.self_id {
                state.store.put_shard(&shard.data)?;
                continue;
            }
            send_shard(&mut clients, owner, &shard.data).await?;
        }
        stripes_meta.push(StripeMeta {
            data_len: filled,
            shard_hashes: shards.iter().map(|s| s.hash.clone()).collect(),
        });
    }

    let manifest = FileManifest {
        file_hash,
        file_size: size,
        name,
        config: cfg,
        stripes: stripes_meta,
    };
    // Disponible immédiatement en local, puis répliqué par le registre.
    state.store.put_manifest(&manifest)?;
    let resp = state
        .app
        .write(yog_raft::types::AppCommand::RegisterManifest(manifest.clone()))
        .await
        .context("enregistrement dans le registre Raft")?;
    if !resp.ok {
        bail!("le registre a refusé le manifest");
    }
    Ok(manifest)
}

/// Envoie un shard à un pair, avec reconnexion (idempotent : le stockage
/// est content-addressed, un renvoi ne duplique rien).
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
    bail!("shard non transmis à {owner}")
}

async fn download(
    State(state): State<Arc<ApiState>>,
    Path(hash): Path<String>,
) -> Result<Response, ApiError> {
    // Manifest : store local (matérialisé), sinon registre répliqué.
    let manifest = match state.store.get_manifest(&hash) {
        Ok(m) => m,
        Err(_) => match state.app.app_state().manifests.get(&hash) {
            Some(m) => m.clone(),
            None => return Ok((StatusCode::NOT_FOUND, "fichier inconnu").into_response()),
        },
    };

    // Reconstruction en streaming : une stripe à la fois vers le client.
    // Par stripe : les k shards de DONNÉES sont récupérés en parallèle ;
    // la parité n'est demandée que si l'un d'eux manque — sur un cluster
    // sain, zéro octet de parité ne transite.
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<bytes::Bytes, std::io::Error>>(4);
    let fetcher = Arc::new(Fetcher::new(state.clone()));
    let expected_hash = manifest.file_hash.clone();
    let m = manifest.clone();
    tokio::spawn(async move {
        let mut hasher = blake3::Hasher::new();
        let k = m.config.data_shards;
        for stripe in &m.stripes {
            // 1) Les shards de données, en parallèle.
            let data_fetches = stripe.shard_hashes[..k]
                .iter()
                .map(|h| fetcher.clone().fetch(h.clone()));
            let mut slots: Vec<Option<Vec<u8>>> =
                futures_join_all(data_fetches).await;
            let missing = slots.iter().filter(|s| s.is_none()).count();
            // 2) La parité, seulement si nécessaire (aussi en parallèle).
            if missing > 0 {
                let parity_fetches = stripe.shard_hashes[k..]
                    .iter()
                    .map(|h| fetcher.clone().fetch(h.clone()));
                slots.extend(futures_join_all(parity_fetches).await);
            } else {
                slots.resize(stripe.shard_hashes.len(), None);
            }
            let data = match decode_stripe(slots, stripe, &m.config) {
                Ok(d) => d,
                Err(e) => {
                    let _ = tx
                        .send(Err(std::io::Error::other(format!("stripe irrécupérable: {e}"))))
                        .await;
                    return;
                }
            };
            hasher.update(&data);
            if tx.send(Ok(bytes::Bytes::from(data))).await.is_err() {
                return; // client parti
            }
        }
        if hasher.finalize().to_hex().to_string() != expected_hash {
            let _ = tx.send(Err(std::io::Error::other("intégrité violée"))).await;
        }
    });

    let body = Body::from_stream(tokio_stream::wrappers::ReceiverStream::new(rx));
    let mut response = Response::builder()
        .header(header::CONTENT_TYPE, "application/octet-stream")
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

/// `join_all` maison (ordre préservé) — évite une dépendance de plus.
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

/// Récupérateur de shards partagé par une requête de download : cache de
/// connexions (échecs mémorisés — un nœud mort n'est contacté qu'une fois
/// par requête) utilisable par des fetches parallèles.
struct Fetcher {
    state: Arc<ApiState>,
    view: Vec<(String, u64)>,
    clients: tokio::sync::Mutex<HashMap<String, Option<PeerClient>>>,
}

impl Fetcher {
    fn new(state: Arc<ApiState>) -> Self {
        let view = state.view();
        Self { state, view, clients: tokio::sync::Mutex::new(HashMap::new()) }
    }

    /// Un client vers `node`, créé au premier besoin. `None` = déjà connu
    /// injoignable.
    async fn client_for(&self, node: &str) -> Option<PeerClient> {
        if let Some(cached) = self.clients.lock().await.get(node) {
            return cached.clone();
        }
        // Connexion hors verrou (3 s max) ; en cas de course, une seule
        // des deux connexions est conservée — sans conséquence.
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

    async fn mark_dead(&self, node: &str) {
        self.clients.lock().await.insert(node.to_string(), None);
    }

    /// Cherche un shard : local d'abord, puis chez chaque membre joignable.
    async fn fetch(self: Arc<Self>, hash: String) -> Option<Vec<u8>> {
        if let Ok(data) = self.state.store.get_shard(&hash) {
            return Some(data);
        }
        for (node, _) in self.view.iter().filter(|(n, _)| *n != self.state.self_id) {
            let Some(client) = self.client_for(node).await else { continue };
            match tokio::time::timeout(SHARD_TIMEOUT, client.get_shard(&hash)).await {
                Ok(Ok(Some(data))) => return Some(data),
                Ok(Ok(None)) => {}
                // Erreur ou timeout : connexion suspecte, on la condamne
                // pour le reste de la requête.
                _ => self.mark_dead(node).await,
            }
        }
        None
    }
}

/// Délai au-delà duquel un pair est considéré injoignable.
const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);
/// Délai d'un transfert de shard.
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
    let entries = state
        .app
        .app_state()
        .manifests
        .values()
        .map(|m| FileEntry {
            hash: m.file_hash.clone(),
            size: m.file_size,
            name: m.name.clone(),
            link: format!("/f/{}", m.file_hash),
        })
        .collect();
    Json(entries)
}

/// Identifiant de fichier temporaire unique (pas besoin de vraie
/// cryptographie ici, juste d'éviter les collisions entre uploads).
fn uuid_ish() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    format!("{nanos:x}-{:x}", std::process::id())
}
