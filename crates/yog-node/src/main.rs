//! Nœud yogfile — v0 : CLI locale qui exerce le cœur (encode Reed-Solomon,
//! stockage content-addressed, reconstruction avec pertes, intégrité).
//! Les couches QUIC (transport inter-nœuds) et Raft (métadonnées cluster)
//! viendront se poser sur ces mêmes primitives.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use yog_erasure::{decode_file, encode_file, ErasureConfig, FileManifest};
use yog_store::ShardStore;
use yog_transport::PeerClient;

#[derive(Parser)]
#[command(name = "yog-node", about = "Serveur de fichiers distribué yogfile (cœur v0)")]
struct Cli {
    /// Répertoire de données du nœud.
    #[arg(long, default_value = "./yog-data")]
    data_dir: PathBuf,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Encode un fichier en shards Reed-Solomon et le stocke.
    Put {
        file: PathBuf,
        /// k : shards de données par stripe.
        #[arg(long, default_value_t = 4)]
        data_shards: usize,
        /// m : shards de parité par stripe (tolérance de perte).
        #[arg(long, default_value_t = 2)]
        parity_shards: usize,
    },
    /// Reconstruit un fichier depuis ses shards (tolère pertes/corruptions).
    Get {
        file_hash: String,
        #[arg(short, long)]
        output: PathBuf,
    },
    /// Vérifie qu'un fichier est reconstructible et intact.
    Verify { file_hash: String },
    /// Liste les fichiers stockés.
    List,
    /// Démarre le nœud en mode serveur QUIC (cluster si --peers est fourni).
    Serve {
        #[arg(long, default_value = "0.0.0.0:7311")]
        listen: SocketAddr,
        /// Adresse annoncée aux autres nœuds (défaut : adresse d'écoute).
        #[arg(long)]
        advertise: Option<SocketAddr>,
        /// Les autres nœuds du cluster. Active heartbeats + auto-healing.
        #[arg(long, value_delimiter = ',')]
        peers: Vec<SocketAddr>,
        /// Intervalle du scrub d'auto-healing, en secondes.
        #[arg(long, default_value_t = 30)]
        scrub_interval: u64,
        /// Identifiant Raft de ce nœud. Active le mode consensus : le
        /// membership et le registre des fichiers sont répliqués par Raft
        /// (la liste --peers devient inutile pour le healing).
        #[arg(long)]
        node_id: Option<u64>,
    },
    /// Initialise le cluster Raft (une seule fois, via n'importe quel nœud).
    ClusterInit {
        /// Membres au format id@host:port, ex: 1@10.0.0.1:7311 2@10.0.0.2:7311
        #[arg(required = true)]
        members: Vec<String>,
    },
    /// Affiche l'état du cluster Raft vu par un nœud.
    ClusterMetrics {
        #[arg(long, default_value = "127.0.0.1:7311")]
        peer: SocketAddr,
    },
    /// Encode un fichier et dispatche ses shards sur des peers (round-robin).
    PutRemote {
        file: PathBuf,
        /// Adresses des nœuds du cluster, ex: 10.0.0.1:7311,10.0.0.2:7311
        #[arg(long, value_delimiter = ',', required = true)]
        peers: Vec<SocketAddr>,
        #[arg(long, default_value_t = 4)]
        data_shards: usize,
        #[arg(long, default_value_t = 2)]
        parity_shards: usize,
    },
    /// Reconstruit un fichier en lisant les shards depuis des peers.
    GetRemote {
        file_hash: String,
        #[arg(long, value_delimiter = ',', required = true)]
        peers: Vec<SocketAddr>,
        #[arg(short, long)]
        output: PathBuf,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();
    let cli = Cli::parse();
    let store = ShardStore::open(&cli.data_dir)?;

    match cli.cmd {
        Cmd::Put { file, data_shards, parity_shards } => {
            let data = std::fs::read(&file)
                .with_context(|| format!("lecture de {}", file.display()))?;
            let cfg = ErasureConfig {
                data_shards,
                parity_shards,
                ..ErasureConfig::default()
            };
            let (manifest, stripes) = encode_file(&data, &cfg)?;
            let mut shard_count = 0;
            for stripe in &stripes {
                for shard in stripe {
                    store.put_shard(&shard.data)?;
                    shard_count += 1;
                }
            }
            store.put_manifest(&manifest)?;
            println!("stocké : {}", manifest.file_hash);
            println!(
                "  {} octets, {} stripes, {} shards ({}+{}), tolère la perte de {} shards/stripe",
                manifest.file_size,
                manifest.stripes.len(),
                shard_count,
                cfg.data_shards,
                cfg.parity_shards,
                cfg.parity_shards,
            );
        }
        Cmd::Get { file_hash, output } => {
            let data = reconstruct(&store, &file_hash)?;
            std::fs::write(&output, &data)?;
            println!("reconstruit : {} octets → {}", data.len(), output.display());
        }
        Cmd::Verify { file_hash } => {
            let manifest = store.get_manifest(&file_hash)?;
            let mut missing = 0usize;
            let mut total = 0usize;
            for stripe in &manifest.stripes {
                for hash in &stripe.shard_hashes {
                    total += 1;
                    if store.get_shard(hash).is_err() {
                        missing += 1;
                    }
                }
            }
            match reconstruct(&store, &file_hash) {
                Ok(_) => println!(
                    "OK : intègre et reconstructible ({missing}/{total} shards indisponibles)"
                ),
                Err(e) => bail!("IRRÉCUPÉRABLE ({missing}/{total} shards indisponibles) : {e}"),
            }
        }
        Cmd::List => {
            for hash in store.list_manifests()? {
                let m = store.get_manifest(&hash)?;
                println!("{hash}  {} octets", m.file_size);
            }
        }
        Cmd::Serve { listen, advertise, peers, scrub_interval, node_id } => {
            let store = Arc::new(store);
            let interval = std::time::Duration::from_secs(scrub_interval);
            let mut raft_handler: Option<Arc<dyn yog_transport::server::RaftHandler>> = None;

            if let Some(id) = node_id {
                // Mode consensus : membership et registre viennent de Raft.
                let app = yog_raft::RaftApp::start(id).await?;
                raft_handler = Some(app.clone());
                let self_id = advertise.unwrap_or(listen).to_string();
                let store_bg = store.clone();
                tokio::spawn(async move {
                    let mut ticker = tokio::time::interval(interval);
                    loop {
                        ticker.tick().await;
                        // Le registre répliqué est la source de vérité :
                        // matérialise localement les manifests que ce nœud
                        // ne connaît pas encore, puis scrub.
                        let state = app.app_state();
                        for manifest in state.manifests.values() {
                            if store_bg.get_manifest(&manifest.file_hash).is_err() {
                                let _ = store_bg.put_manifest(manifest);
                            }
                        }
                        let members = app.members();
                        if members.len() < 2 || !members.values().any(|a| *a == self_id) {
                            continue;
                        }
                        let mut nodes: Vec<String> = members.into_values().collect();
                        nodes.sort();
                        match yog_cluster::healer::scrub_once(&store_bg, &self_id, &nodes).await {
                            Ok(r) if r.shards_healed > 0 || r.shards_unrecoverable > 0 => {
                                eprintln!(
                                    "scrub: {} vérifiés, {} régénérés, {} irréparables",
                                    r.shards_checked, r.shards_healed, r.shards_unrecoverable
                                );
                            }
                            Ok(_) => {}
                            Err(e) => eprintln!("scrub en échec: {e}"),
                        }
                    }
                });
            } else if !peers.is_empty() {
                // Mode statique (sans consensus) : vue du cluster en config.
                let view = yog_cluster::ClusterView::new(advertise.unwrap_or(listen), &peers);
                tokio::spawn(yog_cluster::run_background(store.clone(), view, interval));
            }
            yog_transport::serve(store, listen, raft_handler).await?;
        }
        Cmd::ClusterInit { members } => {
            let mut map = std::collections::BTreeMap::new();
            for m in &members {
                let (id, addr) = m
                    .split_once('@')
                    .with_context(|| format!("format attendu id@host:port, reçu {m}"))?;
                map.insert(id.parse::<u64>()?, addr.to_string());
            }
            let first: SocketAddr = map.values().next().unwrap().parse()?;
            let client = PeerClient::connect(first).await?;
            match yog_raft::admin_call(&client, &yog_raft::types::AdminRequest::Init(map)).await? {
                yog_raft::types::AdminResponse::Ok(_) => println!("cluster initialisé"),
                other => bail!("échec de l'init: {other:?}"),
            }
        }
        Cmd::ClusterMetrics { peer } => {
            let client = PeerClient::connect(peer).await?;
            match yog_raft::admin_call(&client, &yog_raft::types::AdminRequest::Metrics).await? {
                yog_raft::types::AdminResponse::Metrics { id, leader, members, last_applied } => {
                    println!("nœud {id} — leader: {leader:?}, log appliqué: {last_applied:?}");
                    for (id, addr) in members {
                        println!("  membre {id} @ {addr}");
                    }
                }
                other => bail!("réponse inattendue: {other:?}"),
            }
        }
        Cmd::PutRemote { file, peers, data_shards, parity_shards } => {
            let data = std::fs::read(&file)
                .with_context(|| format!("lecture de {}", file.display()))?;
            let cfg = ErasureConfig {
                data_shards,
                parity_shards,
                ..ErasureConfig::default()
            };
            let clients = connect_all(&peers).await?;
            let (manifest, stripes) = encode_file(&data, &cfg)?;

            // Placement rendezvous-hash : le même calcul que fait le scrubber
            // des nœuds, pour que chaque shard parte directement chez son
            // propriétaire. Adresses triées = même vue que le cluster.
            let addrs: Vec<String> = clients.iter().map(|c| c.addr.to_string()).collect();
            let mut view: Vec<&str> = addrs.iter().map(String::as_str).collect();
            view.sort();
            for (si, stripe) in stripes.iter().enumerate() {
                for shard in stripe {
                    let owner = yog_cluster::placement::shard_owner(
                        &manifest.file_hash,
                        si,
                        shard.index,
                        &view,
                    );
                    let client = clients
                        .iter()
                        .find(|c| c.addr.to_string() == owner)
                        .expect("owner vient de la liste des clients");
                    client.put_shard(shard.data.clone()).await?;
                }
            }
            // Le manifest (métadonnées seulement) est répliqué sur tous les
            // nœuds — en attendant Raft, chacun sait reconstruire.
            for client in &clients {
                client.put_manifest(&manifest).await?;
            }
            // Si le cluster tourne en mode Raft, enregistre aussi le fichier
            // dans le registre répliqué (best effort sinon).
            match tokio::time::timeout(
                std::time::Duration::from_secs(3),
                yog_raft::write_via_leader(
                    &peers,
                    yog_raft::types::AppCommand::RegisterManifest(manifest.clone()),
                ),
            )
            .await
            {
                Ok(Ok(_)) => println!("enregistré dans le registre Raft"),
                _ => println!("registre Raft indisponible (cluster en mode statique ?)"),
            }
            println!("dispatché : {}", manifest.file_hash);
            println!(
                "  {} octets, {} stripes ({}+{}) sur {} nœuds",
                manifest.file_size,
                manifest.stripes.len(),
                cfg.data_shards,
                cfg.parity_shards,
                clients.len(),
            );
        }
        Cmd::GetRemote { file_hash, peers, output } => {
            let clients = connect_all(&peers).await?;
            let manifest = fetch_manifest(&clients, &file_hash).await?;

            let mut stripes_slots = Vec::new();
            for stripe in &manifest.stripes {
                let mut slots = Vec::new();
                for hash in &stripe.shard_hashes {
                    slots.push(fetch_shard(&clients, hash).await);
                }
                stripes_slots.push(slots);
            }
            let data = decode_file(&manifest, stripes_slots)?;
            std::fs::write(&output, &data)?;
            println!("reconstruit : {} octets → {}", data.len(), output.display());
        }
    }
    Ok(())
}

/// Se connecte aux peers joignables ; échoue seulement si aucun ne répond.
async fn connect_all(peers: &[SocketAddr]) -> Result<Vec<PeerClient>> {
    let mut clients = Vec::new();
    for addr in peers {
        match PeerClient::connect(*addr).await {
            Ok(c) => clients.push(c),
            Err(e) => eprintln!("peer {addr} injoignable ({e}), on continue sans lui"),
        }
    }
    if clients.is_empty() {
        bail!("aucun peer joignable");
    }
    Ok(clients)
}

async fn fetch_manifest(clients: &[PeerClient], file_hash: &str) -> Result<FileManifest> {
    for client in clients {
        if let Ok(Some(m)) = client.get_manifest(file_hash).await {
            return Ok(m);
        }
    }
    bail!("manifest {file_hash} introuvable sur les peers");
}

/// Premier peer qui possède le shard gagne ; introuvable → None, le
/// Reed-Solomon compensera si assez de shards survivent.
async fn fetch_shard(clients: &[PeerClient], hash: &str) -> Option<Vec<u8>> {
    for client in clients {
        if let Ok(Some(data)) = client.get_shard(hash).await {
            return Some(data);
        }
    }
    None
}

/// Charge les shards disponibles (les manquants/corrompus deviennent `None`)
/// et laisse Reed-Solomon reconstruire.
fn reconstruct(store: &ShardStore, file_hash: &str) -> Result<Vec<u8>> {
    let manifest = store.get_manifest(file_hash)?;
    let stripes = manifest
        .stripes
        .iter()
        .map(|stripe| {
            stripe
                .shard_hashes
                .iter()
                .map(|hash| store.get_shard(hash).ok())
                .collect()
        })
        .collect();
    Ok(decode_file(&manifest, stripes)?)
}
