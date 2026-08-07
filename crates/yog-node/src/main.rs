//! Nœud yogfile — v0 : CLI locale qui exerce le cœur (encode Reed-Solomon,
//! stockage content-addressed, reconstruction avec pertes, intégrité).
//! Les couches QUIC (transport inter-nœuds) et Raft (métadonnées cluster)
//! viendront se poser sur ces mêmes primitives.

mod api;
mod e2e;

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
    /// Répertoire contenant la clé de cluster (cluster-ca.key/.pem).
    /// Active le mTLS : seuls les porteurs d'un certificat signé passent.
    #[arg(long)]
    keys: Option<PathBuf>,
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
    /// Chiffre un fichier (AES-256-GCM, clé locale) puis l'uploade sur un
    /// nœud. Les serveurs ne voient QUE du ciphertext ; le lien affiché
    /// contient la clé dans son fragment (#…), jamais transmis au serveur.
    Upload {
        file: PathBuf,
        /// URL de l'API d'un nœud du cluster.
        #[arg(long, default_value = "http://127.0.0.1:8080")]
        api: String,
        /// Nom public (métadonnée EN CLAIR côté serveur — omis par défaut).
        #[arg(long)]
        name: Option<String>,
    },
    /// Télécharge un lien de partage complet (avec #clé) et déchiffre.
    Download {
        link: String,
        #[arg(short, long)]
        output: PathBuf,
    },
    /// Génère la clé de cluster (CA Ed25519) à distribuer aux nœuds.
    Keygen {
        #[arg(long, default_value = "./yog-keys")]
        out: PathBuf,
    },
    /// Affiche l'identité de ce nœud (node-id dérivé de sa clé publique).
    NodeInfo,
    /// Démarre le nœud en mode serveur QUIC (cluster si --peers est fourni).
    /// En mode consensus (--node-id), le port+1 est réservé au plan Raft :
    /// plusieurs nœuds sur un même hôte doivent espacer leurs ports de 2.
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
        /// Capacité de stockage de ce nœud en octets (poids du placement
        /// pondéré). Défaut : taille du système de fichiers du data-dir.
        #[arg(long)]
        capacity: Option<u64>,
        /// Adresse de l'API HTTP publique (upload/download).
        #[arg(long, default_value = "0.0.0.0:8080")]
        http: SocketAddr,
        /// Répertoire de l'interface web à servir (dist de webui/).
        /// Défaut : ./webui/dist s'il existe.
        #[arg(long)]
        webui: Option<PathBuf>,
        /// Désactive l'API HTTP.
        #[arg(long)]
        no_http: bool,
        /// Désactive la découverte DHT (implicite dès que --keys est fourni
        /// sans --peers) : cluster statique / air-gapped.
        #[arg(long)]
        no_discover: bool,
        /// Nœuds d'amorçage DHT alternatifs (tests sur DHT locale).
        #[arg(long, value_delimiter = ',', hide = true)]
        dht_bootstrap: Vec<String>,
    },
    /// Initialise le cluster Raft (une seule fois, via n'importe quel nœud).
    ClusterInit {
        /// Membres au format id@host:port, ex: 1@10.0.0.1:7311 2@10.0.0.2:7311
        #[arg(required = true)]
        members: Vec<String>,
    },
    /// Bannit un fichier : retiré du registre, refusé au téléchargement
    /// (410) et purgé par le GC. Pour honorer un signalement ou une
    /// réquisition sans lire le contenu.
    Ban {
        file_hash: String,
        /// Motif consigné dans le registre (référence du signalement…).
        #[arg(long, default_value = "signalement")]
        reason: String,
        #[arg(long, value_delimiter = ',', required = true)]
        peers: Vec<SocketAddr>,
    },
    /// Lève un bannissement.
    Unban {
        file_hash: String,
        #[arg(long, value_delimiter = ',', required = true)]
        peers: Vec<SocketAddr>,
    },
    /// Affiche l'état du cluster Raft vu par un nœud.
    ClusterMetrics {
        #[arg(long, default_value = "127.0.0.1:7311")]
        peer: SocketAddr,
    },
    /// Ajoute un nœud au cluster à chaud (learner → votant). Le nouveau
    /// nœud doit déjà tourner (serve --node-id <id>). Le rebalancement des
    /// shards suit automatiquement (scrub + GC).
    ClusterAdd {
        /// Le nœud à ajouter, format id@host:port.
        member: String,
        /// N'importe quels nœuds actuels du cluster.
        #[arg(long, value_delimiter = ',', required = true)]
        peers: Vec<SocketAddr>,
    },
    /// Retire un nœud du cluster à chaud. Ses shards sont re-répliqués par
    /// les scrubbers des autres nœuds ; le retiré peut ensuite être éteint.
    ClusterRemove {
        node_id: u64,
        #[arg(long, value_delimiter = ',', required = true)]
        peers: Vec<SocketAddr>,
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

    // Identité cluster : à installer avant tout usage réseau. Un nœud
    // (serve/node-info) utilise sa clé persistée ; les commandes clientes
    // utilisent une identité éphémère signée par la même CA.
    let node_tls = if let Some(keys_dir) = &cli.keys {
        let identity = match &cli.cmd {
            Cmd::Serve { .. } | Cmd::NodeInfo => Some(cli.data_dir.join("node.key")),
            _ => None,
        };
        let tls = yog_transport::load_cluster_tls(keys_dir, identity.as_deref())?;
        let info = (tls.node_id, tls.fingerprint.clone());
        yog_transport::set_cluster_tls(tls);
        Some(info)
    } else {
        None
    };

    let store = ShardStore::open(&cli.data_dir)?;

    match cli.cmd {
        Cmd::Upload { file, api, name } => {
            e2e::upload(&api, &file, name).await?;
        }
        Cmd::Download { link, output } => {
            e2e::download(&link, &output).await?;
        }
        Cmd::Keygen { out } => {
            yog_transport::generate_cluster_ca(&out)?;
            println!("clé de cluster générée dans {}", out.display());
            println!("  à copier sur chaque nœud, puis: serve --keys {}", out.display());
        }
        Cmd::NodeInfo => {
            let (node_id, fingerprint) =
                node_tls.context("node-info nécessite --keys <dir>")?;
            println!("node-id     : {node_id}");
            println!("fingerprint : {fingerprint}");
        }
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
        Cmd::Serve {
            listen,
            advertise,
            peers,
            scrub_interval,
            node_id,
            capacity,
            http,
            webui,
            no_http,
            no_discover,
            dht_bootstrap,
        } => {
            // Découverte implicite : des clés de cluster, pas de liste
            // statique, pas d'opt-out → le nœud se débrouille tout seul.
            let discover = cli.keys.is_some() && peers.is_empty() && !no_discover;
            let store = Arc::new(store);
            let interval = std::time::Duration::from_secs(scrub_interval);
            let mut raft_handler: Option<Arc<dyn yog_transport::server::RaftHandler>> = None;

            // Avec une identité crypto, le node-id se PROUVE (dérivé de la
            // clé publique) au lieu de se décréter.
            let node_id = match (&node_tls, node_id) {
                (Some((derived, fp)), cli_id) => {
                    if let Some(cli_id) = cli_id {
                        if cli_id != *derived {
                            eprintln!(
                                "--node-id {cli_id} ignoré: l'identité crypto impose {derived} \
                                 (fingerprint {})",
                                &fp[..16]
                            );
                        }
                    }
                    println!("identité: node-id {derived} (fingerprint {})", &fp[..16]);
                    Some(*derived)
                }
                (None, id) => id,
            };

            let boots: Option<Vec<String>> =
                if dht_bootstrap.is_empty() { None } else { Some(dht_bootstrap.clone()) };

            // Adresse annoncée aux autres nœuds : explicite (--advertise),
            // sinon auto-détectée via la DHT en mode découverte, sinon
            // l'adresse d'écoute.
            let advertise_addr = match advertise {
                Some(a) => a,
                None if discover => {
                    match yog_discovery::detect_public_ip(boots.as_deref()).await {
                        Ok(Some(ip)) => {
                            let a = SocketAddr::new(ip, listen.port());
                            println!(
                                "IP publique détectée via la DHT: {ip} — adresse annoncée {a} \
                                 (le port {} et le port {} doivent être joignables en UDP)",
                                listen.port(),
                                listen.port() + 1
                            );
                            a
                        }
                        Ok(None) => {
                            eprintln!(
                                "IP publique indétectable via la DHT — repli sur l'adresse \
                                 d'écoute {listen} (utiliser --advertise si elle n'est pas \
                                 joignable par les autres nœuds)"
                            );
                            listen
                        }
                        Err(e) => {
                            eprintln!("détection d'IP publique en échec ({e:#}) — repli sur {listen}");
                            listen
                        }
                    }
                }
                None => listen,
            };

            if let Some(id) = node_id {
                // Mode consensus : membership et registre viennent de Raft.
                let app = yog_raft::RaftApp::start(id, &cli.data_dir.join("raft")).await?;
                raft_handler = Some(app.clone());
                let self_id = advertise_addr.to_string();

                if discover {
                    let keys_dir = cli
                        .keys
                        .clone()
                        .context("--discover nécessite --keys (identité du cluster)")?;
                    let dht_kp = yog_discovery::derive_dht_keypair(&keys_dir)?;
                    let client = yog_discovery::make_client(boots.as_deref())?;
                    tokio::spawn(run_discovery(app.clone(), client, dht_kp, advertise_addr));
                }

                if !no_http {
                    let api_state = Arc::new(api::ApiState {
                        store: store.clone(),
                        app: app.clone(),
                        self_id: self_id.clone(),
                        config: ErasureConfig::default(),
                        tmp_dir: cli.data_dir.join("tmp"),
                    });
                    let webui_dir = webui.or_else(|| {
                        let default = PathBuf::from("webui/dist");
                        default.join("index.html").exists().then_some(default)
                    });
                    tokio::spawn(async move {
                        if let Err(e) = api::serve_http(http, api_state, webui_dir).await {
                            eprintln!("API HTTP arrêtée: {e:#}");
                        }
                    });
                }
                let store_bg = store.clone();
                let data_dir_bg = cli.data_dir.clone();
                tokio::spawn(async move {
                    let mut ticker = tokio::time::interval(interval);
                    let mut declared_capacity: Option<u64> = None;
                    let mut my_coord = yog_cluster::vivaldi::Coord::default();
                    loop {
                        ticker.tick().await;
                        // Déclare la capacité de ce nœud dans l'état répliqué
                        // (poids du placement) — au premier tick puis quand
                        // elle change de plus de 1 %.
                        if app.members().contains_key(&app.id) {
                            let cap = capacity
                                .unwrap_or_else(|| filesystem_capacity(&data_dir_bg));
                            let changed = match declared_capacity {
                                None => true,
                                Some(prev) => {
                                    (cap as i128 - prev as i128).unsigned_abs()
                                        > (prev as u128) / 100
                                }
                            };
                            if changed {
                                match app
                                    .write(yog_raft::types::AppCommand::UpdateNodeStats {
                                        addr: self_id.clone(),
                                        capacity_bytes: cap,
                                    })
                                    .await
                                {
                                    Ok(_) => {
                                        eprintln!(
                                            "capacité déclarée: {:.1} Go",
                                            cap as f64 / 1e9
                                        );
                                        declared_capacity = Some(cap);
                                    }
                                    Err(e) => eprintln!("déclaration de capacité: {e:#}"),
                                }
                            }
                        }
                        // Le registre répliqué est la source de vérité :
                        // matérialise localement les manifests que ce nœud
                        // ne connaît pas encore, puis scrub.
                        let state = app.app_state();
                        for manifest in state.manifests.values() {
                            if store_bg.get_manifest(&manifest.file_hash).is_err() {
                                let _ = store_bg.put_manifest(manifest);
                            }
                        }

                        // Expiration : le leader retire du registre les
                        // fichiers dont le TTL est échu (une seule fois pour
                        // tout le cluster, la réplication fait le reste).
                        let is_leader =
                            app.raft.metrics().borrow().current_leader == Some(app.id);
                        if is_leader {
                            let now = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs();
                            for m in state.manifests.values() {
                                if m.expires_at.is_some_and(|e| e <= now) {
                                    match app
                                        .write(yog_raft::types::AppCommand::UnregisterManifest {
                                            file_hash: m.file_hash.clone(),
                                        })
                                        .await
                                    {
                                        Ok(_) => eprintln!("expiré: {}", m.file_hash),
                                        Err(e) => eprintln!("expiration en échec: {e:#}"),
                                    }
                                }
                            }
                        }

                        // Purge locale : manifests absents du registre et
                        // shards que plus aucun fichier vivant ne référence.
                        // `registry_ready` évite qu'un nœud fraîchement
                        // démarré, au registre encore vide, n'efface tout.
                        let live: std::collections::BTreeSet<String> =
                            app.app_state().manifests.keys().cloned().collect();
                        let registry_ready = app.members().contains_key(&app.id)
                            && app.raft.metrics().borrow().current_leader.is_some();
                        match yog_cluster::healer::purge_deleted(
                            &store_bg,
                            &live,
                            registry_ready,
                        ) {
                            Ok(p) if p.manifests_purged > 0 || p.orphans_purged > 0 => {
                                eprintln!(
                                    "purge: {} manifest(s), {} shard(s) orphelin(s)",
                                    p.manifests_purged, p.orphans_purged
                                );
                            }
                            Ok(_) => {}
                            Err(e) => eprintln!("purge en échec: {e}"),
                        }
                        let members = app.members();
                        if members.len() < 2 || !members.values().any(|a| *a == self_id) {
                            continue;
                        }
                        let nodes =
                            app.weighted_view(yog_cluster::placement::DEFAULT_CAPACITY);

                        // Coordonnées réseau : mesure les RTT vers les pairs,
                        // ajuste notre position Vivaldi, et la publie si elle
                        // a bougé sensiblement. Le placement s'en sert pour
                        // écarter géographiquement les shards d'une stripe.
                        let known = app.coords();
                        for (peer, _) in nodes.iter().filter(|(n, _)| *n != self_id) {
                            let Ok(addr) = peer.parse::<SocketAddr>() else { continue };
                            let t0 = std::time::Instant::now();
                            let ok = match yog_transport::PeerClient::connect(addr).await {
                                Ok(c) => c.ping().await.is_ok(),
                                Err(_) => false,
                            };
                            if !ok {
                                continue;
                            }
                            let rtt_ms = t0.elapsed().as_secs_f64() * 1000.0;
                            let peer_coord = known.get(peer).copied().unwrap_or_default();
                            my_coord.observe(&peer_coord, rtt_ms);
                        }
                        let published = known.get(&self_id).copied().unwrap_or_default();
                        let moved = my_coord.distance(&published) > 2.0
                            || (my_coord.error - published.error).abs() > 0.1;
                        if moved {
                            let _ = app
                                .write(yog_raft::types::AppCommand::UpdateNodeCoord {
                                    addr: self_id.clone(),
                                    coord: my_coord,
                                })
                                .await;
                        }
                        let coords = app.coords();

                        match yog_cluster::healer::scrub_once_geo(
                            &store_bg, &self_id, &nodes, &coords,
                        )
                        .await
                        {
                            Ok(r) if r.shards_healed > 0 || r.shards_unrecoverable > 0 => {
                                eprintln!(
                                    "scrub: {} vérifiés, {} régénérés, {} irréparables",
                                    r.shards_checked, r.shards_healed, r.shards_unrecoverable
                                );
                            }
                            Ok(_) => {}
                            Err(e) => eprintln!("scrub en échec: {e}"),
                        }
                        // Rebalancement : libère ce qui ne nous appartient
                        // plus (après confirmation chez le propriétaire).
                        match yog_cluster::healer::gc_once_geo(
                            &store_bg, &self_id, &nodes, &coords,
                        )
                        .await
                        {
                            Ok(g) if g.shards_released > 0 => {
                                eprintln!("gc: {} shards libérés", g.shards_released);
                            }
                            Ok(_) => {}
                            Err(e) => eprintln!("gc en échec: {e}"),
                        }
                        // Attestation : les pairs détiennent-ils vraiment ce
                        // qu'ils déclarent ? (échantillonnage, coût minime)
                        match yog_cluster::audit::audit_once_geo(
                            &store_bg, &self_id, &nodes, &coords,
                        )
                        .await
                        {
                            Ok(a) if a.failed > 0 => eprintln!(
                                "AUDIT: {} preuve(s) invalide(s) sur {} challenges — \
                                 un pair ne détient pas ce qu'il déclare",
                                a.failed, a.challenged
                            ),
                            Ok(_) => {}
                            Err(e) => eprintln!("audit en échec: {e}"),
                        }
                    }
                });
            } else if !peers.is_empty() {
                // Mode statique (sans consensus) : vue du cluster en config.
                let view = yog_cluster::ClusterView::new(advertise_addr, &peers);
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
            // Pre-flight : chaque membre doit répondre sur ses DEUX plans,
            // et le node-id qui répond au plan consensus doit être le bon —
            // attrape les nœuds morts et les collisions de ports (nœuds
            // co-hébergés dont les ports ne sont pas espacés d'au moins 2).
            for (id, addr_str) in &map {
                let addr: SocketAddr = addr_str.parse()?;
                let data = PeerClient::connect(addr)
                    .await
                    .with_context(|| format!("nœud {id}: plan data {addr} injoignable"))?;
                data.ping()
                    .await
                    .with_context(|| format!("nœud {id}: plan data {addr} ne répond pas"))?;
                let cons_addr = yog_transport::consensus_addr(addr);
                let cons = PeerClient::connect_consensus(cons_addr).await.with_context(|| {
                    format!("nœud {id}: plan consensus {cons_addr} injoignable")
                })?;
                match yog_raft::admin_call(&cons, &yog_raft::types::AdminRequest::Metrics).await {
                    Ok(yog_raft::types::AdminResponse::Metrics { id: got, .. }) if got == *id => {}
                    Ok(yog_raft::types::AdminResponse::Metrics { id: got, .. }) => bail!(
                        "collision de ports: {cons_addr} répond avec le node-id {got} au lieu \
                         de {id} — espacez les ports d'au moins 2 sur un même hôte"
                    ),
                    other => bail!("nœud {id}: réponse consensus inattendue: {other:?}"),
                }
            }
            let first: SocketAddr = map.values().next().unwrap().parse()?;
            let client = PeerClient::connect(first).await?;
            match yog_raft::admin_call(&client, &yog_raft::types::AdminRequest::Init(map)).await? {
                yog_raft::types::AdminResponse::Ok(_) => println!("cluster initialisé"),
                other => bail!("échec de l'init: {other:?}"),
            }
        }
        Cmd::Ban { file_hash, reason, peers } => {
            let resp = yog_raft::write_via_leader(
                &peers,
                yog_raft::types::AppCommand::BanHash {
                    file_hash: file_hash.clone(),
                    reason: reason.clone(),
                },
            )
            .await?;
            if resp.ok {
                println!("banni : {file_hash} ({reason})");
                println!("  retiré du registre, refusé en 410, shards purgés au prochain GC");
            } else {
                bail!("bannissement refusé");
            }
        }
        Cmd::Unban { file_hash, peers } => {
            let resp = yog_raft::write_via_leader(
                &peers,
                yog_raft::types::AppCommand::UnbanHash { file_hash: file_hash.clone() },
            )
            .await?;
            if resp.ok {
                println!("bannissement levé : {file_hash}");
            } else {
                println!("ce hash n'était pas banni");
            }
        }
        Cmd::ClusterMetrics { peer } => {
            let client = PeerClient::connect(peer).await?;
            match yog_raft::admin_call(&client, &yog_raft::types::AdminRequest::Metrics).await? {
                yog_raft::types::AdminResponse::Metrics {
                    id,
                    leader,
                    members,
                    last_applied,
                    capacities,
                } => {
                    println!("nœud {id} — leader: {leader:?}, log appliqué: {last_applied:?}");
                    for (id, addr) in members {
                        match capacities.get(&addr) {
                            Some(cap) => println!(
                                "  membre {id} @ {addr} — capacité {:.1} Go",
                                *cap as f64 / 1e9
                            ),
                            None => println!("  membre {id} @ {addr} — capacité non déclarée"),
                        }
                    }
                }
                other => bail!("réponse inattendue: {other:?}"),
            }
        }
        Cmd::ClusterAdd { member, peers } => {
            use yog_raft::types::{AdminRequest, AdminResponse};
            let (id, addr) = member
                .split_once('@')
                .with_context(|| format!("format attendu id@host:port, reçu {member}"))?;
            let id: u64 = id.parse()?;
            // 1. Learner : le nœud rattrape le log/snapshot sans voter.
            match yog_raft::admin_via_leader(
                &peers,
                &AdminRequest::AddLearner { id, addr: addr.to_string() },
            )
            .await?
            {
                AdminResponse::Ok(_) => println!("nœud {id} ajouté comme learner"),
                other => bail!("add-learner: {other:?}"),
            }
            // 2. Promotion en votant : membership = membres actuels + lui.
            let current = match yog_raft::admin_via_leader(&peers, &AdminRequest::Metrics).await? {
                AdminResponse::Metrics { members, .. } => members,
                other => bail!("metrics: {other:?}"),
            };
            let mut ids: Vec<u64> = current.keys().copied().collect();
            if !ids.contains(&id) {
                ids.push(id);
            }
            match yog_raft::admin_via_leader(&peers, &AdminRequest::ChangeMembership(ids)).await? {
                AdminResponse::Ok(_) => {
                    println!("nœud {id} promu votant — le rebalancement suivra au fil des scrubs")
                }
                other => bail!("change-membership: {other:?}"),
            }
        }
        Cmd::ClusterRemove { node_id, peers } => {
            use yog_raft::types::{AdminRequest, AdminResponse};
            let current = match yog_raft::admin_via_leader(&peers, &AdminRequest::Metrics).await? {
                AdminResponse::Metrics { members, .. } => members,
                other => bail!("metrics: {other:?}"),
            };
            let ids: Vec<u64> = current.keys().copied().filter(|i| *i != node_id).collect();
            if ids.len() == current.len() {
                bail!("le nœud {node_id} n'est pas membre du cluster");
            }
            match yog_raft::admin_via_leader(&peers, &AdminRequest::ChangeMembership(ids)).await? {
                AdminResponse::Ok(_) => println!(
                    "nœud {node_id} retiré — laisser tourner le temps que les scrubs \
                     re-répliquent ses shards, puis l'éteindre"
                ),
                other => bail!("change-membership: {other:?}"),
            }
        }
        Cmd::PutRemote { file, peers, data_shards, parity_shards } => {
            use std::io::Read;
            let cfg = ErasureConfig {
                data_shards,
                parity_shards,
                ..ErasureConfig::default()
            };
            let file_size = std::fs::metadata(&file)
                .with_context(|| format!("lecture de {}", file.display()))?
                .len();

            // Passe 1 : hash du fichier en streaming (le placement et le
            // manifest sont keyés sur ce hash).
            let mut hasher = blake3::Hasher::new();
            {
                let mut f = std::fs::File::open(&file)?;
                let mut buf = vec![0u8; 4 * 1024 * 1024];
                loop {
                    let n = f.read(&mut buf)?;
                    if n == 0 {
                        break;
                    }
                    hasher.update(&buf[..n]);
                }
            }
            let file_hash = hasher.finalize().to_hex().to_string();

            let clients = connect_all(&peers).await?;
            // Placement pondéré : les capacités viennent des Metrics du
            // cluster (mode Raft) ; à défaut, poids par défaut uniformes.
            let capacities = match yog_raft::admin_via_leader(
                &peers,
                &yog_raft::types::AdminRequest::Metrics,
            )
            .await
            {
                Ok(yog_raft::types::AdminResponse::Metrics { capacities, .. }) => capacities,
                _ => Default::default(),
            };
            let addrs: Vec<String> = clients.iter().map(|c| c.addr.to_string()).collect();
            let mut view: Vec<(&str, u64)> = addrs
                .iter()
                .map(|a| {
                    let w = capacities
                        .get(a)
                        .copied()
                        .unwrap_or(yog_cluster::placement::DEFAULT_CAPACITY);
                    (a.as_str(), w)
                })
                .collect();
            view.sort();

            // Passe 2 : encode et dispatche stripe par stripe — la mémoire
            // reste bornée à une stripe quel que soit la taille du fichier.
            // 16 Mo en vol par upload : assez pour saturer un lien, sans
            // écrouler le cluster quand plusieurs uploads tournent en rafale.
            const MAX_IN_FLIGHT: usize = 16;
            let mut in_flight: tokio::task::JoinSet<Result<()>> = tokio::task::JoinSet::new();
            let mut f = std::fs::File::open(&file)?;
            let mut stripe_buf = vec![0u8; cfg.stripe_data_len()];
            let mut stripes_meta = Vec::new();
            let start = std::time::Instant::now();
            loop {
                let mut filled = 0;
                while filled < stripe_buf.len() {
                    let n = f.read(&mut stripe_buf[filled..])?;
                    if n == 0 {
                        break;
                    }
                    filled += n;
                }
                if filled == 0 {
                    break;
                }
                let si = stripes_meta.len();
                let shards = yog_erasure::encode_stripe(&stripe_buf[..filled], &cfg)?;
                // Envois pipelinés : on n'attend pas la fin d'une stripe pour
                // encoder la suivante, seule la fenêtre borne la mémoire.
                for shard in &shards {
                    let owner =
                        yog_cluster::placement::shard_owner(&file_hash, si, shard.index, &view);
                    let client = clients
                        .iter()
                        .find(|c| c.addr.to_string() == owner)
                        .expect("owner vient de la liste des clients")
                        .clone();
                    let data = shard.data.clone();
                    let addr = client.addr;
                    while in_flight.len() >= MAX_IN_FLIGHT {
                        in_flight.join_next().await.unwrap()??;
                    }
                    // Une connexion tuée par la congestion ne condamne pas
                    // l'upload : reconnexion + renvoi (idempotent, le shard
                    // est content-addressed).
                    in_flight.spawn(async move {
                        if client.put_shard(data.clone()).await.is_ok() {
                            return Ok(());
                        }
                        for attempt in 1..=4u32 {
                            tokio::time::sleep(std::time::Duration::from_millis(
                                300 * attempt as u64,
                            ))
                            .await;
                            if let Ok(c) = PeerClient::connect(addr).await {
                                if c.put_shard(data.clone()).await.is_ok() {
                                    return Ok(());
                                }
                            }
                        }
                        bail!("shard non envoyé à {addr} après 5 tentatives")
                    });
                }
                stripes_meta.push(yog_erasure::StripeMeta {
                    data_len: filled,
                    shard_hashes: shards.iter().map(|s| s.hash.clone()).collect(),
                });
            }
            while let Some(j) = in_flight.join_next().await {
                j??;
            }
            let manifest = FileManifest {
                file_hash,
                file_size,
                name: file.file_name().map(|n| n.to_string_lossy().into_owned()),
                expires_at: None,
                config: cfg,
                stripes: stripes_meta,
            };
            let secs = start.elapsed().as_secs_f64();
            println!(
                "  débit: {:.0} Mo/s",
                file_size as f64 / 1_000_000.0 / secs.max(0.001)
            );
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
            use std::io::Write;
            let clients = connect_all(&peers).await?;
            let manifest = fetch_manifest(&clients, &file_hash).await?;

            // Reconstruction en streaming : une stripe en mémoire à la fois,
            // hash global vérifié au fil de l'eau.
            let mut out = std::io::BufWriter::new(std::fs::File::create(&output)?);
            let mut hasher = blake3::Hasher::new();
            for stripe in &manifest.stripes {
                let mut slots = Vec::new();
                for hash in &stripe.shard_hashes {
                    slots.push(fetch_shard(&clients, hash).await);
                }
                let data = yog_erasure::decode_stripe(slots, stripe, &manifest.config)?;
                hasher.update(&data);
                out.write_all(&data)?;
            }
            out.flush()?;
            if hasher.finalize().to_hex().to_string() != manifest.file_hash {
                bail!("intégrité violée : hash du fichier reconstruit différent du manifest");
            }
            println!(
                "reconstruit : {} octets → {} (intégrité vérifiée)",
                manifest.file_size,
                output.display()
            );
        }
    }
    Ok(())
}

/// Cycle de vie découverte d'un nœud, entièrement implicite : résoudre le
/// cluster sur la DHT et le rejoindre ; si la DHT est vierge, élection de
/// genèse (le plus petit node-id fonde le cluster — déterministe, sans
/// nœud désigné) ; puis republier les seeds tant qu'on est leader.
async fn run_discovery(
    app: Arc<yog_raft::RaftApp>,
    client: yog_discovery::pkarr::Client,
    dht_kp: yog_discovery::pkarr::Keypair,
    advertise: SocketAddr,
) {
    use std::time::{Duration, Instant};
    use yog_raft::types::{AdminRequest, AdminResponse};

    /// Cadence de scrutation de la DHT.
    const POLL: Duration = Duration::from_secs(5);
    /// Notre candidature doit rester incontestée aussi longtemps avant de
    /// fonder (laisse aux démarrages simultanés le temps de se voir).
    const GENESIS_CONFIRM: Duration = Duration::from_secs(12);
    /// Un candidat étranger qui ne fonde jamais est déclaré mort après ça.
    const FOREIGN_STALE: Duration = Duration::from_secs(45);

    let mut our_candidacy_at: Option<Instant> = None;
    let mut foreign_since: Option<(u64, Instant)> = None;

    // Phase 1 : entrer dans le cluster (sauté au redémarrage — l'état Raft
    // durable connaît déjà le membership).
    while !app.members().contains_key(&app.id) {
        // 1) Un cluster existe-t-il ?
        match yog_discovery::resolve_seeds(&client, &dht_kp.public_key()).await {
            Ok(seeds) if !seeds.is_empty() => {
                eprintln!("cluster découvert sur la DHT: {seeds:?} — adhésion…");
                let join = async {
                    match yog_raft::admin_via_leader(
                        &seeds,
                        &AdminRequest::AddLearner { id: app.id, addr: advertise.to_string() },
                    )
                    .await?
                    {
                        AdminResponse::Ok(_) => {}
                        other => bail!("add-learner: {other:?}"),
                    }
                    let members = match yog_raft::admin_via_leader(&seeds, &AdminRequest::Metrics)
                        .await?
                    {
                        AdminResponse::Metrics { members, .. } => members,
                        other => bail!("metrics: {other:?}"),
                    };
                    let mut ids: Vec<u64> = members.keys().copied().collect();
                    if !ids.contains(&app.id) {
                        ids.push(app.id);
                    }
                    match yog_raft::admin_via_leader(&seeds, &AdminRequest::ChangeMembership(ids))
                        .await?
                    {
                        AdminResponse::Ok(_) => Ok(()),
                        other => bail!("promotion: {other:?}"),
                    }
                };
                match join.await {
                    Ok(()) => {
                        eprintln!("adhésion réussie — membre votant du cluster");
                        break;
                    }
                    Err(e) => eprintln!("adhésion en échec ({e:#}), nouvel essai…"),
                }
                tokio::time::sleep(POLL).await;
                continue;
            }
            Ok(_) => {}
            Err(e) => {
                eprintln!("résolution DHT en échec ({e:#}), nouvel essai…");
                tokio::time::sleep(POLL).await;
                continue;
            }
        }

        // 2) DHT vierge : élection de genèse par candidatures signées.
        match yog_discovery::resolve_genesis_candidacy(&client, &dht_kp.public_key()).await {
            Ok(Some((cid, _))) if cid == app.id => {
                // Notre candidature est la plus récente visible.
                if our_candidacy_at.is_some_and(|t| t.elapsed() >= GENESIS_CONFIRM) {
                    let mut members = std::collections::BTreeMap::new();
                    members.insert(
                        app.id,
                        yog_raft::openraft::BasicNode { addr: advertise.to_string() },
                    );
                    match app.raft.initialize(members).await {
                        Ok(()) => {
                            eprintln!("genèse: candidature incontestée — cluster fondé");
                            break;
                        }
                        Err(e) => eprintln!("initialize: {e}"),
                    }
                }
            }
            Ok(Some((cid, _))) if cid < app.id => {
                // Un candidat prioritaire (id plus petit) : on le laisse
                // fonder — sauf s'il ne fonde jamais (crashé).
                let since = match foreign_since {
                    Some((id, t)) if id == cid => t,
                    _ => {
                        let now = Instant::now();
                        foreign_since = Some((cid, now));
                        eprintln!("genèse: candidat prioritaire {cid} vu — on attend");
                        now
                    }
                };
                if since.elapsed() >= FOREIGN_STALE {
                    eprintln!("genèse: candidat {cid} silencieux — on reprend la main");
                    if publish_candidacy(&client, &dht_kp, &app, advertise).await {
                        our_candidacy_at = Some(Instant::now());
                        foreign_since = None;
                    }
                }
            }
            Ok(Some((cid, _))) => {
                // Candidat moins prioritaire : notre id est plus petit, on
                // (re)publie — il nous verra et s'inclinera.
                eprintln!("genèse: candidat {cid} moins prioritaire — on publie notre candidature");
                if publish_candidacy(&client, &dht_kp, &app, advertise).await {
                    our_candidacy_at = Some(Instant::now());
                }
            }
            Ok(None) => {
                eprintln!("aucun cluster sur la DHT — candidature de genèse");
                if publish_candidacy(&client, &dht_kp, &app, advertise).await {
                    our_candidacy_at = Some(Instant::now());
                }
            }
            Err(e) => eprintln!("lecture des candidatures en échec ({e:#})"),
        }
        tokio::time::sleep(POLL).await;
    }

    // Phase 2 : battement de cœur DHT — le leader republie le membership.
    let app_pub = app.clone();
    yog_discovery::run_publisher(
        client,
        dht_kp,
        std::time::Duration::from_secs(120),
        move || {
            let metrics = app_pub.raft.metrics().borrow().clone();
            if metrics.current_leader != Some(app_pub.id) {
                return None;
            }
            Some(
                app_pub
                    .members()
                    .values()
                    .filter_map(|a| a.parse::<SocketAddr>().ok())
                    .collect(),
            )
        },
    )
    .await;
}

/// Capacité totale du système de fichiers hébergeant `path` (statvfs).
/// Repli sur la capacité par défaut si la mesure échoue.
fn filesystem_capacity(path: &std::path::Path) -> u64 {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        let c_path = match std::ffi::CString::new(path.as_os_str().as_bytes()) {
            Ok(p) => p,
            Err(_) => return yog_cluster::placement::DEFAULT_CAPACITY,
        };
        let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
        if unsafe { libc::statvfs(c_path.as_ptr(), &mut stat) } == 0 {
            return (stat.f_blocks as u64).saturating_mul(stat.f_frsize as u64);
        }
    }
    yog_cluster::placement::DEFAULT_CAPACITY
}

/// Publie notre candidature de genèse ; false si la DHT n'a pas pris.
async fn publish_candidacy(
    client: &yog_discovery::pkarr::Client,
    dht_kp: &yog_discovery::pkarr::Keypair,
    app: &Arc<yog_raft::RaftApp>,
    advertise: SocketAddr,
) -> bool {
    match yog_discovery::publish_genesis_candidacy(client, dht_kp, app.id, advertise).await {
        Ok(()) => true,
        Err(e) => {
            eprintln!("publication de candidature en échec ({e:#})");
            false
        }
    }
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
