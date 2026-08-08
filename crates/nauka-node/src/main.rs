//! Nauka — the engine binary: CLI and server.
//!
//! Ties together erasure coding, content-addressed storage, QUIC transport,
//! Raft consensus, placement/healing and DHT discovery. Also exposes the
//! HTTP API and the web UI of the Yogfile service built on top of it.

mod api;
mod cache;
mod e2e;
mod egress;
mod s3;
mod update;
mod webui;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use nauka_erasure::{decode_file, encode_file, ErasureConfig, FileManifest};
use nauka_store::ShardStore;
use nauka_transport::PeerClient;

#[derive(Parser)]
#[command(
    name = "nauka",
    about = "Nauka — distributed storage engine (erasure coding, self-healing, zero-config)"
)]
struct Cli {
    /// Data directory of the node.
    #[arg(long, default_value = "./nauka-data")]
    data_dir: PathBuf,
    /// Directory holding the cluster key (cluster-ca.key/.pem).
    /// Enables mTLS: only holders of a signed certificate get through.
    #[arg(long)]
    keys: Option<PathBuf>,
    /// Cluster token (nauka1_…): one string instead of a key directory.
    /// The cluster key is derived from it — same security, nothing to
    /// copy around. Prefer the environment variable over the flag: command
    /// lines are visible in `ps`.
    #[arg(
        long,
        env = "NAUKA_TOKEN",
        conflicts_with = "keys",
        hide_env_values = true
    )]
    token: Option<String>,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Encode a file into Reed-Solomon shards and store it.
    Put {
        file: PathBuf,
        /// k: data shards per stripe.
        #[arg(long, default_value_t = 4)]
        data_shards: usize,
        /// m: parity shards per stripe (loss tolerance).
        #[arg(long, default_value_t = 2)]
        parity_shards: usize,
    },
    /// Rebuild a file from its shards (tolerates losses/corruption).
    Get {
        file_hash: String,
        #[arg(short, long)]
        output: PathBuf,
    },
    /// Check that a file is reconstructible and intact.
    Verify { file_hash: String },
    /// List stored files.
    List,
    /// Encrypt a file (AES-256-GCM, local key) then upload it to a node.
    /// Servers see ONLY ciphertext; the printed link carries the key in
    /// its fragment (#…), which is never sent to the server.
    Upload {
        file: PathBuf,
        /// API URL of a cluster node.
        #[arg(long, default_value = "http://127.0.0.1:8080")]
        api: String,
        /// Public name (PLAINTEXT metadata server-side — omitted by default).
        #[arg(long)]
        name: Option<String>,
    },
    /// Download a full share link (with #key) and decrypt it.
    Download {
        link: String,
        #[arg(short, long)]
        output: PathBuf,
    },
    /// Generate the cluster key (Ed25519 CA) to distribute to the nodes.
    Keygen {
        #[arg(long, default_value = "./nauka-keys")]
        out: PathBuf,
    },
    /// Print this node's identity (node-id derived from its public key).
    NodeInfo,
    /// Generate a cluster token: the one string every machine needs.
    Token,
    /// Create a set of S3 credentials (prints the secret once).
    S3KeyCreate {
        /// Label to recognize the key later.
        #[arg(long)]
        name: Option<String>,
        /// Canonical user id shown in ACLs and matched by bucket-policy
        /// principals. Defaults to the access key id.
        #[arg(long)]
        user_id: Option<String>,
        /// Use these exact credentials instead of generating a pair.
        /// For reproducible setups (conformance CI, fixed dev keys);
        /// both must be given together.
        #[arg(long, requires = "secret_key")]
        access_key: Option<String>,
        #[arg(long, requires = "access_key")]
        secret_key: Option<String>,
        #[arg(long, default_value = "127.0.0.1:7311")]
        peer: SocketAddr,
    },
    /// List the S3 access keys (never the secrets).
    S3KeyList {
        #[arg(long, default_value = "127.0.0.1:7311")]
        peer: SocketAddr,
    },
    /// Revoke a set of S3 credentials.
    S3KeyDelete {
        access_key_id: String,
        #[arg(long, default_value = "127.0.0.1:7311")]
        peer: SocketAddr,
    },
    /// Update this binary to the latest release (checksum verified).
    Update {
        /// Only report whether an update exists, without installing it.
        #[arg(long)]
        check: bool,
    },
    /// Start the node in QUIC server mode (cluster if --peers is given).
    /// In consensus mode (--node-id), port+1 is reserved for the Raft
    /// plane: several nodes on one host must space their ports by 2.
    Serve {
        #[arg(long, default_value = "0.0.0.0:7311")]
        listen: SocketAddr,
        /// Address advertised to the other nodes (default: listen address).
        #[arg(long)]
        advertise: Option<SocketAddr>,
        /// The other cluster nodes. Enables heartbeats + auto-healing.
        #[arg(long, value_delimiter = ',')]
        peers: Vec<SocketAddr>,
        /// Auto-healing scrub interval, in seconds.
        #[arg(long, default_value_t = 30)]
        scrub_interval: u64,
        /// Raft identifier of this node. Enables consensus mode:
        /// membership and the file registry are replicated by Raft
        /// (the --peers list becomes useless for healing).
        #[arg(long)]
        node_id: Option<u64>,
        /// Storage capacity of this node in bytes (weight in weighted
        /// placement). Default: size of the data-dir filesystem.
        #[arg(long)]
        capacity: Option<u64>,
        /// Monthly egress budget of this node — plain bytes or a human
        /// size ("500GB", "20TB", "1TiB"). Reads prefer pulling shards
        /// from nodes with budget to spare; a node past its budget is
        /// deprioritized, never refused. Unset = unmetered.
        #[arg(long, env = "NAUKA_EGRESS_QUOTA")]
        egress_quota: Option<String>,
        /// Disk budget for the stripe cache — decoded stripes that
        /// crossed the cluster once are then served from local disk.
        /// Content-addressed, so entries never go stale; LRU eviction.
        /// Unset = cache disabled.
        #[arg(long, env = "NAUKA_CACHE_SIZE")]
        cache_size: Option<String>,
        /// Address of the public HTTP API (upload/download).
        #[arg(long, default_value = "0.0.0.0:8080")]
        http: SocketAddr,
        /// Serve the web UI from this directory instead of the one built
        /// into the binary (front-end development).
        #[arg(long)]
        webui: Option<PathBuf>,
        /// Address of the S3-compatible endpoint.
        #[arg(long, default_value = "0.0.0.0:8333")]
        s3: SocketAddr,
        /// Disable the S3 endpoint.
        #[arg(long)]
        no_s3: bool,
        /// Disable the HTTP API.
        #[arg(long)]
        no_http: bool,
        /// Disable DHT discovery (implied as soon as --keys is given
        /// without --peers): static / air-gapped cluster.
        #[arg(long)]
        no_discover: bool,
        /// Alternate DHT bootstrap nodes (tests against a local DHT).
        #[arg(long, value_delimiter = ',', hide = true)]
        dht_bootstrap: Vec<String>,
    },
    /// Initialize the Raft cluster (once only, from any node).
    ClusterInit {
        /// Members as id@host:port, e.g. 1@10.0.0.1:7311 2@10.0.0.2:7311
        #[arg(required = true)]
        members: Vec<String>,
    },
    /// Ban a file: removed from the registry, refused on download (410)
    /// and purged by the GC. To honor a report or a legal request
    /// without reading the content.
    Ban {
        file_hash: String,
        /// Reason recorded in the registry (report reference…).
        #[arg(long, default_value = "report")]
        reason: String,
        #[arg(long, value_delimiter = ',', required = true)]
        peers: Vec<SocketAddr>,
    },
    /// Lift a ban.
    Unban {
        file_hash: String,
        #[arg(long, value_delimiter = ',', required = true)]
        peers: Vec<SocketAddr>,
    },
    /// Print the Raft cluster state as seen by a node.
    ClusterMetrics {
        #[arg(long, default_value = "127.0.0.1:7311")]
        peer: SocketAddr,
    },
    /// Add a node to the cluster live (learner → voter). The new node
    /// must already be running (serve --node-id <id>). Shard
    /// rebalancing follows automatically (scrub + GC).
    ClusterAdd {
        /// The node to add, as id@host:port.
        member: String,
        /// Any current nodes of the cluster.
        #[arg(long, value_delimiter = ',', required = true)]
        peers: Vec<SocketAddr>,
    },
    /// Remove a node from the cluster live. Its shards are re-replicated
    /// by the other nodes' scrubbers; it can then be shut down.
    ClusterRemove {
        node_id: u64,
        #[arg(long, value_delimiter = ',', required = true)]
        peers: Vec<SocketAddr>,
    },
    /// Encode a file and dispatch its shards across peers (round-robin).
    PutRemote {
        file: PathBuf,
        /// Cluster node addresses, e.g. 10.0.0.1:7311,10.0.0.2:7311
        #[arg(long, value_delimiter = ',', required = true)]
        peers: Vec<SocketAddr>,
        #[arg(long, default_value_t = 4)]
        data_shards: usize,
        #[arg(long, default_value_t = 2)]
        parity_shards: usize,
    },
    /// Rebuild a file by reading its shards from peers.
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
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let mut cli = Cli::parse();
    if let Cmd::Token = cli.cmd {
        // Straight to stdout and nothing else: pipeable into a secret
        // store. The reminder goes to stderr.
        println!("{}", nauka_transport::generate_token());
        eprintln!("# every machine joins with: NAUKA_TOKEN=<token> nauka serve");
        eprintln!(
            "# anyone holding this token is a member of the cluster — treat it like a password"
        );
        return Ok(());
    }
    // A token is sugar over the key directory: derive the key material into
    // a private corner of the data dir, then follow the exact same paths as
    // --keys. One trust model, two spellings.
    if let Some(token) = cli.token.clone() {
        let dir = cli.data_dir.join("token-keys");
        nauka_transport::materialize_token_keys(&token, &dir)
            .context("deriving the cluster key from the token")?;
        cli.keys = Some(dir);
    }

    // Cluster identity: to be installed before any network use. A node
    // (serve/node-info) uses its persisted key; client commands use an
    // ephemeral identity signed by the same CA.
    let node_tls = if let Some(keys_dir) = &cli.keys {
        let identity = match &cli.cmd {
            Cmd::Serve { .. } | Cmd::NodeInfo => Some(cli.data_dir.join("node.key")),
            _ => None,
        };
        let tls = nauka_transport::load_cluster_tls(keys_dir, identity.as_deref())?;
        let info = (tls.node_id, tls.fingerprint.clone());
        nauka_transport::set_cluster_tls(tls);
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
            nauka_transport::generate_cluster_ca(&out)?;
            println!("cluster key generated in {}", out.display());
            println!(
                "  copy it to every node, then: serve --keys {}",
                out.display()
            );
        }
        Cmd::NodeInfo => {
            let (node_id, fingerprint) = node_tls.context("node-info requires --keys <dir>")?;
            println!("node-id     : {node_id}");
            println!("fingerprint : {fingerprint}");
        }
        Cmd::S3KeyCreate {
            name,
            user_id,
            access_key,
            secret_key,
            peer,
        } => {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            let cred = match (access_key, secret_key) {
                (Some(ak), Some(sk)) => nauka_s3::Credential {
                    access_key_id: ak,
                    secret_access_key: sk,
                    name,
                    user_id,
                    created_at: now,
                    buckets: None,
                },
                _ => nauka_s3::Credential {
                    user_id,
                    ..nauka_s3::generate_credential(name, now)
                },
            };
            // The secret is printed once and never again: the cluster keeps
            // it to verify signatures, but nothing else ever displays it.
            let secret = cred.secret_access_key.clone();
            let id = cred.access_key_id.clone();
            let resp = nauka_raft::write_via_leader(
                &[peer],
                nauka_raft::types::AppCommand::PutCredential(cred),
            )
            .await?;
            if !resp.ok {
                bail!("the cluster refused the credential");
            }
            println!("access key id     : {id}");
            println!("secret access key : {secret}");
            eprintln!();
            eprintln!("# the secret is shown once — store it now");
            eprintln!("# aws --endpoint-url http://<node>:8333 s3 ls");
        }
        Cmd::S3KeyList { peer } => {
            let client = PeerClient::connect(peer).await?;
            let state = nauka_raft::fetch_s3_state(&client).await?;
            if state.credentials.is_empty() {
                println!("no S3 credentials — create one with `nauka s3-key-create`");
            }
            for c in state.credentials.values() {
                println!(
                    "{}  {}  {}",
                    c.access_key_id,
                    c.name.as_deref().unwrap_or("-"),
                    match &c.buckets {
                        None => "full access".to_string(),
                        Some(g) => format!("{} bucket(s)", g.len()),
                    }
                );
            }
        }
        Cmd::S3KeyDelete {
            access_key_id,
            peer,
        } => {
            let resp = nauka_raft::write_via_leader(
                &[peer],
                nauka_raft::types::AppCommand::DeleteCredential {
                    access_key_id: access_key_id.clone(),
                },
            )
            .await?;
            if resp.ok {
                println!("revoked {access_key_id}");
            } else {
                bail!("unknown access key {access_key_id}");
            }
        }
        Cmd::Update { check } => update::run(check).await?,
        // Handled before the match (needs no data dir, no keys).
        Cmd::Token => unreachable!("handled at startup"),
        Cmd::Put {
            file,
            data_shards,
            parity_shards,
        } => {
            let data =
                std::fs::read(&file).with_context(|| format!("reading {}", file.display()))?;
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
            println!("stored: {}", manifest.file_hash);
            println!(
                "  {} bytes, {} stripes, {} shards ({}+{}), tolerates the loss of {} shards/stripe",
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
            println!("reconstructed: {} bytes → {}", data.len(), output.display());
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
                    "OK: intact and reconstructible ({missing}/{total} shards unavailable)"
                ),
                Err(e) => bail!("UNRECOVERABLE ({missing}/{total} shards unavailable): {e}"),
            }
        }
        Cmd::List => {
            for hash in store.list_manifests()? {
                let m = store.get_manifest(&hash)?;
                println!("{hash}  {} bytes", m.file_size);
            }
        }
        Cmd::Serve {
            listen,
            advertise,
            peers,
            scrub_interval,
            node_id,
            capacity,
            egress_quota,
            cache_size,
            http,
            s3: s3_addr,
            no_s3,
            webui,
            no_http,
            no_discover,
            dht_bootstrap,
        } => {
            // Implicit discovery: cluster keys present, no static list, no
            // opt-out → the node figures everything out on its own.
            let discover = cli.keys.is_some() && peers.is_empty() && !no_discover;
            // The monthly egress budget: refuse to start on a value we
            // cannot read rather than silently serving unmetered.
            let egress_quota = match &egress_quota {
                Some(raw) => Some(egress::parse_size(raw).with_context(|| {
                    format!("unreadable egress quota {raw:?} (try \"500GB\", \"20TB\", \"1TiB\")")
                })?),
                None => None,
            };
            let cache_budget = match &cache_size {
                Some(raw) => Some(egress::parse_size(raw).with_context(|| {
                    format!("unreadable cache size {raw:?} (try \"10GB\", \"512MiB\")")
                })?),
                None => None,
            };
            let store = Arc::new(store);
            let interval = std::time::Duration::from_secs(scrub_interval);
            let mut raft_handler: Option<Arc<dyn nauka_transport::server::RaftHandler>> = None;

            // With a crypto identity, the node-id is PROVEN (derived from
            // the public key) instead of being self-declared.
            let node_id = match (&node_tls, node_id) {
                (Some((derived, fp)), cli_id) => {
                    if let Some(cli_id) = cli_id {
                        if cli_id != *derived {
                            eprintln!(
                                "--node-id {cli_id} ignored: the crypto identity imposes {derived} \
                                 (fingerprint {})",
                                &fp[..16]
                            );
                        }
                    }
                    println!("identity: node-id {derived} (fingerprint {})", &fp[..16]);
                    Some(*derived)
                }
                (None, id) => id,
            };

            let boots: Option<Vec<String>> = if dht_bootstrap.is_empty() {
                None
            } else {
                Some(dht_bootstrap.clone())
            };

            // Address advertised to the other nodes: explicit
            // (--advertise), otherwise auto-detected through the DHT in
            // discovery mode, otherwise the listen address.
            let advertise_addr = match advertise {
                Some(a) => a,
                None if discover => {
                    match nauka_discovery::detect_public_ip(boots.as_deref()).await {
                        Ok(Some(ip)) => {
                            let a = SocketAddr::new(ip, listen.port());
                            println!(
                                "public IP detected through the DHT: {ip} — advertised address {a} \
                                 (port {} and port {} must be reachable over UDP)",
                                listen.port(),
                                listen.port() + 1
                            );
                            a
                        }
                        Ok(None) => {
                            eprintln!(
                                "public IP undetectable through the DHT — falling back to the \
                                 listen address {listen} (use --advertise if it is not reachable \
                                 by the other nodes)"
                            );
                            listen
                        }
                        Err(e) => {
                            eprintln!(
                                "public IP detection failed ({e:#}) — falling back to {listen}"
                            );
                            listen
                        }
                    }
                }
                None => listen,
            };

            if let Some(id) = node_id {
                // Consensus mode: membership and registry come from Raft.
                let app = nauka_raft::RaftApp::start(id, &cli.data_dir.join("raft")).await?;
                raft_handler = Some(app.clone());
                let self_id = advertise_addr.to_string();

                if discover {
                    let keys_dir = cli
                        .keys
                        .clone()
                        .context("--discover requires --keys (cluster identity)")?;
                    let dht_kp = nauka_discovery::derive_dht_keypair(&keys_dir)?;
                    let client = nauka_discovery::make_client(boots.as_deref())?;
                    tokio::spawn(run_discovery(app.clone(), client, dht_kp, advertise_addr));
                }

                // Liveness map: a light pinger probes every member on the
                // data plane; placement (uploads, scrub targets) only
                // routes at peers that answer. Membership itself — votes,
                // identity — is untouched.
                let health = Arc::new(nauka_cluster::health::PeerHealth::default());
                {
                    let health = health.clone();
                    let app = app.clone();
                    let self_id = self_id.clone();
                    tokio::spawn(async move {
                        let mut tick = tokio::time::interval(std::time::Duration::from_secs(5));
                        loop {
                            tick.tick().await;
                            for (peer, _) in
                                app.weighted_view(nauka_cluster::placement::DEFAULT_CAPACITY)
                            {
                                if peer == self_id {
                                    continue;
                                }
                                let Ok(addr) = peer.parse::<std::net::SocketAddr>() else {
                                    continue;
                                };
                                let alive = match tokio::time::timeout(
                                    std::time::Duration::from_secs(2),
                                    nauka_transport::PeerClient::connect(addr),
                                )
                                .await
                                {
                                    Ok(Ok(c)) => c.ping().await.is_ok(),
                                    _ => false,
                                };
                                if alive {
                                    health.record_success(&peer);
                                } else {
                                    health.record_miss(&peer);
                                }
                            }
                        }
                    });
                }

                // Shared by the HTTP API and the S3 endpoint: same engine,
                // two front doors.
                let now_secs = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                let api_state = Arc::new(api::ApiState {
                    store: store.clone(),
                    app: app.clone(),
                    self_id: self_id.clone(),
                    config: ErasureConfig::default(),
                    tmp_dir: cli.data_dir.join("tmp"),
                    health: health.clone(),
                    egress: Arc::new(egress::EgressMeter::new(egress_quota, now_secs)),
                    cache: match cache_budget {
                        Some(budget) => Some(Arc::new(
                            cache::StripeCache::open(cli.data_dir.join("cache"), budget)
                                .context("opening the stripe cache")?,
                        )),
                        None => None,
                    },
                });

                if !no_http {
                    // No fallback to ./webui/dist: the binary carries its own
                    // UI, so behaviour no longer depends on the directory the
                    // node happens to be started from.
                    let webui_dir = webui;
                    let state = api_state.clone();
                    tokio::spawn(async move {
                        if let Err(e) = api::serve_http(http, state, webui_dir).await {
                            eprintln!("HTTP API stopped: {e:#}");
                        }
                    });
                }

                if !no_s3 {
                    let state = api_state.clone();
                    tokio::spawn(async move {
                        if let Err(e) = s3::serve(s3_addr, state).await {
                            eprintln!("S3 endpoint stopped: {e:#}");
                        }
                    });
                }
                let store_bg = store.clone();
                let data_dir_bg = cli.data_dir.clone();
                let health_bg = health.clone();
                let meter_bg = api_state.egress.clone();
                let cache_bg = api_state.cache.clone();
                tokio::spawn(async move {
                    let mut ticker = tokio::time::interval(interval);
                    let mut declared_capacity: Option<u64> = None;
                    let mut published_egress: Option<(String, u64)> = None;
                    let mut my_coord = nauka_cluster::vivaldi::Coord::default();
                    loop {
                        ticker.tick().await;
                        // Declare this node's capacity in the replicated
                        // state (placement weight) — on the first tick,
                        // then whenever it moves by more than 1%.
                        if app.members().contains_key(&app.id) {
                            let cap = capacity.unwrap_or_else(|| filesystem_capacity(&data_dir_bg));
                            let changed = match declared_capacity {
                                None => true,
                                Some(prev) => {
                                    (cap as i128 - prev as i128).unsigned_abs()
                                        > (prev as u128) / 100
                                }
                            };
                            if changed {
                                match app
                                    .write(nauka_raft::types::AppCommand::UpdateNodeStats {
                                        addr: self_id.clone(),
                                        capacity_bytes: cap,
                                    })
                                    .await
                                {
                                    Ok(_) => {
                                        eprintln!("capacity declared: {:.1} GB", cap as f64 / 1e9);
                                        declared_capacity = Some(cap);
                                    }
                                    Err(e) => eprintln!("capacity declaration failed: {e:#}"),
                                }
                            }
                        }
                        // Publish the monthly egress ledger. On the first
                        // tick, adopt what the replicated state remembers
                        // of this node — a mid-month restart must not zero
                        // the ledger. Then re-publish when the counter has
                        // moved enough, or the month rolled over.
                        if app.members().contains_key(&app.id) {
                            let now = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs();
                            if published_egress.is_none() {
                                if let Some(rec) = app.app_state().node_egress.get(&self_id) {
                                    meter_bg.seed(rec, now);
                                }
                            }
                            let (month, served) = meter_bg.snapshot(now);
                            const REPUBLISH_DELTA: u64 = 256 * 1024 * 1024;
                            let due = match &published_egress {
                                None => true,
                                Some((m, b)) => {
                                    *m != month || served.saturating_sub(*b) >= REPUBLISH_DELTA
                                }
                            };
                            if due {
                                match app
                                    .write(nauka_raft::types::AppCommand::UpdateNodeEgress {
                                        addr: self_id.clone(),
                                        egress: nauka_raft::types::NodeEgress {
                                            month: month.clone(),
                                            served_bytes: served,
                                            quota_bytes: meter_bg.quota(),
                                        },
                                    })
                                    .await
                                {
                                    Ok(_) => {
                                        if let Some(q) = meter_bg.quota() {
                                            eprintln!(
                                                "egress declared: {:.2} GB / {:.2} GB for {month}",
                                                served as f64 / 1e9,
                                                q as f64 / 1e9
                                            );
                                        }
                                        published_egress = Some((month, served));
                                    }
                                    Err(e) => eprintln!("egress declaration failed: {e:#}"),
                                }
                            }
                        }
                        // The replicated registry is the source of truth:
                        // materialize locally the manifests this node does
                        // not know yet, then scrub.
                        let state = app.app_state();
                        for manifest in state.manifests.values() {
                            if store_bg.get_manifest(&manifest.file_hash).is_err() {
                                let _ = store_bg.put_manifest(manifest);
                            }
                        }
                        // The stripe cache follows the registry the same
                        // way the shard GC does: entries of deleted or
                        // banned content are purged.
                        if let Some(cache) = &cache_bg {
                            let live: std::collections::HashSet<String> = state
                                .manifests
                                .values()
                                .flat_map(cache::StripeCache::keys_of)
                                .collect();
                            cache.sweep(&live);
                        }

                        // Expiration: the leader drops from the registry
                        // the files whose TTL has elapsed (once for the
                        // whole cluster, replication does the rest).
                        let is_leader = app.raft.metrics().borrow().current_leader == Some(app.id);
                        if is_leader {
                            let now = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs();
                            for m in state.manifests.values() {
                                if m.expires_at.is_some_and(|e| e <= now) {
                                    match app
                                        .write(nauka_raft::types::AppCommand::UnregisterManifest {
                                            file_hash: m.file_hash.clone(),
                                        })
                                        .await
                                    {
                                        Ok(_) => eprintln!("expired: {}", m.file_hash),
                                        Err(e) => eprintln!("expiration failed: {e:#}"),
                                    }
                                }
                            }
                        }

                        // Local purge: manifests absent from the registry
                        // and shards no live file references any more.
                        // `registry_ready` keeps a freshly started node,
                        // whose registry is still empty, from wiping all.
                        let live: std::collections::BTreeSet<String> =
                            app.app_state().manifests.keys().cloned().collect();
                        let registry_ready = app.members().contains_key(&app.id)
                            && app.raft.metrics().borrow().current_leader.is_some();
                        match nauka_cluster::healer::purge_deleted(
                            &store_bg,
                            &live,
                            registry_ready,
                            nauka_cluster::healer::ORPHAN_GRACE,
                        ) {
                            Ok(p) if p.manifests_purged > 0 || p.orphans_purged > 0 => {
                                eprintln!(
                                    "purge: {} manifest(s), {} orphan shard(s)",
                                    p.manifests_purged, p.orphans_purged
                                );
                            }
                            Ok(_) => {}
                            Err(e) => eprintln!("purge failed: {e}"),
                        }
                        let members = app.members();
                        if members.len() < 2 || !members.values().any(|a| *a == self_id) {
                            continue;
                        }
                        let nodes = app.weighted_view(nauka_cluster::placement::DEFAULT_CAPACITY);

                        // Network coordinates: measure the RTTs to the
                        // peers, adjust our Vivaldi position, and publish
                        // it if it moved noticeably. Placement uses it to
                        // spread a stripe's shards geographically.
                        let known = app.coords();
                        for (peer, _) in nodes.iter().filter(|(n, _)| *n != self_id) {
                            let Ok(addr) = peer.parse::<SocketAddr>() else {
                                continue;
                            };
                            let t0 = std::time::Instant::now();
                            let ok = match nauka_transport::PeerClient::connect(addr).await {
                                Ok(c) => c.ping().await.is_ok(),
                                Err(_) => false,
                            };
                            // Free liveness signal: this loop already pings
                            // everyone for the Vivaldi coordinates.
                            if ok {
                                health_bg.record_success(peer);
                            } else {
                                health_bg.record_miss(peer);
                            }
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
                                .write(nauka_raft::types::AppCommand::UpdateNodeCoord {
                                    addr: self_id.clone(),
                                    coord: my_coord,
                                })
                                .await;
                        }
                        let coords = app.coords();

                        // The scrubber repairs towards the LIVE view: with
                        // a member down, its shards become the living
                        // nodes' responsibility and redundancy climbs back
                        // during the outage. The rebalancing GC and the
                        // audit below keep the FULL view on purpose — a
                        // liveness flap must never be a reason to release
                        // a shard.
                        let nodes_live = health_bg.filter_view(nodes.clone());
                        match nauka_cluster::healer::scrub_once_geo(
                            &store_bg,
                            &self_id,
                            &nodes_live,
                            &coords,
                        )
                        .await
                        {
                            Ok(r) if r.shards_healed > 0 || r.shards_unrecoverable > 0 => {
                                eprintln!(
                                    "scrub: {} checked, {} regenerated, {} unrecoverable",
                                    r.shards_checked, r.shards_healed, r.shards_unrecoverable
                                );
                            }
                            Ok(_) => {}
                            Err(e) => eprintln!("scrub failed: {e}"),
                        }
                        // Rebalancing: release what no longer belongs to
                        // us (once the owner has confirmed it holds it).
                        match nauka_cluster::healer::gc_once_geo(
                            &store_bg, &self_id, &nodes, &coords,
                        )
                        .await
                        {
                            Ok(g) if g.shards_released > 0 => {
                                eprintln!("gc: {} shards released", g.shards_released);
                            }
                            Ok(_) => {}
                            Err(e) => eprintln!("gc failed: {e}"),
                        }
                        // Attestation: do the peers really hold what they
                        // claim? (sampled, negligible cost)
                        match nauka_cluster::audit::audit_once_geo(
                            &store_bg, &self_id, &nodes, &coords,
                        )
                        .await
                        {
                            Ok(a) if a.failed > 0 => eprintln!(
                                "AUDIT: {} invalid proof(s) out of {} challenges — \
                                 a peer does not hold what it claims",
                                a.failed, a.challenged
                            ),
                            Ok(_) => {}
                            Err(e) => eprintln!("audit failed: {e}"),
                        }
                    }
                });
            } else if !peers.is_empty() {
                // Static mode (no consensus): cluster view from config.
                let view = nauka_cluster::ClusterView::new(advertise_addr, &peers);
                tokio::spawn(nauka_cluster::run_background(store.clone(), view, interval));
            }
            nauka_transport::serve(store, listen, raft_handler).await?;
        }
        Cmd::ClusterInit { members } => {
            let mut map = std::collections::BTreeMap::new();
            for m in &members {
                let (id, addr) = m
                    .split_once('@')
                    .with_context(|| format!("expected format id@host:port, got {m}"))?;
                map.insert(id.parse::<u64>()?, addr.to_string());
            }
            // Pre-flight: every member must answer on BOTH planes, and the
            // node-id answering on the consensus plane must be the right
            // one — this catches dead nodes and port collisions (co-hosted
            // nodes whose ports are not spaced by at least 2).
            for (id, addr_str) in &map {
                let addr: SocketAddr = addr_str.parse()?;
                let data = PeerClient::connect(addr)
                    .await
                    .with_context(|| format!("node {id}: data plane {addr} unreachable"))?;
                data.ping()
                    .await
                    .with_context(|| format!("node {id}: data plane {addr} does not answer"))?;
                let cons_addr = nauka_transport::consensus_addr(addr);
                let cons = PeerClient::connect_consensus(cons_addr)
                    .await
                    .with_context(|| {
                        format!("node {id}: consensus plane {cons_addr} unreachable")
                    })?;
                match nauka_raft::admin_call(&cons, &nauka_raft::types::AdminRequest::Metrics).await
                {
                    Ok(nauka_raft::types::AdminResponse::Metrics { id: got, .. }) if got == *id => {
                    }
                    Ok(nauka_raft::types::AdminResponse::Metrics { id: got, .. }) => bail!(
                        "port collision: {cons_addr} answers with node-id {got} instead of \
                         {id} — space the ports by at least 2 on the same host"
                    ),
                    other => bail!("node {id}: unexpected consensus response: {other:?}"),
                }
            }
            let first: SocketAddr = map.values().next().unwrap().parse()?;
            let client = PeerClient::connect(first).await?;
            match nauka_raft::admin_call(&client, &nauka_raft::types::AdminRequest::Init(map))
                .await?
            {
                nauka_raft::types::AdminResponse::Ok(_) => println!("cluster initialized"),
                other => bail!("init failed: {other:?}"),
            }
        }
        Cmd::Ban {
            file_hash,
            reason,
            peers,
        } => {
            let resp = nauka_raft::write_via_leader(
                &peers,
                nauka_raft::types::AppCommand::BanHash {
                    file_hash: file_hash.clone(),
                    reason: reason.clone(),
                },
            )
            .await?;
            if resp.ok {
                println!("banned: {file_hash} ({reason})");
                println!(
                    "  removed from the registry, refused with 410, shards purged at the next GC"
                );
            } else {
                bail!("ban refused");
            }
        }
        Cmd::Unban { file_hash, peers } => {
            let resp = nauka_raft::write_via_leader(
                &peers,
                nauka_raft::types::AppCommand::UnbanHash {
                    file_hash: file_hash.clone(),
                },
            )
            .await?;
            if resp.ok {
                println!("ban lifted: {file_hash}");
            } else {
                println!("this hash was not banned");
            }
        }
        Cmd::ClusterMetrics { peer } => {
            let client = PeerClient::connect(peer).await?;
            match nauka_raft::admin_call(&client, &nauka_raft::types::AdminRequest::Metrics).await?
            {
                nauka_raft::types::AdminResponse::Metrics {
                    id,
                    leader,
                    members,
                    last_applied,
                    capacities,
                } => {
                    println!("node {id} — leader: {leader:?}, applied log: {last_applied:?}");
                    for (id, addr) in members {
                        match capacities.get(&addr) {
                            Some(cap) => println!(
                                "  member {id} @ {addr} — capacity {:.1} GB",
                                *cap as f64 / 1e9
                            ),
                            None => println!("  member {id} @ {addr} — capacity not declared"),
                        }
                    }
                }
                other => bail!("unexpected response: {other:?}"),
            }
        }
        Cmd::ClusterAdd { member, peers } => {
            use nauka_raft::types::{AdminRequest, AdminResponse};
            let (id, addr) = member
                .split_once('@')
                .with_context(|| format!("expected format id@host:port, got {member}"))?;
            let id: u64 = id.parse()?;
            // 1. Learner: the node catches up on the log/snapshot without voting.
            match nauka_raft::admin_via_leader(
                &peers,
                &AdminRequest::AddLearner {
                    id,
                    addr: addr.to_string(),
                },
            )
            .await?
            {
                AdminResponse::Ok(_) => println!("node {id} added as a learner"),
                other => bail!("add-learner: {other:?}"),
            }
            // 2. Promotion to voter: membership = current members + it.
            let current = match nauka_raft::admin_via_leader(&peers, &AdminRequest::Metrics).await?
            {
                AdminResponse::Metrics { members, .. } => members,
                other => bail!("metrics: {other:?}"),
            };
            let mut ids: Vec<u64> = current.keys().copied().collect();
            if !ids.contains(&id) {
                ids.push(id);
            }
            match nauka_raft::admin_via_leader(&peers, &AdminRequest::ChangeMembership(ids)).await?
            {
                AdminResponse::Ok(_) => {
                    println!(
                        "node {id} promoted to voter — rebalancing will follow across the scrubs"
                    )
                }
                other => bail!("change-membership: {other:?}"),
            }
        }
        Cmd::ClusterRemove { node_id, peers } => {
            use nauka_raft::types::{AdminRequest, AdminResponse};
            let current = match nauka_raft::admin_via_leader(&peers, &AdminRequest::Metrics).await?
            {
                AdminResponse::Metrics { members, .. } => members,
                other => bail!("metrics: {other:?}"),
            };
            let ids: Vec<u64> = current.keys().copied().filter(|i| *i != node_id).collect();
            if ids.len() == current.len() {
                bail!("node {node_id} is not a member of the cluster");
            }
            match nauka_raft::admin_via_leader(&peers, &AdminRequest::ChangeMembership(ids)).await?
            {
                AdminResponse::Ok(_) => println!(
                    "node {node_id} removed — leave it running long enough for the scrubs \
                     to re-replicate its shards, then shut it down"
                ),
                other => bail!("change-membership: {other:?}"),
            }
        }
        Cmd::PutRemote {
            file,
            peers,
            data_shards,
            parity_shards,
        } => {
            use std::io::Read;
            let cfg = ErasureConfig {
                data_shards,
                parity_shards,
                ..ErasureConfig::default()
            };
            let file_size = std::fs::metadata(&file)
                .with_context(|| format!("reading {}", file.display()))?
                .len();

            // Pass 1: hash the file in streaming (placement and the
            // manifest are keyed on this hash).
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
            // Weighted placement: capacities come from the cluster
            // Metrics (Raft mode); failing that, uniform default weights.
            let capacities = match nauka_raft::admin_via_leader(
                &peers,
                &nauka_raft::types::AdminRequest::Metrics,
            )
            .await
            {
                Ok(nauka_raft::types::AdminResponse::Metrics { capacities, .. }) => capacities,
                _ => Default::default(),
            };
            let addrs: Vec<String> = clients.iter().map(|c| c.addr.to_string()).collect();
            let mut view: Vec<(&str, u64)> = addrs
                .iter()
                .map(|a| {
                    let w = capacities
                        .get(a)
                        .copied()
                        .unwrap_or(nauka_cluster::placement::DEFAULT_CAPACITY);
                    (a.as_str(), w)
                })
                .collect();
            view.sort();

            // Pass 2: encode and dispatch stripe by stripe — memory stays
            // bounded to a single stripe whatever the file size.
            // 16 MB in flight per upload: enough to saturate a link,
            // without crushing the cluster when uploads come in bursts.
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
                let shards = nauka_erasure::encode_stripe(&stripe_buf[..filled], &cfg)?;
                // Pipelined sends: we don't wait for one stripe to land
                // before encoding the next, only the window bounds memory.
                for shard in &shards {
                    let owner =
                        nauka_cluster::placement::shard_owner(&file_hash, si, shard.index, &view);
                    let client = clients
                        .iter()
                        .find(|c| c.addr.to_string() == owner)
                        .expect("the owner comes from the client list")
                        .clone();
                    let data = shard.data.clone();
                    let addr = client.addr;
                    while in_flight.len() >= MAX_IN_FLIGHT {
                        in_flight.join_next().await.unwrap()??;
                    }
                    // A connection killed by congestion does not doom the
                    // upload: reconnect + resend (idempotent, the shard is
                    // content-addressed).
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
                        bail!("shard not sent to {addr} after 5 attempts")
                    });
                }
                stripes_meta.push(nauka_erasure::StripeMeta {
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
                "  throughput: {:.0} MB/s",
                file_size as f64 / 1_000_000.0 / secs.max(0.001)
            );
            // The manifest (metadata only) is replicated to every node —
            // until Raft is in play, each of them can rebuild.
            for client in &clients {
                client.put_manifest(&manifest).await?;
            }
            // If the cluster runs in Raft mode, also record the file in
            // the replicated registry (best effort otherwise).
            match tokio::time::timeout(
                std::time::Duration::from_secs(3),
                nauka_raft::write_via_leader(
                    &peers,
                    nauka_raft::types::AppCommand::RegisterManifest(manifest.clone()),
                ),
            )
            .await
            {
                Ok(Ok(_)) => println!("recorded in the Raft registry"),
                _ => println!("Raft registry unavailable (cluster in static mode?)"),
            }
            println!("dispatched: {}", manifest.file_hash);
            println!(
                "  {} bytes, {} stripes ({}+{}) across {} nodes",
                manifest.file_size,
                manifest.stripes.len(),
                cfg.data_shards,
                cfg.parity_shards,
                clients.len(),
            );
        }
        Cmd::GetRemote {
            file_hash,
            peers,
            output,
        } => {
            use std::io::Write;
            let clients = connect_all(&peers).await?;
            let manifest = fetch_manifest(&clients, &file_hash).await?;

            // Streaming reconstruction: one stripe in memory at a time,
            // global hash verified as we go.
            let mut out = std::io::BufWriter::new(std::fs::File::create(&output)?);
            let mut hasher = blake3::Hasher::new();
            for stripe in &manifest.stripes {
                let mut slots = Vec::new();
                for hash in &stripe.shard_hashes {
                    slots.push(fetch_shard(&clients, hash).await);
                }
                let data = nauka_erasure::decode_stripe(slots, stripe, &manifest.config)?;
                hasher.update(&data);
                out.write_all(&data)?;
            }
            out.flush()?;
            if hasher.finalize().to_hex().to_string() != manifest.file_hash {
                bail!("integrity violated: hash of the rebuilt file differs from the manifest");
            }
            println!(
                "reconstructed: {} bytes → {} (integrity verified)",
                manifest.file_size,
                output.display()
            );
        }
    }
    Ok(())
}

/// Discovery lifecycle of a node, entirely implicit: resolve the cluster on
/// the DHT and join it; if the DHT is blank, run a genesis election (the
/// smallest node-id founds the cluster — deterministic, no designated
/// node); then republish the seeds for as long as we stay leader.
async fn run_discovery(
    app: Arc<nauka_raft::RaftApp>,
    client: nauka_discovery::pkarr::Client,
    dht_kp: nauka_discovery::pkarr::Keypair,
    advertise: SocketAddr,
) {
    use nauka_raft::types::{AdminRequest, AdminResponse};
    use std::time::{Duration, Instant};

    /// DHT polling cadence.
    const POLL: Duration = Duration::from_secs(5);
    /// Our candidacy must stay uncontested this long before founding
    /// (gives simultaneous startups time to see each other).
    const GENESIS_CONFIRM: Duration = Duration::from_secs(12);
    /// A foreign candidate that never founds is declared dead after this.
    const FOREIGN_STALE: Duration = Duration::from_secs(45);

    let mut our_candidacy_at: Option<Instant> = None;
    let mut foreign_since: Option<(u64, Instant)> = None;

    // Phase 1: enter the cluster (skipped on restart — the durable Raft
    // state already knows the membership).
    while !app.members().contains_key(&app.id) {
        // 1) Does a cluster already exist?
        match nauka_discovery::resolve_seeds(&client, &dht_kp.public_key()).await {
            Ok(seeds) if !seeds.is_empty() => {
                eprintln!("cluster discovered on the DHT: {seeds:?} — joining…");
                let join = async {
                    match nauka_raft::admin_via_leader(
                        &seeds,
                        &AdminRequest::AddLearner {
                            id: app.id,
                            addr: advertise.to_string(),
                        },
                    )
                    .await?
                    {
                        AdminResponse::Ok(_) => {}
                        other => bail!("add-learner: {other:?}"),
                    }
                    let members =
                        match nauka_raft::admin_via_leader(&seeds, &AdminRequest::Metrics).await? {
                            AdminResponse::Metrics { members, .. } => members,
                            other => bail!("metrics: {other:?}"),
                        };
                    let mut ids: Vec<u64> = members.keys().copied().collect();
                    if !ids.contains(&app.id) {
                        ids.push(app.id);
                    }
                    match nauka_raft::admin_via_leader(&seeds, &AdminRequest::ChangeMembership(ids))
                        .await?
                    {
                        AdminResponse::Ok(_) => Ok(()),
                        other => bail!("promotion: {other:?}"),
                    }
                };
                match join.await {
                    Ok(()) => {
                        eprintln!("join succeeded — voting member of the cluster");
                        break;
                    }
                    Err(e) => eprintln!("join failed ({e:#}), retrying…"),
                }
                tokio::time::sleep(POLL).await;
                continue;
            }
            Ok(_) => {}
            Err(e) => {
                eprintln!("DHT resolution failed ({e:#}), retrying…");
                tokio::time::sleep(POLL).await;
                continue;
            }
        }

        // 2) Blank DHT: genesis election through signed candidacies.
        match nauka_discovery::resolve_genesis_candidacy(&client, &dht_kp.public_key()).await {
            Ok(Some((cid, _))) if cid == app.id => {
                // Our candidacy is the most recent one visible.
                if our_candidacy_at.is_some_and(|t| t.elapsed() >= GENESIS_CONFIRM) {
                    let mut members = std::collections::BTreeMap::new();
                    members.insert(
                        app.id,
                        nauka_raft::openraft::BasicNode {
                            addr: advertise.to_string(),
                        },
                    );
                    match app.raft.initialize(members).await {
                        Ok(()) => {
                            eprintln!("genesis: candidacy uncontested — cluster founded");
                            break;
                        }
                        Err(e) => eprintln!("initialize: {e}"),
                    }
                }
            }
            Ok(Some((cid, _))) if cid < app.id => {
                // A higher-priority candidate (smaller id): we let it
                // found — unless it never does (crashed).
                let since = match foreign_since {
                    Some((id, t)) if id == cid => t,
                    _ => {
                        let now = Instant::now();
                        foreign_since = Some((cid, now));
                        eprintln!("genesis: higher-priority candidate {cid} seen — waiting");
                        now
                    }
                };
                if since.elapsed() >= FOREIGN_STALE {
                    eprintln!("genesis: candidate {cid} silent — taking over");
                    if publish_candidacy(&client, &dht_kp, &app, advertise).await {
                        our_candidacy_at = Some(Instant::now());
                        foreign_since = None;
                    }
                }
            }
            Ok(Some((cid, _))) => {
                // Lower-priority candidate: our id is smaller, so we
                // (re)publish — it will see us and stand down.
                eprintln!("genesis: candidate {cid} lower priority — publishing our candidacy");
                if publish_candidacy(&client, &dht_kp, &app, advertise).await {
                    our_candidacy_at = Some(Instant::now());
                }
            }
            Ok(None) => {
                eprintln!("no cluster on the DHT — standing as genesis candidate");
                if publish_candidacy(&client, &dht_kp, &app, advertise).await {
                    our_candidacy_at = Some(Instant::now());
                }
            }
            Err(e) => eprintln!("reading the candidacies failed ({e:#})"),
        }
        tokio::time::sleep(POLL).await;
    }

    // Phase 2: DHT heartbeat — the leader republishes the membership.
    let app_pub = app.clone();
    nauka_discovery::run_publisher(
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

/// Total capacity of the filesystem hosting `path` (statvfs).
/// Falls back to the default capacity if the measurement fails.
fn filesystem_capacity(path: &std::path::Path) -> u64 {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        let c_path = match std::ffi::CString::new(path.as_os_str().as_bytes()) {
            Ok(p) => p,
            Err(_) => return nauka_cluster::placement::DEFAULT_CAPACITY,
        };
        let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
        if unsafe { libc::statvfs(c_path.as_ptr(), &mut stat) } == 0 {
            return (stat.f_blocks as u64).saturating_mul(stat.f_frsize as u64);
        }
    }
    nauka_cluster::placement::DEFAULT_CAPACITY
}

/// Publishes our genesis candidacy; false if the DHT did not take it.
async fn publish_candidacy(
    client: &nauka_discovery::pkarr::Client,
    dht_kp: &nauka_discovery::pkarr::Keypair,
    app: &Arc<nauka_raft::RaftApp>,
    advertise: SocketAddr,
) -> bool {
    match nauka_discovery::publish_genesis_candidacy(client, dht_kp, app.id, advertise).await {
        Ok(()) => true,
        Err(e) => {
            eprintln!("candidacy publication failed ({e:#})");
            false
        }
    }
}

/// Connects to the reachable peers; fails only if none of them answers.
async fn connect_all(peers: &[SocketAddr]) -> Result<Vec<PeerClient>> {
    let mut clients = Vec::new();
    for addr in peers {
        match PeerClient::connect(*addr).await {
            Ok(c) => clients.push(c),
            Err(e) => eprintln!("peer {addr} unreachable ({e}), carrying on without it"),
        }
    }
    if clients.is_empty() {
        bail!("no reachable peer");
    }
    Ok(clients)
}

async fn fetch_manifest(clients: &[PeerClient], file_hash: &str) -> Result<FileManifest> {
    for client in clients {
        if let Ok(Some(m)) = client.get_manifest(file_hash).await {
            return Ok(m);
        }
    }
    bail!("manifest {file_hash} not found on any peer");
}

/// The first peer that holds the shard wins; not found → None, and
/// Reed-Solomon will make up for it if enough shards survive.
async fn fetch_shard(clients: &[PeerClient], hash: &str) -> Option<Vec<u8>> {
    for client in clients {
        if let Ok(Some(data)) = client.get_shard(hash).await {
            return Some(data);
        }
    }
    None
}

/// Loads the available shards (missing/corrupt ones become `None`) and
/// lets Reed-Solomon rebuild.
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
