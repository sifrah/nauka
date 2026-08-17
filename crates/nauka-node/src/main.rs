//! Nauka — the engine binary: CLI and server.
//!
//! Ties together erasure coding, content-addressed storage, QUIC transport,
//! Raft consensus, and placement/healing. Exposes the HTTP API. Cluster
//! membership is managed explicitly from the CLI (`node add` /
//! `node remove`); there is no discovery layer.

mod api;
mod cache;
mod dns;
mod e2e;
mod egress;
mod ingest;
mod node;
#[cfg(feature = "s3")]
mod s3;
mod spaceauth;
mod telemetry;
mod tls;
mod top;
mod update;

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
    version,
    about = "Nauka — a distributed storage engine that heals itself",
    long_about = "Nauka — a distributed storage engine that heals itself.\n\
                  Files are split into Reed-Solomon shards (4+2 by default) spread across\n\
                  the nodes of a cluster; any 2 shards per stripe can vanish and the file\n\
                  reads back byte-identical.",
    after_help = "Quickstart:\n  \
                  curl -sSfL https://sh.getnauka.com | sh   # first machine: founds a cluster\n  \
                  nauka node add <ip>:7311                  # grows it from that machine\n  \
                  nauka status                              # who is in, who is alive\n\n\
                  Docs: https://getnauka.com"
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
// One value of this enum exists per process, parsed once at startup: the
// size imbalance clippy flags (Serve dwarfs the rest, especially once the
// s3 feature removes its variants) has no runtime cost worth boxing for.
#[allow(clippy::large_enum_variant)]
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
    /// Rebuild a file: from this machine's own store if it is there,
    /// otherwise downloaded from the cluster and BLAKE3-verified locally.
    Get {
        file_hash: String,
        #[arg(short, long)]
        output: PathBuf,
        /// HTTP API of a cluster node, for files not in the local store.
        #[arg(long, default_value = "http://127.0.0.1:8080")]
        api: String,
    },
    /// Check that a file is intact and reconstructible: locally if this
    /// machine's own store holds it, otherwise by having the cluster
    /// serve it and verifying the BLAKE3 hash end-to-end.
    Verify {
        file_hash: String,
        /// HTTP API of a cluster node, for files not in the local store.
        #[arg(long, default_value = "http://127.0.0.1:8080")]
        api: String,
    },
    /// List the cluster's files (hash, size, name). --local lists this
    /// machine's own store instead.
    List {
        /// List the local store (./nauka-data) rather than the cluster.
        #[arg(long)]
        local: bool,
        /// Print full 64-character hashes (short unique prefixes work
        /// everywhere a hash is expected).
        #[arg(long)]
        full: bool,
        /// HTTP API of a cluster node.
        #[arg(long, default_value = "http://127.0.0.1:8080")]
        api: String,
    },
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
    /// Turn this machine into the first node of a new cluster: dedicated
    /// user, cluster identity, systemd service enabled and started (the
    /// node survives reboots and restarts on failure). Root + systemd
    /// only. Grow the cluster afterwards with `nauka node add <ip>`.
    Init {
        /// Address advertised to future members (default: the
        /// default-route address, port 7311).
        #[arg(long)]
        advertise: Option<SocketAddr>,
    },
    /// Create a set of S3 credentials (prints the secret once).
    #[cfg(feature = "s3")]
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
    #[cfg(feature = "s3")]
    S3KeyList {
        #[arg(long, default_value = "127.0.0.1:7311")]
        peer: SocketAddr,
    },
    /// Revoke a set of S3 credentials.
    #[cfg(feature = "s3")]
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
    /// Run a node. A blank data dir founds a single-node cluster; grow
    /// it with `nauka node add <ip>`. The Raft plane uses port+1, so
    /// co-hosted nodes must space their `--listen` ports by at least 2.
    Serve {
        #[arg(long, default_value = "0.0.0.0:7311")]
        listen: SocketAddr,
        /// Address advertised to the other nodes (default: listen address).
        #[arg(long)]
        advertise: Option<SocketAddr>,
        /// Auto-healing scrub interval, in seconds.
        #[arg(long, default_value_t = 30)]
        scrub_interval: u64,
        /// Obsolete and ignored: the Raft node id is derived from the
        /// cluster identity (the token or key directory), not declared.
        /// Kept only so existing scripts that pass it still start.
        #[arg(long, hide = true)]
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
        /// Unset = AUTOMATIC: 10% of the free disk at startup, floored at
        /// 1GB and capped at 50GB. `0` disables the cache entirely.
        #[arg(long, env = "NAUKA_CACHE_SIZE")]
        cache_size: Option<String>,
        /// Address of the public HTTP API (upload/download).
        #[arg(long, default_value = "0.0.0.0:8080")]
        http: SocketAddr,
        /// Address of the S3-compatible endpoint.
        #[cfg(feature = "s3")]
        #[arg(long, default_value = "0.0.0.0:8333")]
        s3: SocketAddr,
        /// Disable the S3 endpoint.
        #[cfg(feature = "s3")]
        #[arg(long)]
        no_s3: bool,
        /// Disable the HTTP API.
        #[arg(long)]
        no_http: bool,
        /// Address of the Prometheus metrics endpoint. Loopback by
        /// default: the exposition describes cluster topology, node
        /// capacities and peer addresses, which have no business on a
        /// public interface. Widen it explicitly to scrape from a
        /// private network.
        #[arg(long, default_value = "127.0.0.1:9100")]
        metrics: SocketAddr,
        /// Disable the metrics endpoint. Also leaves every instrumentation
        /// site in the binary inert: the recording macros are no-ops when
        /// no recorder has been installed.
        #[arg(long)]
        no_metrics: bool,
        /// Disable the built-in geo-DNS front door (env
        /// NAUKA_NO_DNS=true|false). On by default: delegate a name to a
        /// few nodes and the cluster answers with the closest living
        /// members.
        #[arg(long, env = "NAUKA_NO_DNS")]
        no_dns: bool,
        /// Serve the API over TLS on :443 for this domain, with a
        /// Let's Encrypt certificate the node obtains and renews by
        /// itself through the cluster's own DNS. Unset = HTTP only.
        #[arg(long, env = "NAUKA_HTTPS_DOMAIN")]
        https_domain: Option<String>,
        /// Do not found a cluster on a blank data dir: wait to be added
        /// by a member. This is what `nauka node add` passes to the
        /// machines it provisions.
        #[arg(long)]
        join: bool,
    },
    /// Ban a file: removed from the registry, refused on download (410)
    /// and purged by the GC. To honor a report or a legal request
    /// without reading the content.
    Ban {
        file_hash: String,
        /// Reason recorded in the registry (report reference…).
        #[arg(long, default_value = "report")]
        reason: String,
        /// Cluster members to drive the write through.
        #[arg(long, value_delimiter = ',', default_value = "127.0.0.1:7311")]
        peers: Vec<SocketAddr>,
    },
    /// Lift a ban.
    Unban {
        file_hash: String,
        /// Cluster members to drive the write through.
        #[arg(long, value_delimiter = ',', default_value = "127.0.0.1:7311")]
        peers: Vec<SocketAddr>,
    },
    /// Live, full-screen cluster view (htop-style): per-node fill and
    /// migration rates, sparklines, the registry one keypress away.
    /// Read-only. Plain HTTP — no cluster identity needed.
    Top {
        /// HTTP API of any node (the rest are discovered from it).
        #[arg(long, default_value = "http://127.0.0.1:8080")]
        api: String,
        /// Seconds between refreshes.
        #[arg(long, default_value_t = 2)]
        interval: u64,
    },
    /// Show the cluster as this node sees it: members, leader, health,
    /// capacities, stored bytes. Reads the HTTP API — no cluster identity
    /// needed.
    Status {
        /// HTTP API address of any node.
        #[arg(long, default_value = "http://127.0.0.1:8080")]
        api: String,
        /// Print the raw JSON the node reports, for scripts.
        #[arg(long)]
        json: bool,
    },
    /// Add or remove cluster nodes.
    #[command(subcommand)]
    Node(NodeCmd),
    /// Manage organisations — the engine's clients. An organisation is an
    /// APPLICATION (a file-sharing product, a gateway…), never an end
    /// user; its record is replicated to every node.
    #[command(subcommand)]
    Org(OrgCmd),
    /// Manage storage spaces within an organisation (`org/name`). Each
    /// space carries its own policies; keep them to dozens per org —
    /// split by usage, never one per end user.
    #[command(subcommand)]
    Space(SpaceCmd),
    /// Encode a file and dispatch its shards across peers (round-robin).
    /// Niche: the raw shard path, without the HTTP API — kept for
    /// air-gapped tooling, hidden from the daily surface.
    #[command(hide = true)]
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
    /// Rebuild a file by reading its shards from peers. Niche and hidden,
    /// like put-remote.
    #[command(hide = true)]
    GetRemote {
        file_hash: String,
        #[arg(long, value_delimiter = ',', required = true)]
        peers: Vec<SocketAddr>,
        #[arg(short, long)]
        output: PathBuf,
    },
}

#[derive(Subcommand)]
enum OrgCmd {
    /// Create an organisation.
    Create {
        /// Lowercase letters, digits and dashes (1–32 chars).
        name: String,
        #[arg(long, value_delimiter = ',', default_value = "127.0.0.1:7311")]
        peers: Vec<SocketAddr>,
    },
    /// List organisations and their spaces.
    List {
        #[arg(long, value_delimiter = ',', default_value = "127.0.0.1:7311")]
        peers: Vec<SocketAddr>,
    },
    /// Suspend an organisation: every space under it goes dark on every
    /// node — reads and writes refused — until `org resume`.
    Suspend {
        name: String,
        #[arg(long, value_delimiter = ',', default_value = "127.0.0.1:7311")]
        peers: Vec<SocketAddr>,
    },
    /// Lift a suspension.
    Resume {
        name: String,
        #[arg(long, value_delimiter = ',', default_value = "127.0.0.1:7311")]
        peers: Vec<SocketAddr>,
    },
    /// Delete an organisation. Refused while it still has spaces.
    Rm {
        name: String,
        #[arg(long, value_delimiter = ',', default_value = "127.0.0.1:7311")]
        peers: Vec<SocketAddr>,
    },
    /// Change an organisation's policies.
    Set {
        name: String,
        /// Cap on the SUM of its spaces' logical bytes — or `off`.
        #[arg(long)]
        quota: Option<String>,
        #[arg(long, value_delimiter = ',', default_value = "127.0.0.1:7311")]
        peers: Vec<SocketAddr>,
    },
    /// Storage consumption of every space of the organisation.
    Usage {
        name: String,
        #[arg(long, value_delimiter = ',', default_value = "127.0.0.1:7311")]
        peers: Vec<SocketAddr>,
    },
}

#[derive(Subcommand)]
enum SpaceCmd {
    /// Create a space, e.g. `nauka space create yogfile/uploads`.
    Create {
        /// Full path `org/name` (each part: lowercase, digits, dashes).
        name: String,
        /// Serve this space's files bare, without a signed link (direct
        /// links). Default: private.
        #[arg(long)]
        public: bool,
        #[arg(long, value_delimiter = ',', default_value = "127.0.0.1:7311")]
        peers: Vec<SocketAddr>,
    },
    /// List spaces (optionally of one organisation).
    List {
        org: Option<String>,
        #[arg(long, value_delimiter = ',', default_value = "127.0.0.1:7311")]
        peers: Vec<SocketAddr>,
    },
    /// Suspend a space: reads and writes refused cluster-wide.
    Suspend {
        name: String,
        #[arg(long, value_delimiter = ',', default_value = "127.0.0.1:7311")]
        peers: Vec<SocketAddr>,
    },
    /// Lift a suspension.
    Resume {
        name: String,
        #[arg(long, value_delimiter = ',', default_value = "127.0.0.1:7311")]
        peers: Vec<SocketAddr>,
    },
    /// Delete a space.
    Rm {
        name: String,
        #[arg(long, value_delimiter = ',', default_value = "127.0.0.1:7311")]
        peers: Vec<SocketAddr>,
    },
    /// List the files a space references.
    Files {
        /// The space, e.g. yogfile/uploads.
        space: String,
        #[arg(long, value_delimiter = ',', default_value = "127.0.0.1:7311")]
        peers: Vec<SocketAddr>,
    },
    /// Manage a space's Ed25519 keys. The private key is generated on
    /// YOUR machine and never transmitted — the cluster only ever holds
    /// the public half.
    #[command(subcommand)]
    Key(SpaceKeyCmd),
    /// Reference an existing file from another space of the SAME org —
    /// no re-upload, the bytes never move. Publish a private file by
    /// targeting a public-read space; adopt an unowned legacy file by
    /// targeting the signing space itself.
    Publish {
        /// The signing space (must already reference the file — or be
        /// the target itself, for adoption).
        space: String,
        /// The file's full BLAKE3 hash.
        hash: String,
        /// Target space receiving the reference (default: the signing
        /// space — adoption).
        #[arg(long)]
        to: Option<String>,
        /// An admin private key (`nsk_…`) of the signing space.
        #[arg(long)]
        key: String,
        #[arg(long, value_delimiter = ',', default_value = "127.0.0.1:7311")]
        peers: Vec<SocketAddr>,
    },
    /// Mint a signed READ link for a file the space references — offline,
    /// no cluster round-trip. Works with `signer` and `admin` keys; the
    /// link carries its own expiry and dies with it (or with the key, or
    /// with the space).
    Link {
        /// The space, e.g. yogfile/uploads.
        space: String,
        /// The file's BLAKE3 hash (full).
        hash: String,
        /// The private key (`nsk_…`).
        #[arg(long)]
        key: String,
        /// Lifetime in seconds from now (default 15 minutes).
        #[arg(long, default_value_t = 900)]
        ttl: u64,
        /// Absolute unix expiry — overrides --ttl (long-lived links).
        #[arg(long)]
        exp: Option<u64>,
        /// Per-connection speed ceiling in bytes/s, cryptographically
        /// bound into the link (the recipient cannot remove it).
        #[arg(long)]
        rate: Option<u64>,
        /// Cap on SIMULTANEOUS connections per node, cryptographically
        /// bound into the link — closes the parallel-connection hole of
        /// --rate (download accelerators open N streams; with --conc N
        /// the real ceiling is rate x conc, whatever the client does).
        #[arg(long)]
        conc: Option<u32>,
        /// Serve the file INLINE as this content type instead of as an
        /// octet-stream attachment — the browser plays or displays it
        /// in place. Signed like the rest, and restricted to the types
        /// a browser cannot execute (no HTML, no SVG).
        #[arg(long = "content-type")]
        content_type: Option<String>,
    },
    /// Change a space's policies (read-modify-write, replicated).
    Set {
        /// The space, e.g. yogfile/uploads.
        name: String,
        /// Default per-connection speed for BARE public reads, bytes/s —
        /// or `off` to remove the ceiling. Signed links carry their own.
        #[arg(long)]
        rate_default: Option<String>,
        /// Storage cap in logical bytes (sum of referenced file sizes) —
        /// or `off`. Uploads and publishes past it are refused.
        #[arg(long)]
        quota: Option<String>,
        /// Monthly egress cap in bytes — or `off`. Past it, reads slow
        /// to a crawl instead of dying.
        #[arg(long)]
        egress_quota: Option<String>,
        #[arg(long, value_delimiter = ',', default_value = "127.0.0.1:7311")]
        peers: Vec<SocketAddr>,
    },
    /// Storage and egress consumption of a space, against its quotas.
    Usage {
        name: String,
        #[arg(long, value_delimiter = ',', default_value = "127.0.0.1:7311")]
        peers: Vec<SocketAddr>,
    },
    /// Sign a write request offline with a space's private key; prints
    /// the headers to attach (and a ready-to-paste curl). No network, no
    /// cluster round-trip — signing IS the permission.
    Sign {
        /// The space, e.g. yogfile/uploads.
        space: String,
        /// The private key (`nsk_…`) printed once by `space key add`.
        #[arg(long)]
        key: String,
        #[arg(long, default_value = "PUT")]
        method: String,
        #[arg(long, default_value = "/api/upload")]
        path: String,
        /// BLAKE3 of the exact bytes about to be uploaded. Binds the
        /// signature to the content; without it the signature only
        /// covers method/path/space/time.
        #[arg(long)]
        content_hash: Option<String>,
    },
}

#[derive(Subcommand)]
enum SpaceKeyCmd {
    /// Generate a keypair (or register a provided public key) on a space.
    /// Prints the private key ONCE — it is never stored anywhere.
    Add {
        /// The space, e.g. yogfile/uploads.
        space: String,
        /// `admin` (writes + everything) or `signer` (read links only —
        /// the role for exposed frontends).
        #[arg(long)]
        role: String,
        /// Handle for rotation (default: the role plus a random suffix).
        #[arg(long)]
        name: Option<String>,
        /// Register an externally-generated Ed25519 public key (64 hex
        /// chars) instead of generating here.
        #[arg(long)]
        public_key: Option<String>,
        #[arg(long, value_delimiter = ',', default_value = "127.0.0.1:7311")]
        peers: Vec<SocketAddr>,
    },
    /// List a space's keys.
    Ls {
        space: String,
        #[arg(long, value_delimiter = ',', default_value = "127.0.0.1:7311")]
        peers: Vec<SocketAddr>,
    },
    /// Remove a key (by name or public-key prefix). Signatures made with
    /// it die cluster-wide within one replication round-trip.
    Rm {
        space: String,
        /// The key's name, or a unique prefix of its public key.
        selector: String,
        #[arg(long, value_delimiter = ',', default_value = "127.0.0.1:7311")]
        peers: Vec<SocketAddr>,
    },
}

#[derive(Subcommand)]
enum NodeCmd {
    /// Provision a machine over SSH and add it to the cluster: install the
    /// binary and a systemd unit, hand it the cluster identity, start it,
    /// and make it a voting member. Run this on any existing member; your
    /// forwarded SSH agent key is what reaches the target.
    Add {
        /// The new node's address, host:port (the port it advertises,
        /// default plane is 7311).
        target: SocketAddr,
        /// SSH login on the target.
        #[arg(long, default_value = "root")]
        ssh_user: String,
        /// Existing cluster members to drive the join through.
        #[arg(long, value_delimiter = ',', default_value = "127.0.0.1:7311")]
        peers: Vec<SocketAddr>,
        /// Wipe the target's existing data dir instead of refusing.
        #[arg(long)]
        force: bool,
    },
    /// Drain a node WITHOUT removing it: it stays a member and keeps
    /// serving reads, but leaves the placement view — the other nodes
    /// take over its shards (proof-gated), and its store empties. Watch
    /// it in `nauka top`; when it reads 0 B, `node remove` is instant
    /// and safe.
    Disable {
        /// The member's advertised address (as shown by `nauka status`).
        target: SocketAddr,
        #[arg(long, value_delimiter = ',', default_value = "127.0.0.1:7311")]
        peers: Vec<SocketAddr>,
    },
    /// Put a drained (disabled) node back into the placement view.
    Enable {
        target: SocketAddr,
        #[arg(long, value_delimiter = ',', default_value = "127.0.0.1:7311")]
        peers: Vec<SocketAddr>,
    },
    /// Remove a node from the cluster (by node-id). Its shards are
    /// re-replicated by the others; then it can be shut down. A safety
    /// pre-flight refuses the removal if it would leave any file with
    /// fewer than k shards — override with --force.
    Remove {
        node_id: u64,
        #[arg(long, value_delimiter = ',', default_value = "127.0.0.1:7311")]
        peers: Vec<SocketAddr>,
        /// Remove even if the safety check says a file would become
        /// unrecoverable (accepts the risk of permanent data loss).
        #[arg(long)]
        force: bool,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    // The dependency graph carries two rustls crypto backends (quinn
    // brings ring, instant-acme brings aws-lc-rs); rustls refuses to
    // guess between them and panics on first use inside whichever task
    // touches TLS first. Pick one for the whole process, up front.
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
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
        eprintln!(
            "# the token IS the cluster: anyone holding it is a member — treat it like a password"
        );
        eprintln!("# found the first node:   NAUKA_TOKEN=<token> nauka init");
        eprintln!(
            "# (or by hand:            NAUKA_TOKEN=<token> nauka serve --advertise <ip>:7311)"
        );
        return Ok(());
    }
    // Also before any store or key materialization: `init` is run as root
    // and must not scatter a data dir or derived keys into root's cwd —
    // it manages /var/lib/nauka and /etc/nauka itself.
    if let Cmd::Init { advertise } = &cli.cmd {
        return node::init(node::InitOpts {
            advertise: *advertise,
            token: cli.token,
        })
        .await;
    }
    // Which commands actually speak the cluster's mTLS. Everything else —
    // status (plain HTTP), local store ops, keygen, update — must neither
    // require an identity nor leave derived key material behind.
    let needs_cluster_identity = match &cli.cmd {
        Cmd::Serve { .. }
        | Cmd::NodeInfo
        | Cmd::Node(_)
        | Cmd::Org(_)
        | Cmd::Space(_)
        | Cmd::Ban { .. }
        | Cmd::Unban { .. }
        | Cmd::PutRemote { .. }
        | Cmd::GetRemote { .. } => true,
        #[cfg(feature = "s3")]
        Cmd::S3KeyCreate { .. } | Cmd::S3KeyList { .. } | Cmd::S3KeyDelete { .. } => true,
        _ => false,
    };
    // `nauka top` speaks plain HTTP to READ, but its interactive actions
    // (disable/enable/remove) need the cluster's mTLS. So it WANTS the
    // identity if one is around — inherited from an initialized machine or
    // passed explicitly — but never REQUIRES it: absent, top runs
    // read-only and says so. The load below is fatal only for the commands
    // that truly need it.
    let is_top = matches!(&cli.cmd, Cmd::Top { .. });
    let wants_identity = needs_cluster_identity || is_top;
    if wants_identity && cli.token.is_none() && cli.keys.is_none() {
        // `nauka init` leaves the cluster identity in /etc/nauka/nauka.env;
        // speaking to the cluster from an initialized machine must not
        // require re-exporting it by hand.
        let (token, keys) = node::identity_from_env_file();
        if (token.is_some() || keys.is_some())
            && cli.data_dir == std::path::Path::new("./nauka-data")
        {
            // A command inheriting the SERVICE's identity must also speak
            // about the service's store: with the cwd default kept,
            // node-info would derive its answer from an accidental
            // ./nauka-data instead of the node everyone asks about.
            if let Some(dir) = node::service_data_dir() {
                cli.data_dir = dir;
            }
        }
        cli.token = token;
        cli.keys = keys;
    }
    // A token is sugar over the key directory: derive the key material into
    // a private corner of the data dir, then follow the exact same paths as
    // --keys. One trust model, two spellings.
    if wants_identity {
        if let Some(token) = cli.token.clone() {
            let dir = cli.data_dir.join("token-keys");
            nauka_transport::materialize_token_keys(&token, &dir)
                .context("deriving the cluster key from the token")?;
            cli.keys = Some(dir);
        }
    }

    // Whether `nauka top` may perform admin actions — set once the mTLS
    // identity is actually installed below.
    let mut top_can_admin = false;
    // Cluster identity: to be installed before any network use. A node
    // (serve/node-info) uses its persisted key; client commands use an
    // ephemeral identity signed by the same CA.
    let node_tls = if let Some(keys_dir) = cli.keys.as_ref().filter(|_| wants_identity) {
        let identity = match &cli.cmd {
            Cmd::Serve { .. } | Cmd::NodeInfo => Some(cli.data_dir.join("node.key")),
            _ => None,
        };
        match nauka_transport::load_cluster_tls(keys_dir, identity.as_deref()) {
            Ok(tls) => {
                let info = (tls.node_id, tls.fingerprint.clone());
                nauka_transport::set_cluster_tls(tls);
                top_can_admin = true;
                Some(info)
            }
            // A real admin command must fail loudly; `top` just stays
            // read-only when the identity cannot be loaded.
            Err(e) if needs_cluster_identity => return Err(e),
            Err(_) => None,
        }
    } else {
        None
    };

    match cli.cmd {
        // Dispatched before the store opens; only here for exhaustiveness.
        Cmd::Init { .. } => unreachable!("init returns before the store opens"),
        Cmd::Upload { file, api, name } => {
            e2e::upload(&api, &file, name).await?;
        }
        Cmd::Download { link, output } => {
            e2e::download(&link, &output).await?;
        }
        Cmd::Keygen { out } => {
            nauka_transport::generate_cluster_ca(&out)?;
            println!(
                "{} cluster key generated in {}",
                console::style("✓").green().bold(),
                out.display()
            );
            println!(
                "  key files instead of a token — for deployments that keep material on disk.\n  \
                 Copy the directory to each machine, then on every node:\n    \
                 nauka --keys {} serve --advertise <ip>:7311\n  \
                 (the one-string alternative: `nauka token` + `nauka init`)",
                out.display()
            );
        }
        Cmd::NodeInfo => {
            let (node_id, fingerprint) = node_tls.context(
                "node-info needs the cluster identity: set NAUKA_TOKEN, pass --keys <dir>, \
                 or run it on an initialized node (it reads /etc/nauka/nauka.env)",
            )?;
            println!("node-id     : {node_id}");
            println!("fingerprint : {fingerprint}");
        }
        #[cfg(feature = "s3")]
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
        #[cfg(feature = "s3")]
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
        #[cfg(feature = "s3")]
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
            let store = ShardStore::open(&cli.data_dir)?;
            let data =
                std::fs::read(&file).with_context(|| format!("reading {}", file.display()))?;
            let mut cfg = ErasureConfig {
                data_shards,
                parity_shards,
                ..ErasureConfig::default()
            };
            // A file that fits in one stripe gets shards sized to its
            // content — padding them to the fixed stripe size made every
            // small file cost a full stripe on disk.
            if !data.is_empty() && data.len() < cfg.stripe_data_len() {
                cfg = if data.len() <= 128 * 1024 {
                    cfg.replicated_for(data.len())
                } else {
                    cfg.densified_for(data.len())
                };
            }
            let (manifest, stripes) = encode_file(&data, &cfg)?;
            let mut shard_count = 0;
            for stripe in &stripes {
                for shard in stripe {
                    store.put_shard(&shard.data)?;
                    shard_count += 1;
                }
            }
            store.put_manifest(&manifest)?;
            println!(
                "{} stored: {}",
                console::style("✓").green().bold(),
                manifest.file_hash
            );
            println!(
                "  {} · {} stripe{} · {} shards ({}+{}) — survives the loss of any {} shards/stripe",
                indicatif::HumanBytes(manifest.file_size),
                manifest.stripes.len(),
                if manifest.stripes.len() == 1 { "" } else { "s" },
                shard_count,
                cfg.data_shards,
                cfg.parity_shards,
                cfg.parity_shards,
            );
        }
        Cmd::Get {
            file_hash,
            output,
            api,
        } => {
            // Local store first — free and offline. Only an EXISTING dir:
            // opening would create ./nauka-data in the cwd for nothing.
            let store = open_existing_store(&cli.data_dir)?;
            let file_hash = resolve_hash(&file_hash, store.as_ref(), &api).await?;
            if let Some(store) = &store {
                if let Ok(data) = reconstruct(store, &file_hash) {
                    std::fs::write(&output, &data)?;
                    println!(
                        "{} reconstructed from the local store: {} → {}",
                        console::style("✓").green().bold(),
                        indicatif::HumanBytes(data.len() as u64),
                        output.display()
                    );
                    return Ok(());
                }
            }
            let n = cluster_fetch_verified(&api, &file_hash, Some(&output))
                .await
                .with_context(|| {
                    format!(
                        "neither in the local store ({}) nor served by the node at {api}",
                        cli.data_dir.display()
                    )
                })?;
            println!(
                "{} downloaded from the cluster, BLAKE3 verified: {} → {}",
                console::style("✓").green().bold(),
                indicatif::HumanBytes(n),
                output.display()
            );
        }
        Cmd::Verify { file_hash, api } => {
            // Local store when it holds the file; otherwise the honest
            // cluster check — have a node reconstruct and serve it, and
            // verify the bytes hash back to the requested address. Same
            // guarantee, exercised over the real read path.
            let store = open_existing_store(&cli.data_dir)?;
            let file_hash = resolve_hash(&file_hash, store.as_ref(), &api).await?;
            let local = store.filter(|s| s.get_manifest(&file_hash).is_ok());
            match local {
                Some(store) => {
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
                        Err(e) => {
                            bail!("UNRECOVERABLE ({missing}/{total} shards unavailable): {e}")
                        }
                    }
                }
                None => {
                    let n = cluster_fetch_verified(&api, &file_hash, None)
                        .await
                        .with_context(|| {
                            format!(
                                "neither in the local store ({}) nor served by the node at {api}",
                                cli.data_dir.display()
                            )
                        })?;
                    println!(
                        "OK: the cluster reconstructed and served it intact \
                         ({n} bytes, BLAKE3 verified end-to-end)"
                    );
                }
            }
        }
        Cmd::List { local, full, api } => {
            // Short hashes by default: 16 characters collide on nothing
            // human-scale, every command accepts them as prefixes, and
            // the eye finds the NAME instead of drowning in hex. --full
            // restores the 64 characters for scripts.
            let cut = |h: &str| {
                if full {
                    h.to_string()
                } else {
                    h.chars().take(16).collect()
                }
            };
            if !local {
                match cluster_files(&api).await {
                    Ok(files) => {
                        let total: u64 = files.iter().map(|f| f.size).sum();
                        println!(
                            "{} — {} file{}, {}",
                            console::style("cluster").bold(),
                            files.len(),
                            if files.len() == 1 { "" } else { "s" },
                            indicatif::HumanBytes(total),
                        );
                        for f in &files {
                            println!(
                                "  {}  {:>10}  {}",
                                console::style(cut(&f.hash)).dim(),
                                indicatif::HumanBytes(f.size).to_string(),
                                f.name.as_deref().unwrap_or("—")
                            );
                        }
                        return Ok(());
                    }
                    Err(e) => {
                        eprintln!(
                            "# no node answering at {api} ({e}) — listing the local store instead"
                        );
                    }
                }
            }
            match open_existing_store(&cli.data_dir)? {
                Some(store) => {
                    eprintln!("# local store {}", cli.data_dir.display());
                    for hash in store.list_manifests()? {
                        let m = store.get_manifest(&hash)?;
                        println!(
                            "  {}  {:>10}",
                            console::style(cut(&hash)).dim(),
                            indicatif::HumanBytes(m.file_size).to_string()
                        );
                    }
                }
                None => eprintln!("# no local store at {}", cli.data_dir.display()),
            }
        }
        Cmd::Serve {
            listen,
            advertise,
            scrub_interval,
            node_id,
            capacity,
            egress_quota,
            cache_size,
            http,
            #[cfg(feature = "s3")]
                s3: s3_addr,
            #[cfg(feature = "s3")]
            no_s3,
            no_http,
            metrics: metrics_addr,
            no_metrics,
            no_dns,
            https_domain,
            join,
        } => {
            // The monthly egress budget: refuse to start on a value we
            // cannot read rather than silently serving unmetered.
            let egress_quota = match &egress_quota {
                Some(raw) => Some(egress::parse_size(raw).with_context(|| {
                    format!("unreadable egress quota {raw:?} (try \"500GB\", \"20TB\", \"1TiB\")")
                })?),
                None => None,
            };
            let cache_budget = match &cache_size {
                Some(raw) => match egress::parse_size(raw).with_context(|| {
                    format!("unreadable cache size {raw:?} (try \"10GB\", \"512MiB\", \"0\")")
                })? {
                    // `0` is the explicit off-switch now that the default
                    // is on: a knob that can only enable needs a way out.
                    0 => None,
                    n => Some(n),
                },
                // The cache pays for itself the first time a stripe is
                // read twice, and content addressing means it can never
                // serve stale bytes — there is no reason to make every
                // operator discover an env var. Auto: 10% of the free
                // disk at startup, floor 1GB (below that it thrashes),
                // cap 50GB (beyond that LRU churn outgrows the benefit).
                // The shard store stays the priority: the cache only
                // takes a slice of what is FREE after it.
                None => {
                    let free = crate::ingest::fs_available(&cli.data_dir);
                    let auto = (free / 10).clamp(1_000_000_000, 50_000_000_000);
                    if free < 2_000_000_000 {
                        // A nearly-full disk gets no cache at all rather
                        // than a starved one fighting the shards for
                        // space.
                        None
                    } else {
                        eprintln!(
                            "stripe cache: auto {:.1} GB (10% of free disk — override with \
                             NAUKA_CACHE_SIZE, 0 disables)",
                            auto as f64 / 1e9
                        );
                        Some(auto)
                    }
                }
            };
            // The neighborhood's conc-gossip landing pad, shared between
            // the ApiState (admission reads it) and the transport server
            // (peer pushes land in it) — declared here because the two
            // live in different scopes of this arm.
            #[allow(clippy::type_complexity)]
            let link_conc_remote: Arc<
                std::sync::Mutex<
                    std::collections::HashMap<
                        String,
                        (std::time::Instant, std::collections::HashMap<String, u32>),
                    >,
                >,
            > = Arc::new(Default::default());
            let stripe_cache = match cache_budget {
                Some(budget) => Some(Arc::new(
                    cache::StripeCache::open(cli.data_dir.join("cache"), budget)
                        .context("opening the stripe cache")?,
                )),
                None => None,
            };
            let store = Arc::new(ShardStore::open(&cli.data_dir)?);
            let interval = std::time::Duration::from_secs(scrub_interval);
            // The shards this node currently claims ownership of, under its
            // own placement view — refreshed once per maintenance pass,
            // served to peers through the transport's `OwnershipView` so
            // their rebalancing GC can demand an ownership claim on top of
            // the proof of possession. Empty until the first pass: every
            // claim answers `false`, and peers keep their copies — the safe
            // direction.
            let claimed_shards: Arc<std::sync::RwLock<std::collections::BTreeSet<String>>> =
                Arc::new(Default::default());
            // Assigned once, below, now that consensus is the only mode.
            let raft_handler: Option<Arc<dyn nauka_transport::server::RaftHandler>>;

            // With a crypto identity, the node-id is PROVEN (derived from
            // the public key) instead of being self-declared. --node-id is
            // therefore obsolete whenever a token or key directory is in
            // use (i.e. always, in practice): we ignore it — never crash on
            // it — and say so once, plainly.
            let node_id = match (&node_tls, node_id) {
                (Some((derived, fp)), cli_id) => {
                    if cli_id.is_some() {
                        eprintln!(
                            "note: --node-id is obsolete and ignored — the node id is derived \
                             from the cluster identity ({derived})"
                        );
                    }
                    println!("identity: node-id {derived} (fingerprint {})", &fp[..16]);
                    Some(*derived)
                }
                (None, id) => id,
            };

            // Advertised address = this node's identity (placement,
            // membership). Explicit, else the listen address; a wildcard is
            // a name nobody can dial, so warn rather than fail later in
            // ways that look like network trouble.
            let advertise_addr = advertise.unwrap_or(listen);
            if advertise_addr.ip().is_unspecified() {
                eprintln!(
                    "warning: advertised address {advertise_addr} is a wildcard — other \
                     nodes cannot reach it; pass --advertise <public-ip>:<port>"
                );
            }

            // One aligned block an operator can read at a glance —
            // journalctl's first screen answers "what is this node and
            // where does it listen" without grepping.
            eprintln!("nauka {} — serving", env!("CARGO_PKG_VERSION"));
            eprintln!("  data      : {}", cli.data_dir.display());
            eprintln!("  listen    : {listen} (consensus on port+1, UDP)");
            eprintln!("  advertise : {advertise_addr}");
            if !no_http {
                eprintln!("  http      : {http}");
            }

            // Telemetry, before any subsystem starts, so nothing that
            // happens during startup is recorded into a void. Placed ahead
            // of the consensus/static split because both modes are worth
            // observing. Like the other two front doors, a listener that
            // dies is reported and survived: losing observability must
            // never take the data plane down with it.
            if !no_metrics {
                let handle = telemetry::install()?;
                telemetry::seed(&advertise_addr.to_string());
                tokio::spawn(async move {
                    if let Err(e) = telemetry::serve(metrics_addr, handle).await {
                        eprintln!("metrics endpoint stopped: {e:#}");
                    }
                });
            }

            // One mode. A node without a cluster identity has no cluster,
            // registry or API — refuse to start with a clear remedy rather
            // than run half-alive.
            let Some(id) = node_id else {
                anyhow::bail!(
                    "serve needs a cluster identity: set NAUKA_TOKEN (from `nauka token`) \
                     or pass --keys <dir> (from `nauka keygen`)"
                );
            };
            // The doors first, the cluster second. Founding on a blank
            // data dir is IRREVERSIBLE — it writes a cluster's birth into
            // the Raft log — so every socket this node needs must be
            // provably free before it happens. A busy port must die with
            // NOTHING written; founding first then failing to bind left a
            // fully-founded 1-node cluster behind, which a later start
            // would resurrect as a fork of the deployment whose token it
            // was given. Probe-and-release: the real listeners bind a
            // moment later, and losing that benign race to another
            // process still fails the honest way, at bind time.
            {
                let probe = |addr: SocketAddr, what: &str| -> Result<()> {
                    std::net::UdpSocket::bind(addr).map(drop).with_context(|| {
                        format!("cannot bind the {what} at {addr} — port already in use?")
                    })
                };
                probe(listen, "data plane")?;
                probe(nauka_transport::consensus_addr(listen), "consensus plane")?;
                if !no_http {
                    std::net::TcpListener::bind(http)
                        .map(drop)
                        .with_context(|| {
                            format!("cannot bind the HTTP API at {http} — port already in use?")
                        })?;
                }
            }
            {
                let app = nauka_raft::RaftApp::start(id, &cli.data_dir.join("raft")).await?;
                raft_handler = Some(app.clone());
                let self_id = advertise_addr.to_string();

                // Cluster birth is explicit and race-free. A node with NO
                // persisted Raft state FOUNDS a single-node cluster; with
                // --join it waits to be added instead. A node that already
                // has state is restarting — it neither founds nor waits,
                // it just resumes (founding would crash openraft, and this
                // is exactly the case `members().is_empty()` got wrong: an
                // engine mid-reload reports no members for an instant).
                // No discovery means nobody to race, so the fork machinery
                // this replaces is gone with the fork it could produce.
                if !app.has_cluster_state().await {
                    if join {
                        let app_wait = app.clone();
                        let self_addr = advertise_addr;
                        tokio::spawn(async move {
                            let mut tick =
                                tokio::time::interval(std::time::Duration::from_secs(30));
                            loop {
                                tick.tick().await;
                                if !app_wait.members().is_empty() {
                                    tracing::info!("joined a cluster");
                                    return;
                                }
                                tracing::warn!(
                                    "waiting to be added — run `nauka node add {self_addr}` \
                                     from any cluster member"
                                );
                            }
                        });
                    } else {
                        app.found_alone(self_id.clone())
                            .await
                            .context("founding the initial cluster")?;
                        println!(
                            "founded a new cluster (this node alone) — grow it with \
                             `nauka node add <ip>`"
                        );
                    }
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
                            let mut probed: Vec<String> = Vec::new();
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
                                probed.push(peer);
                            }
                            // Published from this loop rather than from the
                            // maintenance ticker: both feed the same map,
                            // but this one runs every 5 s, and a gauge must
                            // never be staler than the fastest writer that
                            // moves it.
                            nauka_cluster::telemetry::record_peer_liveness(
                                probed.iter().map(String::as_str),
                                &health,
                            );
                        }
                    });
                }

                // Shared by the HTTP API and the S3 endpoint: same engine,
                // two front doors.
                let now_secs = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                // Both front doors stage uploads here; create it now rather
                // than in serve_http — an S3-only node (--no-http) needs it
                // too, and PUTs fail with a bare ENOENT without it.
                let tmp_dir = cli.data_dir.join("tmp");
                std::fs::create_dir_all(&tmp_dir).context("creating the upload tmp dir")?;
                let (warm_tx, warm_rx) = tokio::sync::mpsc::channel::<String>(8);
                let api_state = Arc::new(api::ApiState {
                    store: store.clone(),
                    app: app.clone(),
                    self_id: self_id.clone(),
                    node_location: std::sync::RwLock::new(None),
                    space_egress_local: Arc::new(Default::default()),
                    link_conc: Arc::new(Default::default()),
                    link_conc_remote: link_conc_remote.clone(),
                    warm_tx: stripe_cache.as_ref().map(|_| warm_tx.clone()),
                    hot_reads: Default::default(),
                    config: ErasureConfig::default(),
                    tmp_dir,
                    health: health.clone(),
                    egress: Arc::new(egress::EgressMeter::new(egress_quota, now_secs)),
                    cache: stripe_cache.clone(),
                    // An eighth of the machine for upload buffering,
                    // decided now: what uploads may hold in RAM must not
                    // depend on what happens to be free later.
                    ingest_pool: ingest::RamPool::sized_from_system(8),
                    staged_bytes: Arc::new(std::sync::atomic::AtomicU64::new(0)),
                });
                // Uploads this node acked locally but had not finished
                // dispersing when it stopped: finish them before serving.
                tokio::spawn(api::recover_staged_uploads(api_state.clone()));
                if stripe_cache.is_some() {
                    // The signal-driven warmer: publishes into public
                    // spaces and hot partial reads queue files here.
                    tokio::spawn(api::warmer_loop(api_state.clone(), warm_rx));
                } else {
                    drop(warm_rx);
                }
                let no_dns = no_dns
                    || std::env::var("NAUKA_NO_DNS").is_ok_and(|v| !v.is_empty() && v != "0");
                let mut geodns_for_gossip: Option<Arc<dns::GeoDns>> = None;
                if !no_dns {
                    // The geo-DNS front door: on by default, a bind
                    // failure only warns (nodes without the capability
                    // keep serving storage).
                    let geodns = dns::GeoDns::new(api_state.clone());
                    geodns_for_gossip = Some(geodns.clone());
                    tokio::spawn(dns::mmdb_keeper(geodns.clone(), cli.data_dir.clone()));
                    let bind_ip = self_id
                        .split(':')
                        .next()
                        .and_then(|ip| ip.parse().ok())
                        .unwrap_or(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED));
                    tokio::spawn(dns::serve(geodns, bind_ip, 53));
                }
                tokio::spawn(api::conc_gossip_loop(api_state.clone(), geodns_for_gossip));
                if let Some(domain) = https_domain.clone() {
                    // HTTPS with self-obtained certificates: the ACME
                    // challenges ride the cluster's own DNS.
                    tokio::spawn(tls::run(api_state.clone(), domain, cli.data_dir.clone()));
                }
                tracing::info!(
                    "upload buffer pool: {} MiB (fixed at startup)",
                    api_state.ingest_pool.capacity() >> 20
                );

                if !no_http {
                    let state = api_state.clone();
                    tokio::spawn(async move {
                        if let Err(e) = api::serve_http(http, state).await {
                            eprintln!("HTTP API stopped: {e:#}");
                        }
                    });
                }

                #[cfg(feature = "s3")]
                if !no_s3 {
                    let state = api_state.clone();
                    tokio::spawn(async move {
                        if let Err(e) = s3::serve(s3_addr, state).await {
                            eprintln!("S3 endpoint stopped: {e:#}");
                        }
                    });
                }
                // First capacity declaration, urgently. Placement weighs an
                // undeclared member at DEFAULT_CAPACITY (100 GiB flat), and
                // at the scrub cadence that window lasts a whole round —
                // long enough to mistarget every write toward a node whose
                // real disk may be a tenth of that. A tight loop closes the
                // window in seconds; the 1%-delta refreshes stay with the
                // scrub tick below (a second declaration of the same value
                // is idempotent).
                {
                    let app = app.clone();
                    let self_id = self_id.clone();
                    let data_dir = cli.data_dir.clone();
                    tokio::spawn(async move {
                        for _ in 0..60 {
                            if app.members().contains_key(&app.id) {
                                let cap =
                                    capacity.unwrap_or_else(|| filesystem_capacity(&data_dir));
                                if app
                                    .write(nauka_raft::types::AppCommand::UpdateNodeStats {
                                        addr: self_id.clone(),
                                        capacity_bytes: cap,
                                    })
                                    .await
                                    .is_ok()
                                {
                                    eprintln!("capacity declared: {:.1} GB", cap as f64 / 1e9);
                                    return;
                                }
                            }
                            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                        }
                    });
                }
                let store_bg = store.clone();
                let data_dir_bg = cli.data_dir.clone();
                let health_bg = health.clone();
                let meter_bg = api_state.egress.clone();
                let cache_bg = api_state.cache.clone();
                let staged_bg = api_state.staged_bytes.clone();
                let claimed_bg = claimed_shards.clone();
                let space_egress_bg = api_state.space_egress_local.clone();
                tokio::spawn(async move {
                    let mut ticker = tokio::time::interval(interval);
                    let mut declared_capacity: Option<u64> = None;
                    let mut published_egress: Option<(String, u64)> = None;
                    let mut cache_report: Option<(usize, u64, u64, u64)> = None;
                    // Seeded from the PUBLISHED position, never the origin:
                    // a process restarting at (0,0) reads as a huge drift
                    // from its own published point, republishes an
                    // unconverged coordinate, and geo placement re-decides
                    // ownership — every restart used to cost the cluster a
                    // rebalance wave (observed live, twice, before the
                    // cause was found). Starting where the cluster already
                    // believes we are makes a restart a non-event; a first
                    // boot has nothing published and starts at the origin
                    // like before.
                    let mut my_coord = app.coords().get(&self_id).copied().unwrap_or_default();
                    loop {
                        ticker.tick().await;
                        // Undispersed locally-acked bytes: the live size of
                        // the local-ack window, and what the backlog cap
                        // acts on.
                        metrics::gauge!("nauka_staged_bytes")
                            .set(staged_bg.load(std::sync::atomic::Ordering::Relaxed) as f64);
                        // Timed as a whole: if the pass outlasts the scrub
                        // interval, the ticker silently falls behind and
                        // the cluster heals less often than the operator
                        // configured. The histogram makes that visible.
                        let pass_started = std::time::Instant::now();
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
                            // Gauges, refreshed every tick regardless of
                            // the publication delta below: the ledger only
                            // replicates every 256 MiB, but the operator's
                            // view should not be that coarse. Quota only
                            // when metered — an absent series reads as
                            // "unmetered", a 0 would read as "exhausted".
                            metrics::gauge!("nauka_egress_served_bytes").set(served as f64);
                            if let Some(q) = meter_bg.quota() {
                                metrics::gauge!("nauka_egress_quota_bytes").set(q as f64);
                            }
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
                        // Fold this node's per-space egress deltas into
                        // the replicated ledger (own row only, like the
                        // node-level meter above). Base = the published
                        // row when months match, else a fresh month.
                        {
                            let deltas: Vec<(String, String, u64)> = {
                                match space_egress_bg.lock() {
                                    Ok(mut m) => {
                                        let out = m
                                            .iter()
                                            .filter(|(_, (_, b))| *b > 0)
                                            .map(|(sp, (mo, b))| (sp.clone(), mo.clone(), *b))
                                            .collect();
                                        m.clear();
                                        out
                                    }
                                    Err(_) => Vec::new(),
                                }
                            };
                            for (space, month, bytes) in deltas {
                                let base = app
                                    .app_state()
                                    .space_egress
                                    .get(&space)
                                    .and_then(|rows| rows.get(&self_id))
                                    .filter(|e| e.month == month)
                                    .map(|e| e.served_bytes)
                                    .unwrap_or(0);
                                let _ = app
                                    .write(nauka_raft::types::AppCommand::UpdateSpaceEgress {
                                        node_addr: self_id.clone(),
                                        space,
                                        egress: nauka_raft::types::NodeEgress {
                                            month,
                                            served_bytes: base + bytes,
                                            quota_bytes: None,
                                        },
                                    })
                                    .await;
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
                            // Occupancy and hit rate: the two numbers that
                            // say whether --cache-size is sized right. Only
                            // when something moved, so an idle node stays
                            // quiet.
                            let (entries, bytes) = cache.stats();
                            let (hits, misses) = cache.hit_stats();
                            // Metrics every tick — the log line below is
                            // deduplicated for humans, but a scrape must
                            // never miss the current level. The counters
                            // are absolute: the cache owns the running
                            // totals, the recorder just mirrors them.
                            metrics::gauge!("nauka_cache_entries").set(entries as f64);
                            metrics::gauge!("nauka_cache_size_bytes").set(bytes as f64);
                            metrics::gauge!("nauka_cache_budget_bytes").set(cache.budget() as f64);
                            metrics::counter!("nauka_cache_hits_total").absolute(hits);
                            metrics::counter!("nauka_cache_misses_total").absolute(misses);
                            if cache_report != Some((entries, bytes, hits, misses)) {
                                cache_report = Some((entries, bytes, hits, misses));
                                let lookups = hits + misses;
                                let rate = match lookups {
                                    0 => 0.0,
                                    n => hits as f64 * 100.0 / n as f64,
                                };
                                eprintln!(
                                    "stripe cache: {entries} entries, {:.2}/{:.2} GB, {hits} hits / {misses} misses ({rate:.0}% hit rate)",
                                    bytes as f64 / 1e9,
                                    cache.budget() as f64 / 1e9,
                                );
                            }
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
                        // Member + leader is not enough: a node that lags
                        // on the log sees a PARTIAL registry, and files it
                        // has not applied yet would read as orphans. Zero
                        // apply lag makes the registry view trustworthy.
                        let raft_metrics = app.raft.metrics().borrow().clone();
                        let registry_ready = app.members().contains_key(&app.id)
                            && raft_metrics.current_leader.is_some()
                            && raft_metrics.last_log_index
                                == raft_metrics.last_applied.map(|l| l.index);
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
                            // The MINIMUM of three pings, not one sample:
                            // a single congested ping yanks this node's
                            // coordinate by tens of ms, and everything
                            // downstream re-decides on it. The propagation
                            // delay — the quantity geography actually
                            // cares about — is what the minimum estimates.
                            let mut rtt_ms = f64::MAX;
                            if let Ok(c) = nauka_transport::PeerClient::connect(addr).await {
                                for _ in 0..3 {
                                    let t0 = std::time::Instant::now();
                                    if c.ping().await.is_ok() {
                                        rtt_ms = rtt_ms.min(t0.elapsed().as_secs_f64() * 1000.0);
                                    }
                                }
                            }
                            let ok = rtt_ms < f64::MAX;
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
                            let peer_coord = known.get(peer).copied().unwrap_or_default();
                            my_coord.observe(&peer_coord, rtt_ms);
                        }
                        // Published SNAPPED and sticky, never raw: the
                        // replicated coordinates drive shard ownership on
                        // every node, and millisecond drift in them sets
                        // the scrubber and the GC chasing each other (see
                        // Coord::snapped). Between republications the
                        // placement inputs are bit-identical.
                        let published = known.get(&self_id).copied().unwrap_or_default();
                        if my_coord.should_republish(&published) {
                            let _ = app
                                .write(nauka_raft::types::AppCommand::UpdateNodeCoord {
                                    addr: self_id.clone(),
                                    coord: my_coord.snapped(),
                                })
                                .await;
                        }
                        let coords = app.coords();

                        // Export the estimated distances the placement and
                        // read-routing decisions below are about to be made
                        // on. Without them, a shard landing on a node that
                        // looks arbitrary from the outside has no
                        // explanation anywhere.
                        nauka_cluster::telemetry::record_peer_rtt(
                            &my_coord,
                            coords
                                .iter()
                                .filter(|(peer, _)| **peer != self_id)
                                .map(|(peer, coord)| (peer.as_str(), coord)),
                        );

                        // The scrubber repairs towards the LIVE view: with
                        // a member down, its shards become the living
                        // nodes' responsibility and redundancy climbs back
                        // during the outage. The rebalancing GC and the
                        // audit below keep the FULL view on purpose — a
                        // liveness flap must never be a reason to release
                        // a shard.
                        let nodes_live = health_bg.filter_view(nodes.clone());
                        // Refresh the advertised claim set BEFORE scrub and
                        // gc: the peers' rebalancing GC releases a shard
                        // only against a proof + ownership claim served
                        // from this set, and our own gc below never
                        // releases anything in it. One snapshot per pass —
                        // the claim answered to a peer and the skip
                        // decision made locally must come from the same
                        // view, or the mutual-release race reopens.
                        let claimed_now: std::collections::BTreeSet<String> = {
                            let refs: Vec<(&str, u64)> =
                                nodes.iter().map(|(n, w)| (n.as_str(), *w)).collect();
                            let mut owned = std::collections::BTreeSet::new();
                            if let Ok(hashes) = store_bg.list_manifests() {
                                for fh in hashes {
                                    if let Ok(m) = store_bg.get_manifest(&fh) {
                                        for (_, _, h) in
                                            nauka_cluster::placement::shards_owned_by_geo(
                                                &m, &self_id, &refs, &coords,
                                            )
                                        {
                                            owned.insert(h.to_string());
                                        }
                                    }
                                }
                            }
                            owned
                        };
                        if let Ok(mut published) = claimed_bg.write() {
                            *published = claimed_now.clone();
                        }
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
                                // The counter alone hid a dead file for a
                                // day; the operator needs names.
                                for f in r.unrecoverable_files.iter().take(5) {
                                    eprintln!("scrub: UNRECOVERABLE file {f}");
                                }
                                if r.unrecoverable_files.len() > 5 {
                                    eprintln!(
                                        "scrub: … and {} more unrecoverable file(s)",
                                        r.unrecoverable_files.len() - 5
                                    );
                                }
                            }
                            Ok(_) => {}
                            Err(e) => eprintln!("scrub failed: {e}"),
                        }
                        // Rebalancing: release what no longer belongs to
                        // us (once the owner has confirmed it holds it AND
                        // claims it under its own view).
                        match nauka_cluster::healer::gc_once_geo(
                            &store_bg,
                            &self_id,
                            &nodes,
                            &coords,
                            &claimed_now,
                        )
                        .await
                        {
                            Ok(g) if g.shards_released > 0 => {
                                eprintln!("gc: {} shards released", g.shards_released);
                            }
                            Ok(_) => {}
                            Err(e) => eprintln!("gc failed: {e}"),
                        }
                        // Abandoned ingest spools: a client that dies
                        // mid-upload leaves its spool file in tmp/
                        // (observed: 97 MB after one aborted curl). Same
                        // cadence, same grace as orphan shards.
                        if let Ok(entries) = std::fs::read_dir(data_dir_bg.join("tmp")) {
                            for entry in entries.flatten() {
                                let stale =
                                    entry.file_name().to_string_lossy().starts_with("ingest-")
                                        && entry
                                            .metadata()
                                            .ok()
                                            .and_then(|m| m.modified().ok())
                                            .and_then(|t| t.elapsed().ok())
                                            .is_some_and(|age| {
                                                age >= nauka_cluster::healer::ORPHAN_GRACE
                                            });
                                if stale {
                                    let _ = std::fs::remove_file(entry.path());
                                }
                            }
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
                        nauka_cluster::telemetry::record_maintenance_pass(
                            pass_started.elapsed().as_secs_f64(),
                        );
                    }
                });
            }
            nauka_transport::serve(
                store,
                listen,
                raft_handler,
                Some(claimed_shards as Arc<dyn nauka_transport::server::OwnershipView>),
                stripe_cache
                    .clone()
                    .map(|c| c as Arc<dyn nauka_transport::server::CacheView>),
                Some(Arc::new(api::ConcAbsorber(link_conc_remote.clone()))
                    as Arc<dyn nauka_transport::server::ConcView>),
            )
            .await?;
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
                println!(
                    "{} banned: {file_hash} ({reason})",
                    console::style("✓").green().bold()
                );
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
                println!(
                    "{} ban lifted: {file_hash}",
                    console::style("✓").green().bold()
                );
            } else {
                println!("this hash was not banned");
            }
        }
        Cmd::Status { api, json } => {
            node::status(&api, json).await?;
        }
        Cmd::Top { api, interval } => {
            top::run(api, interval, top_can_admin).await?;
        }
        Cmd::Node(node_cmd) => match node_cmd {
            NodeCmd::Add {
                target,
                ssh_user,
                peers,
                force,
            } => {
                // The identity to hand the new node is the one THIS command
                // was invoked with — token or key dir, whichever the
                // operator uses for the cluster.
                node::add(node::AddOpts {
                    target,
                    ssh_user,
                    peers,
                    token: cli.token.clone(),
                    keys_dir: cli.keys.clone(),
                    force,
                })
                .await?;
            }
            NodeCmd::Remove {
                node_id,
                peers,
                force,
            } => {
                node::remove(node::RemoveOpts {
                    node_id,
                    peers,
                    force,
                })
                .await?;
            }
            NodeCmd::Disable { target, peers } => {
                node::set_disabled(&peers, target, true).await?;
            }
            NodeCmd::Enable { target, peers } => {
                node::set_disabled(&peers, target, false).await?;
            }
        },
        Cmd::Org(cmd) => match cmd {
            OrgCmd::Create { name, peers } => node::org_create(&peers, &name).await?,
            OrgCmd::List { peers } => node::org_list(&peers, None).await?,
            OrgCmd::Suspend { name, peers } => node::org_set_suspended(&peers, &name, true).await?,
            OrgCmd::Resume { name, peers } => node::org_set_suspended(&peers, &name, false).await?,
            OrgCmd::Rm { name, peers } => node::org_delete(&peers, &name).await?,
            OrgCmd::Set { name, quota, peers } => {
                node::org_set(&peers, &name, quota.as_deref()).await?
            }
            OrgCmd::Usage { name, peers } => node::org_usage(&peers, &name).await?,
        },
        Cmd::Space(cmd) => match cmd {
            SpaceCmd::Create {
                name,
                public,
                peers,
            } => node::space_create(&peers, &name, public).await?,
            SpaceCmd::List { org, peers } => node::org_list(&peers, org.as_deref()).await?,
            SpaceCmd::Suspend { name, peers } => {
                node::space_set_suspended(&peers, &name, true).await?
            }
            SpaceCmd::Resume { name, peers } => {
                node::space_set_suspended(&peers, &name, false).await?
            }
            SpaceCmd::Rm { name, peers } => node::space_delete(&peers, &name).await?,
            SpaceCmd::Files { space, peers } => node::space_files(&peers, &space).await?,
            SpaceCmd::Key(cmd) => match cmd {
                SpaceKeyCmd::Add {
                    space,
                    role,
                    name,
                    public_key,
                    peers,
                } => {
                    node::space_key_add(
                        &peers,
                        &space,
                        &role,
                        name.as_deref(),
                        public_key.as_deref(),
                    )
                    .await?
                }
                SpaceKeyCmd::Ls { space, peers } => node::space_key_ls(&peers, &space).await?,
                SpaceKeyCmd::Rm {
                    space,
                    selector,
                    peers,
                } => node::space_key_rm(&peers, &space, &selector).await?,
            },
            SpaceCmd::Sign {
                space,
                key,
                method,
                path,
                content_hash,
            } => node::space_sign(&space, &key, &method, &path, content_hash.as_deref())?,
            SpaceCmd::Link {
                space,
                hash,
                key,
                ttl,
                exp,
                rate,
                conc,
                content_type,
            } => node::space_link(
                &space,
                &hash,
                &key,
                ttl,
                exp,
                rate,
                conc,
                content_type.as_deref(),
            )?,
            SpaceCmd::Set {
                name,
                rate_default,
                quota,
                egress_quota,
                peers,
            } => {
                node::space_set(
                    &peers,
                    &name,
                    rate_default.as_deref(),
                    quota.as_deref(),
                    egress_quota.as_deref(),
                )
                .await?
            }
            SpaceCmd::Usage { name, peers } => node::space_usage(&peers, &name).await?,
            SpaceCmd::Publish {
                space,
                hash,
                to,
                key,
                peers,
            } => node::space_publish(&peers, &space, &hash, to.as_deref(), &key).await?,
        },
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
/// Open the local store only if its directory already exists — opening
/// unconditionally would scatter a fresh ./nauka-data into whatever cwd
/// the command happens to run from.
fn open_existing_store(dir: &std::path::Path) -> Result<Option<ShardStore>> {
    if dir.exists() {
        Ok(Some(ShardStore::open(dir)?))
    } else {
        Ok(None)
    }
}

#[derive(serde::Deserialize)]
struct ClusterFile {
    hash: String,
    size: u64,
    name: Option<String>,
}

/// Git-style unique prefixes wherever a file hash is expected: resolved
/// against the local store first (offline-friendly), then the cluster's
/// registry. Full 64-character hashes pass through untouched.
async fn resolve_hash(prefix: &str, store: Option<&ShardStore>, api: &str) -> Result<String> {
    if prefix.len() == 64 {
        return Ok(prefix.to_string());
    }
    if prefix.len() < 4 {
        bail!("'{prefix}' is too short — give at least 4 characters of the hash");
    }
    if let Some(store) = store {
        let hits: Vec<String> = store
            .list_manifests()?
            .into_iter()
            .filter(|h| h.starts_with(prefix))
            .collect();
        match hits.len() {
            0 => {}
            1 => return Ok(hits.into_iter().next().expect("one hit")),
            n => bail!("'{prefix}' is ambiguous in the local store — {n} files match"),
        }
    }
    let files = cluster_files(api).await.with_context(|| {
        format!("'{prefix}' matches nothing locally, and no node answered at {api} to resolve it")
    })?;
    let hits: Vec<&ClusterFile> = files
        .iter()
        .filter(|f| f.hash.starts_with(prefix))
        .collect();
    match hits.len() {
        0 => bail!("no file matches '{prefix}' — neither the local store nor the cluster"),
        1 => Ok(hits[0].hash.clone()),
        n => bail!(
            "'{prefix}' is ambiguous — {n} cluster files match; add characters (see `nauka list`)"
        ),
    }
}

/// The cluster's registry, as any node's HTTP API reports it.
async fn cluster_files(api: &str) -> Result<Vec<ClusterFile>> {
    Ok(reqwest::Client::new()
        .get(format!("{}/api/files", api.trim_end_matches('/')))
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?)
}

/// Stream `/f/<hash>` from a node, recomputing BLAKE3 along the way —
/// content addressing makes the download self-verifying: bytes that hash
/// back to the requested address are intact by construction, whatever
/// happened to them in between. Writes to `out` when given (get), or
/// just counts and hashes (verify). Returns the byte count.
async fn cluster_fetch_verified(
    api: &str,
    file_hash: &str,
    out: Option<&std::path::Path>,
) -> Result<u64> {
    use futures::StreamExt;
    use tokio::io::AsyncWriteExt;
    let resp = reqwest::Client::new()
        .get(format!("{}/f/{file_hash}", api.trim_end_matches('/')))
        .send()
        .await
        .with_context(|| format!("no node answering at {api}"))?
        .error_for_status()
        .context("the cluster does not serve this hash")?;
    let mut hasher = blake3::Hasher::new();
    let mut file = match out {
        Some(p) => Some(
            tokio::fs::File::create(p)
                .await
                .with_context(|| format!("creating {}", p.display()))?,
        ),
        None => None,
    };
    let mut stream = resp.bytes_stream();
    let mut total: u64 = 0;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.context("reading the download stream")?;
        hasher.update(&chunk);
        if let Some(f) = &mut file {
            f.write_all(&chunk).await?;
        }
        total += chunk.len() as u64;
    }
    if let Some(mut f) = file {
        f.flush().await?;
    }
    let got = hasher.finalize().to_hex().to_string();
    if got != file_hash {
        // Never leave bytes on disk under a name they do not hash to.
        if let Some(p) = out {
            let _ = std::fs::remove_file(p);
        }
        bail!(
            "integrity check FAILED: the node returned {total} bytes hashing to {got}, \
             not {file_hash} — do not trust this node's read path"
        );
    }
    Ok(total)
}

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
