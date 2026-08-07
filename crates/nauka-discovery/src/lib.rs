//! Peer discovery over the Mainline DHT (BitTorrent) + pkarr.
//!
//! The cluster publishes a signed DNS record (`_seeds` TXT records) under an
//! Ed25519 key **derived from the cluster key**: holding the cluster keys is
//! enough to publish AND resolve — nothing else to distribute, no
//! infrastructure to deploy. The Mainline DHT (~10M nodes, 20 years old)
//! acts as the public bulletin board.
//!
//! Discovery is not admission: the DHT hands out addresses; cluster mTLS
//! still decides who gets in.

use std::net::SocketAddr;
use std::time::Duration;

use anyhow::{Context, Result};
use pkarr::{Client, Keypair, PublicKey, SignedPacket};

pub use pkarr;
use tracing::{debug, info};

/// TTL of the published DNS records (a hint for caches).
const RECORD_TTL_SECS: u32 = 300;
/// Max number of published addresses: a pkarr packet is capped at ~1000
/// bytes, and any reachable seed is enough (full membership then comes from
/// the cluster itself).
pub const MAX_SEEDS: usize = 8;

/// Derives the cluster's pkarr keypair from its CA key (deterministic: every
/// holder of the keys ends up with the same DHT identity).
pub fn derive_dht_keypair(keys_dir: &std::path::Path) -> Result<Keypair> {
    let ca_pem = std::fs::read_to_string(keys_dir.join("cluster-ca.key"))
        .with_context(|| format!("reading {}", keys_dir.join("cluster-ca.key").display()))?;
    let ca_key = rcgen::KeyPair::from_pem(&ca_pem)?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"nauka-discovery-v1");
    hasher.update(&ca_key.serialize_der());
    let seed: [u8; 32] = *hasher.finalize().as_bytes();
    Ok(Keypair::from_secret_key(&seed))
}

/// DHT client. `bootstrap`: alternative bootstrap nodes (tests/demos on a
/// local DHT); None = the real Mainline.
pub fn make_client(bootstrap: Option<&[String]>) -> Result<Client> {
    let mut builder = Client::builder();
    if let Some(nodes) = bootstrap {
        builder.bootstrap(nodes);
        // Local DHT only: no public relays, which would answer with the state
        // of the real Mainline.
        builder.no_relays();
    }
    Ok(builder.build()?)
}

/// Publishes the cluster's seed list (overwrites the previous version —
/// pkarr timestamps packets and resolvers keep the most recent one).
pub async fn publish_seeds(client: &Client, keypair: &Keypair, addrs: &[SocketAddr]) -> Result<()> {
    let mut builder = SignedPacket::builder();
    for addr in addrs.iter().take(MAX_SEEDS) {
        builder = builder.txt(
            "_seeds".try_into().expect("valid DNS name"),
            addr.to_string()
                .as_str()
                .try_into()
                .expect("valid TXT value"),
            RECORD_TTL_SECS,
        );
    }
    let packet = builder.sign(keypair)?;
    client.publish(&packet, None).await?;
    info!(
        "seeds published on the DHT: {} address(es)",
        addrs.len().min(MAX_SEEDS)
    );
    Ok(())
}

/// Publishes a genesis candidacy: "no cluster exists yet, I offer to found
/// it". The record lives under the same key as the seeds — as soon as the
/// cluster exists, publishing the seeds wipes it out.
pub async fn publish_genesis_candidacy(
    client: &Client,
    keypair: &Keypair,
    node_id: u64,
    addr: SocketAddr,
) -> Result<()> {
    let value = format!("{node_id}|{addr}");
    let packet = SignedPacket::builder()
        .txt(
            "_genesis".try_into().expect("valid DNS name"),
            value.as_str().try_into().expect("valid TXT value"),
            RECORD_TTL_SECS,
        )
        .sign(keypair)?;
    client.publish(&packet, None).await?;
    Ok(())
}

/// Genesis candidacy currently visible on the DHT, if there is one.
pub async fn resolve_genesis_candidacy(
    client: &Client,
    public_key: &PublicKey,
) -> Result<Option<(u64, SocketAddr)>> {
    let Some(packet) = client.resolve_most_recent(public_key).await else {
        return Ok(None);
    };
    for record in packet.resource_records("_genesis") {
        if let pkarr::dns::rdata::RData::TXT(txt) = &record.rdata {
            let text: String = txt.clone().try_into().unwrap_or_default();
            if let Some((id, addr)) = text.split_once('|') {
                if let (Ok(id), Ok(addr)) = (id.parse(), addr.parse()) {
                    return Ok(Some((id, addr)));
                }
            }
        }
    }
    Ok(None)
}

/// Resolves the cluster's seeds from the DHT. Empty when there is no record
/// (cluster never bootstrapped, or record expired everywhere).
pub async fn resolve_seeds(client: &Client, public_key: &PublicKey) -> Result<Vec<SocketAddr>> {
    let Some(packet) = client.resolve_most_recent(public_key).await else {
        return Ok(Vec::new());
    };
    let mut addrs = Vec::new();
    for record in packet.resource_records("_seeds") {
        if let pkarr::dns::rdata::RData::TXT(txt) = &record.rdata {
            let text: String = txt.clone().try_into().unwrap_or_default();
            for part in text.split(',') {
                if let Ok(addr) = part.trim().parse::<SocketAddr>() {
                    addrs.push(addr);
                }
            }
        }
    }
    addrs.sort();
    addrs.dedup();
    debug!("seeds resolved from the DHT: {addrs:?}");
    Ok(addrs)
}

/// Detects this machine's public IP through the DHT itself: Mainline nodes
/// report the address they see us from (BEP42) and the client turns those
/// into a consensus. No third-party service, no infrastructure.
///
/// `None` if the DHT has not converged (yet). The detected address is the one
/// seen from the internet — it is only reachable if the port is open/forwarded.
pub async fn detect_public_ip(bootstrap: Option<&[String]>) -> Result<Option<std::net::IpAddr>> {
    let bootstrap: Option<Vec<String>> = bootstrap.map(|b| b.to_vec());
    tokio::task::spawn_blocking(move || -> Result<Option<std::net::IpAddr>> {
        let mut builder = mainline::Dht::builder();
        if let Some(nodes) = &bootstrap {
            builder.bootstrap(nodes);
        }
        let dht = builder.build()?;
        dht.bootstrapped();
        Ok(dht
            .info()
            .public_address()
            .map(|a| std::net::IpAddr::V4(*a.ip())))
    })
    .await?
}

/// Publishing loop: as long as `is_leader` holds, periodically republishes
/// the addresses returned by `current_seeds`. DHT records evaporate on their
/// own (caches last ~hours): republishing is the cluster's heartbeat on the
/// DHT.
pub async fn run_publisher(
    client: Client,
    keypair: Keypair,
    interval: Duration,
    current_seeds: impl Fn() -> Option<Vec<SocketAddr>> + Send + 'static,
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        // None = not the leader right now: stay silent (the leader publishes).
        let Some(addrs) = current_seeds() else {
            continue;
        };
        if addrs.is_empty() {
            continue;
        }
        if let Err(e) = publish_seeds(&client, &keypair, &addrs).await {
            tracing::warn!("DHT publishing failed (will retry): {e:#}");
        }
    }
}
