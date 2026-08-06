//! Découverte de pairs via la DHT Mainline (BitTorrent) + pkarr.
//!
//! Le cluster publie un enregistrement DNS signé (records TXT `_seeds`)
//! sous une clé Ed25519 **dérivée de la clé de cluster** : posséder les
//! clés du cluster suffit pour publier ET résoudre — rien d'autre à
//! distribuer, aucune infrastructure à déployer. La DHT Mainline (~10 M de
//! nœuds, 20 ans d'ancienneté) sert de tableau d'affichage public.
//!
//! Découverte ≠ admission : la DHT donne les adresses ; le mTLS de cluster
//! décide toujours qui entre.

use std::net::SocketAddr;
use std::time::Duration;

use anyhow::{Context, Result};
use pkarr::{Client, Keypair, PublicKey, SignedPacket};

pub use pkarr;
use tracing::{debug, info};

/// TTL des records DNS publiés (indicatif pour les caches).
const RECORD_TTL_SECS: u32 = 300;
/// Nombre max d'adresses publiées : un paquet pkarr est limité à ~1000
/// octets, et n'importe quel seed joignable suffit (le membership complet
/// vient ensuite du cluster lui-même).
pub const MAX_SEEDS: usize = 8;

/// Dérive la keypair pkarr du cluster depuis sa clé CA (déterministe :
/// tous les détenteurs des clés obtiennent la même identité DHT).
pub fn derive_dht_keypair(keys_dir: &std::path::Path) -> Result<Keypair> {
    let ca_pem = std::fs::read_to_string(keys_dir.join("cluster-ca.key"))
        .with_context(|| format!("lecture de {}", keys_dir.join("cluster-ca.key").display()))?;
    let ca_key = rcgen::KeyPair::from_pem(&ca_pem)?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"yog-discovery-v1");
    hasher.update(&ca_key.serialize_der());
    let seed: [u8; 32] = *hasher.finalize().as_bytes();
    Ok(Keypair::from_secret_key(&seed))
}

/// Client DHT. `bootstrap` : nœuds d'amorçage alternatifs (tests/démos sur
/// DHT locale) ; None = la vraie Mainline.
pub fn make_client(bootstrap: Option<&[String]>) -> Result<Client> {
    let mut builder = Client::builder();
    if let Some(nodes) = bootstrap {
        builder.bootstrap(nodes);
        // DHT locale uniquement : pas de relays publics qui répondraient
        // avec l'état de la vraie Mainline.
        builder.no_relays();
    }
    Ok(builder.build()?)
}

/// Publie la liste des seeds du cluster (écrase la version précédente —
/// pkarr horodate et les résolveurs prennent le plus récent).
pub async fn publish_seeds(
    client: &Client,
    keypair: &Keypair,
    addrs: &[SocketAddr],
) -> Result<()> {
    let mut builder = SignedPacket::builder();
    for addr in addrs.iter().take(MAX_SEEDS) {
        builder = builder.txt(
            "_seeds".try_into().expect("nom DNS valide"),
            addr.to_string().as_str().try_into().expect("valeur TXT valide"),
            RECORD_TTL_SECS,
        );
    }
    let packet = builder.sign(keypair)?;
    client.publish(&packet, None).await?;
    info!("seeds publiés sur la DHT: {} adresse(s)", addrs.len().min(MAX_SEEDS));
    Ok(())
}

/// Publie une candidature de genèse : « aucun cluster n'existe, je propose
/// de le fonder ». Le record vit sous la même clé que les seeds — dès que
/// le cluster existe, la publication des seeds l'efface.
pub async fn publish_genesis_candidacy(
    client: &Client,
    keypair: &Keypair,
    node_id: u64,
    addr: SocketAddr,
) -> Result<()> {
    let value = format!("{node_id}|{addr}");
    let packet = SignedPacket::builder()
        .txt(
            "_genesis".try_into().expect("nom DNS valide"),
            value.as_str().try_into().expect("valeur TXT valide"),
            RECORD_TTL_SECS,
        )
        .sign(keypair)?;
    client.publish(&packet, None).await?;
    Ok(())
}

/// Candidature de genèse actuellement visible sur la DHT, s'il y en a une.
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

/// Résout les seeds du cluster depuis la DHT. Vide si aucun enregistrement
/// (cluster jamais amorcé, ou record expiré partout).
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
    debug!("seeds résolus depuis la DHT: {addrs:?}");
    Ok(addrs)
}

/// Détecte l'IP publique de cette machine via la DHT elle-même : les nœuds
/// Mainline renvoient l'adresse d'où ils nous voient (BEP42) et le client
/// en fait un consensus. Aucun service tiers, aucune infra.
///
/// `None` si la DHT n'a pas (encore) convergé. L'adresse détectée est celle
/// vue d'internet — elle n'est joignable que si le port est ouvert/forwardé.
pub async fn detect_public_ip(bootstrap: Option<&[String]>) -> Result<Option<std::net::IpAddr>> {
    let bootstrap: Option<Vec<String>> = bootstrap.map(|b| b.to_vec());
    tokio::task::spawn_blocking(move || -> Result<Option<std::net::IpAddr>> {
        let mut builder = mainline::Dht::builder();
        if let Some(nodes) = &bootstrap {
            builder.bootstrap(nodes);
        }
        let dht = builder.build()?;
        dht.bootstrapped();
        Ok(dht.info().public_address().map(|a| std::net::IpAddr::V4(*a.ip())))
    })
    .await?
}

/// Boucle de publication : tant que `is_leader` est vrai, republie
/// périodiquement les adresses fournies par `current_seeds`. Les records
/// DHT s'évaporent naturellement (caches ~heures) : la republication est
/// le battement de cœur du cluster sur la DHT.
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
        // None = pas leader en ce moment : silence (le leader publie).
        let Some(addrs) = current_seeds() else { continue };
        if addrs.is_empty() {
            continue;
        }
        if let Err(e) = publish_seeds(&client, &keypair, &addrs).await {
            tracing::warn!("publication DHT en échec (retentera): {e:#}");
        }
    }
}
