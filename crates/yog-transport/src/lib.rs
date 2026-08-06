//! Transport QUIC inter-nœuds de yogfile (quinn).
//!
//! Un échange = un stream bidirectionnel : le client écrit une [`Request`],
//! le serveur répond une [`Response`]. Les connexions sont multiplexées par
//! quinn — plusieurs shards peuvent transiter en parallèle sur une même
//! connexion.
//!
//! TLS v0 : certificat auto-signé généré au démarrage, le client ne vérifie
//! pas le certificat (cluster fermé). L'authentification mutuelle par clés
//! de cluster viendra avec la couche membership.

pub mod client;
pub mod protocol;
pub mod server;
pub mod tls;

pub use client::PeerClient;
pub use protocol::{Request, Response};
pub use server::serve;
pub use tls::{generate_cluster_ca, load_cluster_tls, set_cluster_tls, ClusterTls};

use std::sync::Arc;

fn crypto_provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

/// Réglages de transport communs client/serveur, orientés débit de shards :
/// MTU découvert jusqu'au maximum du chemin (16k sur loopback, jumbo frames
/// en datacenter) et fenêtres larges pour garder plusieurs shards en vol.
fn transport_config() -> Arc<quinn::TransportConfig> {
    let mut t = quinn::TransportConfig::default();
    let mut mtu = quinn::MtuDiscoveryConfig::default();
    mtu.upper_bound(65_527);
    t.mtu_discovery_config(Some(mtu));
    // RTT initial réaliste pour un cluster (défaut quinn: 333 ms, qui
    // étrangle le pacer tant qu'aucun échantillon n'existe).
    t.initial_rtt(std::time::Duration::from_millis(2));
    // Datagrammes initiaux plus grands : 1200 octets/paquet est le pire cas
    // Internet ; nos nœuds sont sur des réseaux à jumbo frames ou loopback.
    t.initial_mtu(8940);
    t.min_mtu(1200);
    t.stream_receive_window(quinn::VarInt::from_u32(16 * 1024 * 1024));
    t.receive_window(quinn::VarInt::from_u32(256 * 1024 * 1024));
    t.send_window(256 * 1024 * 1024);
    // BBR avec une fenêtre initiale large : entre nœuds de stockage le
    // trafic est continu et volumineux — le slow-start classique (14 Ko)
    // combiné aux ACKs retardés plafonne le débit à ~8 Mo/s par connexion.
    // BBR : mesure le débit réel au lieu de sonder par pertes ; sur des
    // liens rapides à petit buffer (loopback, datacenter) Cubic s'effondre
    // (essayé : 7 Mo/s, 5k pertes) là où BBR tient le débit du chemin.
    let mut bbr = quinn::congestion::BbrConfig::default();
    bbr.initial_window(4 * 1024 * 1024);
    t.congestion_controller_factory(Arc::new(bbr));
    // Sous congestion sévère, une connexion silencieuse ne doit pas mourir
    // en douce : keep-alive actif, timeout d'inactivité explicite.
    t.keep_alive_interval(Some(std::time::Duration::from_secs(2)));
    t.max_idle_timeout(Some(
        quinn::IdleTimeout::try_from(std::time::Duration::from_secs(30)).unwrap(),
    ));
    Arc::new(t)
}

/// Config d'endpoint : autorise les gros datagrammes (le défaut de 1472
/// octets plafonne le MTU quel que soit initial_mtu/la découverte).
pub(crate) fn endpoint_config() -> quinn::EndpointConfig {
    let mut ec = quinn::EndpointConfig::default();
    ec.max_udp_payload_size(65_527).expect("payload size valide");
    ec
}

/// Socket UDP avec des buffers dimensionnés : les valeurs par défaut du
/// système (souvent < 1 Mo) font déborder les rafales de shards et
/// écroulent le débit.
pub(crate) fn make_socket(
    addr: std::net::SocketAddr,
    buf_size: usize,
) -> std::io::Result<std::net::UdpSocket> {
    use socket2::{Domain, Protocol, Socket, Type};
    let socket = Socket::new(Domain::for_address(addr), Type::DGRAM, Some(Protocol::UDP))?;
    let _ = socket.set_recv_buffer_size(buf_size);
    let _ = socket.set_send_buffer_size(buf_size);
    socket.bind(&addr.into())?;
    Ok(socket.into())
}

/// Buffers du plan de données : larges, pour le débit.
pub(crate) const DATA_SOCKET_BUF: usize = 8 * 1024 * 1024;
/// Buffers du plan consensus : petits, pour borner le délai de queue —
/// un heartbeat qui attend derrière 8 Mo de shards est un heartbeat mort.
pub(crate) const CONSENSUS_SOCKET_BUF: usize = 1024 * 1024;

/// Adresse du plan consensus d'un nœud : même hôte, port data + 1.
pub fn consensus_addr(data_addr: std::net::SocketAddr) -> std::net::SocketAddr {
    std::net::SocketAddr::new(data_addr.ip(), data_addr.port() + 1)
}
