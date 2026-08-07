//! Nauka's inter-node QUIC transport (quinn).
//!
//! One exchange = one bidirectional stream: the client writes a [`Request`],
//! the server answers with a [`Response`]. Connections are multiplexed by
//! quinn — several shards can travel in parallel over a single connection.
//!
//! TLS v0: self-signed certificate generated at startup, the client does not
//! verify the certificate (closed cluster). Mutual authentication with cluster
//! keys will come with the membership layer.

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

/// Transport settings shared by client and server, tuned for shard throughput:
/// MTU discovered up to the path maximum (16k on loopback, jumbo frames in a
/// datacenter) and wide windows to keep several shards in flight.
fn transport_config() -> Arc<quinn::TransportConfig> {
    let mut t = quinn::TransportConfig::default();
    let mut mtu = quinn::MtuDiscoveryConfig::default();
    mtu.upper_bound(65_527);
    t.mtu_discovery_config(Some(mtu));
    // Realistic initial RTT for a cluster (quinn's default is 333 ms, which
    // throttles the pacer as long as no sample has been taken).
    t.initial_rtt(std::time::Duration::from_millis(2));
    // Larger initial datagrams: 1200 bytes/packet is the Internet worst case;
    // our nodes sit on jumbo-frame networks or on loopback.
    t.initial_mtu(8940);
    t.min_mtu(1200);
    t.stream_receive_window(quinn::VarInt::from_u32(16 * 1024 * 1024));
    t.receive_window(quinn::VarInt::from_u32(256 * 1024 * 1024));
    t.send_window(256 * 1024 * 1024);
    // BBR with a large initial window: between storage nodes the traffic is
    // continuous and bulky — classic slow-start (14 kB) combined with delayed
    // ACKs caps throughput at ~8 MB/s per connection.
    // BBR measures the actual throughput instead of probing through losses; on
    // fast links with small buffers (loopback, datacenter) Cubic collapses
    // (measured: 7 MB/s, 5k losses) where BBR sustains the path's throughput.
    let mut bbr = quinn::congestion::BbrConfig::default();
    bbr.initial_window(4 * 1024 * 1024);
    t.congestion_controller_factory(Arc::new(bbr));
    // Under severe congestion, a quiet connection must not die silently:
    // keep-alive enabled, explicit idle timeout.
    t.keep_alive_interval(Some(std::time::Duration::from_secs(2)));
    t.max_idle_timeout(Some(
        quinn::IdleTimeout::try_from(std::time::Duration::from_secs(30)).unwrap(),
    ));
    Arc::new(t)
}

/// Endpoint config: allows large datagrams (the 1472-byte default caps the MTU
/// regardless of initial_mtu and of discovery).
pub(crate) fn endpoint_config() -> quinn::EndpointConfig {
    let mut ec = quinn::EndpointConfig::default();
    ec.max_udp_payload_size(65_527).expect("valid payload size");
    ec
}

/// UDP socket with sized buffers: the system defaults (often < 1 MB) overflow
/// on shard bursts and tank the throughput.
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

/// Data-plane buffers: large, for throughput.
pub(crate) const DATA_SOCKET_BUF: usize = 8 * 1024 * 1024;
/// Consensus-plane buffers: small, to bound the queueing delay — a heartbeat
/// waiting behind 8 MB of shards is a dead heartbeat.
pub(crate) const CONSENSUS_SOCKET_BUF: usize = 1024 * 1024;

/// Consensus-plane address of a node: same host, data port + 1.
pub fn consensus_addr(data_addr: std::net::SocketAddr) -> std::net::SocketAddr {
    std::net::SocketAddr::new(data_addr.ip(), data_addr.port() + 1)
}
