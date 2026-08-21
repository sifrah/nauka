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
pub mod telemetry;
pub mod tls;

pub use client::PeerClient;
pub use protocol::{Request, Response};
pub use server::serve;
pub use tls::{
    generate_cluster_ca, generate_token, load_cluster_tls, materialize_token_keys, set_cluster_tls,
    ClusterTls,
};

use std::sync::Arc;

static SOCKET_BUFFER_WARNING_EMITTED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// Largest UDP datagram we send or accept: a standard jumbo frame.
pub(crate) const JUMBO_MTU: u16 = 9000;

fn crypto_provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

/// Transport settings shared by client and server, tuned for shard throughput:
/// MTU discovered up to the path maximum (16k on loopback, jumbo frames in a
/// datacenter) and wide windows to keep several shards in flight.
fn transport_config() -> Arc<quinn::TransportConfig> {
    let mut t = quinn::TransportConfig::default();
    // Ceiling at jumbo-frame size. Anything larger only exists on loopback,
    // and asking for it makes bulk transfers stall outright on some hosts
    // (virtualised Linux, container networking) — the datagrams simply never
    // arrive and the connection wedges. 9000 is what real datacenter links
    // offer and already lifts throughput an order of magnitude over quinn's
    // 1472-byte default.
    let mut mtu = quinn::MtuDiscoveryConfig::default();
    mtu.upper_bound(JUMBO_MTU);
    t.mtu_discovery_config(Some(mtu));
    // Realistic initial RTT for a cluster (quinn's default is 333 ms, which
    // throttles the pacer as long as no sample has been taken).
    t.initial_rtt(std::time::Duration::from_millis(2));
    // Start at the safe 1200-byte datagram and let MTU discovery climb to the
    // upper bound above. Forcing a large initial MTU is tempting on loopback
    // but fatal anywhere the path cannot carry it: every early packet is
    // dropped and the connection stalls with no error (observed hanging a
    // 1.5 MB transfer indefinitely on Linux CI while macOS was fine).
    t.initial_mtu(1200);
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
    // The initial window stays moderate: OS receive buffers are capped well
    // below what we request (Linux honours net.core.rmem_max, often 208 kB),
    // and firing multiple megabytes before the first ACK simply overruns them.
    let mut bbr = quinn::congestion::BbrConfig::default();
    bbr.initial_window(256 * 1024);
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
    ec.max_udp_payload_size(JUMBO_MTU)
        .expect("valid payload size");
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
    if let Err(error) = socket.set_recv_buffer_size(buf_size) {
        tracing::warn!(requested = buf_size, %error, "could not size the QUIC receive buffer");
    }
    if let Err(error) = socket.set_send_buffer_size(buf_size) {
        tracing::warn!(requested = buf_size, %error, "could not size the QUIC send buffer");
    }
    let received = socket.recv_buffer_size()?;
    let sent = socket.send_buffer_size()?;
    if (received < buf_size || sent < buf_size)
        && !SOCKET_BUFFER_WARNING_EMITTED.swap(true, std::sync::atomic::Ordering::Relaxed)
    {
        tracing::warn!(
            requested = buf_size,
            effective_receive = received,
            effective_send = sent,
            "kernel socket maxima clamp Nauka's QUIC buffers; raise net.core.rmem_max and net.core.wmem_max"
        );
    }
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
