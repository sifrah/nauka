//! Nauka cluster layer — v0: static cluster view (peer list from config),
//! deterministic placement via rendezvous hashing, heartbeats and
//! self-healing. Raft consensus (openraft) will replace the static view to
//! provide dynamic membership and strongly consistent metadata.

pub mod audit;
pub mod healer;
pub mod health;
pub mod placement;
pub mod telemetry;
pub mod vivaldi;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use nauka_store::ShardStore;
use nauka_transport::PeerClient;
use tracing::{info, warn};

/// This node's view of the cluster.
#[derive(Debug, Clone)]
pub struct ClusterView {
    /// This node's identity = its advertised address (host:port).
    pub self_id: String,
    /// Every node in the cluster, this one included. Sorted so that all
    /// nodes share the same view.
    pub nodes: Vec<String>,
}

impl ClusterView {
    pub fn new(self_addr: SocketAddr, peers: &[SocketAddr]) -> Self {
        let self_id = self_addr.to_string();
        let mut nodes: Vec<String> = peers.iter().map(|a| a.to_string()).collect();
        if !nodes.contains(&self_id) {
            nodes.push(self_id.clone());
        }
        nodes.sort();
        nodes.dedup();
        Self { self_id, nodes }
    }
}

/// Background loop of a cluster node: peer heartbeats + periodic scrub.
/// Runs forever; spawn it with `tokio::spawn` alongside the QUIC server.
pub async fn run_background(store: Arc<ShardStore>, view: ClusterView, scrub_interval: Duration) {
    let mut ticker = tokio::time::interval(scrub_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;

        // Heartbeat: log unreachable peers (healing still works as long as
        // k shards per stripe remain reachable).
        for node in view.nodes.iter().filter(|n| **n != view.self_id) {
            if let Ok(addr) = node.parse::<SocketAddr>() {
                match PeerClient::connect(addr).await {
                    Ok(c) if c.ping().await.is_ok() => {}
                    _ => warn!("peer {node} unreachable"),
                }
            }
        }

        // Static mode: no declared capacities, uniform weights.
        let weighted: Vec<(String, u64)> = view.nodes.iter().map(|n| (n.clone(), 1)).collect();
        match healer::scrub_once(&store, &view.self_id, &weighted).await {
            Ok(r) if r.shards_healed > 0 || r.shards_unrecoverable > 0 => {
                info!(
                    "scrub: {} checked, {} healed, {} unrecoverable",
                    r.shards_checked, r.shards_healed, r.shards_unrecoverable
                );
            }
            Ok(_) => {}
            Err(e) => warn!("scrub failed: {e}"),
        }
    }
}
