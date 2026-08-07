//! Couche cluster de yogfile — v0 : vue statique du cluster (liste de peers
//! en config), placement déterministe par rendezvous hashing, heartbeats et
//! auto-healing. Le consensus Raft (openraft) remplacera la vue statique
//! pour un membership dynamique et des métadonnées fortement cohérentes.

pub mod healer;
pub mod placement;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tracing::{info, warn};
use yog_store::ShardStore;
use yog_transport::PeerClient;

/// Vue du cluster pour ce nœud.
#[derive(Debug, Clone)]
pub struct ClusterView {
    /// Identité de ce nœud = son adresse annoncée (host:port).
    pub self_id: String,
    /// Tous les nœuds du cluster, ce nœud inclus. Trié pour que tous les
    /// nœuds partagent la même vue.
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

/// Boucle de fond d'un nœud cluster : heartbeat des peers + scrub périodique.
/// Tourne indéfiniment ; à lancer via `tokio::spawn` à côté du serveur QUIC.
pub async fn run_background(
    store: Arc<ShardStore>,
    view: ClusterView,
    scrub_interval: Duration,
) {
    let mut ticker = tokio::time::interval(scrub_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;

        // Heartbeat : trace les peers injoignables (le healing s'en sort
        // quand même tant que k shards par stripe restent accessibles).
        for node in view.nodes.iter().filter(|n| **n != view.self_id) {
            if let Ok(addr) = node.parse::<SocketAddr>() {
                match PeerClient::connect(addr).await {
                    Ok(c) if c.ping().await.is_ok() => {}
                    _ => warn!("peer {node} injoignable"),
                }
            }
        }

        // Mode statique : pas de capacités déclarées, poids uniformes.
        let weighted: Vec<(String, u64)> =
            view.nodes.iter().map(|n| (n.clone(), 1)).collect();
        match healer::scrub_once(&store, &view.self_id, &weighted).await {
            Ok(r) if r.shards_healed > 0 || r.shards_unrecoverable > 0 => {
                info!(
                    "scrub: {} vérifiés, {} régénérés, {} irréparables",
                    r.shards_checked, r.shards_healed, r.shards_unrecoverable
                );
            }
            Ok(_) => {}
            Err(e) => warn!("scrub en échec: {e}"),
        }
    }
}
