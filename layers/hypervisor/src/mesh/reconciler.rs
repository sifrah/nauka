use defguard_wireguard_rs::{Kernel, WGApi, WireguardInterfaceApi};
use nauka_state::Database;
use serde::Deserialize;
use std::str::FromStr;
use surrealdb::types::SurrealValue;

#[derive(Deserialize, SurrealValue, Debug)]
struct PeerRecord {
    public_key: String,
    endpoint: Option<String>,
    #[serde(default)]
    allowed_ips: Vec<String>,
    keepalive: Option<i64>,
}

pub async fn run(db: &Database, interface_name: &str, own_public_key: &str) {
    loop {
        if let Err(e) = reconcile(db, interface_name, own_public_key).await {
            eprintln!("  reconciler error: {e}");
        }
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    }
}

async fn reconcile(
    db: &Database,
    interface_name: &str,
    own_public_key: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let db_peers: Vec<PeerRecord> = db
        .query_take("SELECT * FROM peer")
        .await
        .unwrap_or_default();

    let api = WGApi::<Kernel>::new(interface_name.to_string())?;
    let host = api.read_interface_data()?;
    let wg_keys: std::collections::HashSet<String> =
        host.peers.keys().map(|k| k.to_string()).collect();

    let db_keys: std::collections::HashSet<String> =
        db_peers.iter().map(|p| p.public_key.clone()).collect();

    // Add peers in DB but not in WG
    for record in &db_peers {
        if record.public_key == own_public_key {
            continue;
        }
        if wg_keys.contains(&record.public_key) {
            continue;
        }

        let Ok(key) = defguard_wireguard_rs::key::Key::from_str(&record.public_key) else {
            continue;
        };
        let mut peer = defguard_wireguard_rs::peer::Peer::new(key);
        if let Some(ref ep) = record.endpoint {
            let _ = peer.set_endpoint(ep);
        }
        peer.persistent_keepalive_interval = record.keepalive.map(|k| k as u16);
        for cidr in &record.allowed_ips {
            if let Ok(addr) = cidr.parse() {
                peer.allowed_ips.push(addr);
            }
        }
        if api.configure_peer(&peer).is_ok() {
            let _ = api.configure_peer_routing(&[peer]);
            eprintln!("  reconciler: +peer {}", record.public_key);
        }
    }

    // Remove peers in WG but not in DB (peer removal propagation)
    for (wg_key, _) in &host.peers {
        let key_str = wg_key.to_string();
        if !db_keys.contains(&key_str) {
            if api.remove_peer(wg_key).is_ok() {
                eprintln!("  reconciler: -peer {key_str}");
            }
        }
    }

    Ok(())
}
