use defguard_wireguard_rs::{Kernel, WGApi, WireguardInterfaceApi};
use nauka_core::LogErr;
use nauka_state::Database;
use serde::Deserialize;
use std::str::FromStr;
use surrealdb::types::SurrealValue;

#[derive(Deserialize, SurrealValue, Debug)]
struct HypervisorRecord {
    public_key: String,
    endpoint: Option<String>,
    #[serde(default)]
    allowed_ips: Vec<String>,
    keepalive: Option<i64>,
}

pub async fn run(db: &Database, interface_name: &str, own_public_key: &str) {
    loop {
        let _ = reconcile(db, interface_name, own_public_key)
            .await
            .warn_if_err("reconciler");
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    }
}

async fn reconcile(
    db: &Database,
    interface_name: &str,
    own_public_key: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let db_hypervisors: Vec<HypervisorRecord> = db
        .query_take("SELECT public_key, endpoint, allowed_ips, keepalive FROM hypervisor")
        .await
        .unwrap_or_default();

    let api = WGApi::<Kernel>::new(interface_name.to_string())?;
    let host = api.read_interface_data()?;

    let db_keys: std::collections::HashSet<String> = db_hypervisors
        .iter()
        .map(|p| p.public_key.clone())
        .collect();

    // Add hypervisors in DB but not in WG, or update WG peers whose endpoint
    // no longer matches the DB record (happens after another node restarts
    // with a new public IP).
    for record in &db_hypervisors {
        if record.public_key == own_public_key {
            continue;
        }

        let Ok(key) = defguard_wireguard_rs::key::Key::from_str(&record.public_key) else {
            continue;
        };

        let existing = host.peers.get(&key);
        let wg_endpoint_str = existing.and_then(|p| p.endpoint).map(|e| e.to_string());
        if existing.is_some() && wg_endpoint_str == record.endpoint {
            continue; // already configured with the right endpoint
        }

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
            let action = if existing.is_some() { "update" } else { "add" };
            tracing::info!(
                event = "reconciler.peer.upsert",
                action,
                public_key = %record.public_key,
                endpoint = ?record.endpoint,
                "reconciler upserted peer"
            );
        }
    }

    // Remove WG peers not in DB
    for wg_key in host.peers.keys() {
        let key_str = wg_key.to_string();
        if !db_keys.contains(&key_str) && api.remove_peer(wg_key).is_ok() {
            tracing::info!(
                event = "reconciler.peer.remove",
                public_key = %key_str,
                "reconciler removed peer"
            );
        }
    }

    Ok(())
}
