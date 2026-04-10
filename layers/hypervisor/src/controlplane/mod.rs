//! Control plane — TiKV distributed KV store.
//!
//! Manages PD + TiKV as systemd services and exposes `ClusterDb`
//! for distributed state (VMs, VPCs, users, etc.).
//! All traffic flows over the encrypted WireGuard mesh.
//!
//! # Sub-modules
//!
//! - `service`: Low-level systemd management (install, start, stop, reload, status)
//! - `ops`: High-level orchestration (bootstrap, join, leave)
//! - `store`: `ClusterDb` — async TiKV client

pub mod ops;
pub mod service;
pub mod store;

/// Default ports (on mesh IPv6).
pub const PD_CLIENT_PORT: u16 = 2379;
pub const PD_PEER_PORT: u16 = 2380;
pub const TIKV_PORT: u16 = 20160;
pub const TIKV_STATUS_PORT: u16 = 20180;

pub use store::ClusterDb;

/// Connect to the TiKV cluster using PD endpoints from local fabric state.
///
/// This is the standard way for any layer to get a ClusterDb connection.
/// Reads the local hypervisor state to discover PD endpoints on the mesh.
pub async fn connect() -> anyhow::Result<ClusterDb> {
    let dir = nauka_core::process::nauka_dir();
    let _ = std::fs::create_dir_all(&dir);
    let db = nauka_state::LocalDb::open("hypervisor")?;

    let state = crate::fabric::state::FabricState::load(&db)
        .map_err(|e| anyhow::anyhow!("{e}"))?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "cluster not initialized.\n\n\
                 Initialize a cluster first with:\n\
                 \x20 nauka hypervisor init"
            )
        })?;

    let self_endpoint = format!("http://[{}]:{}", state.hypervisor.mesh_ipv6, PD_CLIENT_PORT,);
    let mut endpoints = vec![self_endpoint];
    for peer in &state.peers.peers {
        endpoints.push(format!("http://[{}]:{}", peer.mesh_ipv6, PD_CLIENT_PORT,));
    }
    let refs: Vec<&str> = endpoints.iter().map(|s| s.as_str()).collect();

    ClusterDb::connect(&refs).await.map_err(Into::into)
}
