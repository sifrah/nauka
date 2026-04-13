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

pub mod backup;
pub mod ops;
pub mod pd_client;
pub mod service;
pub mod store;

/// Expected component versions (used by upgrade-check and doctor).
pub const PD_VERSION: &str = "v8.5.5";
pub const TIKV_VERSION: &str = "v8.5.5";

/// Default maximum PD members. Raft works best with odd numbers (3, 5, 7).
pub const DEFAULT_MAX_PD_MEMBERS: usize = 3;

/// Valid values for max PD members.
pub const VALID_PD_MEMBER_COUNTS: &[usize] = &[1, 3, 5, 7];

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
///
/// P2.3 (sifrah/nauka#207) replaced the per-call `format!` ladder with
/// the shared [`crate::fabric::state::FabricState::pd_endpoints`] +
/// [`nauka_state::pd_endpoints_for`] helpers so every Phase-2 call site
/// lands on the same PD list shape — and so `EmbeddedDb::open_tikv`
/// (P2.2) and `ClusterDb::connect` both see the exact same endpoints.
pub async fn connect() -> anyhow::Result<ClusterDb> {
    let db = nauka_state::EmbeddedDb::open_default().await?;

    let state = crate::fabric::state::FabricState::load(&db)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "cluster not initialized.\n\n\
                 Initialize a cluster first with:\n\
                 \x20 nauka hypervisor init"
            )
        })?;
    // Explicit shutdown so the SurrealKV flock is released before the caller
    // issues its next `connect()` or local-state read.
    db.shutdown().await?;

    // Self first, peers after. The self-first contract matters for
    // single-node clusters (nothing in peers) and for always routing
    // through the cheapest hop when multiple PDs are live.
    let mesh_addrs = state.pd_endpoints();
    let endpoints = nauka_state::pd_endpoints_for(&mesh_addrs, PD_CLIENT_PORT);
    let refs: Vec<&str> = endpoints.iter().map(|s| s.as_str()).collect();

    ClusterDb::connect(&refs).await.map_err(Into::into)
}
