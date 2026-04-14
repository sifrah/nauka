//! Control plane — distributed cluster state.
//!
//! Manages PD + TiKV as systemd services and exposes an
//! [`nauka_state::EmbeddedDb`] (on the SurrealDB TiKv backend) for the
//! distributed cluster state (orgs, projects, envs, vpcs, subnets,
//! vms, ...). All traffic flows over the encrypted WireGuard mesh.
//!
//! # Sub-modules
//!
//! - `service`: Low-level systemd management (install, start, stop, reload, status)
//! - `ops`: High-level orchestration (bootstrap, join, leave)
//! - `backup`: S3-backed logical + physical cluster backup
//!
//! # P2.16 — `ClusterDb` wrapper removed
//!
//! Until P2.16 (sifrah/nauka#220) a thin `ClusterDb` struct wrapped
//! [`nauka_state::EmbeddedDb`] on the TiKv backend and exposed a
//! legacy raw-KV `put`/`get`/`delete`/... surface so the pre-P2.8
//! stores could keep compiling while they migrated, one at a time,
//! to the native SurrealDB SDK. Every store has now moved, so the
//! wrapper is gone — [`connect`] returns the SurrealDB handle
//! directly and callers drive it through `db.client().query(...)`
//! exactly like the local bootstrap store.

pub mod backup;
pub mod ops;
pub mod pd_client;
pub mod service;

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

/// Connect to the cluster-side SurrealDB (TiKv backend) using PD
/// endpoints from local fabric state.
///
/// This is the standard way for any layer to get a cluster-scoped
/// [`nauka_state::EmbeddedDb`] handle. Reads the local hypervisor
/// state to discover PD endpoints on the mesh, then hands them to
/// [`nauka_state::EmbeddedDb::open_tikv`], which strips the `http://`
/// prefix and tries each endpoint in order until one answers.
pub async fn connect() -> anyhow::Result<nauka_state::EmbeddedDb> {
    let local_db = nauka_state::EmbeddedDb::open_default().await?;

    let state = crate::fabric::state::FabricState::load(&local_db)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "cluster not initialized.\n\n\
                 Initialize a cluster first with:\n\
                 \x20 nauka hypervisor init"
            )
        })?;
    // Release the SurrealKV flock before opening the TiKv handle —
    // the cluster-side EmbeddedDb is a separate Surreal instance
    // against TiKv, but the caller may re-open bootstrap.skv right
    // after we return.
    local_db.shutdown().await?;

    // Self first, peers after. The self-first contract matters for
    // single-node clusters (nothing in peers) and for always routing
    // through the cheapest hop when multiple PDs are live.
    let mesh_addrs = state.pd_endpoints();
    let endpoints = nauka_state::pd_endpoints_for(&mesh_addrs, PD_CLIENT_PORT);
    let refs: Vec<&str> = endpoints.iter().map(|s| s.as_str()).collect();
    nauka_state::EmbeddedDb::open_tikv(&refs)
        .await
        .map_err(|e| anyhow::anyhow!("TiKV connect failed: {e}"))
}
