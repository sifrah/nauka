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
/// Since P2.3 (sifrah/nauka#207) the PD-endpoint discovery half of this
/// function lives in two thin helpers:
///
/// - [`crate::fabric::state::FabricState::pd_endpoints`] collects
///   `self.hypervisor.mesh_ipv6` followed by every peer's `mesh_ipv6`
///   into a `Vec<Ipv6Addr>`. It lives in `nauka-hypervisor` because
///   `FabricState` lives there, and it's a pure function of the state.
/// - [`nauka_state::EmbeddedDb::open_tikv_from_pd_addresses`] takes that
///   slice + the PD client port and returns a TiKV-backed `EmbeddedDb`.
///   It lives in `nauka-state` because that's where the surrealdb
///   client lives; the `&[Ipv6Addr]` signature (rather than
///   `&FabricState`) is what lets it stay one layer below
///   `nauka-hypervisor` — `nauka-state` must never depend on
///   `nauka-hypervisor` (that would close a layer cycle).
///
/// `connect()` itself stays a thin wrapper: load FabricState → collect
/// PD addresses → open the TiKV cluster. The legacy [`ClusterDb`] return
/// type is preserved for call-site wire compatibility until P2.16
/// (sifrah/nauka#220) retires `ClusterDb` outright.
pub async fn connect() -> anyhow::Result<ClusterDb> {
    let pd_addresses = pd_addresses_from_fabric().await?;

    // TODO(P2.16, sifrah/nauka#220): replace this leg with
    // `EmbeddedDb::open_tikv_from_pd_addresses(&pd_addresses, PD_CLIENT_PORT).await?`
    // once every caller of `connect()` has been migrated to the
    // SurrealDB-SDK-based cluster client. Until then, keep returning a
    // legacy [`ClusterDb`] handle so forge / network / etc. continue
    // to compile unchanged, but build its PD endpoint list the same
    // way `EmbeddedDb::open_tikv_from_pd_addresses` does internally.
    let endpoints: Vec<String> = pd_addresses
        .iter()
        .map(|addr| format!("http://[{addr}]:{PD_CLIENT_PORT}"))
        .collect();
    let refs: Vec<&str> = endpoints.iter().map(String::as_str).collect();

    ClusterDb::connect(&refs).await.map_err(Into::into)
}

/// Open the bootstrap [`EmbeddedDb`], load fabric state, close it again,
/// and return the list of PD mesh IPv6 addresses the local node knows
/// about (self first, then peers).
///
/// Extracted out of [`connect`] in P2.3 (sifrah/nauka#207) so the
/// FabricState-loading half has a name and can be reused by anything
/// else that needs "where is this cluster's PD quorum?" without
/// going through the legacy [`ClusterDb`] helper.
///
/// # Errors
///
/// - The error message for an un-initialised cluster is a deliberately
///   human-readable string ("cluster not initialized. Initialize a
///   cluster first with: `nauka hypervisor init`"), matching the
///   pre-P2.3 behaviour so CLI users see the same instructions on an
///   uninitialised node.
/// - Any underlying [`nauka_state::StateError`] is propagated through
///   `anyhow::Error`.
async fn pd_addresses_from_fabric() -> anyhow::Result<Vec<std::net::Ipv6Addr>> {
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
    // Explicit shutdown so the SurrealKV flock is released before the
    // caller issues its next `connect()` or local-state read.
    db.shutdown().await?;

    Ok(state.pd_endpoints())
}
