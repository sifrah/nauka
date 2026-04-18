//! Mesh resource definition — see ADR 0006.
//!
//! The per-node mesh identity (`scope = "local"`): WireGuard
//! identity, mesh prefix, PKI material, optional peering PIN. Each
//! node stores exactly one row of this in its local SurrealKV;
//! there is no Raft replication because the private key never
//! leaves this node.
//!
//! Secrets are stored encrypted: `private_key`, `ca_key`, and
//! `tls_key` are ciphertexts. Encryption happens in
//! `mesh::state::MeshState::save`; `Mesh` itself is the on-disk
//! byte-for-byte shape.

use nauka_core::resource::SurrealValue;
use nauka_core_macros::resource;
use serde::{Deserialize, Serialize};

#[resource(table = "mesh", scope = "local")]
#[derive(Serialize, Deserialize, SurrealValue, Debug, Clone)]
pub struct Mesh {
    /// The mesh's 48-bit ULA prefix, serialised to its canonical
    /// textual form (`fdXX:XX:XX::/48`). Every node in the same
    /// mesh shares this value.
    #[id]
    pub mesh_id: String,
    pub interface_name: String,
    pub listen_port: u16,
    /// Encrypted WireGuard private key (plaintext never hits disk).
    pub private_key: String,
    pub ca_cert: Option<String>,
    /// Encrypted CA private key.
    pub ca_key: Option<String>,
    pub tls_cert: Option<String>,
    /// Encrypted TLS private key.
    pub tls_key: Option<String>,
    /// Set on the node that accepted `hypervisor init`; the daemon
    /// reads it at startup to decide whether to accept further
    /// joins.
    pub peering_pin: Option<String>,
    // created_at / updated_at / version: injected by `#[resource]`.
}
