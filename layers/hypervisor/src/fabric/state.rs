//! Fabric state — persisted mesh configuration.
//!
//! The fabric state is the complete snapshot of a node's mesh membership:
//! mesh identity, node identity, secret, and list of peers.
//!
//! Persisted in the SurrealKV-backed [`EmbeddedDb`] (P1.10, sifrah/nauka#200)
//! as a single JSON blob at `fabric:state`. Read/written via the async
//! `save`/`load`/`delete`/`exists` API. P1.11 (sifrah/nauka#201) removed
//! the legacy sync JSON-file API; every caller now goes through
//! [`EmbeddedDb`].

use std::fmt;

use nauka_state::EmbeddedDb;
use serde::{Deserialize, Serialize};

use super::backend::NetworkMode;
use super::mesh::{HypervisorIdentity, MeshIdentity};
use super::peer::PeerList;

/// Scheduling state of a node (maintenance mode).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NodeState {
    /// Normal operation — accepts new VM placements.
    #[default]
    Available,
    /// Draining — no new VMs; existing VMs continue until migrated.
    Draining,
}

impl fmt::Display for NodeState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Available => write!(f, "available"),
            Self::Draining => write!(f, "draining"),
        }
    }
}

/// Complete fabric state for a node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FabricState {
    /// Mesh identity (name, prefix).
    pub mesh: MeshIdentity,
    /// This node's identity.
    pub hypervisor: HypervisorIdentity,
    /// The mesh secret (encrypted at rest in future).
    pub secret: String,
    /// Known peers.
    pub peers: PeerList,
    /// Network backend mode.
    #[serde(default)]
    pub network_mode: NetworkMode,
    /// Scheduling state (available or draining).
    #[serde(default)]
    pub node_state: NodeState,
    /// Maximum PD (Placement Driver) members in the cluster.
    /// Must be odd (1, 3, 5, 7). Defaults to 3.
    #[serde(default = "default_max_pd_members")]
    pub max_pd_members: usize,
}

fn default_max_pd_members() -> usize {
    3
}

/// SurrealDB table + record id for the single fabric-state blob.
///
/// FabricState contains nested types (Ipv6Addr, custom typed IDs, enums)
/// that don't have native `surrealdb::types::SurrealValue` impls. Rather
/// than derive `SurrealValue` on the entire tree (a much wider change
/// that touches MeshIdentity, HypervisorIdentity, PeerList, Peer,
/// PeerStatus, NetworkMode, NodeState, MeshId, HypervisorId, NodeId, ...),
/// P1.10 (sifrah/nauka#200) bridges through `serde_json::Value` —
/// surrealdb-types ships a `SurrealValue` impl for `serde_json::Value`
/// that handles arbitrary JSON, so anything FabricState can already
/// serde-serialize round-trips through it for free.
///
/// P3 codegen (sifrah/nauka#225 ff) will replace this single-blob storage
/// with a SCHEMAFULL split of FabricState across the proper
/// `mesh` / `hypervisor` / `peer` / `wg_key` tables defined in
/// `bootstrap.surql`. Until then the embedded path is just "store the
/// JSON blob in `fabric:state`".
const FABRIC_TABLE: &str = "fabric";
const FABRIC_RECORD_ID: &str = "state";

impl FabricState {
    /// Save state to the SurrealKV-backed [`EmbeddedDb`].
    ///
    /// The full `FabricState` is stored as a single record at
    /// `fabric:state`. Written via SurrealQL `UPSERT fabric:state CONTENT
    /// $data` so the first save creates the row and subsequent saves
    /// replace it in place.
    pub async fn save(&self, db: &EmbeddedDb) -> Result<(), nauka_state::StateError> {
        ensure_fabric_table(db).await?;

        let json = serde_json::to_value(self)
            .map_err(|e| nauka_state::StateError::Serialization(e.to_string()))?;
        let result = db
            .client()
            .query("UPSERT type::record($tbl, $id) CONTENT $data")
            .bind(("tbl", FABRIC_TABLE))
            .bind(("id", FABRIC_RECORD_ID))
            .bind(("data", json))
            .await?;
        result.check()?;
        Ok(())
    }

    /// Load state from the SurrealKV-backed [`EmbeddedDb`]. Returns `None`
    /// if no state exists.
    pub async fn load(db: &EmbeddedDb) -> Result<Option<Self>, nauka_state::StateError> {
        ensure_fabric_table(db).await?;

        let mut response = db
            .client()
            .query("SELECT * FROM type::record($tbl, $id)")
            .bind(("tbl", FABRIC_TABLE))
            .bind(("id", FABRIC_RECORD_ID))
            .await?;

        // The result is `Option<serde_json::Value>` because we round-trip
        // through JSON (see the FABRIC_TABLE rationale comment above).
        // FabricState fields live at the top level of the row alongside
        // the SurrealDB-auto-added `id` field. serde will ignore the
        // unknown `id` field when deserialising into FabricState (the
        // struct does not opt into `deny_unknown_fields`).
        let row: Option<serde_json::Value> = response.take(0)?;
        let Some(row) = row else { return Ok(None) };

        let state: FabricState = serde_json::from_value(row)
            .map_err(|e| nauka_state::StateError::Serialization(e.to_string()))?;
        Ok(Some(state))
    }

    /// Delete state from the SurrealKV-backed [`EmbeddedDb`] (used by `leave`).
    pub async fn delete(db: &EmbeddedDb) -> Result<(), nauka_state::StateError> {
        ensure_fabric_table(db).await?;

        let result = db
            .client()
            .query("DELETE type::record($tbl, $id)")
            .bind(("tbl", FABRIC_TABLE))
            .bind(("id", FABRIC_RECORD_ID))
            .await?;
        result.check()?;
        Ok(())
    }

    /// Check whether fabric state exists in the SurrealKV-backed
    /// [`EmbeddedDb`].
    pub async fn exists(db: &EmbeddedDb) -> Result<bool, nauka_state::StateError> {
        Ok(Self::load(db).await?.is_some())
    }
}

/// Lazily create the SCHEMALESS `fabric` table on first use.
///
/// Idempotent thanks to `IF NOT EXISTS`. Called by every FabricState
/// method that touches the table, so callers don't need to remember to
/// do it themselves.
///
/// This lives outside `bootstrap.surql` because the table is a
/// transitional artefact for P1 — Phase 3 (sifrah/nauka#225 ff) replaces
/// this with a SCHEMAFULL split across the existing
/// `mesh` / `hypervisor` / `peer` / `wg_key` tables, at which point this
/// function and the `fabric` table both go away.
async fn ensure_fabric_table(db: &EmbeddedDb) -> Result<(), nauka_state::StateError> {
    db.client()
        .query("DEFINE TABLE IF NOT EXISTS fabric SCHEMALESS")
        .await?
        .check()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::mesh;
    use super::*;

    async fn temp_db() -> (tempfile::TempDir, EmbeddedDb) {
        let dir = tempfile::tempdir().unwrap();
        let db = EmbeddedDb::open(&dir.path().join("test.skv"))
            .await
            .expect("open EmbeddedDb at temp path");
        (dir, db)
    }

    fn make_state() -> FabricState {
        let (mesh_id, secret) = mesh::create_mesh();
        let node = mesh::create_hypervisor(&mesh::CreateHypervisorConfig {
            name: "node-1",
            region: "eu",
            zone: "fsn1",
            port: 51820,
            endpoint: None,
            fabric_interface: "",
            mesh_prefix: &mesh_id.prefix,
            ipv6_block: None,
            ipv4_public: None,
        })
        .unwrap();

        FabricState {
            mesh: mesh_id,
            hypervisor: node,
            secret: secret.to_string(),
            peers: PeerList::new(),
            network_mode: super::super::backend::NetworkMode::default(),
            node_state: NodeState::default(),
            max_pd_members: 3,
        }
    }

    #[tokio::test]
    async fn save_and_load() {
        let (_d, db) = temp_db().await;
        let state = make_state();

        state.save(&db).await.unwrap();
        let loaded = FabricState::load(&db).await.unwrap().unwrap();

        assert_eq!(loaded.hypervisor.name, "node-1");
        assert_eq!(loaded.hypervisor.region, "eu");

        db.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn load_empty() {
        let (_d, db) = temp_db().await;
        assert!(FabricState::load(&db).await.unwrap().is_none());
        db.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn exists_check() {
        let (_d, db) = temp_db().await;
        assert!(!FabricState::exists(&db).await.unwrap());
        make_state().save(&db).await.unwrap();
        assert!(FabricState::exists(&db).await.unwrap());
        db.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn delete_state() {
        let (_d, db) = temp_db().await;
        make_state().save(&db).await.unwrap();
        FabricState::delete(&db).await.unwrap();
        assert!(!FabricState::exists(&db).await.unwrap());
        db.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn save_with_peers() {
        let (_d, db) = temp_db().await;
        let mut state = make_state();

        state
            .peers
            .add(super::super::peer::Peer::new(
                "node-2".into(),
                "eu".into(),
                "nbg1".into(),
                "key-n2".into(),
                51820,
                Some("1.2.3.4:51820".into()),
                "fd01::2".parse().unwrap(),
            ))
            .unwrap();

        state.save(&db).await.unwrap();
        let loaded = FabricState::load(&db).await.unwrap().unwrap();
        assert_eq!(loaded.peers.len(), 1);
        assert_eq!(loaded.peers.find_by_name("node-2").unwrap().zone, "nbg1");

        db.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn secret_persists() {
        let (_d, db) = temp_db().await;
        let state = make_state();
        let secret = state.secret.clone();

        state.save(&db).await.unwrap();
        let loaded = FabricState::load(&db).await.unwrap().unwrap();
        assert_eq!(loaded.secret, secret);
        assert!(loaded.secret.starts_with("syf_sk_"));

        db.shutdown().await.unwrap();
    }

    #[test]
    fn serde_roundtrip() {
        let state = make_state();
        let json = serde_json::to_string(&state).unwrap();
        let back: FabricState = serde_json::from_str(&json).unwrap();
        assert_eq!(back.hypervisor.name, state.hypervisor.name);
        assert_eq!(
            back.hypervisor.wg_public_key,
            state.hypervisor.wg_public_key
        );
    }

    #[tokio::test]
    async fn private_key_persists() {
        let (_d, db) = temp_db().await;
        let state = make_state();
        let original_key = state.hypervisor.wg_private_key.clone();
        assert!(!original_key.is_empty());

        state.save(&db).await.unwrap();
        let loaded = FabricState::load(&db).await.unwrap().unwrap();
        assert_eq!(loaded.hypervisor.wg_private_key, original_key);

        db.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn update_peer_and_save() {
        let (_d, db) = temp_db().await;
        let mut state = make_state();

        state
            .peers
            .add(super::super::peer::Peer::new(
                "node-2".into(),
                "eu".into(),
                "nbg1".into(),
                "key-n2".into(),
                51820,
                None,
                "fd01::2".parse().unwrap(),
            ))
            .unwrap();

        // Update peer handshake
        state.peers.peers[0].update_handshake(99999);
        state.save(&db).await.unwrap();

        let loaded = FabricState::load(&db).await.unwrap().unwrap();
        assert_eq!(loaded.peers.peers[0].last_handshake, 99999);
        assert!(loaded.peers.peers[0].is_active());

        db.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn save_overwrites_previous() {
        let (_d, db) = temp_db().await;
        let mut state = make_state();
        state.save(&db).await.unwrap();

        // Mutate then re-save: the row at fabric:state should reflect
        // the new value, not append a duplicate.
        state.max_pd_members = 5;
        state.save(&db).await.unwrap();

        let loaded = FabricState::load(&db).await.unwrap().unwrap();
        assert_eq!(loaded.max_pd_members, 5);

        db.shutdown().await.unwrap();
    }

    /// Cross-process round-trip: write via the async API, drop the
    /// EmbeddedDb, reopen at the same path, read back. Mirrors what
    /// the P1.5 in-process persistence test proves for arbitrary
    /// records, but for FabricState specifically.
    #[tokio::test]
    async fn persistence_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("persist.skv");

        let original_secret = {
            let db = EmbeddedDb::open(&path).await.unwrap();
            let state = make_state();
            let secret = state.secret.clone();
            state.save(&db).await.unwrap();
            db.shutdown().await.unwrap();
            secret
        };

        let db = EmbeddedDb::open(&path).await.unwrap();
        let loaded = FabricState::load(&db).await.unwrap().unwrap();
        assert_eq!(loaded.secret, original_secret);
        db.shutdown().await.unwrap();
    }
}
