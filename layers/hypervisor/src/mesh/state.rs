use nauka_state::Database;
use serde::Deserialize;
use surrealdb::types::SurrealValue;

use super::{KeyPair, MeshError, MeshId};

#[derive(Debug, Clone)]
pub struct MeshState {
    pub interface_name: String,
    pub keypair: KeyPair,
    pub listen_port: u16,
    pub mesh_id: MeshId,
    pub address: String,
}

#[derive(Deserialize, SurrealValue)]
struct MeshRecord {
    interface_name: String,
    listen_port: i64,
    mesh_id: String,
    address: String,
    public_key: String,
    private_key: String,
}

impl MeshState {
    pub async fn save(&self, db: &Database) -> Result<(), MeshError> {
        let record = serde_json::json!({
            "interface_name": self.interface_name,
            "listen_port": self.listen_port as i64,
            "mesh_id": self.mesh_id.to_string(),
            "address": self.address,
            "public_key": self.keypair.public_key(),
            "private_key": self.keypair.private_key(),
        });
        let surql = format!(
            "DELETE mesh:default; CREATE mesh:default CONTENT {}",
            serde_json::to_string(&record).expect("serialize mesh record")
        );
        db.query(&surql)
            .await
            .map_err(|e| MeshError::State(e.to_string()))?;
        Ok(())
    }

    pub async fn load(db: &Database) -> Result<Self, MeshError> {
        let rows: Vec<MeshRecord> = db
            .query_take("SELECT * FROM mesh:default")
            .await
            .map_err(|e| MeshError::State(e.to_string()))?;

        let row = rows
            .into_iter()
            .next()
            .ok_or_else(|| MeshError::State("no mesh found — run 'nauka mesh up' first".into()))?;

        let keypair = KeyPair::from_private(&row.private_key)?;
        let mesh_id: MeshId = row.mesh_id.parse()?;

        Ok(Self {
            interface_name: row.interface_name,
            keypair,
            listen_port: row.listen_port as u16,
            mesh_id,
            address: row.address,
        })
    }

    pub async fn delete(db: &Database) -> Result<(), MeshError> {
        db.query("DELETE mesh; DELETE hypervisor; DELETE _raft_meta; DELETE _raft_log; DELETE _raft_snapshot;")
            .await
            .map_err(|e| MeshError::State(e.to_string()))?;
        Ok(())
    }
}
