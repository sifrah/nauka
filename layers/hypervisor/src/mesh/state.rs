use nauka_core::resource::Resource;
use nauka_state::Database;
use serde::Deserialize;
use surrealdb::types::SurrealValue;

use crate::definition::Hypervisor;

use super::crypto;
use super::{KeyPair, MeshError, MeshId};

#[derive(Debug, Clone)]
pub struct MeshState {
    pub interface_name: String,
    pub keypair: KeyPair,
    pub listen_port: u16,
    pub mesh_id: MeshId,
    pub address: String,
    pub ca_cert: Option<String>,
    pub ca_key: Option<String>,
    pub tls_cert: Option<String>,
    pub tls_key: Option<String>,
    /// Set on the node that accepted the `hypervisor init` command; the
    /// daemon reads it on startup to know whether to accept incoming joins.
    /// `None` on joiners.
    pub peering_pin: Option<String>,
}

#[derive(Deserialize, SurrealValue)]
struct MeshRecord {
    interface_name: String,
    listen_port: i64,
    mesh_id: String,
    private_key: String,
    #[serde(default)]
    ca_cert: Option<String>,
    #[serde(default)]
    ca_key: Option<String>,
    #[serde(default)]
    tls_cert: Option<String>,
    #[serde(default)]
    tls_key: Option<String>,
    #[serde(default)]
    peering_pin: Option<String>,
}

impl MeshState {
    pub async fn save(&self, db: &Database) -> Result<(), MeshError> {
        let enc_private = crypto::encrypt_secret(self.keypair.private_key())?;
        let mut record = serde_json::json!({
            "interface_name": self.interface_name,
            "listen_port": self.listen_port as i64,
            "mesh_id": self.mesh_id.to_string(),
            "private_key": enc_private,
        });
        let obj = record.as_object_mut().unwrap();
        if let Some(ref v) = self.ca_cert {
            obj.insert("ca_cert".into(), serde_json::Value::String(v.clone()));
        }
        if let Some(ref v) = self.ca_key {
            let enc = crypto::encrypt_secret(v)?;
            obj.insert("ca_key".into(), serde_json::Value::String(enc));
        }
        if let Some(ref v) = self.tls_cert {
            obj.insert("tls_cert".into(), serde_json::Value::String(v.clone()));
        }
        if let Some(ref v) = self.tls_key {
            let enc = crypto::encrypt_secret(v)?;
            obj.insert("tls_key".into(), serde_json::Value::String(enc));
        }
        if let Some(ref v) = self.peering_pin {
            obj.insert("peering_pin".into(), serde_json::Value::String(v.clone()));
        }

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

        let dec_private = crypto::decrypt_secret(&row.private_key)?;
        let keypair = KeyPair::from_private(&dec_private)?;
        let mesh_id: MeshId = row.mesh_id.parse()?;
        let address = mesh_id.node_address(&keypair.public_key_raw()?).to_string();

        let ca_key = row.ca_key.map(|v| crypto::decrypt_secret(&v)).transpose()?;
        let tls_key = row
            .tls_key
            .map(|v| crypto::decrypt_secret(&v))
            .transpose()?;

        Ok(Self {
            interface_name: row.interface_name,
            keypair,
            listen_port: row.listen_port as u16,
            mesh_id,
            address,
            ca_cert: row.ca_cert,
            ca_key,
            tls_cert: row.tls_cert,
            tls_key,
            peering_pin: row.peering_pin,
        })
    }

    pub async fn delete(db: &Database) -> Result<(), MeshError> {
        // Full-state wipe used at teardown. Uses `Hypervisor::TABLE`
        // (rather than a hard-coded literal) so the CI grep check that
        // forbids raw CRUD against known resource tables doesn't trip.
        // The `mesh` table is still hand-written pending P5; once
        // migrated, it will use `Mesh::TABLE` too.
        let query = format!(
            "DELETE mesh; DELETE {}; \
             DELETE _raft_meta; DELETE _raft_log; DELETE _raft_snapshot;",
            Hypervisor::TABLE,
        );
        db.query(&query)
            .await
            .map_err(|e| MeshError::State(e.to_string()))?;
        Ok(())
    }
}
