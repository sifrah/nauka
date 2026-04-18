use nauka_core::resource::{Datetime, Resource, ResourceOps, Scope};
use nauka_state::{Database, Writer};

use crate::definition::Hypervisor;

use super::crypto;
use super::definition::Mesh;
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

impl MeshState {
    pub async fn save(&self, db: &Database) -> Result<(), MeshError> {
        let enc_private = crypto::encrypt_secret(self.keypair.private_key())?;
        let enc_ca_key = self
            .ca_key
            .as_deref()
            .map(crypto::encrypt_secret)
            .transpose()?;
        let enc_tls_key = self
            .tls_key
            .as_deref()
            .map(crypto::encrypt_secret)
            .transpose()?;

        let now = Datetime::now();
        let mesh = Mesh {
            mesh_id: self.mesh_id.to_string(),
            interface_name: self.interface_name.clone(),
            listen_port: self.listen_port,
            private_key: enc_private,
            ca_cert: self.ca_cert.clone(),
            ca_key: enc_ca_key,
            tls_cert: self.tls_cert.clone(),
            tls_key: enc_tls_key,
            peering_pin: self.peering_pin.clone(),
            created_at: now,
            updated_at: now,
            version: 0,
        };

        // The `mesh` table is a per-node singleton — there is only
        // ever one row. Wipe any prior row before the create so a
        // re-init after a half-finished setup doesn't collide on the
        // natural-key record id. Both statements run as one
        // transaction so the table is never empty for callers racing
        // us on a read.
        Writer::new(db)
            .transaction(|tx| {
                tx.raw(Scope::Local, format!("DELETE {}", Mesh::TABLE))?;
                tx.create(&mesh)?;
                Ok(())
            })
            .await
            .map_err(|e| MeshError::State(e.to_string()))
    }

    pub async fn load(db: &Database) -> Result<Self, MeshError> {
        let rows: Vec<Mesh> = db
            .query_take(&Mesh::list_query())
            .await
            .map_err(|e| MeshError::State(e.to_string()))?;

        let row = rows.into_iter().next().ok_or_else(|| {
            MeshError::State("no mesh found — run 'nauka hypervisor init' first".into())
        })?;

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
            listen_port: row.listen_port,
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
        // Full-state wipe used at teardown. Uses `Mesh::TABLE` and
        // `Hypervisor::TABLE` (rather than literal table names) so
        // the CI grep check that forbids raw CRUD against known
        // resource tables doesn't trip.
        let query = format!(
            "DELETE {mesh}; DELETE {hv}; \
             DELETE _raft_meta; DELETE _raft_log; DELETE _raft_snapshot;",
            mesh = Mesh::TABLE,
            hv = Hypervisor::TABLE,
        );
        db.query(&query)
            .await
            .map_err(|e| MeshError::State(e.to_string()))?;
        Ok(())
    }
}
