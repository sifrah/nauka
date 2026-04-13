//! Region storage configuration — S3 backend per region.
//!
//! Each region maps to one S3 bucket. Credentials are stored
//! locally in the fabric state (future: encrypted in TiKV).

use nauka_core::error::NaukaError;
use nauka_state::EmbeddedDb;
use serde::{Deserialize, Serialize};

/// S3 storage configuration for a region.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegionStorage {
    /// Region identifier (e.g., "eu", "us").
    pub region: String,
    /// S3 endpoint URL.
    pub s3_endpoint: String,
    /// S3 bucket name.
    pub s3_bucket: String,
    /// S3 access key.
    pub s3_access_key: String,
    /// S3 secret key.
    pub s3_secret_key: String,
    /// S3 region (e.g., "eu-central-1"). Empty for non-AWS.
    #[serde(default)]
    pub s3_region: String,
    /// Encryption password for ZeroFS at-rest encryption.
    #[serde(default)]
    pub encryption_password: String,
    /// Whether this is the default region.
    #[serde(default)]
    pub is_default: bool,
}

impl RegionStorage {
    /// Validate the configuration.
    pub fn validate(&self) -> Result<(), NaukaError> {
        if self.region.is_empty() {
            return Err(NaukaError::validation("region cannot be empty"));
        }
        if self.s3_endpoint.is_empty() {
            return Err(NaukaError::validation("s3_endpoint cannot be empty"));
        }
        if self.s3_bucket.is_empty() {
            return Err(NaukaError::validation("s3_bucket cannot be empty"));
        }
        if self.s3_access_key.is_empty() {
            return Err(NaukaError::validation("s3_access_key cannot be empty"));
        }
        if self.s3_secret_key.is_empty() {
            return Err(NaukaError::validation("s3_secret_key cannot be empty"));
        }
        if !self.s3_endpoint.starts_with("http://") && !self.s3_endpoint.starts_with("https://") {
            return Err(NaukaError::validation(
                "s3_endpoint must start with http:// or https://",
            ));
        }
        Ok(())
    }
}

/// Local registry of region storage configs.
///
/// Persisted in the SurrealKV-backed [`EmbeddedDb`] at
/// `regions:current`. Like [`crate::fabric::state::FabricState`], the whole
/// registry is stored as a single JSON blob rather than split row-per-region
/// — the cost of a SurrealValue derive cascade isn't worth it for a
/// collection that is only ever read/written as a whole.
///
/// The transitional SCHEMALESS `regions` table is created lazily on first
/// use by [`ensure_regions_table`]. Phase 3 codegen (sifrah/nauka#225 ff)
/// will replace this with a proper SCHEMAFULL per-region table in the
/// bootstrap schema.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct RegionRegistry {
    pub regions: Vec<RegionStorage>,
}

/// Table name for the transitional JSON-blob region registry.
const REGIONS_TABLE: &str = "regions";
/// Record id that holds the whole registry.
const REGIONS_RECORD_ID: &str = "current";

/// Lazily create the SCHEMALESS `regions` table on first use.
///
/// Idempotent thanks to `IF NOT EXISTS`. Same rationale as the `fabric`
/// table in `FabricState::save_async` — transitional artefact for P1.11
/// that Phase 3 will replace with a SCHEMAFULL per-region table.
async fn ensure_regions_table(db: &EmbeddedDb) -> Result<(), NaukaError> {
    db.client()
        .query("DEFINE TABLE IF NOT EXISTS regions SCHEMALESS")
        .await
        .map_err(|e| NaukaError::internal(e.to_string()))?
        .check()
        .map_err(|e| NaukaError::internal(e.to_string()))?;
    Ok(())
}

impl RegionRegistry {
    /// Load the registry from the SurrealKV-backed [`EmbeddedDb`]. Returns
    /// an empty registry if no record exists yet.
    pub async fn load(db: &EmbeddedDb) -> Result<Self, NaukaError> {
        ensure_regions_table(db).await?;

        let mut response = db
            .client()
            .query("SELECT * FROM type::record($tbl, $id)")
            .bind(("tbl", REGIONS_TABLE))
            .bind(("id", REGIONS_RECORD_ID))
            .await
            .map_err(|e| NaukaError::internal(e.to_string()))?;

        let row: Option<serde_json::Value> = response
            .take(0)
            .map_err(|e| NaukaError::internal(e.to_string()))?;
        let Some(row) = row else {
            return Ok(Self::default());
        };

        let registry: RegionRegistry = serde_json::from_value(row)
            .map_err(|e| NaukaError::internal(format!("deserialize RegionRegistry: {e}")))?;
        Ok(registry)
    }

    /// Persist the registry to the SurrealKV-backed [`EmbeddedDb`].
    ///
    /// Stored as a single record at `regions:current`, same JSON-bridge
    /// pattern as `FabricState::save_async`.
    pub async fn save(&self, db: &EmbeddedDb) -> Result<(), NaukaError> {
        ensure_regions_table(db).await?;

        let json = serde_json::to_value(self)
            .map_err(|e| NaukaError::internal(format!("serialize RegionRegistry: {e}")))?;
        db.client()
            .query("UPSERT type::record($tbl, $id) CONTENT $data")
            .bind(("tbl", REGIONS_TABLE))
            .bind(("id", REGIONS_RECORD_ID))
            .bind(("data", json))
            .await
            .map_err(|e| NaukaError::internal(e.to_string()))?
            .check()
            .map_err(|e| NaukaError::internal(e.to_string()))?;
        Ok(())
    }

    /// Add or update a region.
    pub fn upsert(&mut self, config: RegionStorage) {
        if let Some(existing) = self.regions.iter_mut().find(|r| r.region == config.region) {
            *existing = config;
        } else {
            self.regions.push(config);
        }
    }

    /// Remove a region.
    pub fn remove(&mut self, region: &str) -> bool {
        let before = self.regions.len();
        self.regions.retain(|r| r.region != region);
        self.regions.len() < before
    }

    /// Find a region config.
    pub fn find(&self, region: &str) -> Option<&RegionStorage> {
        self.regions.iter().find(|r| r.region == region)
    }

    /// Get the default region (first one marked default, or first one).
    pub fn default_region(&self) -> Option<&RegionStorage> {
        self.regions
            .iter()
            .find(|r| r.is_default)
            .or_else(|| self.regions.first())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_region(name: &str) -> RegionStorage {
        RegionStorage {
            region: name.into(),
            s3_endpoint: "https://s3.example.com".into(),
            s3_bucket: format!("nauka-{name}"),
            s3_access_key: "AKID".into(),
            s3_secret_key: "SECRET".into(),
            s3_region: String::new(),
            encryption_password: "test-password".into(),
            is_default: false,
        }
    }

    #[test]
    fn validate_ok() {
        make_region("eu").validate().unwrap();
    }

    #[test]
    fn validate_empty_region() {
        let mut r = make_region("eu");
        r.region = String::new();
        assert!(r.validate().is_err());
    }

    #[test]
    fn validate_bad_endpoint() {
        let mut r = make_region("eu");
        r.s3_endpoint = "ftp://bad".into();
        assert!(r.validate().is_err());
    }

    #[test]
    fn registry_upsert() {
        let mut reg = RegionRegistry::default();
        reg.upsert(make_region("eu"));
        reg.upsert(make_region("us"));
        assert_eq!(reg.regions.len(), 2);

        // Update eu
        let mut eu = make_region("eu");
        eu.s3_bucket = "new-bucket".into();
        reg.upsert(eu);
        assert_eq!(reg.regions.len(), 2);
        assert_eq!(reg.find("eu").unwrap().s3_bucket, "new-bucket");
    }

    #[test]
    fn registry_remove() {
        let mut reg = RegionRegistry::default();
        reg.upsert(make_region("eu"));
        reg.upsert(make_region("us"));
        assert!(reg.remove("eu"));
        assert_eq!(reg.regions.len(), 1);
        assert!(!reg.remove("eu")); // already removed
    }

    #[test]
    fn registry_default() {
        let mut reg = RegionRegistry::default();
        assert!(reg.default_region().is_none());

        reg.upsert(make_region("eu"));
        assert_eq!(reg.default_region().unwrap().region, "eu");

        let mut us = make_region("us");
        us.is_default = true;
        reg.upsert(us);
        assert_eq!(reg.default_region().unwrap().region, "us");
    }

    #[tokio::test]
    async fn registry_persistence() {
        let dir = tempfile::tempdir().unwrap();
        let db = EmbeddedDb::open(&dir.path().join("test.skv"))
            .await
            .unwrap();

        let mut reg = RegionRegistry::default();
        reg.upsert(make_region("eu"));
        reg.save(&db).await.unwrap();

        let loaded = RegionRegistry::load(&db).await.unwrap();
        assert_eq!(loaded.regions.len(), 1);
        assert_eq!(loaded.find("eu").unwrap().s3_bucket, "nauka-eu");

        db.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn registry_empty_on_fresh_db() {
        let dir = tempfile::tempdir().unwrap();
        let db = EmbeddedDb::open(&dir.path().join("empty.skv"))
            .await
            .unwrap();

        let loaded = RegionRegistry::load(&db).await.unwrap();
        assert!(loaded.regions.is_empty());

        db.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn registry_save_overwrites_previous() {
        let dir = tempfile::tempdir().unwrap();
        let db = EmbeddedDb::open(&dir.path().join("overwrite.skv"))
            .await
            .unwrap();

        let mut reg = RegionRegistry::default();
        reg.upsert(make_region("eu"));
        reg.save(&db).await.unwrap();

        reg.upsert(make_region("us"));
        reg.save(&db).await.unwrap();

        let loaded = RegionRegistry::load(&db).await.unwrap();
        assert_eq!(loaded.regions.len(), 2);

        db.shutdown().await.unwrap();
    }

    #[test]
    fn serde_roundtrip() {
        let r = make_region("eu");
        let json = serde_json::to_string(&r).unwrap();
        let back: RegionStorage = serde_json::from_str(&json).unwrap();
        assert_eq!(back.region, "eu");
        assert_eq!(back.s3_access_key, "AKID");
    }
}
