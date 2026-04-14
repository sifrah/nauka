//! Storage operations — high-level orchestration.
//!
//! Called by the hypervisor handler for storage lifecycle.

use nauka_core::error::NaukaError;
use nauka_state::EmbeddedDb;

use super::region::{RegionRegistry, RegionStorage};
use super::service;

/// SurrealDB table (SCHEMALESS) that holds inter-node region-config
/// handoff records. Each row is keyed by the region name and stores
/// the full [`RegionStorage`] as JSON under a `data` wrapper field,
/// following the same JSON-bridge pattern the legacy
/// `ClusterDb::put`/`get` path used before P2.16 (sifrah/nauka#220).
const REGION_TABLE: &str = "storage_regions";

/// Setup storage for a region on this node.
///
/// 1. Validate config
/// 2. Install ZeroFS if needed
/// 3. Install config + systemd unit
/// 4. Start the service
pub async fn setup_region(db: &EmbeddedDb, config: RegionStorage) -> Result<(), NaukaError> {
    config.validate()?;

    tracing::info!(region = %config.region, "storage: setting up region");

    // Install ZeroFS binary
    service::ensure_installed()?;

    // Install config + systemd unit
    service::install_region(&config)?;

    // Start
    service::start_region(&config.region)?;

    // Persist region config
    let mut registry = RegionRegistry::load(db).await?;
    registry.upsert(config.clone());
    registry.save(db).await?;

    tracing::info!(region = %config.region, "storage: region ready");

    Ok(())
}

/// Remove storage for a region on this node.
pub async fn remove_region(db: &EmbeddedDb, region: &str) -> Result<(), NaukaError> {
    tracing::info!(region, "storage: removing region");

    service::uninstall_region(region)?;

    let mut registry = RegionRegistry::load(db).await?;
    registry.remove(region);
    registry.save(db).await?;

    Ok(())
}

/// Start all configured storage regions on this node.
pub async fn start_all(db: &EmbeddedDb) -> Result<(), NaukaError> {
    let registry = RegionRegistry::load(db).await?;
    for region in &registry.regions {
        if !service::is_region_active(&region.region) {
            let _ = service::start_region(&region.region);
        }
    }
    Ok(())
}

/// Stop all storage regions.
pub async fn stop_all(db: &EmbeddedDb) -> Result<(), NaukaError> {
    let registry = RegionRegistry::load(db).await?;
    for region in &registry.regions {
        let _ = service::stop_region(&region.region);
    }
    Ok(())
}

/// Uninstall all storage (called by hypervisor leave).
pub fn leave() -> Result<(), NaukaError> {
    service::uninstall_all()
}

/// Publish a region's S3 config to the distributed KV (TiKV).
///
/// Opens a fresh `EmbeddedDb<TiKv>` against the supplied PD endpoints,
/// makes sure the `storage_regions` SCHEMALESS catch-all table exists,
/// and UPSERTs the config under `storage_regions:{region}` wrapped in
/// a `data` field. The JSON-bridge shape mirrors what the legacy
/// `ClusterDb::put` did so joining nodes running older binaries can
/// still read rows written here.
pub async fn publish_region_config(
    pd_endpoints: &[&str],
    config: &RegionStorage,
) -> Result<(), NaukaError> {
    let db = EmbeddedDb::open_tikv(pd_endpoints)
        .await
        .map_err(|e| NaukaError::internal(format!("TiKV connect failed: {e}")))?;

    db.client()
        .query(format!(
            "DEFINE TABLE IF NOT EXISTS {REGION_TABLE} SCHEMALESS"
        ))
        .await
        .map_err(|e| NaukaError::internal(format!("DEFINE TABLE failed: {e}")))?
        .check()
        .map_err(|e| NaukaError::internal(format!("DEFINE TABLE check failed: {e}")))?;

    let data = serde_json::to_value(config)
        .map_err(|e| NaukaError::internal(format!("serialize region: {e}")))?;

    db.client()
        .query("UPSERT type::record($tbl, $id) CONTENT { data: $data }")
        .bind(("tbl", REGION_TABLE.to_string()))
        .bind(("id", config.region.clone()))
        .bind(("data", data))
        .await
        .map_err(|e| NaukaError::internal(format!("UPSERT region failed: {e}")))?
        .check()
        .map_err(|e| NaukaError::internal(format!("UPSERT region check: {e}")))?;

    tracing::info!(region = %config.region, "storage config published to cluster");
    Ok(())
}

/// Max retries for TiKV reads (leader election can take a few seconds).
const FETCH_RETRIES: u32 = 6;
const FETCH_RETRY_DELAY_MS: u64 = 2000;

/// Fetch a region's S3 config from the distributed KV (TiKV).
///
/// Retries on transient errors (e.g. "Leader not found") that occur
/// right after a node joins the cluster and Raft is still electing.
pub async fn fetch_region_config(
    pd_endpoints: &[&str],
    region: &str,
) -> Result<Option<RegionStorage>, NaukaError> {
    let mut last_err = None;
    for attempt in 1..=FETCH_RETRIES {
        match fetch_region_config_once(pd_endpoints, region).await {
            Ok(result) => return Ok(result),
            Err(e) => {
                tracing::debug!(attempt, error = %e, "TiKV read failed, retrying...");
                last_err = Some(e);
            }
        }
        if attempt < FETCH_RETRIES {
            tokio::time::sleep(std::time::Duration::from_millis(FETCH_RETRY_DELAY_MS)).await;
        }
    }
    Err(last_err.unwrap_or_else(|| NaukaError::internal("TiKV fetch failed after retries")))
}

/// Single-shot fetch used by [`fetch_region_config`]. Split out so the
/// retry loop can treat connect + read as one atomic attempt instead
/// of retrying them independently.
async fn fetch_region_config_once(
    pd_endpoints: &[&str],
    region: &str,
) -> Result<Option<RegionStorage>, NaukaError> {
    let db = EmbeddedDb::open_tikv(pd_endpoints)
        .await
        .map_err(|e| NaukaError::internal(format!("TiKV connect failed: {e}")))?;

    let mut res = db
        .client()
        .query("SELECT data FROM type::record($tbl, $id)")
        .bind(("tbl", REGION_TABLE.to_string()))
        .bind(("id", region.to_string()))
        .await
        .map_err(|e| {
            // Missing table is an expected "no rows yet" signal — the
            // first reader on a fresh cluster always races the first
            // writer. Surface that as `Ok(None)` by re-mapping below.
            NaukaError::internal(format!("SELECT region failed: {e}"))
        })?;

    let rows: Vec<serde_json::Value> = res
        .take("data")
        .map_err(|e| NaukaError::internal(format!("take region rows: {e}")))?;

    match rows.into_iter().next() {
        None => Ok(None),
        Some(data) => {
            let parsed: RegionStorage = serde_json::from_value(data)
                .map_err(|e| NaukaError::internal(format!("deserialize region: {e}")))?;
            Ok(Some(parsed))
        }
    }
}

/// Get storage status for all regions.
pub async fn status(db: &EmbeddedDb) -> Vec<RegionStatus> {
    let registry = RegionRegistry::load(db).await.unwrap_or_default();
    registry
        .regions
        .iter()
        .map(|r| RegionStatus {
            region: r.region.clone(),
            s3_endpoint: r.s3_endpoint.clone(),
            s3_bucket: r.s3_bucket.clone(),
            active: service::is_region_active(&r.region),
            is_default: r.is_default,
        })
        .collect()
}

/// Status of a region's storage.
#[derive(Debug, Clone)]
pub struct RegionStatus {
    pub region: String,
    pub s3_endpoint: String,
    pub s3_bucket: String,
    pub active: bool,
    pub is_default: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn temp_db() -> (tempfile::TempDir, EmbeddedDb) {
        let dir = tempfile::tempdir().unwrap();
        let db = EmbeddedDb::open(&dir.path().join("test.skv"))
            .await
            .unwrap();
        (dir, db)
    }

    #[tokio::test]
    async fn status_empty() {
        let (_d, db) = temp_db().await;
        let s = status(&db).await;
        assert!(s.is_empty());
        db.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn setup_validates() {
        let (_d, db) = temp_db().await;

        let bad = RegionStorage {
            region: String::new(), // invalid
            s3_endpoint: "https://s3.example.com".into(),
            s3_bucket: "bucket".into(),
            s3_access_key: "key".into(),
            s3_secret_key: "secret".into(),
            s3_region: String::new(),
            encryption_password: "test".into(),
            is_default: false,
        };
        assert!(setup_region(&db, bad).await.is_err());

        db.shutdown().await.unwrap();
    }
}
