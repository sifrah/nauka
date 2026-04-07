//! Storage operations — high-level orchestration.
//!
//! Called by the hypervisor handler for storage lifecycle.

use nauka_core::error::NaukaError;
use nauka_state::LocalDb;

use super::region::{RegionRegistry, RegionStorage};
use super::service;
use crate::controlplane::ClusterDb;

/// Namespace for region storage configs in TiKV.
const TIKV_STORAGE_NS: &str = "storage/regions";

/// Setup storage for a region on this node.
///
/// 1. Validate config
/// 2. Install ZeroFS if needed
/// 3. Install config + systemd unit
/// 4. Start the service
pub fn setup_region(db: &LocalDb, config: RegionStorage) -> Result<(), NaukaError> {
    config.validate()?;

    tracing::info!(region = %config.region, "storage: setting up region");

    // Install ZeroFS binary
    service::ensure_installed()?;

    // Install config + systemd unit
    service::install_region(&config)?;

    // Start
    service::start_region(&config.region)?;

    // Persist region config
    let mut registry = RegionRegistry::load(db)?;
    registry.upsert(config.clone());
    registry.save(db)?;

    tracing::info!(region = %config.region, "storage: region ready");

    Ok(())
}

/// Remove storage for a region on this node.
pub fn remove_region(db: &LocalDb, region: &str) -> Result<(), NaukaError> {
    tracing::info!(region, "storage: removing region");

    service::uninstall_region(region)?;

    let mut registry = RegionRegistry::load(db)?;
    registry.remove(region);
    registry.save(db)?;

    Ok(())
}

/// Start all configured storage regions on this node.
pub fn start_all(db: &LocalDb) -> Result<(), NaukaError> {
    let registry = RegionRegistry::load(db)?;
    for region in &registry.regions {
        if !service::is_region_active(&region.region) {
            let _ = service::start_region(&region.region);
        }
    }
    Ok(())
}

/// Stop all storage regions.
pub fn stop_all(db: &LocalDb) -> Result<(), NaukaError> {
    let registry = RegionRegistry::load(db)?;
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
pub async fn publish_region_config(
    pd_endpoints: &[&str],
    config: &RegionStorage,
) -> Result<(), NaukaError> {
    let cluster_db = ClusterDb::connect(pd_endpoints).await?;
    cluster_db
        .put(TIKV_STORAGE_NS, &config.region, config)
        .await?;
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
        match ClusterDb::connect(pd_endpoints).await {
            Ok(db) => match db.get(TIKV_STORAGE_NS, region).await {
                Ok(result) => return Ok(result),
                Err(e) => {
                    tracing::debug!(attempt, error = %e, "TiKV read failed, retrying...");
                    last_err = Some(e);
                }
            },
            Err(e) => {
                tracing::debug!(attempt, error = %e, "TiKV connect failed, retrying...");
                last_err = Some(e);
            }
        }
        if attempt < FETCH_RETRIES {
            tokio::time::sleep(std::time::Duration::from_millis(FETCH_RETRY_DELAY_MS)).await;
        }
    }
    Err(last_err.unwrap_or_else(|| NaukaError::internal("TiKV fetch failed after retries")))
}

/// Get storage status for all regions.
pub fn status(db: &LocalDb) -> Vec<RegionStatus> {
    let registry = RegionRegistry::load(db).unwrap_or_default();
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

    #[test]
    fn status_empty() {
        let dir = tempfile::tempdir().unwrap();
        let db = LocalDb::open_at(&dir.path().join("test.json")).unwrap();
        let s = status(&db);
        assert!(s.is_empty());
    }

    #[test]
    fn setup_validates() {
        let dir = tempfile::tempdir().unwrap();
        let db = LocalDb::open_at(&dir.path().join("test.json")).unwrap();

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
        assert!(setup_region(&db, bad).is_err());
    }
}
