//! IPAM — IP Address Management for subnets.
//!
//! Allocates private IPv4 addresses from a subnet's CIDR range. Each
//! subnet gets one row in the SCHEMALESS `subnet_ipam` table holding
//! the list of `(ip, vm_id)` allocations as a JSON blob; that blob is
//! swapped on every allocate/release.
//!
//! P2.12 (sifrah/nauka#216) migrated this helper from the legacy
//! raw-KV cluster surface to the native SurrealDB SDK. The
//! `subnet_ipam` table is deliberately SCHEMALESS (not in the
//! `apply_cluster_schemas` bundle) because it's a transitional
//! single-blob-per-subnet shape — Phase 3 codegen or a dedicated
//! IPAM rewrite may normalise it into a `subnet_ip` table with one
//! row per allocated address, at which point this helper goes away.
//!
//! Reserved addresses (skipped by the allocator):
//! - `.0`  = network address
//! - `.1`  = gateway (assigned to the bridge)
//! - `.N`  = any IP already present in the allocation list

use nauka_state::EmbeddedDb;
use serde::{Deserialize, Serialize};

/// Transitional SCHEMALESS table that holds the IPAM state. One row
/// per subnet, keyed by the subnet's record id, with a `data` column
/// containing the JSON blob of allocations.
const IPAM_TABLE: &str = "subnet_ipam";

/// Allocated IPs for a subnet, stored as a single JSON blob per row.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct Allocations {
    ips: Vec<Allocation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Allocation {
    ip: String,
    vm_id: String,
}

/// Ensure the transitional SCHEMALESS `subnet_ipam` table exists.
/// Idempotent thanks to `IF NOT EXISTS`.
async fn ensure_table(db: &EmbeddedDb) -> anyhow::Result<()> {
    db.client()
        .query("DEFINE TABLE IF NOT EXISTS subnet_ipam SCHEMALESS")
        .await
        .map_err(|e| anyhow::anyhow!("define subnet_ipam: {e}"))?
        .check()
        .map_err(|e| anyhow::anyhow!("define subnet_ipam check: {e}"))?;
    Ok(())
}

/// Load the allocation blob for one subnet. Returns an empty
/// [`Allocations`] if no row exists yet.
async fn load(db: &EmbeddedDb, subnet_id: &str) -> anyhow::Result<Allocations> {
    ensure_table(db).await?;
    let mut response = db
        .client()
        .query("SELECT data FROM type::record($tbl, $id)")
        .bind(("tbl", IPAM_TABLE))
        .bind(("id", subnet_id.to_string()))
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    // SurrealDB returns one row per record; `.take("data")` pulls
    // the `data` column into a flat `Vec<serde_json::Value>`.
    let rows: Vec<serde_json::Value> = response.take("data").map_err(|e| anyhow::anyhow!("{e}"))?;
    let first = rows.into_iter().next();
    match first {
        None | Some(serde_json::Value::Null) => Ok(Allocations::default()),
        Some(v) => serde_json::from_value::<Allocations>(v)
            .map_err(|e| anyhow::anyhow!("deserialise ipam blob: {e}")),
    }
}

/// Persist the allocation blob for one subnet. Creates the row on
/// first write via `UPSERT`.
async fn save(db: &EmbeddedDb, subnet_id: &str, allocs: &Allocations) -> anyhow::Result<()> {
    ensure_table(db).await?;
    let data =
        serde_json::to_value(allocs).map_err(|e| anyhow::anyhow!("serialise ipam blob: {e}"))?;
    db.client()
        .query("UPSERT type::record($tbl, $id) CONTENT { data: $data }")
        .bind(("tbl", IPAM_TABLE))
        .bind(("id", subnet_id.to_string()))
        .bind(("data", data))
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?
        .check()
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(())
}

/// Allocate the next available IP from a subnet.
///
/// Skips the network address (`.0`), the gateway, and any IPs that
/// are already allocated. If the VM already has an allocation in
/// this subnet, returns the existing IP unchanged (idempotent).
pub async fn allocate(
    db: &EmbeddedDb,
    subnet_id: &str,
    subnet_cidr: &str,
    gateway: &str,
    vm_id: &str,
) -> anyhow::Result<String> {
    let net: ipnet::Ipv4Net = subnet_cidr
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid subnet CIDR: {e}"))?;

    let mut allocs = load(db, subnet_id).await?;

    // Check if this VM already has an IP in this subnet.
    if let Some(existing) = allocs.ips.iter().find(|a| a.vm_id == vm_id) {
        return Ok(existing.ip.clone());
    }

    let allocated_ips: Vec<&str> = allocs.ips.iter().map(|a| a.ip.as_str()).collect();

    for host_ip in net.hosts() {
        let ip_str = host_ip.to_string();
        if ip_str == gateway {
            continue;
        }
        if allocated_ips.contains(&ip_str.as_str()) {
            continue;
        }
        allocs.ips.push(Allocation {
            ip: ip_str.clone(),
            vm_id: vm_id.to_string(),
        });
        save(db, subnet_id, &allocs).await?;
        return Ok(ip_str);
    }

    anyhow::bail!(
        "no IPs available in subnet {} (all {} addresses allocated)",
        subnet_cidr,
        allocs.ips.len()
    )
}

/// Release the IP allocation for a VM.
pub async fn release(db: &EmbeddedDb, subnet_id: &str, vm_id: &str) -> anyhow::Result<()> {
    let mut allocs = load(db, subnet_id).await?;
    allocs.ips.retain(|a| a.vm_id != vm_id);
    save(db, subnet_id, &allocs).await
}

/// Get the allocated IP for a VM in a subnet, or `None` if no
/// allocation exists.
pub async fn get_allocation(
    db: &EmbeddedDb,
    subnet_id: &str,
    vm_id: &str,
) -> anyhow::Result<Option<String>> {
    let allocs = load(db, subnet_id).await?;
    Ok(allocs
        .ips
        .iter()
        .find(|a| a.vm_id == vm_id)
        .map(|a| a.ip.clone()))
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn temp_db() -> (tempfile::TempDir, EmbeddedDb) {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = EmbeddedDb::open(&dir.path().join("ipam.skv"))
            .await
            .expect("open EmbeddedDb");
        (dir, db)
    }

    #[test]
    fn allocations_serde_roundtrip() {
        let allocs = Allocations {
            ips: vec![
                Allocation {
                    ip: "10.0.1.2".to_string(),
                    vm_id: "vm-01".to_string(),
                },
                Allocation {
                    ip: "10.0.1.3".to_string(),
                    vm_id: "vm-02".to_string(),
                },
            ],
        };
        let json = serde_json::to_string(&allocs).unwrap();
        let back: Allocations = serde_json::from_str(&json).unwrap();
        assert_eq!(back.ips.len(), 2);
        assert_eq!(back.ips[0].ip, "10.0.1.2");
    }

    #[tokio::test]
    async fn allocate_first_ip_skips_gateway() {
        let (_d, db) = temp_db().await;
        let ip = allocate(&db, "subnet-test", "10.0.1.0/24", "10.0.1.1", "vm-01")
            .await
            .unwrap();
        assert_eq!(ip, "10.0.1.2");
    }

    #[tokio::test]
    async fn allocate_is_idempotent_per_vm() {
        let (_d, db) = temp_db().await;
        let ip_a = allocate(&db, "subnet-test", "10.0.1.0/24", "10.0.1.1", "vm-01")
            .await
            .unwrap();
        let ip_b = allocate(&db, "subnet-test", "10.0.1.0/24", "10.0.1.1", "vm-01")
            .await
            .unwrap();
        assert_eq!(ip_a, ip_b);
    }

    #[tokio::test]
    async fn allocate_multiple_vms_returns_distinct_ips() {
        let (_d, db) = temp_db().await;
        let a = allocate(&db, "subnet-test", "10.0.1.0/24", "10.0.1.1", "vm-01")
            .await
            .unwrap();
        let b = allocate(&db, "subnet-test", "10.0.1.0/24", "10.0.1.1", "vm-02")
            .await
            .unwrap();
        assert_ne!(a, b);
    }

    #[tokio::test]
    async fn release_frees_ip_for_reuse() {
        let (_d, db) = temp_db().await;
        allocate(&db, "subnet-test", "10.0.1.0/24", "10.0.1.1", "vm-01")
            .await
            .unwrap();
        release(&db, "subnet-test", "vm-01").await.unwrap();
        let again = allocate(&db, "subnet-test", "10.0.1.0/24", "10.0.1.1", "vm-02")
            .await
            .unwrap();
        // vm-02 should be able to grab the first available slot
        // (10.0.1.2), not a later one, because vm-01's release
        // cleared the slot.
        assert_eq!(again, "10.0.1.2");
    }

    #[tokio::test]
    async fn get_allocation_returns_none_for_unknown_vm() {
        let (_d, db) = temp_db().await;
        let got = get_allocation(&db, "subnet-test", "vm-01").await.unwrap();
        assert!(got.is_none());
    }
}
