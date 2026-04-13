//! IPv6 address allocator for NAT gateways.
//!
//! Allocates deterministic `/128` addresses from a hypervisor's
//! public `/64` block. Each NAT gateway gets a unique IPv6 derived
//! from the prefix + a SHA-256 hash of the NAT gateway id, so the
//! allocation is stable across restarts and identical across nodes
//! without coordination.
//!
//! P2.12 (sifrah/nauka#216) migrated this helper from the legacy
//! raw-KV cluster surface to the native SurrealDB SDK. The
//! allocation ledger lives in a transitional SCHEMALESS
//! `natgw_ipv6_alloc` table, keyed by the hypervisor id, with a
//! `data` column holding the JSON blob of `{ipv6, nat_gw_id}`
//! entries. The table is intentionally NOT in the
//! `apply_cluster_schemas` bundle because it's a transitional
//! single-blob-per-hypervisor shape — Phase 3 or a dedicated IPAM
//! rewrite can normalise it into one row per address.

use std::net::Ipv6Addr;

use nauka_state::EmbeddedDb;
use serde::{Deserialize, Serialize};

/// Transitional SCHEMALESS table that holds the IPv6 allocation
/// ledger. One row per hypervisor, keyed by the hypervisor's record
/// id, with a `data` column holding the JSON blob.
const IPV6_ALLOC_TABLE: &str = "natgw_ipv6_alloc";

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct Ipv6Allocations {
    entries: Vec<Ipv6Allocation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Ipv6Allocation {
    ipv6: Ipv6Addr,
    nat_gw_id: String,
}

/// Derive a deterministic IPv6 /128 from a /64 prefix and a NAT
/// gateway id.
///
/// Uses SHA-256 of the NAT GW id to fill the lower 64 bits (interface
/// id). Skips `::1` which is conventionally reserved for the host.
fn derive_ipv6(prefix_str: &str, nat_gw_id: &str) -> anyhow::Result<Ipv6Addr> {
    let net: ipnet::Ipv6Net = prefix_str
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid IPv6 block '{}': {}", prefix_str, e))?;
    if net.prefix_len() != 64 {
        anyhow::bail!("expected /64 block, got /{}", net.prefix_len());
    }

    let prefix = net.network();
    let segs = prefix.segments();

    use sha2::{Digest, Sha256};
    let hash = Sha256::digest(nat_gw_id.as_bytes());

    let iid = [
        u16::from_be_bytes([hash[0], hash[1]]),
        u16::from_be_bytes([hash[2], hash[3]]),
        u16::from_be_bytes([hash[4], hash[5]]),
        u16::from_be_bytes([hash[6], hash[7]]),
    ];

    let addr = Ipv6Addr::new(
        segs[0], segs[1], segs[2], segs[3], iid[0], iid[1], iid[2], iid[3],
    );

    // Avoid `::` (network) and `::1` (host).
    if addr == prefix || addr.segments()[4..] == [0, 0, 0, 1] {
        let addr2 = Ipv6Addr::new(
            segs[0],
            segs[1],
            segs[2],
            segs[3],
            iid[0],
            iid[1],
            iid[2],
            iid[3].wrapping_add(1),
        );
        return Ok(addr2);
    }

    Ok(addr)
}

/// Ensure the transitional SCHEMALESS `natgw_ipv6_alloc` table
/// exists. Idempotent thanks to `IF NOT EXISTS`.
async fn ensure_table(db: &EmbeddedDb) -> anyhow::Result<()> {
    db.client()
        .query("DEFINE TABLE IF NOT EXISTS natgw_ipv6_alloc SCHEMALESS")
        .await
        .map_err(|e| anyhow::anyhow!("define natgw_ipv6_alloc: {e}"))?
        .check()
        .map_err(|e| anyhow::anyhow!("define natgw_ipv6_alloc check: {e}"))?;
    Ok(())
}

/// Load the allocation blob for one hypervisor. Returns an empty
/// [`Ipv6Allocations`] if no row exists yet.
async fn load(db: &EmbeddedDb, hypervisor_id: &str) -> anyhow::Result<Ipv6Allocations> {
    ensure_table(db).await?;
    let mut response = db
        .client()
        .query("SELECT data FROM type::record($tbl, $id)")
        .bind(("tbl", IPV6_ALLOC_TABLE))
        .bind(("id", hypervisor_id.to_string()))
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let rows: Vec<serde_json::Value> = response.take("data").map_err(|e| anyhow::anyhow!("{e}"))?;
    let first = rows.into_iter().next();
    match first {
        None | Some(serde_json::Value::Null) => Ok(Ipv6Allocations::default()),
        Some(v) => serde_json::from_value::<Ipv6Allocations>(v)
            .map_err(|e| anyhow::anyhow!("deserialise ipv6 alloc blob: {e}")),
    }
}

/// Persist the allocation blob for one hypervisor. Creates the row
/// on first write via `UPSERT`.
async fn save(
    db: &EmbeddedDb,
    hypervisor_id: &str,
    allocs: &Ipv6Allocations,
) -> anyhow::Result<()> {
    ensure_table(db).await?;
    let data = serde_json::to_value(allocs)
        .map_err(|e| anyhow::anyhow!("serialise ipv6 alloc blob: {e}"))?;
    db.client()
        .query("UPSERT type::record($tbl, $id) CONTENT { data: $data }")
        .bind(("tbl", IPV6_ALLOC_TABLE))
        .bind(("id", hypervisor_id.to_string()))
        .bind(("data", data))
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?
        .check()
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(())
}

/// Allocate a public IPv6 address for a NAT gateway from the
/// hypervisor's /64 block. Idempotent: if the NAT gateway already
/// has an allocation for this hypervisor, returns the existing
/// address unchanged.
pub async fn allocate(
    db: &EmbeddedDb,
    hypervisor_id: &str,
    ipv6_block: &str,
    nat_gw_id: &str,
) -> anyhow::Result<Ipv6Addr> {
    let mut allocs = load(db, hypervisor_id).await?;

    if let Some(existing) = allocs.entries.iter().find(|a| a.nat_gw_id == nat_gw_id) {
        return Ok(existing.ipv6);
    }

    let addr = derive_ipv6(ipv6_block, nat_gw_id)?;

    // Collision check — extremely unlikely with SHA-256, but cheap.
    if allocs.entries.iter().any(|a| a.ipv6 == addr) {
        anyhow::bail!("IPv6 address collision for {addr} — this should not happen");
    }

    allocs.entries.push(Ipv6Allocation {
        ipv6: addr,
        nat_gw_id: nat_gw_id.to_string(),
    });
    save(db, hypervisor_id, &allocs).await?;
    Ok(addr)
}

/// Release the IPv6 address allocated to a NAT gateway.
pub async fn release(db: &EmbeddedDb, hypervisor_id: &str, nat_gw_id: &str) -> anyhow::Result<()> {
    let mut allocs = load(db, hypervisor_id).await?;
    allocs.entries.retain(|a| a.nat_gw_id != nat_gw_id);
    save(db, hypervisor_id, &allocs).await
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn temp_db() -> (tempfile::TempDir, EmbeddedDb) {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = EmbeddedDb::open(&dir.path().join("ipv6.skv"))
            .await
            .expect("open EmbeddedDb");
        (dir, db)
    }

    #[test]
    fn derive_ipv6_deterministic() {
        let a = derive_ipv6("2a01:4f8:c012:abcd::/64", "nat-01ABCDEF").unwrap();
        let b = derive_ipv6("2a01:4f8:c012:abcd::/64", "nat-01ABCDEF").unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn derive_ipv6_different_ids_different_addrs() {
        let a = derive_ipv6("2a01:4f8:c012:abcd::/64", "nat-01ABCDEF").unwrap();
        let b = derive_ipv6("2a01:4f8:c012:abcd::/64", "nat-02GHIJKL").unwrap();
        assert_ne!(a, b);
    }

    #[test]
    fn derive_ipv6_preserves_prefix() {
        let addr = derive_ipv6("2a01:4f8:c012:abcd::/64", "nat-test").unwrap();
        let segs = addr.segments();
        assert_eq!(segs[0], 0x2a01);
        assert_eq!(segs[1], 0x04f8);
        assert_eq!(segs[2], 0xc012);
        assert_eq!(segs[3], 0xabcd);
    }

    #[test]
    fn derive_ipv6_rejects_non_64() {
        assert!(derive_ipv6("2a01:4f8:c012::/48", "nat-test").is_err());
        assert!(derive_ipv6("2a01:4f8:c012:abcd::1/128", "nat-test").is_err());
    }

    #[tokio::test]
    async fn allocate_returns_same_ip_for_same_nat_gw() {
        let (_d, db) = temp_db().await;
        let a = allocate(&db, "hv-1", "2a01:4f8:c012:abcd::/64", "nat-01")
            .await
            .unwrap();
        let b = allocate(&db, "hv-1", "2a01:4f8:c012:abcd::/64", "nat-01")
            .await
            .unwrap();
        assert_eq!(a, b);
    }

    #[tokio::test]
    async fn release_frees_slot() {
        let (_d, db) = temp_db().await;
        let a = allocate(&db, "hv-1", "2a01:4f8:c012:abcd::/64", "nat-01")
            .await
            .unwrap();
        release(&db, "hv-1", "nat-01").await.unwrap();
        // Re-allocating the same id should reproduce the same deterministic addr.
        let b = allocate(&db, "hv-1", "2a01:4f8:c012:abcd::/64", "nat-01")
            .await
            .unwrap();
        assert_eq!(a, b);
    }

    #[tokio::test]
    async fn allocations_are_isolated_per_hypervisor() {
        let (_d, db) = temp_db().await;
        allocate(&db, "hv-1", "2a01:4f8:c012:abcd::/64", "nat-01")
            .await
            .unwrap();
        // Hypervisor 2 has a different /64 and should allocate in
        // its own space without touching hv-1's ledger.
        let b = allocate(&db, "hv-2", "2a01:4f8:c012:1234::/64", "nat-02")
            .await
            .unwrap();
        assert_eq!(b.segments()[3], 0x1234);
    }
}
