//! IPv6 address allocator for NAT gateways.
//!
//! Allocates /128 addresses from a hypervisor's public /64 block.
//! Each NAT gateway gets a deterministic, unique IPv6 derived from
//! the /64 prefix + a hash of the NAT gateway ID.

use std::net::Ipv6Addr;

use nauka_hypervisor::controlplane::ClusterDb;
use serde::{Deserialize, Serialize};

const NS_NATGW_IPV6: &str = "natgw-ipv6";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Ipv6Allocations {
    entries: Vec<Ipv6Allocation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Ipv6Allocation {
    ipv6: Ipv6Addr,
    nat_gw_id: String,
}

/// Derive a deterministic IPv6 /128 from a /64 prefix and a NAT gateway ID.
///
/// Uses SHA-256 of the NAT GW ID to fill the lower 64 bits (interface ID).
/// Skips ::1 which is conventionally reserved for the host.
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

    // Take 8 bytes from hash to form the interface ID (lower 64 bits)
    let iid = [
        u16::from_be_bytes([hash[0], hash[1]]),
        u16::from_be_bytes([hash[2], hash[3]]),
        u16::from_be_bytes([hash[4], hash[5]]),
        u16::from_be_bytes([hash[6], hash[7]]),
    ];

    let addr = Ipv6Addr::new(
        segs[0], segs[1], segs[2], segs[3], iid[0], iid[1], iid[2], iid[3],
    );

    // Avoid ::1 (host) and :: (network)
    if addr == prefix || addr.segments()[4..] == [0, 0, 0, 1] {
        // Extremely unlikely with SHA-256, but handle it
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

/// Allocate a public IPv6 address for a NAT gateway from the hypervisor's /64 block.
pub async fn allocate(
    db: &ClusterDb,
    hypervisor_id: &str,
    ipv6_block: &str,
    nat_gw_id: &str,
) -> anyhow::Result<Ipv6Addr> {
    let mut allocs = load(db, hypervisor_id).await?;

    // Check if already allocated
    if let Some(existing) = allocs.entries.iter().find(|a| a.nat_gw_id == nat_gw_id) {
        return Ok(existing.ipv6);
    }

    let addr = derive_ipv6(ipv6_block, nat_gw_id)?;

    // Check for collision (extremely unlikely with SHA-256)
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
pub async fn release(db: &ClusterDb, hypervisor_id: &str, nat_gw_id: &str) -> anyhow::Result<()> {
    let mut allocs = load(db, hypervisor_id).await?;
    allocs.entries.retain(|a| a.nat_gw_id != nat_gw_id);
    save(db, hypervisor_id, &allocs).await
}

async fn load(db: &ClusterDb, hypervisor_id: &str) -> anyhow::Result<Ipv6Allocations> {
    let allocs: Option<Ipv6Allocations> = db.get(NS_NATGW_IPV6, hypervisor_id).await?;
    Ok(allocs.unwrap_or(Ipv6Allocations {
        entries: Vec::new(),
    }))
}

async fn save(db: &ClusterDb, hypervisor_id: &str, allocs: &Ipv6Allocations) -> anyhow::Result<()> {
    db.put(NS_NATGW_IPV6, hypervisor_id, allocs).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
