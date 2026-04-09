//! IPAM — IP Address Management for subnets.
//!
//! Allocates private IPs from a subnet's CIDR range. Uses a simple
//! list of allocated IPs stored in TiKV (one key per subnet).
//!
//! Reserved addresses:
//! - .0 = network address
//! - .1 = gateway (assigned to the bridge)
//! - .255 = broadcast (for /24)

use nauka_hypervisor::controlplane::ClusterDb;

const NS_IPAM: &str = "ipam";

/// Allocated IPs for a subnet, stored as a list in TiKV.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Default)]
struct Allocations {
    ips: Vec<Allocation>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct Allocation {
    ip: String,
    vm_id: String,
}

/// Allocate the next available IP from a subnet.
///
/// Skips network (.0), gateway (.1), and broadcast addresses.
/// Returns the allocated IP as a string (e.g., "10.0.1.2").
pub async fn allocate(
    db: &ClusterDb,
    subnet_id: &str,
    subnet_cidr: &str,
    gateway: &str,
    vm_id: &str,
) -> anyhow::Result<String> {
    let net: ipnet::Ipv4Net = subnet_cidr
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid subnet CIDR: {e}"))?;

    // Load current allocations
    let mut allocs: Allocations = db.get(NS_IPAM, subnet_id).await?.unwrap_or_default();

    // Check if this VM already has an IP
    if let Some(existing) = allocs.ips.iter().find(|a| a.vm_id == vm_id) {
        return Ok(existing.ip.clone());
    }

    let allocated_ips: Vec<&str> = allocs.ips.iter().map(|a| a.ip.as_str()).collect();

    // Find next available host IP
    for host_ip in net.hosts() {
        let ip_str = host_ip.to_string();

        // Skip gateway
        if ip_str == gateway {
            continue;
        }

        // Skip already allocated
        if allocated_ips.contains(&ip_str.as_str()) {
            continue;
        }

        // Found one — allocate it
        allocs.ips.push(Allocation {
            ip: ip_str.clone(),
            vm_id: vm_id.to_string(),
        });
        db.put(NS_IPAM, subnet_id, &allocs).await?;

        return Ok(ip_str);
    }

    anyhow::bail!(
        "no IPs available in subnet {} (all {} addresses allocated)",
        subnet_cidr,
        allocs.ips.len()
    )
}

/// Release an IP allocation for a VM.
pub async fn release(db: &ClusterDb, subnet_id: &str, vm_id: &str) -> anyhow::Result<()> {
    let mut allocs: Allocations = db.get(NS_IPAM, subnet_id).await?.unwrap_or_default();

    allocs.ips.retain(|a| a.vm_id != vm_id);
    db.put(NS_IPAM, subnet_id, &allocs).await?;

    Ok(())
}

/// Get the allocated IP for a VM in a subnet.
pub async fn get_allocation(
    db: &ClusterDb,
    subnet_id: &str,
    vm_id: &str,
) -> anyhow::Result<Option<String>> {
    let allocs: Allocations = db.get(NS_IPAM, subnet_id).await?.unwrap_or_default();

    Ok(allocs
        .ips
        .iter()
        .find(|a| a.vm_id == vm_id)
        .map(|a| a.ip.clone()))
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
