//! VM scheduler — assigns VMs to hypervisors.
//!
//! Minimal implementation: picks the current node (or filters by zone).
//! Future: capacity-based scoring, anti-affinity, taints/tolerations.

use nauka_hypervisor::fabric;
use nauka_state::LocalDb;

/// A candidate hypervisor for VM placement.
#[derive(Debug)]
#[allow(dead_code)]
struct Candidate {
    id: String,
    name: String,
    region: String,
    zone: String,
}

/// Schedule a VM to a hypervisor. Returns the hypervisor ID.
///
/// Reads the local fabric state to build the list of available nodes.
/// Filters by region/zone if specified. Picks the first match.
pub fn schedule(region: &str, zone: &str) -> anyhow::Result<String> {
    let dir = nauka_core::process::nauka_dir();
    let _ = std::fs::create_dir_all(&dir);
    let db = LocalDb::open("hypervisor")?;

    let state = fabric::state::FabricState::load(&db)
        .map_err(|e| anyhow::anyhow!("{e}"))?
        .ok_or_else(|| anyhow::anyhow!("cluster not initialized"))?;

    // Build candidate list: self + peers
    let mut candidates = vec![Candidate {
        id: state.hypervisor.id.as_str().to_string(),
        name: state.hypervisor.name.clone(),
        region: state.hypervisor.region.clone(),
        zone: state.hypervisor.zone.clone(),
    }];

    for peer in &state.peers.peers {
        if peer.status == fabric::peer::PeerStatus::Active {
            candidates.push(Candidate {
                id: peer.id.as_str().to_string(),
                name: peer.name.clone(),
                region: peer.region.clone(),
                zone: peer.zone.clone(),
            });
        }
    }

    // Filter by zone (if not "default")
    if zone != "default" {
        let filtered: Vec<&Candidate> = candidates.iter().filter(|c| c.zone == zone).collect();
        if filtered.is_empty() {
            anyhow::bail!("no hypervisor available in zone '{zone}'");
        }
        // Pick first match (future: least loaded)
        return Ok(filtered[0].id.clone());
    }

    // Filter by region (if not "default")
    if region != "default" {
        let filtered: Vec<&Candidate> = candidates.iter().filter(|c| c.region == region).collect();
        if filtered.is_empty() {
            anyhow::bail!("no hypervisor available in region '{region}'");
        }
        return Ok(filtered[0].id.clone());
    }

    // No filter — pick first (self)
    Ok(candidates[0].id.clone())
}

#[cfg(test)]
mod tests {
    #[test]
    fn schedule_without_cluster_returns_error() {
        // Without a cluster, schedule should fail gracefully
        let result = super::schedule("default", "default");
        assert!(result.is_err());
    }
}
