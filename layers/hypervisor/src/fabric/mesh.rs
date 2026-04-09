//! Mesh identity and configuration.
//!
//! A mesh is defined by:
//! - A shared secret (syf_sk_...)
//! - A /48 ULA IPv6 prefix
//! - Each node gets a /128 address derived from the prefix + its WG key

use std::net::Ipv6Addr;

use nauka_core::addressing;
use nauka_core::crypto::{self, MeshSecret};
use nauka_core::id::MeshId;
use serde::{Deserialize, Serialize};

/// Complete mesh identity — everything needed to describe a mesh.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeshIdentity {
    /// Unique mesh ID.
    pub id: MeshId,
    /// The /48 ULA prefix for this mesh.
    pub prefix: Ipv6Addr,
}

/// A node's identity within the mesh.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HypervisorIdentity {
    /// Unique hypervisor ID.
    pub id: nauka_core::id::HypervisorId,
    /// Human-readable node name (usually hostname).
    pub name: String,
    /// Region label.
    pub region: String,
    /// Zone label.
    pub zone: String,
    /// WireGuard private key (base64).
    /// Persisted locally but never shared with peers.
    #[serde(default)]
    pub wg_private_key: String,
    /// WireGuard public key (base64).
    pub wg_public_key: String,
    /// WireGuard listen port.
    pub wg_port: u16,
    /// Public endpoint (IP:port) for other nodes to connect via WG.
    pub endpoint: Option<String>,
    /// Fabric network interface (e.g., "eth1"). Empty = auto.
    #[serde(default)]
    pub fabric_interface: String,
    /// This node's mesh IPv6 address (/128).
    /// In WG mode: ULA derived from prefix + pubkey.
    /// In direct mode: real IP of the fabric interface.
    pub mesh_ipv6: Ipv6Addr,
    /// Compute runtime: "kvm" (Cloud Hypervisor) or "container" (gVisor).
    #[serde(default = "default_runtime")]
    pub runtime: String,
    /// Public IPv6 /64 block allocated by the hosting provider (e.g., "2a01:4f8:c012:abcd::/64").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ipv6_block: Option<String>,
    /// Public IPv4 address of this server.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ipv4_public: Option<String>,
}

fn default_runtime() -> String {
    "kvm".to_string()
}

/// Create a new mesh (called by `hypervisor init`).
pub fn create_mesh() -> (MeshIdentity, MeshSecret) {
    let secret = MeshSecret::generate();
    let prefix = addressing::generate_mesh_prefix();
    let id = MeshId::generate();

    let mesh = MeshIdentity { id, prefix };

    (mesh, secret)
}

/// Configuration for creating a new hypervisor identity.
pub struct CreateHypervisorConfig<'a> {
    pub name: &'a str,
    pub region: &'a str,
    pub zone: &'a str,
    pub port: u16,
    pub endpoint: Option<String>,
    pub fabric_interface: &'a str,
    pub mesh_prefix: &'a Ipv6Addr,
    pub ipv6_block: Option<String>,
    pub ipv4_public: Option<String>,
}

/// Create a new node identity (called by both init and join).
/// Validates name, region, zone, and port.
pub fn create_hypervisor(
    cfg: &CreateHypervisorConfig<'_>,
) -> Result<HypervisorIdentity, nauka_core::error::NaukaError> {
    nauka_core::validate::name(cfg.name)?;
    nauka_core::validate::region(cfg.region)?;
    nauka_core::validate::zone(cfg.zone)?;
    nauka_core::validate::port(cfg.port)?;

    let (wg_private, wg_public) = crypto::generate_wg_keypair();

    let pub_bytes = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, &wg_public)
        .unwrap_or_default();

    let mesh_ipv6 = addressing::derive_node_address(cfg.mesh_prefix, &pub_bytes);

    // Detect compute runtime: KVM if /dev/kvm exists, container (gVisor) otherwise
    let runtime = if std::path::Path::new("/dev/kvm").exists() {
        "kvm".to_string()
    } else {
        "container".to_string()
    };

    Ok(HypervisorIdentity {
        id: nauka_core::id::HypervisorId::generate(),
        name: cfg.name.to_string(),
        region: cfg.region.to_string(),
        zone: cfg.zone.to_string(),
        wg_private_key: wg_private,
        wg_public_key: wg_public,
        wg_port: cfg.port,
        endpoint: cfg.endpoint.clone(),
        fabric_interface: cfg.fabric_interface.to_string(),
        mesh_ipv6,
        runtime,
        ipv6_block: cfg.ipv6_block.clone(),
        ipv4_public: cfg.ipv4_public.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test helper: create a hypervisor with minimal defaults.
    fn make_hv(name: &str, region: &str, zone: &str, port: u16, endpoint: Option<String>, prefix: &Ipv6Addr) -> Result<HypervisorIdentity, nauka_core::error::NaukaError> {
        create_hypervisor(&CreateHypervisorConfig {
            name, region, zone, port, endpoint, fabric_interface: "", mesh_prefix: prefix,
            ipv6_block: None, ipv4_public: None,
        })
    }

    #[test]
    fn create_mesh_generates_valid_identity() {
        let (mesh, secret) = create_mesh();
        assert!(mesh.id.as_str().starts_with("mesh-"));
        assert!(secret.to_string().starts_with("syf_sk_"));
        let first = mesh.prefix.segments()[0];
        assert!((0xfd00..=0xfdff).contains(&first));
    }

    #[test]
    fn create_mesh_unique() {
        let (a, _) = create_mesh();
        let (b, _) = create_mesh();
        assert_ne!(a.id.as_str(), b.id.as_str());
    }

    #[test]
    fn create_node_has_valid_identity() {
        let (mesh, _) = create_mesh();
        let node =
            make_hv("node-1", "eu", "fsn1", 51820, None, &mesh.prefix).unwrap();

        assert_eq!(node.name, "node-1");
        assert_eq!(node.region, "eu");
        assert_eq!(node.zone, "fsn1");
        assert_eq!(node.wg_port, 51820);
        assert!(!node.wg_private_key.is_empty());
        assert!(!node.wg_public_key.is_empty());
        assert!(addressing::is_in_prefix(&node.mesh_ipv6, &mesh.prefix));
    }

    #[test]
    fn create_node_unique_keys() {
        let (mesh, _) = create_mesh();
        let a = make_hv("node-aaa", "eu", "fsn1", 51820, None, &mesh.prefix).unwrap();
        let b = make_hv("node-bbb", "eu", "fsn1", 51820, None, &mesh.prefix).unwrap();
        assert_ne!(a.wg_public_key, b.wg_public_key);
        assert_ne!(a.mesh_ipv6, b.mesh_ipv6);
    }

    #[test]
    fn create_node_with_endpoint() {
        let (mesh, _) = create_mesh();
        let node = make_hv("node-1", "eu", "fsn1", 51820, Some("46.224.166.60:51820".into()), &mesh.prefix).unwrap();
        assert_eq!(node.endpoint, Some("46.224.166.60:51820".into()));
    }

    #[test]
    fn node_identity_serde_roundtrip() {
        let (mesh, _) = create_mesh();
        let node =
            make_hv("node-1", "eu", "fsn1", 51820, None, &mesh.prefix).unwrap();
        let json = serde_json::to_string(&node).unwrap();
        let back: HypervisorIdentity = serde_json::from_str(&json).unwrap();
        assert_eq!(back.name, "node-1");
        assert!(json.contains(&node.wg_public_key));
    }

    // ── #1: Validation tests ──

    #[test]
    fn create_node_rejects_empty_name() {
        let (mesh, _) = create_mesh();
        assert!(make_hv("", "eu", "fsn1", 51820, None, &mesh.prefix).is_err());
    }

    #[test]
    fn create_node_rejects_bad_region() {
        let (mesh, _) = create_mesh();
        assert!(make_hv("node-1", "EU!", "fsn1", 51820, None, &mesh.prefix).is_err());
    }

    #[test]
    fn create_node_rejects_bad_zone() {
        let (mesh, _) = create_mesh();
        assert!(make_hv("node-1", "eu", "FSN 1", 51820, None, &mesh.prefix).is_err());
    }

    #[test]
    fn create_node_rejects_port_zero() {
        let (mesh, _) = create_mesh();
        assert!(make_hv("node-1", "eu", "fsn1", 0, None, &mesh.prefix).is_err());
    }

    // ── #5: Private key persistence ──

    #[test]
    fn private_key_survives_serde() {
        let (mesh, _) = create_mesh();
        let node =
            make_hv("node-1", "eu", "fsn1", 51820, None, &mesh.prefix).unwrap();
        let original_private = node.wg_private_key.clone();
        assert!(!original_private.is_empty());

        let json = serde_json::to_string(&node).unwrap();
        let back: HypervisorIdentity = serde_json::from_str(&json).unwrap();
        assert_eq!(back.wg_private_key, original_private);
    }

    #[test]
    fn private_key_default_when_missing() {
        // Simulate receiving peer info without private key
        let json = r#"{"id":"hv-test","name":"n1","region":"eu","zone":"fsn1","wg_public_key":"abc","wg_port":51820,"mesh_ipv6":"fd01::1"}"#;
        let node: HypervisorIdentity = serde_json::from_str(json).unwrap();
        assert_eq!(node.wg_private_key, ""); // defaults to empty
        assert_eq!(node.name, "n1");
    }

    // ── #2: Limits ──

    #[test]
    fn create_node_long_name() {
        let (mesh, _) = create_mesh();
        let long_name = "a".repeat(63); // max allowed
        assert!(make_hv(&long_name, "eu", "fsn1", 51820, None, &mesh.prefix).is_ok());

        let too_long = "a".repeat(64);
        assert!(make_hv(&too_long, "eu", "fsn1", 51820, None, &mesh.prefix).is_err());
    }

    #[test]
    fn mesh_identity_serde() {
        let (mesh, _) = create_mesh();
        let json = serde_json::to_string(&mesh).unwrap();
        let back: MeshIdentity = serde_json::from_str(&json).unwrap();
        assert_eq!(back.id.as_str(), mesh.id.as_str());
        assert_eq!(back.prefix, mesh.prefix);
    }
}
