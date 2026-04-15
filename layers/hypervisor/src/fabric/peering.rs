//! Peering protocol — TCP-based join/accept for mesh membership.
//!
//! When a new node wants to join:
//! 1. Initiator connects to target's peering port (TCP)
//! 2. Sends JoinRequest (name, public key, endpoint, PIN)
//! 3. Target validates PIN (or manual approval)
//! 4. Target sends JoinResponse (mesh secret, prefix, peer list)
//! 5. Initiator configures WireGuard and joins the mesh
//! 6. Target announces new peer to all existing peers

use serde::{Deserialize, Serialize};
use std::net::Ipv6Addr;

/// Default peering port (WireGuard port + 1).
pub const DEFAULT_PEERING_PORT: u16 = 51821;

/// A join request sent by a new node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinRequest {
    /// Name of the joining node.
    pub name: String,
    /// Region label.
    pub region: String,
    /// Zone label.
    pub zone: String,
    /// WireGuard public key (base64).
    pub wg_public_key: String,
    /// WireGuard listen port.
    pub wg_port: u16,
    /// Public endpoint (IP:port) for WireGuard.
    pub endpoint: Option<String>,
    /// PIN for auto-accept (if provided).
    pub pin: Option<String>,
    /// Trace ID for distributed log correlation.
    #[serde(default)]
    pub trace_id: Option<String>,
}

/// A join response sent by the target node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinResponse {
    /// Whether the join was accepted.
    pub accepted: bool,
    /// Reason for rejection (if not accepted).
    pub reason: Option<String>,
    /// Mesh name.

    /// Mesh secret (only if accepted).
    pub secret: Option<String>,
    /// Mesh prefix (only if accepted).
    pub prefix: Option<Ipv6Addr>,
    /// Mesh ID (only if accepted).
    #[serde(default)]
    pub mesh_id: Option<String>,
    /// Existing peers in the mesh (only if accepted).
    pub peers: Vec<PeerInfo>,
    /// The accepting node's info.
    pub acceptor: Option<PeerInfo>,
    /// Maximum PD members configured for this mesh (1, 3, 5, 7).
    #[serde(default = "default_max_pd_members")]
    pub max_pd_members: usize,
}

fn default_max_pd_members() -> usize {
    3
}

/// Minimal peer information exchanged during join.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerInfo {
    pub name: String,
    pub region: String,
    pub zone: String,
    pub wg_public_key: String,
    pub wg_port: u16,
    pub endpoint: Option<String>,
    pub mesh_ipv6: Ipv6Addr,
}

/// Peer announcement — broadcast to all peers when a new node joins.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerAnnounce {
    pub peer: PeerInfo,
    /// Who sent this announcement.
    pub announced_by: String,
}

/// Peer removal — broadcast to all peers when a node leaves.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerRemove {
    /// Name of the leaving node.
    pub name: String,
    /// WireGuard public key of the leaving node.
    pub wg_public_key: String,
}

/// Promote a TiKV-only peer to a full PD member.
///
/// Sent over the announce protocol when the cluster scales PD from N→N+2
/// (e.g. 1→3, 3→5). The receiving node validates that `target_name` matches
/// its own name, then installs and starts its local PD service in `--join`
/// mode against `primary_pd_url`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PromoteToPd {
    /// Name of the node being promoted — receiver MUST match this.
    pub target_name: String,
    /// Primary PD URL to --join against (existing bootstrap PD).
    pub primary_pd_url: String,
    /// Full PD endpoint list (after scale-up) for the TiKV config rewrite.
    pub pd_endpoints: Vec<String>,
    /// Node name that initiated the scale-up (for audit trail).
    pub requested_by: String,
}

/// Node state change — broadcast when a node enters/exits maintenance mode.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateChange {
    /// Name of the node whose state changed.
    pub name: String,
    /// WireGuard public key (identifies the node).
    pub wg_public_key: String,
    /// New scheduling state ("available" or "draining").
    pub node_state: super::state::NodeState,
}

impl JoinResponse {
    /// Create an accepted response.
    pub fn accepted(
        secret: &str,
        prefix: Ipv6Addr,
        mesh_id: &str,
        peers: Vec<PeerInfo>,
        acceptor: PeerInfo,
        max_pd_members: usize,
    ) -> Self {
        Self {
            accepted: true,
            reason: None,
            secret: Some(secret.to_string()),
            prefix: Some(prefix),
            mesh_id: Some(mesh_id.to_string()),
            peers,
            acceptor: Some(acceptor),
            max_pd_members,
        }
    }

    /// Create a rejected response.
    pub fn rejected(reason: &str) -> Self {
        Self {
            accepted: false,
            reason: Some(reason.to_string()),
            secret: None,
            prefix: None,
            mesh_id: None,
            peers: Vec::new(),
            acceptor: None,
            max_pd_members: 3,
        }
    }
}

/// Validate a PIN against the expected PIN.
pub fn validate_pin(expected: &str, provided: Option<&str>) -> bool {
    match provided {
        Some(pin) => pin == expected,
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn join_request_serde() {
        let req = JoinRequest {
            name: "node-2".into(),
            region: "eu".into(),
            zone: "nbg1".into(),
            wg_public_key: "abc123".into(),
            wg_port: 51820,
            endpoint: Some("1.2.3.4:51820".into()),
            pin: Some("4829".into()),
            trace_id: Some("abcdef0123456789".into()),
        };
        let json = serde_json::to_string(&req).unwrap();
        let back: JoinRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(back.name, "node-2");
        assert_eq!(back.pin, Some("4829".into()));
        assert_eq!(back.trace_id, Some("abcdef0123456789".into()));
    }

    #[test]
    fn join_response_accepted() {
        let resp = JoinResponse::accepted(
            "syf_sk_test",
            "fd01::".parse().unwrap(),
            "mesh-test",
            vec![],
            PeerInfo {
                name: "node-1".into(),
                region: "eu".into(),
                zone: "fsn1".into(),
                wg_public_key: "key1".into(),
                wg_port: 51820,
                endpoint: Some("1.2.3.4:51820".into()),
                mesh_ipv6: "fd01::1".parse().unwrap(),
            },
            3,
        );
        assert!(resp.accepted);
        assert!(resp.secret.is_some());
        assert!(resp.acceptor.is_some());
    }

    #[test]
    fn join_response_rejected() {
        let resp = JoinResponse::rejected("invalid PIN");
        assert!(!resp.accepted);
        assert_eq!(resp.reason.as_deref(), Some("invalid PIN"));
        assert!(resp.secret.is_none());
        assert!(resp.peers.is_empty());
    }

    #[test]
    fn join_response_serde() {
        let resp = JoinResponse::accepted(
            "secret",
            "fd01::".parse().unwrap(),
            "mesh-test",
            vec![PeerInfo {
                name: "p1".into(),
                region: "eu".into(),
                zone: "fsn1".into(),
                wg_public_key: "k1".into(),
                wg_port: 51820,
                endpoint: None,
                mesh_ipv6: "fd01::1".parse().unwrap(),
            }],
            PeerInfo {
                name: "init".into(),
                region: "eu".into(),
                zone: "fsn1".into(),
                wg_public_key: "k0".into(),
                wg_port: 51820,
                endpoint: Some("5.6.7.8:51820".into()),
                mesh_ipv6: "fd01::0".parse().unwrap(),
            },
            5,
        );
        let json = serde_json::to_string(&resp).unwrap();
        let back: JoinResponse = serde_json::from_str(&json).unwrap();
        assert!(back.accepted);
        assert_eq!(back.peers.len(), 1);
        assert_eq!(back.acceptor.unwrap().name, "init");
    }

    #[test]
    fn peer_announce_serde() {
        let ann = PeerAnnounce {
            peer: PeerInfo {
                name: "new-node".into(),
                region: "eu".into(),
                zone: "nbg1".into(),
                wg_public_key: "newkey".into(),
                wg_port: 51820,
                endpoint: Some("9.8.7.6:51820".into()),
                mesh_ipv6: "fd01::99".parse().unwrap(),
            },
            announced_by: "node-1".into(),
        };
        let json = serde_json::to_string(&ann).unwrap();
        let back: PeerAnnounce = serde_json::from_str(&json).unwrap();
        assert_eq!(back.peer.name, "new-node");
        assert_eq!(back.announced_by, "node-1");
    }

    #[test]
    fn validate_pin_correct() {
        assert!(validate_pin("4829", Some("4829")));
    }

    #[test]
    fn validate_pin_wrong() {
        assert!(!validate_pin("4829", Some("0000")));
    }

    #[test]
    fn validate_pin_missing() {
        assert!(!validate_pin("4829", None));
    }

    #[test]
    fn peer_info_minimal() {
        let p = PeerInfo {
            name: "n1".into(),
            region: "eu".into(),
            zone: "fsn1".into(),
            wg_public_key: "key".into(),
            wg_port: 51820,
            endpoint: None,
            mesh_ipv6: "fd01::1".parse().unwrap(),
        };
        assert_eq!(p.name, "n1");
        assert!(p.endpoint.is_none());
    }

    #[test]
    fn join_request_backward_compat_no_trace_id() {
        // Old clients won't send trace_id — serde(default) should handle it
        let json = r#"{"name":"old-node","region":"eu","zone":"fsn1","wg_public_key":"k","wg_port":51820,"endpoint":null,"pin":null}"#;
        let req: JoinRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.name, "old-node");
        assert!(req.trace_id.is_none());
    }
}
