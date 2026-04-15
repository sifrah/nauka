//! Wire protocol for the hypervisor daemon control socket.
//!
//! One request → one response, length-prefixed JSON. No streaming, no
//! subscriptions. Adding a new operation is a matter of adding a new
//! variant to [`ControlRequest`] plus a match arm in
//! [`super::server::dispatch`].

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// Path of the hypervisor daemon's control socket.
///
/// Proxies to [`nauka_core::process::socket_path`] so CLI and daemon
/// agree on the same run-mode-aware location.
pub fn socket_path() -> PathBuf {
    nauka_core::process::socket_path()
}

/// Every operation the CLI can ask the daemon to perform on its
/// behalf. Variants are ordered roughly from trivial (liveness check)
/// to impactful (shutdown).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum ControlRequest {
    /// Zero-arg liveness probe. The client uses it to decide whether
    /// to forward or fall back to a direct `EmbeddedDb` open.
    Ping,
    /// `nauka hypervisor status` — local hypervisor + WG + CP summary.
    Status,
    /// `nauka hypervisor list` — hypervisor row + every peer row.
    List,
    /// `nauka hypervisor get <name>` — single hypervisor lookup.
    Get { name: String },
    /// `nauka hypervisor cp-status` — asks the daemon for the current
    /// `mesh_ipv6`, then the CLI does its own PD HTTP round trips
    /// against it. Not a full "forward everything" path because the
    /// formatting is terminal-side.
    MeshIpv6,
    /// `nauka hypervisor drain` — flip this node to draining + broadcast.
    Drain,
    /// `nauka hypervisor enable` — flip this node back to available + broadcast.
    Enable,
    /// `nauka hypervisor update --ipv6-block ... --ipv4-public ... --name ...`.
    Update {
        ipv6_block: Option<String>,
        ipv4_public: Option<String>,
        name: Option<String>,
    },
    /// `nauka hypervisor leave` — tells the daemon to shut itself down
    /// cleanly so the CLI can finish tearing state down without the
    /// daemon racing it on the flock.
    Shutdown,
}

/// Response envelope. `ok == true` means the operation ran and
/// `data` is valid; `ok == false` means `error` carries a human
/// message and `data` is `null`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControlResponse {
    pub ok: bool,
    pub data: serde_json::Value,
    pub error: Option<String>,
}

impl ControlResponse {
    pub fn ok(data: serde_json::Value) -> Self {
        Self {
            ok: true,
            data,
            error: None,
        }
    }

    pub fn err(msg: impl Into<String>) -> Self {
        Self {
            ok: false,
            data: serde_json::Value::Null,
            error: Some(msg.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_roundtrip_ping() {
        let req = ControlRequest::Ping;
        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("\"op\":\"ping\""));
        let back: ControlRequest = serde_json::from_str(&json).unwrap();
        assert!(matches!(back, ControlRequest::Ping));
    }

    #[test]
    fn request_roundtrip_get() {
        let req = ControlRequest::Get {
            name: "node-1".into(),
        };
        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("\"op\":\"get\""));
        assert!(json.contains("\"name\":\"node-1\""));
        let back: ControlRequest = serde_json::from_str(&json).unwrap();
        match back {
            ControlRequest::Get { name } => assert_eq!(name, "node-1"),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn request_roundtrip_update() {
        let req = ControlRequest::Update {
            ipv6_block: Some("2a01:4f8:c012:abcd::/64".into()),
            ipv4_public: None,
            name: Some("renamed".into()),
        };
        let json = serde_json::to_string(&req).unwrap();
        let back: ControlRequest = serde_json::from_str(&json).unwrap();
        match back {
            ControlRequest::Update {
                ipv6_block,
                ipv4_public,
                name,
            } => {
                assert_eq!(ipv6_block.as_deref(), Some("2a01:4f8:c012:abcd::/64"));
                assert_eq!(ipv4_public, None);
                assert_eq!(name.as_deref(), Some("renamed"));
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn response_ok() {
        let resp = ControlResponse::ok(serde_json::json!({"foo": 1}));
        assert!(resp.ok);
        assert_eq!(resp.data["foo"], 1);
        assert!(resp.error.is_none());
    }

    #[test]
    fn response_err() {
        let resp = ControlResponse::err("boom");
        assert!(!resp.ok);
        assert_eq!(resp.error.as_deref(), Some("boom"));
        assert!(resp.data.is_null());
    }
}
