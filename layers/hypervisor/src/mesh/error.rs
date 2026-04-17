use defguard_wireguard_rs::error::WireguardInterfaceError;
use nauka_core::NaukaError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum MeshError {
    #[error(transparent)]
    WireGuard(#[from] WireguardInterfaceError),

    #[error("invalid key")]
    InvalidKey,

    #[error("invalid address: {0}")]
    InvalidAddress(String),

    #[error("join failed: {0}")]
    Join(String),

    #[error("state: {0}")]
    State(String),
}

impl NaukaError for MeshError {
    fn event_name(&self) -> &'static str {
        match self {
            MeshError::WireGuard(_) => "mesh.wireguard",
            MeshError::InvalidKey => "mesh.key.invalid",
            MeshError::InvalidAddress(_) => "mesh.address.invalid",
            MeshError::Join(_) => "mesh.join.failed",
            MeshError::State(_) => "mesh.state.failed",
        }
    }
}
