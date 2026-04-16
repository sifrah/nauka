use defguard_wireguard_rs::error::WireguardInterfaceError;
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
