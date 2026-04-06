//! Mock network backend — no-op, always up. For CI testing.

use std::net::Ipv6Addr;

use nauka_core::error::NaukaError;

use super::backend::{BackendPeer, NetworkBackend, NetworkMode, NetworkStatus};

/// Mock backend — does nothing, always reports success.
pub struct MockBackend;

impl NetworkBackend for MockBackend {
    fn ensure_installed(&self) -> Result<(), NaukaError> {
        Ok(())
    }

    fn setup(
        &self,
        _private_key: &str,
        _listen_port: u16,
        _mesh_ipv6: &Ipv6Addr,
        _peers: &[BackendPeer],
    ) -> Result<(), NaukaError> {
        tracing::info!("mock: setup (no-op)");
        Ok(())
    }

    fn add_peer(&self, peer: &BackendPeer) -> Result<(), NaukaError> {
        tracing::info!(key = %peer.public_key, "mock: add_peer (no-op)");
        Ok(())
    }

    fn remove_peer(&self, _public_key: &str, _mesh_ipv6: &Ipv6Addr) -> Result<(), NaukaError> {
        Ok(())
    }

    fn update_config(
        &self,
        _private_key: &str,
        _listen_port: u16,
        _mesh_ipv6: &Ipv6Addr,
        _peers: &[BackendPeer],
    ) -> Result<(), NaukaError> {
        Ok(())
    }

    fn is_up(&self) -> bool {
        true
    }

    fn is_active(&self) -> bool {
        true
    }

    fn status(&self) -> Result<NetworkStatus, NaukaError> {
        Ok(NetworkStatus {
            interface_up: true,
            listen_port: 51820,
            peer_count: 0,
            rx_bytes: 0,
            tx_bytes: 0,
        })
    }

    fn start(&self) -> Result<(), NaukaError> {
        Ok(())
    }

    fn stop(&self) -> Result<(), NaukaError> {
        Ok(())
    }

    fn teardown(&self) -> Result<(), NaukaError> {
        Ok(())
    }

    fn mode(&self) -> NetworkMode {
        NetworkMode::Mock
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mock_is_always_up() {
        let b = MockBackend;
        assert!(b.is_up());
        assert!(b.is_active());
        assert_eq!(b.mode(), NetworkMode::Mock);
    }

    #[test]
    fn mock_setup_succeeds() {
        let b = MockBackend;
        b.setup("key", 51820, &"fd01::1".parse().unwrap(), &[]).unwrap();
        b.teardown().unwrap();
    }
}
