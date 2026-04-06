//! WireGuard network backend — encrypted mesh over public internet.

pub mod service;
pub mod wg;

use std::net::Ipv6Addr;

use nauka_core::error::NaukaError;

use super::backend::{BackendPeer, NetworkBackend, NetworkMode, NetworkStatus};

/// WireGuard backend implementation.
pub struct WireGuardBackend;

impl NetworkBackend for WireGuardBackend {
    fn ensure_installed(&self) -> Result<(), NaukaError> {
        service::ensure_wireguard()
    }

    fn setup(
        &self,
        private_key: &str,
        listen_port: u16,
        mesh_ipv6: &Ipv6Addr,
        peers: &[BackendPeer],
    ) -> Result<(), NaukaError> {
        let peer_tuples: Vec<_> = peers
            .iter()
            .map(|p| {
                (
                    p.public_key.clone(),
                    p.keepalive_secs.to_string(),
                    p.mesh_ipv6,
                    p.endpoint.clone(),
                )
            })
            .collect();
        service::install(private_key, listen_port, mesh_ipv6, &peer_tuples)?;
        service::enable_and_start()
    }

    fn add_peer(&self, peer: &BackendPeer) -> Result<(), NaukaError> {
        let p = super::peer::Peer::new(
            String::new(),
            String::new(),
            String::new(),
            peer.public_key.clone(),
            0,
            peer.endpoint.clone(),
            peer.mesh_ipv6,
        );
        wg::add_peer(&p, peer.keepalive_secs)
    }

    fn remove_peer(&self, public_key: &str, mesh_ipv6: &Ipv6Addr) -> Result<(), NaukaError> {
        wg::remove_peer(public_key, mesh_ipv6)
    }

    fn update_config(
        &self,
        private_key: &str,
        listen_port: u16,
        mesh_ipv6: &Ipv6Addr,
        peers: &[BackendPeer],
    ) -> Result<(), NaukaError> {
        let peer_tuples: Vec<_> = peers
            .iter()
            .map(|p| {
                (
                    p.public_key.clone(),
                    p.keepalive_secs.to_string(),
                    p.mesh_ipv6,
                    p.endpoint.clone(),
                )
            })
            .collect();
        service::update_config(private_key, listen_port, mesh_ipv6, &peer_tuples)
    }

    fn is_up(&self) -> bool {
        wg::interface_exists()
    }

    fn is_active(&self) -> bool {
        service::is_active()
    }

    fn status(&self) -> Result<NetworkStatus, NaukaError> {
        let s = wg::get_status()?;
        Ok(NetworkStatus {
            interface_up: true,
            listen_port: s.listen_port,
            peer_count: s.peer_count,
            rx_bytes: s.rx_bytes,
            tx_bytes: s.tx_bytes,
        })
    }

    fn start(&self) -> Result<(), NaukaError> {
        service::start()
    }

    fn stop(&self) -> Result<(), NaukaError> {
        service::stop()
    }

    fn teardown(&self) -> Result<(), NaukaError> {
        service::uninstall()?;
        if wg::interface_exists() {
            let _ = wg::destroy_interface();
        }
        Ok(())
    }

    fn mode(&self) -> NetworkMode {
        NetworkMode::WireGuard
    }
}
