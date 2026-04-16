mod address;
pub mod certs;
pub mod crypto;
mod error;
mod join;
mod key;
mod peer;
pub mod reconciler;
mod state;

pub use address::MeshId;
pub use error::MeshError;
pub use join::{
    generate_pin, join_mesh, mesh_listener, request_peer_removal, request_raft_write, whoami,
    PeerInfo, DEFAULT_JOIN_PORT, DEFAULT_PEERING_TIMEOUT,
};
pub use key::KeyPair;
pub use peer::MeshPeer;
pub use state::MeshState;

use defguard_wireguard_rs::{
    host::Host, key::Key, net::IpAddrMask, peer::Peer, InterfaceConfiguration, Kernel, WGApi,
    WireguardInterfaceApi,
};
use std::str::FromStr;

pub struct Mesh {
    interface_name: String,
    keypair: KeyPair,
    listen_port: u16,
    mesh_id: MeshId,
    address: IpAddrMask,
    api: WGApi<Kernel>,
}

impl Mesh {
    pub fn new(
        interface_name: String,
        listen_port: u16,
        mesh_id: Option<MeshId>,
        keypair: Option<KeyPair>,
        address_override: Option<IpAddrMask>,
    ) -> Result<Self, MeshError> {
        let keypair = keypair.unwrap_or_else(KeyPair::generate);
        let mesh_id = mesh_id.unwrap_or_else(MeshId::generate);
        let address = match address_override {
            Some(addr) => addr,
            None => mesh_id.node_address(&keypair.public_key_raw()?),
        };
        let api = WGApi::<Kernel>::new(interface_name.clone())?;
        Ok(Self { interface_name, keypair, listen_port, mesh_id, address, api })
    }

    pub fn from_state(state: &MeshState) -> Result<Self, MeshError> {
        let address: IpAddrMask = state.address.parse()
            .map_err(|_| MeshError::InvalidAddress(state.address.clone()))?;
        let api = WGApi::<Kernel>::new(state.interface_name.clone())?;
        Ok(Self {
            interface_name: state.interface_name.clone(),
            keypair: state.keypair.clone(),
            listen_port: state.listen_port,
            mesh_id: state.mesh_id.clone(),
            address,
            api,
        })
    }

    pub fn to_state(&self) -> MeshState {
        MeshState {
            interface_name: self.interface_name.clone(),
            keypair: self.keypair.clone(),
            listen_port: self.listen_port,
            mesh_id: self.mesh_id.clone(),
            address: self.address.to_string(),
            ca_cert: None,
            ca_key: None,
            tls_cert: None,
            tls_key: None,
        }
    }

    pub fn up(&mut self) -> Result<(), MeshError> {
        self.api.create_interface()?;
        let config = InterfaceConfiguration {
            name: self.interface_name.clone(),
            prvkey: self.keypair.private_key().to_string(),
            addresses: vec![self.address.clone()],
            port: self.listen_port,
            peers: vec![],
            mtu: None,
            fwmark: None,
        };
        self.api.configure_interface(&config)?;
        Ok(())
    }

    pub fn down(&self) -> Result<(), MeshError> {
        self.api.remove_interface()?;
        Ok(())
    }

    pub fn add_peer(&self, mesh_peer: &MeshPeer) -> Result<(), MeshError> {
        let key = Key::from_str(&mesh_peer.public_key).map_err(|_| MeshError::InvalidKey)?;
        let mut peer = Peer::new(key);
        if let Some(ref endpoint) = mesh_peer.endpoint {
            peer.set_endpoint(endpoint)?;
        }
        peer.persistent_keepalive_interval = mesh_peer.persistent_keepalive;
        for cidr in &mesh_peer.allowed_ips {
            let addr = IpAddrMask::from_str(cidr)
                .map_err(|_| MeshError::InvalidAddress(cidr.clone()))?;
            peer.allowed_ips.push(addr);
        }
        self.api.configure_peer(&peer)?;
        self.api.configure_peer_routing(&[peer])?;
        Ok(())
    }

    pub fn status(&self) -> Result<Host, MeshError> {
        Ok(self.api.read_interface_data()?)
    }

    pub fn public_key(&self) -> &str { self.keypair.public_key() }
    pub fn keypair(&self) -> &KeyPair { &self.keypair }
    pub fn interface_name(&self) -> &str { &self.interface_name }
    pub fn mesh_id(&self) -> &MeshId { &self.mesh_id }
    pub fn address(&self) -> &IpAddrMask { &self.address }
    pub fn listen_port(&self) -> u16 { self.listen_port }

    pub fn down_interface(interface_name: &str) -> Result<(), MeshError> {
        let api = WGApi::<Kernel>::new(interface_name.to_string())?;
        api.remove_interface()?;
        Ok(())
    }

    pub fn interface_status(interface_name: &str) -> Result<Host, MeshError> {
        let api = WGApi::<Kernel>::new(interface_name.to_string())?;
        Ok(api.read_interface_data()?)
    }
}
