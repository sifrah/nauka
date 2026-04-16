use defguard_wireguard_rs::key::Key;
use defguard_wireguard_rs::net::IpAddrMask;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::net::{IpAddr, Ipv6Addr};
use std::str::FromStr;

/// 48-bit ULA mesh prefix: fd + 40 random bits.
///
/// All nodes in the same mesh share this prefix.
/// Each node derives its /128 address from the prefix + its public key.
#[derive(Debug, Clone)]
pub struct MeshId {
    bytes: [u8; 6],
}

impl MeshId {
    pub fn generate() -> Self {
        let entropy = Key::generate();
        let raw = entropy.as_array();
        let mut bytes = [0u8; 6];
        bytes[0] = 0xfd;
        bytes[1..6].copy_from_slice(&raw[0..5]);
        Self { bytes }
    }

    pub fn node_address(&self, public_key: &Key) -> IpAddrMask {
        let pk = public_key.as_array();
        let mut octets = [0u8; 16];
        octets[0..6].copy_from_slice(&self.bytes);
        // subnet 1
        octets[6] = 0x00;
        octets[7] = 0x01;
        // interface ID from first 8 bytes of public key
        octets[8..16].copy_from_slice(&pk[0..8]);
        IpAddrMask::new(IpAddr::V6(Ipv6Addr::from(octets)), 128)
    }
}

impl fmt::Display for MeshId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:02x}{:02x}:{:02x}{:02x}:{:02x}{:02x}::/48",
            self.bytes[0],
            self.bytes[1],
            self.bytes[2],
            self.bytes[3],
            self.bytes[4],
            self.bytes[5],
        )
    }
}

impl FromStr for MeshId {
    type Err = super::MeshError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim();
        let s = s.strip_suffix("/48").unwrap_or(s);
        let s = s.strip_suffix("::").unwrap_or(s);
        let full = format!("{s}::1");
        let addr: Ipv6Addr = full
            .parse()
            .map_err(|_| super::MeshError::InvalidAddress(s.to_string()))?;
        let octets = addr.octets();
        if octets[0] != 0xfd {
            return Err(super::MeshError::InvalidAddress(
                "not a ULA prefix (must start with fd)".into(),
            ));
        }
        let mut bytes = [0u8; 6];
        bytes.copy_from_slice(&octets[0..6]);
        Ok(Self { bytes })
    }
}

impl Serialize for MeshId {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for MeshId {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        Self::from_str(&s).map_err(serde::de::Error::custom)
    }
}
