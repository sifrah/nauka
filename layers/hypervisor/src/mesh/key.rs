use defguard_wireguard_rs::key::Key;
use serde::{Deserialize, Serialize};
use std::str::FromStr;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyPair {
    private_key: String,
    public_key: String,
}

impl KeyPair {
    pub fn generate() -> Self {
        let private = Key::generate();
        let public = private.public_key();
        Self {
            private_key: private.to_string(),
            public_key: public.to_string(),
        }
    }

    pub fn from_private(private_b64: &str) -> Result<Self, super::MeshError> {
        let private: Key = Key::from_str(private_b64).map_err(|_| super::MeshError::InvalidKey)?;
        let public = private.public_key();
        Ok(Self {
            private_key: private.to_string(),
            public_key: public.to_string(),
        })
    }

    pub fn private_key(&self) -> &str {
        &self.private_key
    }

    pub fn public_key(&self) -> &str {
        &self.public_key
    }

    pub fn public_key_raw(&self) -> Result<Key, super::MeshError> {
        Key::from_str(&self.public_key).map_err(|_| super::MeshError::InvalidKey)
    }
}
