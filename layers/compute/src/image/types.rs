//! Image types.

use serde::{Deserialize, Serialize};
use std::fmt;

/// Status of an image on this node.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum ImageStatus {
    /// Being downloaded
    Pulling,
    /// Ready to use
    Ready,
    /// Download failed
    Failed,
}

impl fmt::Display for ImageStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ImageStatus::Pulling => write!(f, "pulling"),
            ImageStatus::Ready => write!(f, "ready"),
            ImageStatus::Failed => write!(f, "failed"),
        }
    }
}

/// An OS image available on this node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Image {
    pub name: String,
    pub arch: String,
    pub size_bytes: u64,
    pub status: ImageStatus,
}
