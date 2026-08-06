//! Protocole inter-nœuds : messages sérialisés en bincode, framés par une
//! longueur u32 LE, un échange requête/réponse par stream bidirectionnel QUIC.

use serde::{Deserialize, Serialize};
use yog_erasure::FileManifest;

/// Garde-fou : taille max d'un message (shard 1 MiB + marge, manifests).
pub const MAX_MESSAGE_SIZE: u32 = 64 * 1024 * 1024;

/// ALPN du protocole yogfile.
pub const ALPN: &[u8] = b"yog/0";

#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    Ping,
    /// Stocke un shard sur le nœud distant (idempotent, content-addressed).
    PutShard(Vec<u8>),
    /// Récupère un shard par hash.
    GetShard(String),
    /// Le nœud possède-t-il ce shard ?
    HasShard(String),
    /// Réplique un manifest sur le nœud distant.
    PutManifest(FileManifest),
    /// Récupère un manifest par hash de fichier.
    GetManifest(String),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    Pong,
    /// Hash du shard stocké.
    PutShardOk(String),
    /// `None` si le shard est absent ou corrompu sur le nœud distant.
    Shard(Option<Vec<u8>>),
    Has(bool),
    PutManifestOk,
    Manifest(Option<FileManifest>),
    /// Erreur applicative côté serveur.
    Error(String),
}

#[derive(Debug, thiserror::Error)]
pub enum WireError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("message trop grand: {0} octets (max {MAX_MESSAGE_SIZE})")]
    TooLarge(u32),
    #[error("sérialisation: {0}")]
    Codec(#[from] bincode::Error),
    #[error("stream quic: {0}")]
    Stream(String),
}

/// Écrit un message framé sur un stream QUIC sortant.
pub async fn write_message<T: Serialize>(
    send: &mut quinn::SendStream,
    msg: &T,
) -> Result<(), WireError> {
    let payload = bincode::serialize(msg)?;
    let len = u32::try_from(payload.len()).map_err(|_| WireError::TooLarge(u32::MAX))?;
    if len > MAX_MESSAGE_SIZE {
        return Err(WireError::TooLarge(len));
    }
    send.write_all(&len.to_le_bytes())
        .await
        .map_err(|e| WireError::Stream(e.to_string()))?;
    send.write_all(&payload)
        .await
        .map_err(|e| WireError::Stream(e.to_string()))?;
    Ok(())
}

/// Lit un message framé sur un stream QUIC entrant.
pub async fn read_message<T: serde::de::DeserializeOwned>(
    recv: &mut quinn::RecvStream,
) -> Result<T, WireError> {
    let mut len_buf = [0u8; 4];
    recv.read_exact(&mut len_buf)
        .await
        .map_err(|e| WireError::Stream(e.to_string()))?;
    let len = u32::from_le_bytes(len_buf);
    if len > MAX_MESSAGE_SIZE {
        return Err(WireError::TooLarge(len));
    }
    let mut payload = vec![0u8; len as usize];
    recv.read_exact(&mut payload)
        .await
        .map_err(|e| WireError::Stream(e.to_string()))?;
    Ok(bincode::deserialize(&payload)?)
}
