//! Transport QUIC inter-nœuds de yogfile (quinn).
//!
//! Un échange = un stream bidirectionnel : le client écrit une [`Request`],
//! le serveur répond une [`Response`]. Les connexions sont multiplexées par
//! quinn — plusieurs shards peuvent transiter en parallèle sur une même
//! connexion.
//!
//! TLS v0 : certificat auto-signé généré au démarrage, le client ne vérifie
//! pas le certificat (cluster fermé). L'authentification mutuelle par clés
//! de cluster viendra avec la couche membership.

pub mod client;
pub mod protocol;
pub mod server;

pub use client::PeerClient;
pub use protocol::{Request, Response};
pub use server::serve;

use std::sync::Arc;

fn crypto_provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}
