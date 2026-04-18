#![deny(clippy::print_stdout, clippy::print_stderr)]

pub mod db;
pub mod error;
pub mod raft;
pub mod schema;
pub mod writer;

pub use db::Database;
pub use error::StateError;
pub use raft::tls::TlsConfig;
pub use raft::{node_id_from_key, RaftNode};
pub use schema::load_schemas;
pub use writer::{TxBuilder, Writer};

pub const SCHEMA: &str = include_str!("../definition.surql");
