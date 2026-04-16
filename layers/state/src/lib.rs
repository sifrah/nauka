pub mod db;
pub mod error;
pub mod raft;
pub mod schema;

pub use db::Database;
pub use error::StateError;
pub use raft::{node_id_from_key, RaftNode};
pub use schema::load_schemas;

pub const SCHEMA: &str = include_str!("../definition.surql");
