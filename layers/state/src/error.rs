use nauka_core::NaukaError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StateError {
    #[error("database: {0}")]
    Db(#[from] surrealdb::Error),

    #[error("schema: {0}")]
    Schema(String),

    #[error("raft: {0}")]
    Raft(String),

    #[error("network: {0}")]
    Network(String),

    #[error("transaction: {0}")]
    Transaction(String),
}

impl NaukaError for StateError {
    fn event_name(&self) -> &'static str {
        match self {
            StateError::Db(_) => "state.db",
            StateError::Schema(_) => "state.schema",
            StateError::Raft(_) => "state.raft",
            StateError::Network(_) => "state.network",
            StateError::Transaction(_) => "state.transaction",
        }
    }
}
