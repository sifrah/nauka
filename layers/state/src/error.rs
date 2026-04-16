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
}
