//! Handler dependencies — the single struct that every REST handler
//! and GraphQL resolver receives via axum's `State` / `async-graphql`
//! context. Kept minimal in 342-A: DB + optional Raft. More fields
//! (auth context, config) land as later phases need them.

use std::sync::Arc;

use nauka_state::{Database, RaftNode};

/// Handles shared by every handler. Clone is cheap (all fields are
/// [`Arc`]) so we pass by value rather than by reference — matches
/// axum's `State<T>` ergonomics.
#[derive(Clone)]
pub struct Deps {
    pub db: Arc<Database>,
    /// `None` in single-node dev runs without Raft wired up. Handlers
    /// that need cluster writes (`Scope::Cluster` resources) return
    /// `NaukaApiError::Internal` when Raft is absent — the same
    /// policy `Writer::create` enforces at the state layer.
    pub raft: Option<Arc<RaftNode>>,
}

impl Deps {
    pub fn new(db: Arc<Database>, raft: Option<Arc<RaftNode>>) -> Self {
        Self { db, raft }
    }
}
