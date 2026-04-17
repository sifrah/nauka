//! Unified write path for [`ResourceOps`] values.
//!
//! Call sites never pick `RaftNode::write` vs `Database::query`
//! themselves: they build a [`Writer`] once (with optional Raft
//! handle) and call `writer.create/update/delete::<MyResource>`. The
//! writer reads `R::SCOPE` and routes to the right transport —
//! `Scope::Cluster` goes through Raft, `Scope::Local` hits the local
//! SurrealKV.
//!
//! This is the single enforcement point for "cluster resources must
//! go through consensus". A layer that forgets to pass a Raft handle
//! and tries to write a cluster-scoped resource gets a clear runtime
//! error at the write site, not a silent bypass.

use nauka_core::resource::{Resource, ResourceOps, Scope};

use crate::{Database, RaftNode, StateError};

/// Router that sends `ResourceOps` writes to the right backend.
/// Construct once per operation; the references are borrowed, no
/// ownership is moved.
pub struct Writer<'a> {
    db: &'a Database,
    raft: Option<&'a RaftNode>,
}

impl<'a> Writer<'a> {
    /// Writer with only the local DB wired. Cannot write any
    /// [`Scope::Cluster`] resource; attempting to do so returns
    /// [`StateError::Raft`].
    pub fn new(db: &'a Database) -> Self {
        Self { db, raft: None }
    }

    /// Attach a Raft handle. Required to write [`Scope::Cluster`]
    /// resources.
    pub fn with_raft(mut self, raft: &'a RaftNode) -> Self {
        self.raft = Some(raft);
        self
    }

    /// Route a `CREATE` through Raft (cluster) or the local DB
    /// (local).
    pub async fn create<R: ResourceOps>(&self, value: &R) -> Result<(), StateError> {
        self.execute::<R>(value.create_query()).await
    }

    /// Route an `UPDATE`. The caller is responsible for having
    /// bumped `updated_at` / `version` on `value` — the writer does
    /// not mutate the input because cluster leaders must set
    /// deterministic timestamps themselves.
    pub async fn update<R: ResourceOps>(&self, value: &R) -> Result<(), StateError> {
        self.execute::<R>(value.update_query()).await
    }

    /// Route a `DELETE` for the record with `id`.
    pub async fn delete<R: ResourceOps>(&self, id: &R::Id) -> Result<(), StateError> {
        self.execute::<R>(R::delete_query(id)).await
    }

    async fn execute<R: Resource>(&self, surql: String) -> Result<(), StateError> {
        match R::SCOPE {
            Scope::Cluster => {
                let raft = self.raft.ok_or_else(|| {
                    StateError::Raft(format!(
                        "resource `{}` has scope=cluster but Writer has no Raft handle — \
                         call Writer::with_raft(&raft) before writing",
                        R::TABLE
                    ))
                })?;
                raft.write(surql).await?;
                Ok(())
            }
            Scope::Local => self.db.query(&surql).await,
        }
    }
}
