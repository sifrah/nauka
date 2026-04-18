//! Unified write path for [`ResourceOps`] values.
//!
//! Call sites never pick `RaftNode::write` vs `Database::query`
//! themselves: they build a [`Writer`] once (with optional Raft
//! handle) and call `writer.create/update/delete::<MyResource>`. The
//! writer reads `R::SCOPE` and routes to the right transport —
//! `Scope::Cluster` goes through Raft, `Scope::Local` hits the local
//! SurrealKV.
//!
//! Multi-resource operations that must be atomic use
//! [`Writer::transaction`] — the closure builds up a statement list
//! and the whole thing is sent as one `BEGIN TRANSACTION; … COMMIT
//! TRANSACTION;` block (one Raft apply for cluster scope; one
//! SurrealKV transaction for local scope).
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

    /// Execute multiple `ResourceOps` calls atomically. The closure
    /// builds up a list of statements by calling
    /// [`TxBuilder::create`] / `update` / `delete`; after it returns
    /// `Ok`, they are wrapped in a single
    /// `BEGIN TRANSACTION; … COMMIT TRANSACTION;` block and
    /// dispatched — one Raft apply for `Scope::Cluster`, one
    /// SurrealKV transaction for `Scope::Local`.
    ///
    /// The closure is **synchronous** because transactions must not
    /// span async points (doing a DB fetch mid-transaction would
    /// defeat the point). If you need to read state first, read it
    /// before opening the transaction and pass the results in.
    ///
    /// The closure returning `Err` aborts without ever sending the
    /// transaction — nothing is written.
    ///
    /// Mixing `Scope::Cluster` and `Scope::Local` resources in a
    /// single transaction is rejected with
    /// [`StateError::Transaction`] because the two backends cannot
    /// share an atomic unit.
    pub async fn transaction<F, T>(&self, f: F) -> Result<T, StateError>
    where
        F: FnOnce(&mut TxBuilder) -> Result<T, StateError>,
    {
        let mut tx = TxBuilder::default();
        let result = f(&mut tx)?;
        self.commit_tx(tx).await?;
        Ok(result)
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

    async fn commit_tx(&self, tx: TxBuilder) -> Result<(), StateError> {
        let Some(scope) = tx.scope else {
            // Empty transaction — nothing to commit.
            return Ok(());
        };
        let body = tx
            .statements
            .iter()
            .map(|s| format!("{s};"))
            .collect::<Vec<_>>()
            .join("\n");
        let surql = format!("BEGIN TRANSACTION;\n{body}\nCOMMIT TRANSACTION;");

        match scope {
            Scope::Cluster => {
                let raft = self.raft.ok_or_else(|| {
                    StateError::Raft(
                        "cluster-scoped transaction requires Writer::with_raft(&raft)".into(),
                    )
                })?;
                raft.write(surql).await?;
                Ok(())
            }
            Scope::Local => self.db.query(&surql).await,
        }
    }
}

/// Statement collector for [`Writer::transaction`]. All statements
/// must share the same [`Scope`] — attempts to mix `Cluster` and
/// `Local` resources error out on the call that introduces the
/// second scope, so the transaction is rejected before it reaches
/// the network.
#[derive(Default)]
pub struct TxBuilder {
    statements: Vec<String>,
    scope: Option<Scope>,
}

impl TxBuilder {
    /// Queue a `CREATE` for `value` in this transaction.
    pub fn create<R: ResourceOps>(&mut self, value: &R) -> Result<(), StateError> {
        self.push::<R>(value.create_query())
    }

    /// Queue an `UPDATE` for `value`.
    pub fn update<R: ResourceOps>(&mut self, value: &R) -> Result<(), StateError> {
        self.push::<R>(value.update_query())
    }

    /// Queue a `DELETE` for the record with `id`.
    pub fn delete<R: ResourceOps>(&mut self, id: &R::Id) -> Result<(), StateError> {
        self.push::<R>(R::delete_query(id))
    }

    /// Queue an arbitrary SurrealQL statement. Escape hatch for
    /// infrastructure statements that don't map to a `Resource` —
    /// avoid for resource CRUD so the CI grep check stays effective.
    /// Scope must still be declared via `for_scope` before mixing.
    pub fn raw(&mut self, scope: Scope, statement: impl Into<String>) -> Result<(), StateError> {
        self.ensure_scope(scope)?;
        self.statements.push(statement.into());
        Ok(())
    }

    fn push<R: Resource>(&mut self, surql: String) -> Result<(), StateError> {
        self.ensure_scope(R::SCOPE)?;
        self.statements.push(surql);
        Ok(())
    }

    fn ensure_scope(&mut self, s: Scope) -> Result<(), StateError> {
        match self.scope {
            None => {
                self.scope = Some(s);
                Ok(())
            }
            Some(existing) if existing == s => Ok(()),
            Some(existing) => Err(StateError::Transaction(format!(
                "cannot mix {existing:?} and {s:?} resources in one transaction — \
                 cluster (Raft) and local (SurrealKV) back-ends can't share atomicity"
            ))),
        }
    }

    /// Number of queued statements. Useful for tests.
    pub fn len(&self) -> usize {
        self.statements.len()
    }

    /// `true` if the transaction is empty (closure ran but added nothing).
    pub fn is_empty(&self) -> bool {
        self.statements.is_empty()
    }
}
