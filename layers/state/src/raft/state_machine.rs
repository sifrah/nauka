use std::io;
use std::io::Cursor;
use std::sync::Arc;

use futures::lock::Mutex;
use futures::{Stream, TryStreamExt};
use openraft::{EntryPayload, OptionalSend, RaftSnapshotBuilder, RaftTypeConfig};
use openraft::alias::{LogIdOf, SnapshotMetaOf, SnapshotOf, StoredMembershipOf};
use openraft::storage::{EntryResponder, RaftStateMachine};
use serde::{Deserialize, Serialize};

use super::types::{SurqlCommand, SurqlResponse};
use crate::db::Database;

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct SmData {
    pub applied_queries: Vec<String>,
}

#[derive(Debug)]
struct SmInner<C: RaftTypeConfig> {
    last_applied_log: Option<LogIdOf<C>>,
    last_membership: StoredMembershipOf<C>,
    data: SmData,
    snapshot: Option<StoredSnapshot<C>>,
    snapshot_idx: u64,
}

impl<C: RaftTypeConfig> Default for SmInner<C> {
    fn default() -> Self {
        Self {
            last_applied_log: None,
            last_membership: StoredMembershipOf::<C>::default(),
            data: SmData::default(),
            snapshot: None,
            snapshot_idx: 0,
        }
    }
}

#[derive(Debug)]
struct StoredSnapshot<C: RaftTypeConfig> {
    meta: SnapshotMetaOf<C>,
    data: Vec<u8>,
}

#[derive(Debug)]
pub struct StateMachineStore<C: RaftTypeConfig> {
    inner: Arc<Mutex<SmInner<C>>>,
    db: Arc<Database>,
}

impl<C: RaftTypeConfig> Clone for StateMachineStore<C> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            db: self.db.clone(),
        }
    }
}

impl<C: RaftTypeConfig> StateMachineStore<C> {
    pub fn new(db: Arc<Database>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(SmInner::default())),
            db,
        }
    }
}

impl<C> RaftSnapshotBuilder<C> for StateMachineStore<C>
where
    C: RaftTypeConfig<
        D = SurqlCommand,
        R = SurqlResponse,
        SnapshotData = Cursor<Vec<u8>>,
    >,
{
    async fn build_snapshot(&mut self) -> Result<SnapshotOf<C>, io::Error> {
        let mut inner = self.inner.lock().await;
        let data = serde_json::to_vec(&inner.data)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        inner.snapshot_idx += 1;
        let snapshot_id = if let Some(ref last) = inner.last_applied_log {
            format!("{}-{}", last.index(), inner.snapshot_idx)
        } else {
            format!("0-{}", inner.snapshot_idx)
        };

        let meta = SnapshotMetaOf::<C> {
            last_log_id: inner.last_applied_log.clone(),
            last_membership: inner.last_membership.clone(),
            snapshot_id,
        };

        inner.snapshot = Some(StoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        });

        Ok(SnapshotOf::<C> {
            meta,
            snapshot: Cursor::new(data),
        })
    }
}

impl<C> RaftStateMachine<C> for StateMachineStore<C>
where
    C: RaftTypeConfig<
        D = SurqlCommand,
        R = SurqlResponse,
        SnapshotData = Cursor<Vec<u8>>,
        Entry = openraft::alias::DefaultEntryOf<C>,
    >,
{
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogIdOf<C>>, StoredMembershipOf<C>), io::Error> {
        let inner = self.inner.lock().await;
        Ok((inner.last_applied_log.clone(), inner.last_membership.clone()))
    }

    async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
    where
        Strm: Stream<Item = Result<EntryResponder<C>, io::Error>> + Unpin + OptionalSend,
    {
        let mut inner = self.inner.lock().await;

        while let Some((entry, responder)) = entries.try_next().await? {
            inner.last_applied_log = Some(entry.log_id.clone());

            let response = match &entry.payload {
                EntryPayload::Blank => {
                    eprintln!("  sm: apply blank entry");
                    SurqlResponse::none()
                }
                EntryPayload::Normal(cmd) => {
                    eprintln!("  sm: apply query: {}", cmd.query);
                    inner.data.applied_queries.push(cmd.query.clone());
                    match self.db.query(&cmd.query).await {
                        Ok(_) => {
                            eprintln!("  sm: query OK");
                            SurqlResponse::ok()
                        }
                        Err(e) => {
                            eprintln!("  sm: query FAILED: {e}");
                            SurqlResponse::none()
                        }
                    }
                }
                EntryPayload::Membership(mem) => {
                    eprintln!("  sm: apply membership change");
                    inner.last_membership = StoredMembershipOf::<C>::new(
                        Some(entry.log_id.clone()),
                        mem.clone(),
                    );
                    SurqlResponse::none()
                }
            };

            if let Some(responder) = responder {
                responder.send(response);
            }
        }
        Ok(())
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<C::SnapshotData, io::Error> {
        Ok(Cursor::new(Vec::new()))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMetaOf<C>,
        snapshot: C::SnapshotData,
    ) -> Result<(), io::Error> {
        let raw = snapshot.into_inner();
        let new_data: SmData = serde_json::from_slice(&raw)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let mut inner = self.inner.lock().await;
        inner.last_applied_log = meta.last_log_id.clone();
        inner.last_membership = meta.last_membership.clone();

        // Replay all queries to rebuild SurrealDB state
        for query in &new_data.applied_queries {
            if let Err(e) = self.db.query(query).await {
                tracing::error!(query = %query, error = %e, "snapshot replay failed");
            }
        }

        inner.data = new_data;
        inner.snapshot = Some(StoredSnapshot {
            meta: meta.clone(),
            data: raw,
        });

        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<SnapshotOf<C>>, io::Error> {
        let inner = self.inner.lock().await;
        Ok(inner.snapshot.as_ref().map(|s| SnapshotOf::<C> {
            meta: s.meta.clone(),
            snapshot: Cursor::new(s.data.clone()),
        }))
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }
}
