use std::io;
use std::io::Cursor;
use std::sync::Arc;

use futures::lock::Mutex;
use futures::{Stream, TryStreamExt};
use openraft::alias::{LogIdOf, SnapshotMetaOf, SnapshotOf, StoredMembershipOf};
use openraft::storage::{EntryResponder, RaftStateMachine};
use openraft::{EntryPayload, OptionalSend, RaftSnapshotBuilder, RaftTypeConfig};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use surrealdb::types::SurrealValue;

use super::types::{SurqlCommand, SurqlResponse};
use crate::db::Database;

fn db_err(e: impl std::fmt::Display) -> io::Error {
    io::Error::other(e.to_string())
}

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

#[derive(Deserialize, SurrealValue)]
struct SnapshotRecord {
    snapshot_id: String,
    #[serde(default)]
    last_applied: Option<String>,
    last_membership: String,
    data: String,
}

#[derive(Debug)]
pub struct StateMachineStore<C: RaftTypeConfig> {
    inner: Arc<Mutex<SmInner<C>>>,
    db: Arc<Database>,
    node_id: u64,
}

impl<C: RaftTypeConfig> Clone for StateMachineStore<C> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            db: self.db.clone(),
            node_id: self.node_id,
        }
    }
}

impl<C: RaftTypeConfig> StateMachineStore<C>
where
    LogIdOf<C>: Serialize + DeserializeOwned,
    StoredMembershipOf<C>: Serialize + DeserializeOwned,
{
    pub async fn new(db: Arc<Database>, node_id: u64) -> Result<Self, io::Error> {
        let mut inner = SmInner::default();

        // Load persisted snapshot if one exists
        let surql = format!(
            "SELECT snapshot_id, last_applied, last_membership, data FROM _raft_snapshot:{}",
            node_id
        );
        if let Ok(records) = db.query_take::<SnapshotRecord>(&surql).await {
            if let Some(rec) = records.into_iter().next() {
                let last_applied: Option<LogIdOf<C>> = rec
                    .last_applied
                    .map(|s| serde_json::from_str(&s))
                    .transpose()
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

                let last_membership: StoredMembershipOf<C> =
                    serde_json::from_str(&rec.last_membership)
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

                let data: SmData = serde_json::from_str(&rec.data)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

                let raw = rec.data.into_bytes();

                let meta = SnapshotMetaOf::<C> {
                    last_log_id: last_applied.clone(),
                    last_membership: last_membership.clone(),
                    snapshot_id: rec.snapshot_id,
                };

                inner.last_applied_log = last_applied;
                inner.last_membership = last_membership;
                inner.data = data;
                inner.snapshot = Some(StoredSnapshot { meta, data: raw });

                eprintln!(
                    "  sm: restored snapshot (applied up to {:?})",
                    inner.last_applied_log.as_ref().map(|l| l.index())
                );
            }
        }

        Ok(Self {
            inner: Arc::new(Mutex::new(inner)),
            db,
            node_id,
        })
    }

    async fn persist_snapshot(
        &self,
        meta: &SnapshotMetaOf<C>,
        data: &[u8],
    ) -> Result<(), io::Error> {
        let last_applied = meta
            .last_log_id
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let last_membership = serde_json::to_string(&meta.last_membership)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let data_str = String::from_utf8_lossy(data);

        let last_applied_val = match last_applied {
            Some(_) => "$last_applied".to_string(),
            None => "NONE".to_string(),
        };

        let mut q = self
            .db
            .inner()
            .query(format!(
                "UPSERT _raft_snapshot:{} SET \
                 hypervisor = {}, \
                 snapshot_id = $snap_id, \
                 last_applied = {last_applied_val}, \
                 last_membership = $membership, \
                 data = $data",
                self.node_id, self.node_id as i64
            ))
            .bind(("snap_id", meta.snapshot_id.clone()))
            .bind(("membership", last_membership))
            .bind(("data", data_str.into_owned()));

        if let Some(ref la) = last_applied {
            q = q.bind(("last_applied", la.clone()));
        }

        q.await.map_err(db_err)?.check().map_err(db_err)?;
        Ok(())
    }
}

impl<C> RaftSnapshotBuilder<C> for StateMachineStore<C>
where
    C: RaftTypeConfig<D = SurqlCommand, R = SurqlResponse, SnapshotData = Cursor<Vec<u8>>>,
    LogIdOf<C>: Serialize + DeserializeOwned,
    StoredMembershipOf<C>: Serialize + DeserializeOwned,
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

        let applied_query_count = inner.data.applied_queries.len();
        inner.snapshot = Some(StoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        });

        drop(inner);
        self.persist_snapshot(&meta, &data).await?;

        tracing::info!(
            node_id = self.node_id,
            snapshot_id = %meta.snapshot_id,
            applied_queries = applied_query_count,
            bytes = data.len(),
            "raft: built snapshot"
        );

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
    LogIdOf<C>: Serialize + DeserializeOwned,
    StoredMembershipOf<C>: Serialize + DeserializeOwned,
{
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogIdOf<C>>, StoredMembershipOf<C>), io::Error> {
        let inner = self.inner.lock().await;
        Ok((
            inner.last_applied_log.clone(),
            inner.last_membership.clone(),
        ))
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
                    match self.db.query(&cmd.query).await {
                        Ok(_) => {
                            eprintln!("  sm: query OK");
                            inner.data.applied_queries.push(cmd.query.clone());
                            SurqlResponse::ok()
                        }
                        Err(e) => {
                            eprintln!("  sm: query FAILED: {e}");
                            SurqlResponse::err(e.to_string())
                        }
                    }
                }
                EntryPayload::Membership(mem) => {
                    eprintln!("  sm: apply membership change");
                    inner.last_membership =
                        StoredMembershipOf::<C>::new(Some(entry.log_id.clone()), mem.clone());
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

        // Wipe Raft-replicated state before replaying the snapshot's queries.
        // The receiving node's state machine usually has rows from log entries
        // it applied before getting this snapshot; without the wipe, a CREATE
        // in `applied_queries` hits the UNIQUE index and the replay silently
        // diverges from the leader. `_raft_meta` / `_raft_log` are intentionally
        // NOT touched — those are per-node state openraft manages. `mesh` is
        // local-only and never in the snapshot.
        self.db
            .query("DELETE hypervisor")
            .await
            .map_err(|e| io::Error::other(format!("wipe pre-replay: {e}")))?;

        // Surface replay errors: an error here means the receiving node's DB
        // is out of sync with the leader, which is a correctness bug, not
        // something to log-and-continue.
        for query in &new_data.applied_queries {
            self.db
                .query(query)
                .await
                .map_err(|e| io::Error::other(format!("snapshot replay: {e}")))?;
        }

        inner.data = new_data;
        inner.snapshot = Some(StoredSnapshot {
            meta: meta.clone(),
            data: raw.clone(),
        });

        drop(inner);
        self.persist_snapshot(meta, &raw).await?;

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
