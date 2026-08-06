//! Stockage openraft v0 : log et state machine en mémoire, snapshot
//! sérialisé bincode. La durabilité disque (redb/sled) viendra quand le
//! membership dynamique sera stabilisé — un nœud qui redémarre aujourd'hui
//! rejoue depuis un snapshot du leader.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::sync::{Arc, Mutex};

use openraft::storage::{LogFlushed, LogState, RaftLogStorage, RaftStateMachine, Snapshot};
use openraft::{
    Entry, EntryPayload, LogId, OptionalSend, RaftLogReader, RaftSnapshotBuilder, SnapshotMeta,
    StorageError, StorageIOError, StoredMembership, Vote,
};

use crate::types::{AppCommand, AppResponse, AppState, NodeId, TypeConfig};

/// Log Raft en mémoire.
#[derive(Debug, Default)]
pub struct LogStore {
    inner: Arc<Mutex<LogStoreInner>>,
}

#[derive(Debug, Default)]
struct LogStoreInner {
    log: BTreeMap<u64, Entry<TypeConfig>>,
    committed: Option<LogId<NodeId>>,
    last_purged: Option<LogId<NodeId>>,
    vote: Option<Vote<NodeId>>,
}

impl Clone for LogStore {
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone() }
    }
}

impl RaftLogReader<TypeConfig> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>> {
        let inner = self.inner.lock().unwrap();
        Ok(inner.log.range(range).map(|(_, e)| e.clone()).collect())
    }
}

impl RaftLogStorage<TypeConfig> for LogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<NodeId>> {
        let inner = self.inner.lock().unwrap();
        let last = inner.log.iter().next_back().map(|(_, e)| e.log_id);
        let last_purged = inner.last_purged;
        Ok(LogState { last_purged_log_id: last_purged, last_log_id: last.or(last_purged) })
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        self.inner.lock().unwrap().committed = committed;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        Ok(self.inner.lock().unwrap().committed)
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        self.inner.lock().unwrap().vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        Ok(self.inner.lock().unwrap().vote)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
    {
        {
            let mut inner = self.inner.lock().unwrap();
            for entry in entries {
                inner.log.insert(entry.log_id.index, entry);
            }
        }
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.lock().unwrap();
        let keys: Vec<u64> = inner.log.range(log_id.index..).map(|(k, _)| *k).collect();
        for k in keys {
            inner.log.remove(&k);
        }
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.lock().unwrap();
        inner.last_purged = Some(log_id);
        let keys: Vec<u64> = inner.log.range(..=log_id.index).map(|(k, _)| *k).collect();
        for k in keys {
            inner.log.remove(&k);
        }
        Ok(())
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }
}

/// State machine répliquée : le registre des manifests.
#[derive(Debug, Default)]
pub struct StateMachineStore {
    inner: Arc<Mutex<StateMachineInner>>,
}

#[derive(Debug, Default)]
struct StateMachineInner {
    state: AppState,
    last_applied: Option<LogId<NodeId>>,
    membership: StoredMembership<NodeId, openraft::BasicNode>,
    snapshot: Option<StoredSnapshot>,
    snapshot_idx: u64,
}

#[derive(Debug, Clone)]
struct StoredSnapshot {
    meta: SnapshotMeta<NodeId, openraft::BasicNode>,
    data: Vec<u8>,
}

impl Clone for StateMachineStore {
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone() }
    }
}

impl StateMachineStore {
    /// Lecture locale de l'état répliqué (pour l'API du nœud et le healer).
    pub fn read_state(&self) -> AppState {
        self.inner.lock().unwrap().state.clone()
    }
}

impl RaftSnapshotBuilder<TypeConfig> for StateMachineStore {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let mut inner = self.inner.lock().unwrap();
        let data = bincode::serialize(&inner.state)
            .map_err(|e| StorageIOError::write_snapshot(None, &e))?;
        inner.snapshot_idx += 1;
        let snapshot_id = format!(
            "{}-{}",
            inner.last_applied.map(|l| l.index).unwrap_or(0),
            inner.snapshot_idx
        );
        let meta = SnapshotMeta {
            last_log_id: inner.last_applied,
            last_membership: inner.membership.clone(),
            snapshot_id,
        };
        inner.snapshot = Some(StoredSnapshot { meta: meta.clone(), data: data.clone() });
        Ok(Snapshot { meta, snapshot: Box::new(Cursor::new(data)) })
    }
}

impl RaftStateMachine<TypeConfig> for StateMachineStore {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<
        (Option<LogId<NodeId>>, StoredMembership<NodeId, openraft::BasicNode>),
        StorageError<NodeId>,
    > {
        let inner = self.inner.lock().unwrap();
        Ok((inner.last_applied, inner.membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<AppResponse>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
    {
        let mut inner = self.inner.lock().unwrap();
        let mut replies = Vec::new();
        for entry in entries {
            inner.last_applied = Some(entry.log_id);
            match entry.payload {
                EntryPayload::Blank => replies.push(AppResponse::default()),
                EntryPayload::Membership(m) => {
                    inner.membership = StoredMembership::new(Some(entry.log_id), m);
                    replies.push(AppResponse::default());
                }
                EntryPayload::Normal(cmd) => {
                    let reply = match cmd {
                        AppCommand::RegisterManifest(manifest) => {
                            let hash = manifest.file_hash.clone();
                            inner.state.manifests.insert(hash.clone(), manifest);
                            AppResponse { ok: true, info: Some(hash) }
                        }
                        AppCommand::UnregisterManifest { file_hash } => {
                            let removed = inner.state.manifests.remove(&file_hash).is_some();
                            AppResponse { ok: removed, info: Some(file_hash) }
                        }
                    };
                    replies.push(reply);
                }
            }
        }
        Ok(replies)
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, openraft::BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<NodeId>> {
        let data = snapshot.into_inner();
        let state: AppState = bincode::deserialize(&data)
            .map_err(|e| StorageIOError::read_snapshot(Some(meta.signature()), &e))?;
        let mut inner = self.inner.lock().unwrap();
        inner.state = state;
        inner.last_applied = meta.last_log_id;
        inner.membership = meta.last_membership.clone();
        inner.snapshot = Some(StoredSnapshot { meta: meta.clone(), data });
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        let inner = self.inner.lock().unwrap();
        Ok(inner.snapshot.as_ref().map(|s| Snapshot {
            meta: s.meta.clone(),
            snapshot: Box::new(Cursor::new(s.data.clone())),
        }))
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }
}
