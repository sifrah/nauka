//! Stockage openraft durable.
//!
//! - Log + vote : redb, fsync AVANT d'acquitter (exigence de correction de
//!   Raft — un vote ou une entrée acquittés doivent survivre au crash).
//! - State machine : en mémoire, reconstruite au démarrage depuis le dernier
//!   snapshot (fichier, écrit atomiquement) + replay du log par openraft.
//!   Aucun fsync sur le chemin d'apply.
//!
//! Un arrêt total du cluster (les n nœuds éteints) redémarre donc sans perte :
//! chaque nœud recharge vote + log + snapshot depuis son data-dir.

use std::fmt::Debug;
use std::fs;
use std::io::Cursor;
use std::io::Write as _;
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use openraft::storage::{LogFlushed, LogState, RaftLogStorage, RaftStateMachine, Snapshot};
use openraft::{
    Entry, EntryPayload, LogId, OptionalSend, RaftLogReader, RaftSnapshotBuilder, SnapshotMeta,
    StorageError, StorageIOError, StoredMembership, Vote,
};
use redb::{Database, ReadableTable, TableDefinition};

use crate::types::{AppCommand, AppResponse, AppState, NodeId, TypeConfig};

const LOG_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("raft_log");
const META_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("raft_meta");

/// Log Raft durable (redb).
#[derive(Clone)]
pub struct LogStore {
    db: Arc<Database>,
}

impl LogStore {
    pub fn open(dir: &Path) -> anyhow::Result<Self> {
        fs::create_dir_all(dir)?;
        let db = Database::create(dir.join("raft-log.redb"))?;
        // Crée les tables si absentes pour simplifier toutes les lectures.
        let tx = db.begin_write()?;
        {
            tx.open_table(LOG_TABLE)?;
            tx.open_table(META_TABLE)?;
        }
        tx.commit()?;
        Ok(Self { db: Arc::new(db) })
    }

    fn read_meta<T: serde::de::DeserializeOwned>(
        &self,
        key: &str,
    ) -> Result<Option<T>, StorageError<NodeId>> {
        let tx = self.db.begin_read().map_err(read_err)?;
        let table = tx.open_table(META_TABLE).map_err(read_err)?;
        match table.get(key).map_err(read_err)? {
            Some(v) => Ok(Some(bincode::deserialize(v.value()).map_err(read_err)?)),
            None => Ok(None),
        }
    }

    fn write_meta<T: serde::Serialize>(
        &self,
        key: &str,
        value: &T,
    ) -> Result<(), StorageError<NodeId>> {
        let payload = bincode::serialize(value).map_err(write_err)?;
        let tx = self.db.begin_write().map_err(write_err)?;
        {
            let mut table = tx.open_table(META_TABLE).map_err(write_err)?;
            table.insert(key, payload.as_slice()).map_err(write_err)?;
        }
        tx.commit().map_err(write_err)?;
        Ok(())
    }
}

fn read_err<E: std::error::Error + 'static>(e: E) -> StorageError<NodeId> {
    StorageIOError::read_logs(&e).into()
}

fn write_err<E: std::error::Error + 'static>(e: E) -> StorageError<NodeId> {
    StorageIOError::write_logs(&e).into()
}

impl RaftLogReader<TypeConfig> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>> {
        let tx = self.db.begin_read().map_err(read_err)?;
        let table = tx.open_table(LOG_TABLE).map_err(read_err)?;
        let mut out = Vec::new();
        for item in table.range(range).map_err(read_err)? {
            let (_, v) = item.map_err(read_err)?;
            out.push(bincode::deserialize(v.value()).map_err(read_err)?);
        }
        Ok(out)
    }
}

impl RaftLogStorage<TypeConfig> for LogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<NodeId>> {
        let last_purged: Option<LogId<NodeId>> = self.read_meta("last_purged")?;
        let tx = self.db.begin_read().map_err(read_err)?;
        let table = tx.open_table(LOG_TABLE).map_err(read_err)?;
        let last = match table.last().map_err(read_err)? {
            Some((_, v)) => {
                let entry: Entry<TypeConfig> =
                    bincode::deserialize(v.value()).map_err(read_err)?;
                Some(entry.log_id)
            }
            None => None,
        };
        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id: last.or(last_purged),
        })
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        self.write_meta("committed", &committed)
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        Ok(self.read_meta("committed")?.flatten())
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        // Durable avant retour : un vote accordé puis oublié permettrait de
        // voter deux fois dans le même terme.
        self.write_meta("vote", vote)
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        self.read_meta("vote")
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
    {
        let tx = self.db.begin_write().map_err(write_err)?;
        {
            let mut table = tx.open_table(LOG_TABLE).map_err(write_err)?;
            for entry in entries {
                let payload = bincode::serialize(&entry).map_err(write_err)?;
                table
                    .insert(entry.log_id.index, payload.as_slice())
                    .map_err(write_err)?;
            }
        }
        // commit() fsync (durabilité Immediate par défaut) — l'ack Raft ne
        // part qu'après.
        tx.commit().map_err(write_err)?;
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let tx = self.db.begin_write().map_err(write_err)?;
        {
            let mut table = tx.open_table(LOG_TABLE).map_err(write_err)?;
            let keys: Vec<u64> = table
                .range(log_id.index..)
                .map_err(write_err)?
                .map(|r| r.map(|(k, _)| k.value()))
                .collect::<Result<_, _>>()
                .map_err(write_err)?;
            for k in keys {
                table.remove(k).map_err(write_err)?;
            }
        }
        tx.commit().map_err(write_err)?;
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        // Nu (pas d'Option) : get_log_state relit ce champ tel quel, la
        // présence de la clé fait office de Some.
        self.write_meta("last_purged", &log_id)?;
        let tx = self.db.begin_write().map_err(write_err)?;
        {
            let mut table = tx.open_table(LOG_TABLE).map_err(write_err)?;
            let keys: Vec<u64> = table
                .range(..=log_id.index)
                .map_err(write_err)?
                .map(|r| r.map(|(k, _)| k.value()))
                .collect::<Result<_, _>>()
                .map_err(write_err)?;
            for k in keys {
                table.remove(k).map_err(write_err)?;
            }
        }
        tx.commit().map_err(write_err)?;
        Ok(())
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }
}

/// State machine : registre en mémoire + snapshot durable sur disque.
#[derive(Clone)]
pub struct StateMachineStore {
    inner: Arc<Mutex<StateMachineInner>>,
    snapshot_path: PathBuf,
}

#[derive(Debug, Default)]
struct StateMachineInner {
    state: AppState,
    last_applied: Option<LogId<NodeId>>,
    membership: StoredMembership<NodeId, openraft::BasicNode>,
    snapshot: Option<StoredSnapshot>,
    snapshot_idx: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct StoredSnapshot {
    meta: SnapshotMeta<NodeId, openraft::BasicNode>,
    data: Vec<u8>,
}

impl StateMachineStore {
    /// Ouvre la state machine ; recharge le dernier snapshot si présent.
    /// openraft ré-appliquera ensuite les entrées du log postérieures.
    pub fn open(dir: &Path) -> anyhow::Result<Self> {
        fs::create_dir_all(dir)?;
        let snapshot_path = dir.join("snapshot.bin");
        let mut inner = StateMachineInner::default();
        if let Ok(bytes) = fs::read(&snapshot_path) {
            let stored: StoredSnapshot = bincode::deserialize(&bytes)?;
            inner.state = bincode::deserialize(&stored.data)?;
            inner.last_applied = stored.meta.last_log_id;
            inner.membership = stored.meta.last_membership.clone();
            inner.snapshot = Some(stored);
        }
        Ok(Self { inner: Arc::new(Mutex::new(inner)), snapshot_path })
    }

    /// Lecture locale de l'état répliqué (API du nœud, healer).
    pub fn read_state(&self) -> AppState {
        self.inner.lock().unwrap().state.clone()
    }

    /// Écrit le snapshot sur disque : fichier temporaire + fsync + rename.
    fn persist_snapshot(&self, stored: &StoredSnapshot) -> Result<(), StorageError<NodeId>> {
        let sig = stored.meta.signature();
        let bytes = bincode::serialize(stored)
            .map_err(|e| StorageIOError::write_snapshot(Some(sig.clone()), &e))?;
        let tmp = self.snapshot_path.with_extension("tmp");
        (|| -> std::io::Result<()> {
            let mut f = fs::File::create(&tmp)?;
            f.write_all(&bytes)?;
            f.sync_all()?;
            fs::rename(&tmp, &self.snapshot_path)
        })()
        .map_err(|e| StorageIOError::write_snapshot(Some(sig), &e))?;
        Ok(())
    }
}

impl RaftSnapshotBuilder<TypeConfig> for StateMachineStore {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let stored = {
            let mut inner = self.inner.lock().unwrap();
            let data = bincode::serialize(&inner.state)
                .map_err(|e| StorageIOError::write_snapshot(None, &e))?;
            inner.snapshot_idx += 1;
            let snapshot_id = format!(
                "{}-{}",
                inner.last_applied.map(|l| l.index).unwrap_or(0),
                inner.snapshot_idx
            );
            let stored = StoredSnapshot {
                meta: SnapshotMeta {
                    last_log_id: inner.last_applied,
                    last_membership: inner.membership.clone(),
                    snapshot_id,
                },
                data,
            };
            inner.snapshot = Some(stored.clone());
            stored
        };
        self.persist_snapshot(&stored)?;
        Ok(Snapshot {
            meta: stored.meta,
            snapshot: Box::new(Cursor::new(stored.data)),
        })
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
                        AppCommand::UpdateNodeStats { addr, capacity_bytes } => {
                            inner.state.node_capacities.insert(addr, capacity_bytes);
                            AppResponse { ok: true, info: None }
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
        let stored = StoredSnapshot { meta: meta.clone(), data };
        {
            let mut inner = self.inner.lock().unwrap();
            inner.state = state;
            inner.last_applied = meta.last_log_id;
            inner.membership = meta.last_membership.clone();
            inner.snapshot = Some(stored.clone());
        }
        self.persist_snapshot(&stored)?;
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

