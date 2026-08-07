//! Durable openraft storage.
//!
//! - Log + vote: redb, fsync BEFORE acking (a Raft correctness requirement —
//!   an acked vote or entry must survive a crash).
//! - State machine: in memory, rebuilt at startup from the last snapshot
//!   (a file, written atomically) and openraft's log replay. No fsync on the
//!   apply path.
//!
//! A full cluster shutdown (all n nodes powered off) therefore restarts
//! without loss: each node reloads vote + log + snapshot from its data-dir.

// openraft's `StorageError` is large, but its size is imposed by the traits
// implemented here — boxing it would not change the public signatures.
#![allow(clippy::result_large_err)]

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

/// Durable Raft log (redb).
#[derive(Clone)]
pub struct LogStore {
    db: Arc<Database>,
}

impl LogStore {
    pub fn open(dir: &Path) -> anyhow::Result<Self> {
        fs::create_dir_all(dir)?;
        let db = Database::create(dir.join("raft-log.redb"))?;
        // Create the tables if missing, to keep every read path simple.
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
                let entry: Entry<TypeConfig> = bincode::deserialize(v.value()).map_err(read_err)?;
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
        // Durable before returning: a vote granted then forgotten would allow
        // voting twice in the same term.
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
        // commit() fsyncs (Immediate durability by default) — the Raft ack
        // only goes out afterwards.
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
        // Stored bare (not an Option): get_log_state reads the field back as
        // is, the presence of the key stands in for Some.
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

/// State machine: in-memory registry + durable snapshot on disk.
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
    /// Opens the state machine; reloads the last snapshot if present.
    /// openraft then re-applies the log entries that come after it.
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
        Ok(Self {
            inner: Arc::new(Mutex::new(inner)),
            snapshot_path,
        })
    }

    /// Local read of the replicated state (node API, healer).
    pub fn read_state(&self) -> AppState {
        self.inner.lock().unwrap().state.clone()
    }

    /// Writes the snapshot to disk: temp file + fsync + rename.
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
        (
            Option<LogId<NodeId>>,
            StoredMembership<NodeId, openraft::BasicNode>,
        ),
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
                            if inner.state.banned.contains_key(&hash) {
                                // Re-upload of banned content: flatly refused.
                                AppResponse {
                                    ok: false,
                                    info: Some("banned".into()),
                                }
                            } else {
                                inner.state.manifests.insert(hash.clone(), manifest);
                                AppResponse {
                                    ok: true,
                                    info: Some(hash),
                                }
                            }
                        }
                        AppCommand::UnregisterManifest { file_hash } => {
                            let removed = inner.state.manifests.remove(&file_hash).is_some();
                            AppResponse {
                                ok: removed,
                                info: Some(file_hash),
                            }
                        }
                        AppCommand::UpdateNodeStats {
                            addr,
                            capacity_bytes,
                        } => {
                            inner.state.node_capacities.insert(addr, capacity_bytes);
                            AppResponse {
                                ok: true,
                                info: None,
                            }
                        }
                        AppCommand::UpdateNodeCoord { addr, coord } => {
                            inner.state.node_coords.insert(addr, coord);
                            AppResponse {
                                ok: true,
                                info: None,
                            }
                        }
                        AppCommand::BanHash { file_hash, reason } => {
                            // Banning also removes from the registry: the file
                            // becomes unreachable AND its shards become
                            // orphans, hence purgeable by the GC.
                            inner.state.manifests.remove(&file_hash);
                            inner.state.banned.insert(file_hash.clone(), reason);
                            AppResponse {
                                ok: true,
                                info: Some(file_hash),
                            }
                        }
                        AppCommand::UnbanHash { file_hash } => {
                            let removed = inner.state.banned.remove(&file_hash).is_some();
                            AppResponse {
                                ok: removed,
                                info: Some(file_hash),
                            }
                        }

                        // ── S3 ────────────────────────────────────────
                        AppCommand::PutCredential(cred) => {
                            let id = cred.access_key_id.clone();
                            inner.state.s3.credentials.insert(id.clone(), cred);
                            AppResponse {
                                ok: true,
                                info: Some(id),
                            }
                        }
                        AppCommand::DeleteCredential { access_key_id } => {
                            let removed =
                                inner.state.s3.credentials.remove(&access_key_id).is_some();
                            AppResponse {
                                ok: removed,
                                info: Some(access_key_id),
                            }
                        }
                        AppCommand::CreateBucket { name, bucket } => {
                            // S3 refuses to recreate an existing bucket
                            // (BucketAlreadyOwnedByYou / AlreadyExists); the
                            // check belongs here, where it is serialized by
                            // the log, not in the HTTP layer where two
                            // concurrent creates could both pass.
                            if inner.state.s3.buckets.contains_key(&name) {
                                AppResponse {
                                    ok: false,
                                    info: Some("bucket exists".into()),
                                }
                            } else {
                                inner.state.s3.buckets.insert(name.clone(), *bucket);
                                AppResponse {
                                    ok: true,
                                    info: Some(name),
                                }
                            }
                        }
                        AppCommand::UpdateBucket { name, bucket } => {
                            let exists = inner.state.s3.buckets.contains_key(&name);
                            if exists {
                                inner.state.s3.buckets.insert(name.clone(), *bucket);
                            }
                            AppResponse {
                                ok: exists,
                                info: Some(name),
                            }
                        }
                        AppCommand::DeleteBucket { name } => {
                            // Only an empty bucket may go, as S3 requires.
                            let has_objects =
                                inner.state.s3.objects.keys().any(|(b, _)| *b == name);
                            if has_objects {
                                AppResponse {
                                    ok: false,
                                    info: Some("bucket not empty".into()),
                                }
                            } else {
                                let removed = inner.state.s3.buckets.remove(&name).is_some();
                                AppResponse {
                                    ok: removed,
                                    info: Some(name),
                                }
                            }
                        }
                        AppCommand::PutObjectVersion {
                            bucket,
                            key,
                            version,
                        } => {
                            let versioned = inner
                                .state
                                .s3
                                .buckets
                                .get(&bucket)
                                .map(|b| b.versioning == nauka_s3::VersioningState::Enabled)
                                .unwrap_or(false);
                            let entry = inner.state.s3.objects.entry((bucket, key)).or_default();
                            if versioned {
                                // Newest first: history is preserved.
                                entry.versions.insert(0, *version);
                            } else {
                                // Unversioned (or suspended): a single
                                // "null" version, replaced in place.
                                entry
                                    .versions
                                    .retain(|v| v.version_id != version.version_id);
                                entry.versions.insert(0, *version);
                            }
                            AppResponse {
                                ok: true,
                                info: None,
                            }
                        }
                        AppCommand::DeleteObjectVersion {
                            bucket,
                            key,
                            version_id,
                        } => {
                            let k = (bucket, key);
                            let mut removed = false;
                            if let Some(entry) = inner.state.s3.objects.get_mut(&k) {
                                let before = entry.versions.len();
                                entry.versions.retain(|v| v.version_id != version_id);
                                removed = entry.versions.len() != before;
                                // An entry with no versions left is gone:
                                // `objects` never holds an empty history.
                                if entry.versions.is_empty() {
                                    inner.state.s3.objects.remove(&k);
                                }
                            }
                            AppResponse {
                                ok: removed,
                                info: None,
                            }
                        }
                        AppCommand::PutUpload(upload) => {
                            let id = upload.upload_id.clone();
                            inner.state.s3.uploads.insert(id.clone(), *upload);
                            AppResponse {
                                ok: true,
                                info: Some(id),
                            }
                        }
                        AppCommand::PutUploadPart {
                            upload_id,
                            part_number,
                            part,
                        } => match inner.state.s3.uploads.get_mut(&upload_id) {
                            Some(upload) => {
                                upload.parts.insert(part_number, *part);
                                AppResponse {
                                    ok: true,
                                    info: None,
                                }
                            }
                            None => AppResponse {
                                ok: false,
                                info: Some("no such upload".into()),
                            },
                        },
                        AppCommand::DeleteUpload { upload_id } => {
                            let removed = inner.state.s3.uploads.remove(&upload_id).is_some();
                            AppResponse {
                                ok: removed,
                                info: Some(upload_id),
                            }
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
        let stored = StoredSnapshot {
            meta: meta.clone(),
            data,
        };
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
