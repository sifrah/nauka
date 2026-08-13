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
            inner.state = crate::types::AppState::from_snapshot_bytes(&stored.data)?;
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

    /// The log index this state machine has actually applied — the index
    /// `read_state` reflects. openraft's reported metric can lead this by a
    /// moment while a freshly-received snapshot is being installed, so a
    /// reader comparing the two can tell whether what it sees is caught up.
    pub fn applied_index(&self) -> u64 {
        self.inner
            .lock()
            .unwrap()
            .last_applied
            .map(|l| l.index)
            .unwrap_or(0)
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
                        AppCommand::SetNodeDisabled { addr, disabled } => {
                            if disabled {
                                inner.state.disabled.insert(addr);
                            } else {
                                inner.state.disabled.remove(&addr);
                            }
                            AppResponse {
                                ok: true,
                                info: None,
                            }
                        }
                        AppCommand::UpsertOrg { name, record } => {
                            inner.state.orgs.insert(name, record);
                            AppResponse {
                                ok: true,
                                info: None,
                            }
                        }
                        AppCommand::DeleteOrg { name } => {
                            // Enforced in the state machine, not the CLI:
                            // every replica refuses identically, whatever
                            // client sent the command.
                            let in_use = inner.state.spaces.values().any(|s| s.org == name);
                            if in_use {
                                AppResponse {
                                    ok: false,
                                    info: Some(format!(
                                        "organisation {name} still has spaces — \
                                             delete them first"
                                    )),
                                }
                            } else if inner.state.orgs.remove(&name).is_none() {
                                AppResponse {
                                    ok: false,
                                    info: Some(format!("no organisation named {name}")),
                                }
                            } else {
                                AppResponse {
                                    ok: true,
                                    info: None,
                                }
                            }
                        }
                        AppCommand::UpsertSpace { name, record } => {
                            // The key must be exactly `<org>/<one segment>`,
                            // with the org matching the record's.
                            let well_formed = name
                                .strip_prefix(&format!("{}/", record.org))
                                .is_some_and(|rest| !rest.is_empty() && !rest.contains('/'));
                            if !inner.state.orgs.contains_key(&record.org) {
                                AppResponse {
                                    ok: false,
                                    info: Some(format!(
                                        "no organisation named {} — create it first",
                                        record.org
                                    )),
                                }
                            } else if !well_formed {
                                AppResponse {
                                    ok: false,
                                    info: Some(format!(
                                        "space name {name} does not match its \
                                             organisation {} (expected {}/<name>)",
                                        record.org, record.org
                                    )),
                                }
                            } else {
                                inner.state.spaces.insert(name, record);
                                AppResponse {
                                    ok: true,
                                    info: None,
                                }
                            }
                        }
                        AppCommand::DeleteSpace { name } => {
                            let refs = inner
                                .state
                                .file_refs
                                .values()
                                .filter(|spaces| spaces.contains(&name))
                                .count();
                            if refs > 0 {
                                // Same shape as org deletion: emptying a
                                // space is a deliberate, file-by-file act,
                                // never a side effect of `space rm`.
                                AppResponse {
                                    ok: false,
                                    info: Some(format!(
                                        "space {name} still references {refs} file(s) — \
                                         release them first"
                                    )),
                                }
                            } else if inner.state.spaces.remove(&name).is_none() {
                                AppResponse {
                                    ok: false,
                                    info: Some(format!("no space named {name}")),
                                }
                            } else {
                                // Keys die with their space: a future
                                // space reusing the name must not
                                // inherit someone else's signers.
                                inner.state.space_keys.remove(&name);
                                AppResponse {
                                    ok: true,
                                    info: None,
                                }
                            }
                        }
                        AppCommand::AddFileRef { file_hash, space } => {
                            if !inner.state.spaces.contains_key(&space) {
                                AppResponse {
                                    ok: false,
                                    info: Some(format!("no space named {space}")),
                                }
                            } else if !inner.state.manifests.contains_key(&file_hash) {
                                AppResponse {
                                    ok: false,
                                    info: Some(format!("no file {file_hash} in the registry")),
                                }
                            } else {
                                inner
                                    .state
                                    .file_refs
                                    .entry(file_hash)
                                    .or_default()
                                    .insert(space);
                                AppResponse {
                                    ok: true,
                                    info: None,
                                }
                            }
                        }
                        AppCommand::UpdateSpaceEgress {
                            node_addr,
                            space,
                            egress,
                        } => {
                            inner
                                .state
                                .space_egress
                                .entry(space)
                                .or_default()
                                .insert(node_addr, egress);
                            AppResponse {
                                ok: true,
                                info: None,
                            }
                        }
                        AppCommand::SetAcmeTxt {
                            name,
                            node_addr,
                            value,
                        } => {
                            inner
                                .state
                                .acme_txt
                                .entry(name)
                                .or_default()
                                .insert(node_addr, value);
                            AppResponse {
                                ok: true,
                                info: None,
                            }
                        }
                        AppCommand::ClearAcmeTxt { name, node_addr } => {
                            if let Some(rows) = inner.state.acme_txt.get_mut(&name) {
                                rows.remove(&node_addr);
                                if rows.is_empty() {
                                    inner.state.acme_txt.remove(&name);
                                }
                            }
                            AppResponse {
                                ok: true,
                                info: None,
                            }
                        }
                        AppCommand::RemoveFileRef { file_hash, space } => {
                            let removed = inner
                                .state
                                .file_refs
                                .get_mut(&file_hash)
                                .is_some_and(|spaces| spaces.remove(&space));
                            if !removed {
                                AppResponse {
                                    ok: false,
                                    info: Some(format!("{space} does not reference {file_hash}")),
                                }
                            } else if inner
                                .state
                                .file_refs
                                .get(&file_hash)
                                .is_some_and(|s| s.is_empty())
                            {
                                // Last reference gone: the file dies in the
                                // SAME apply — every replica reaches the
                                // identical registry, and the GC reclaims
                                // the shards on its following passes.
                                inner.state.file_refs.remove(&file_hash);
                                inner.state.manifests.remove(&file_hash);
                                AppResponse {
                                    ok: true,
                                    info: Some("last reference — file unregistered".into()),
                                }
                            } else {
                                AppResponse {
                                    ok: true,
                                    info: None,
                                }
                            }
                        }
                        AppCommand::AddSpaceKey { space, key } => {
                            if !inner.state.spaces.contains_key(&space) {
                                AppResponse {
                                    ok: false,
                                    info: Some(format!("no space named {space}")),
                                }
                            } else {
                                let keys = inner.state.space_keys.entry(space).or_default();
                                if keys.iter().any(|k| k.public_key == key.public_key) {
                                    AppResponse {
                                        ok: false,
                                        info: Some("this public key is already registered".into()),
                                    }
                                } else if keys.iter().any(|k| k.name == key.name) {
                                    AppResponse {
                                        ok: false,
                                        info: Some(format!(
                                            "a key named {:?} already exists on this \
                                                 space — pick another --name",
                                            key.name
                                        )),
                                    }
                                } else {
                                    keys.push(key);
                                    AppResponse {
                                        ok: true,
                                        info: None,
                                    }
                                }
                            }
                        }
                        AppCommand::RemoveSpaceKey { space, public_key } => {
                            let removed = match inner.state.space_keys.get_mut(&space) {
                                Some(keys) => {
                                    let before = keys.len();
                                    keys.retain(|k| k.public_key != public_key);
                                    if keys.is_empty() {
                                        inner.state.space_keys.remove(&space);
                                    }
                                    before
                                        != inner.state.space_keys.get(&space).map_or(0, |k| k.len())
                                }
                                None => false,
                            };
                            if removed {
                                AppResponse {
                                    ok: true,
                                    info: None,
                                }
                            } else {
                                AppResponse {
                                    ok: false,
                                    info: Some(format!("no such key on space {space}")),
                                }
                            }
                        }
                        AppCommand::UpdateNodeEgress { addr, egress } => {
                            inner.state.node_egress.insert(addr, egress);
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
                        AppCommand::SetObjectTags {
                            bucket,
                            key,
                            version_id,
                            tags,
                        } => {
                            let found =
                                inner
                                    .state
                                    .s3
                                    .objects
                                    .get_mut(&(bucket, key))
                                    .and_then(|entry| match &version_id {
                                        Some(id) => {
                                            entry.versions.iter_mut().find(|v| v.version_id == *id)
                                        }
                                        None => entry.versions.first_mut(),
                                    });
                            match found {
                                Some(v) => {
                                    v.tags = tags;
                                    AppResponse {
                                        ok: true,
                                        info: None,
                                    }
                                }
                                None => AppResponse {
                                    ok: false,
                                    info: Some("no such object".into()),
                                },
                            }
                        }
                        AppCommand::SetObjectAcl {
                            bucket,
                            key,
                            version_id,
                            acl,
                        } => {
                            let found =
                                inner
                                    .state
                                    .s3
                                    .objects
                                    .get_mut(&(bucket, key))
                                    .and_then(|entry| match &version_id {
                                        Some(id) => {
                                            entry.versions.iter_mut().find(|v| v.version_id == *id)
                                        }
                                        None => entry.versions.first_mut(),
                                    });
                            match found {
                                Some(v) => {
                                    v.acl = acl;
                                    AppResponse {
                                        ok: true,
                                        info: None,
                                    }
                                }
                                None => AppResponse {
                                    ok: false,
                                    info: Some("no such object".into()),
                                },
                            }
                        }
                        AppCommand::SetObjectRetention {
                            bucket,
                            key,
                            version_id,
                            retention,
                        } => {
                            let found =
                                inner
                                    .state
                                    .s3
                                    .objects
                                    .get_mut(&(bucket, key))
                                    .and_then(|e| match &version_id {
                                        Some(id) => {
                                            e.versions.iter_mut().find(|v| v.version_id == *id)
                                        }
                                        None => e.versions.first_mut(),
                                    });
                            match found {
                                Some(v) => {
                                    v.retention = retention;
                                    AppResponse {
                                        ok: true,
                                        info: None,
                                    }
                                }
                                None => AppResponse {
                                    ok: false,
                                    info: Some("no such object".into()),
                                },
                            }
                        }
                        AppCommand::SetObjectLegalHold {
                            bucket,
                            key,
                            version_id,
                            on,
                        } => {
                            let found =
                                inner
                                    .state
                                    .s3
                                    .objects
                                    .get_mut(&(bucket, key))
                                    .and_then(|e| match &version_id {
                                        Some(id) => {
                                            e.versions.iter_mut().find(|v| v.version_id == *id)
                                        }
                                        None => e.versions.first_mut(),
                                    });
                            match found {
                                Some(v) => {
                                    v.legal_hold = on;
                                    AppResponse {
                                        ok: true,
                                        info: None,
                                    }
                                }
                                None => AppResponse {
                                    ok: false,
                                    info: Some("no such object".into()),
                                },
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
        let state: AppState = crate::types::AppState::from_snapshot_bytes(&data)
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
