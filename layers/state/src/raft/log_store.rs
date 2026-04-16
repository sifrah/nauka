use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io;
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use futures::lock::Mutex;
use openraft::LogState;
use openraft::RaftTypeConfig;
use openraft::alias::{LogIdOf, VoteOf};
use openraft::entry::RaftEntry;
use openraft::storage::IOFlushed;
use serde::de::DeserializeOwned;
use serde::Serialize;

pub const DEFAULT_RAFT_DIR: &str = "/var/lib/nauka/raft";

#[derive(Debug, Clone)]
pub struct LogStore<C: RaftTypeConfig> {
    inner: Arc<Mutex<LogStoreInner<C>>>,
}

#[derive(Debug)]
struct LogStoreInner<C: RaftTypeConfig> {
    data_dir: PathBuf,
    last_purged_log_id: Option<LogIdOf<C>>,
    log: BTreeMap<u64, C::Entry>,
    committed: Option<LogIdOf<C>>,
    vote: Option<VoteOf<C>>,
}

fn read_json_opt<T: DeserializeOwned>(path: &Path) -> Result<Option<T>, io::Error> {
    match std::fs::read(path) {
        Ok(data) => {
            let val = serde_json::from_slice(&data)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            Ok(Some(val))
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

fn write_json<T: Serialize>(path: &Path, val: &T) -> Result<(), io::Error> {
    let data =
        serde_json::to_vec(val).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    std::fs::write(path, data)
}

fn entry_path(data_dir: &Path, index: u64) -> PathBuf {
    data_dir.join("log").join(format!("{index:020}.json"))
}

impl<C: RaftTypeConfig> LogStore<C>
where
    C::Entry: Serialize + DeserializeOwned,
    VoteOf<C>: Serialize + DeserializeOwned,
    LogIdOf<C>: Serialize + DeserializeOwned,
{
    pub fn open(data_dir: &str) -> Result<Self, io::Error> {
        let data_dir = PathBuf::from(data_dir);
        let log_dir = data_dir.join("log");
        std::fs::create_dir_all(&log_dir)?;

        let vote = read_json_opt(&data_dir.join("vote.json"))?;
        let committed = read_json_opt(&data_dir.join("committed.json"))?;
        let last_purged_log_id = read_json_opt(&data_dir.join("purged.json"))?;

        let mut log = BTreeMap::new();
        for dir_entry in std::fs::read_dir(&log_dir)? {
            let dir_entry = dir_entry?;
            let path = dir_entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("json") {
                continue;
            }
            let data = std::fs::read(&path)?;
            let log_entry: C::Entry = serde_json::from_slice(&data)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let index = log_entry.index();
            log.insert(index, log_entry);
        }

        Ok(Self {
            inner: Arc::new(Mutex::new(LogStoreInner {
                data_dir,
                last_purged_log_id,
                log,
                committed,
                vote,
            })),
        })
    }
}

#[cfg(test)]
impl<C: RaftTypeConfig> LogStore<C>
where
    C::Entry: Serialize + DeserializeOwned,
    VoteOf<C>: Serialize + DeserializeOwned,
    LogIdOf<C>: Serialize + DeserializeOwned,
{
    pub(crate) async fn test_append_entries(
        &mut self,
        entries: Vec<C::Entry>,
    ) -> Result<(), io::Error> {
        let mut inner = self.inner.lock().await;
        for entry in entries {
            let path = entry_path(&inner.data_dir, entry.index());
            write_json(&path, &entry)?;
            inner.log.insert(entry.index(), entry);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use openraft::alias::{DefaultEntryOf, LogIdOf};
    use openraft::impls::leader_id_adv::LeaderId;
    use openraft::storage::RaftLogStorage;
    use openraft::{EntryPayload, LogId, RaftLogReader, Vote};

    use super::*;
    use crate::raft::types::{SurqlCommand, TypeConfig};

    type E = DefaultEntryOf<TypeConfig>;

    fn lid(term: u64, index: u64) -> LogIdOf<TypeConfig> {
        LogId::new(LeaderId { term, node_id: 1 }, index)
    }

    fn blank_entry(term: u64, index: u64) -> E {
        E {
            log_id: lid(term, index),
            payload: EntryPayload::Blank,
        }
    }

    fn normal_entry(term: u64, index: u64, query: &str) -> E {
        E {
            log_id: lid(term, index),
            payload: EntryPayload::Normal(SurqlCommand {
                query: query.into(),
            }),
        }
    }

    fn tmp_dir() -> tempfile::TempDir {
        tempfile::tempdir().unwrap()
    }

    #[tokio::test]
    async fn vote_persists_across_reopen() {
        let dir = tmp_dir();
        let path = dir.path().to_str().unwrap();

        let vote = Vote::new(1u64, 42u64);
        {
            let mut store = LogStore::<TypeConfig>::open(path).unwrap();
            store.save_vote(&vote).await.unwrap();
        }
        {
            let mut store = LogStore::<TypeConfig>::open(path).unwrap();
            let loaded = store.read_vote().await.unwrap();
            assert_eq!(loaded, Some(vote));
        }
    }

    #[tokio::test]
    async fn committed_persists_across_reopen() {
        let dir = tmp_dir();
        let path = dir.path().to_str().unwrap();

        let log_id = lid(1, 5);
        {
            let mut store = LogStore::<TypeConfig>::open(path).unwrap();
            store.save_committed(Some(log_id)).await.unwrap();
        }
        {
            let mut store = LogStore::<TypeConfig>::open(path).unwrap();
            let loaded = store.read_committed().await.unwrap();
            assert_eq!(loaded, Some(log_id));
        }
    }

    #[tokio::test]
    async fn log_entries_persist_across_reopen() {
        let dir = tmp_dir();
        let path = dir.path().to_str().unwrap();

        let entries = vec![
            normal_entry(1, 1, "SELECT * FROM t1"),
            normal_entry(1, 2, "SELECT * FROM t2"),
            normal_entry(1, 3, "SELECT * FROM t3"),
        ];

        {
            let mut store = LogStore::<TypeConfig>::open(path).unwrap();
            store.test_append_entries(entries.clone()).await.unwrap();
        }
        {
            let mut store = LogStore::<TypeConfig>::open(path).unwrap();
            let loaded = store.try_get_log_entries(1..=3).await.unwrap();
            assert_eq!(loaded.len(), 3);
            assert_eq!(loaded[0].log_id, entries[0].log_id);
            assert_eq!(loaded[2].log_id, entries[2].log_id);
        }
    }

    #[tokio::test]
    async fn purge_removes_entries_and_persists() {
        let dir = tmp_dir();
        let path = dir.path().to_str().unwrap();

        let entries: Vec<E> = (1..=5).map(|i| blank_entry(1, i)).collect();

        {
            let mut store = LogStore::<TypeConfig>::open(path).unwrap();
            store.test_append_entries(entries).await.unwrap();
            store.purge(lid(1, 3)).await.unwrap();
        }
        {
            let mut store = LogStore::<TypeConfig>::open(path).unwrap();
            let state = store.get_log_state().await.unwrap();
            assert_eq!(state.last_purged_log_id, Some(lid(1, 3)));
            let remaining = store.try_get_log_entries(1..=5).await.unwrap();
            assert_eq!(remaining.len(), 2);
            assert_eq!(remaining[0].log_id.index(), 4);
        }
    }

    #[tokio::test]
    async fn truncate_removes_tail_and_persists() {
        let dir = tmp_dir();
        let path = dir.path().to_str().unwrap();

        let entries: Vec<E> = (1..=5).map(|i| blank_entry(1, i)).collect();

        {
            let mut store = LogStore::<TypeConfig>::open(path).unwrap();
            store.test_append_entries(entries).await.unwrap();
            store.truncate_after(Some(lid(1, 3))).await.unwrap();
        }
        {
            let mut store = LogStore::<TypeConfig>::open(path).unwrap();
            let remaining = store.try_get_log_entries(1..=5).await.unwrap();
            assert_eq!(remaining.len(), 3);
            assert_eq!(remaining[2].log_id.index(), 3);
        }
    }

    #[tokio::test]
    async fn open_empty_dir_gives_clean_state() {
        let dir = tmp_dir();
        let path = dir.path().to_str().unwrap();

        let mut store = LogStore::<TypeConfig>::open(path).unwrap();
        assert_eq!(store.read_vote().await.unwrap(), None);
        assert_eq!(store.read_committed().await.unwrap(), None);
        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id, None);
        assert_eq!(state.last_log_id, None);
    }
}

mod impl_log_store {
    use super::*;
    use openraft::storage::RaftLogStorage;
    use openraft::RaftLogReader;

    impl<C: RaftTypeConfig> RaftLogReader<C> for LogStore<C>
    where
        C::Entry: Clone,
    {
        async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug>(
            &mut self,
            range: RB,
        ) -> Result<Vec<C::Entry>, io::Error> {
            let inner = self.inner.lock().await;
            Ok(inner.log.range(range).map(|(_, v)| v.clone()).collect())
        }

        async fn read_vote(&mut self) -> Result<Option<VoteOf<C>>, io::Error> {
            let inner = self.inner.lock().await;
            Ok(inner.vote.clone())
        }
    }

    impl<C: RaftTypeConfig> RaftLogStorage<C> for LogStore<C>
    where
        C::Entry: Serialize + DeserializeOwned + Clone,
        VoteOf<C>: Serialize + DeserializeOwned,
        LogIdOf<C>: Serialize + DeserializeOwned,
    {
        type LogReader = Self;

        async fn get_log_state(&mut self) -> Result<LogState<C>, io::Error> {
            let inner = self.inner.lock().await;
            let last = inner.log.iter().next_back().map(|(_, e)| e.log_id());
            let last_purged = inner.last_purged_log_id.clone();
            Ok(LogState {
                last_purged_log_id: last_purged.clone(),
                last_log_id: last.or(last_purged),
            })
        }

        async fn save_committed(
            &mut self,
            committed: Option<LogIdOf<C>>,
        ) -> Result<(), io::Error> {
            let mut inner = self.inner.lock().await;
            let path = inner.data_dir.join("committed.json");
            match committed {
                Some(ref c) => write_json(&path, c)?,
                None => {
                    let _ = std::fs::remove_file(&path);
                }
            }
            inner.committed = committed;
            Ok(())
        }

        async fn read_committed(&mut self) -> Result<Option<LogIdOf<C>>, io::Error> {
            let inner = self.inner.lock().await;
            Ok(inner.committed.clone())
        }

        async fn save_vote(&mut self, vote: &VoteOf<C>) -> Result<(), io::Error> {
            let mut inner = self.inner.lock().await;
            write_json(&inner.data_dir.join("vote.json"), vote)?;
            inner.vote = Some(vote.clone());
            Ok(())
        }

        async fn append<I>(
            &mut self,
            entries: I,
            callback: IOFlushed<C>,
        ) -> Result<(), io::Error>
        where
            I: IntoIterator<Item = C::Entry>,
        {
            let mut inner = self.inner.lock().await;
            for entry in entries {
                let path = entry_path(&inner.data_dir, entry.index());
                write_json(&path, &entry)?;
                inner.log.insert(entry.index(), entry);
            }
            callback.io_completed(Ok(()));
            Ok(())
        }

        async fn truncate_after(
            &mut self,
            last_log_id: Option<LogIdOf<C>>,
        ) -> Result<(), io::Error> {
            let mut inner = self.inner.lock().await;
            let start = match last_log_id {
                Some(id) => id.index() + 1,
                None => 0,
            };
            let keys: Vec<u64> = inner.log.range(start..).map(|(k, _)| *k).collect();
            for k in keys {
                inner.log.remove(&k);
                let _ = std::fs::remove_file(entry_path(&inner.data_dir, k));
            }
            Ok(())
        }

        async fn purge(&mut self, log_id: LogIdOf<C>) -> Result<(), io::Error> {
            let mut inner = self.inner.lock().await;
            let keys: Vec<u64> = inner
                .log
                .range(..=log_id.index())
                .map(|(k, _)| *k)
                .collect();
            for k in keys {
                inner.log.remove(&k);
                let _ = std::fs::remove_file(entry_path(&inner.data_dir, k));
            }
            write_json(&inner.data_dir.join("purged.json"), &log_id)?;
            inner.last_purged_log_id = Some(log_id);
            Ok(())
        }

        async fn get_log_reader(&mut self) -> Self::LogReader {
            self.clone()
        }
    }
}
