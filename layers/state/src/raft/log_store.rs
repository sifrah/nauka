use std::fmt::Debug;
use std::io;
use std::marker::PhantomData;
use std::ops::RangeBounds;
use std::sync::Arc;

use openraft::alias::{LogIdOf, VoteOf};
use openraft::entry::RaftEntry;
use openraft::storage::IOFlushed;
use openraft::LogState;
use openraft::RaftTypeConfig;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use surrealdb::types::SurrealValue;

use crate::db::Database;

fn db_err(e: impl std::fmt::Display) -> io::Error {
    io::Error::other(e.to_string())
}

pub struct LogStore<C: RaftTypeConfig> {
    db: Arc<Database>,
    node_id: u64,
    _phantom: PhantomData<C>,
}

impl<C: RaftTypeConfig> Clone for LogStore<C> {
    fn clone(&self) -> Self {
        Self {
            db: self.db.clone(),
            node_id: self.node_id,
            _phantom: PhantomData,
        }
    }
}

impl<C: RaftTypeConfig> std::fmt::Debug for LogStore<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogStore")
            .field("node_id", &self.node_id)
            .finish()
    }
}

#[derive(Deserialize, SurrealValue)]
struct MetaRecord {
    #[serde(default)]
    vote: Option<String>,
    #[serde(default)]
    committed: Option<String>,
    #[serde(default)]
    purged: Option<String>,
}

#[derive(Deserialize, SurrealValue)]
struct LogRecord {
    #[allow(dead_code)]
    log_index: i64,
    entry: String,
}

impl<C: RaftTypeConfig> LogStore<C>
where
    C::Entry: Serialize + DeserializeOwned,
    VoteOf<C>: Serialize + DeserializeOwned,
    LogIdOf<C>: Serialize + DeserializeOwned,
{
    pub async fn open(db: Arc<Database>, node_id: u64) -> Result<Self, io::Error> {
        let hv = node_id as i64;
        // CREATE if not exists; ignore "already exists" error on restart
        let _ = db
            .inner()
            .query(format!("CREATE _raft_meta:{node_id} SET hypervisor = {hv}"))
            .await;

        Ok(Self {
            db,
            node_id,
            _phantom: PhantomData,
        })
    }

    async fn query_meta(&self) -> Result<Option<MetaRecord>, io::Error> {
        let mut resp = self
            .db
            .inner()
            .query(format!(
                "SELECT vote, committed, purged FROM _raft_meta:{}",
                self.node_id
            ))
            .await
            .map_err(db_err)?;
        let records: Vec<MetaRecord> = resp.take(0).map_err(db_err)?;
        Ok(records.into_iter().next())
    }

    async fn update_meta_field(&self, field: &str, val: &str) -> Result<(), io::Error> {
        self.db
            .inner()
            .query(format!(
                "UPDATE _raft_meta:{} SET {field} = $val",
                self.node_id
            ))
            .bind(("val", val.to_string()))
            .await
            .map_err(db_err)?
            .check()
            .map_err(db_err)?;
        Ok(())
    }

    async fn clear_meta_field(&self, field: &str) -> Result<(), io::Error> {
        self.db
            .inner()
            .query(format!(
                "UPDATE _raft_meta:{} SET {field} = NONE",
                self.node_id
            ))
            .await
            .map_err(db_err)?
            .check()
            .map_err(db_err)?;
        Ok(())
    }
}

mod impl_log_store {
    use super::*;
    use openraft::storage::RaftLogStorage;
    use openraft::RaftLogReader;

    impl<C: RaftTypeConfig> RaftLogReader<C> for LogStore<C>
    where
        C::Entry: Serialize + DeserializeOwned + Clone,
        VoteOf<C>: Serialize + DeserializeOwned,
        LogIdOf<C>: Serialize + DeserializeOwned,
    {
        async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug>(
            &mut self,
            range: RB,
        ) -> Result<Vec<C::Entry>, io::Error> {
            use std::ops::Bound;

            let start = match range.start_bound() {
                Bound::Included(&n) => n as i64,
                Bound::Excluded(&n) => n as i64 + 1,
                Bound::Unbounded => 0i64,
            };
            let end = match range.end_bound() {
                Bound::Included(&n) => n as i64,
                Bound::Excluded(&n) => n as i64 - 1,
                Bound::Unbounded => i64::MAX,
            };

            let hv = self.node_id as i64;
            let mut resp = self
                .db
                .inner()
                .query(format!(
                    "SELECT log_index, entry FROM _raft_log \
                     WHERE hypervisor = {hv} AND log_index >= {start} AND log_index <= {end} \
                     ORDER BY log_index"
                ))
                .await
                .map_err(db_err)?;

            let records: Vec<LogRecord> = resp.take(0).map_err(db_err)?;
            let mut entries = Vec::with_capacity(records.len());
            for r in records {
                let entry: C::Entry = serde_json::from_str(&r.entry)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                entries.push(entry);
            }
            Ok(entries)
        }

        async fn read_vote(&mut self) -> Result<Option<VoteOf<C>>, io::Error> {
            let meta = self.query_meta().await?;
            match meta.and_then(|m| m.vote) {
                Some(json) => {
                    let vote = serde_json::from_str(&json)
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                    Ok(Some(vote))
                }
                None => Ok(None),
            }
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
            let meta = self.query_meta().await?;
            let last_purged: Option<LogIdOf<C>> = meta
                .and_then(|m| m.purged)
                .map(|json| serde_json::from_str(&json))
                .transpose()
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

            let hv = self.node_id as i64;
            let mut resp = self
                .db
                .inner()
                .query(format!(
                    "SELECT log_index, entry FROM _raft_log \
                     WHERE hypervisor = {hv} ORDER BY log_index DESC LIMIT 1"
                ))
                .await
                .map_err(db_err)?;
            let last_entries: Vec<LogRecord> = resp.take(0).map_err(db_err)?;

            let last_log_id = if let Some(r) = last_entries.into_iter().next() {
                let entry: C::Entry = serde_json::from_str(&r.entry)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                Some(entry.log_id())
            } else {
                None
            };

            Ok(LogState {
                last_purged_log_id: last_purged.clone(),
                last_log_id: last_log_id.or(last_purged),
            })
        }

        async fn save_committed(&mut self, committed: Option<LogIdOf<C>>) -> Result<(), io::Error> {
            match committed {
                Some(ref c) => {
                    let json = serde_json::to_string(c)
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                    self.update_meta_field("committed", &json).await
                }
                None => self.clear_meta_field("committed").await,
            }
        }

        async fn read_committed(&mut self) -> Result<Option<LogIdOf<C>>, io::Error> {
            let meta = self.query_meta().await?;
            match meta.and_then(|m| m.committed) {
                Some(json) => {
                    let val = serde_json::from_str(&json)
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                    Ok(Some(val))
                }
                None => Ok(None),
            }
        }

        async fn save_vote(&mut self, vote: &VoteOf<C>) -> Result<(), io::Error> {
            let json = serde_json::to_string(vote)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            self.update_meta_field("vote", &json).await
        }

        async fn append<I>(&mut self, entries: I, callback: IOFlushed<C>) -> Result<(), io::Error>
        where
            I: IntoIterator<Item = C::Entry>,
        {
            let hv = self.node_id as i64;
            for entry in entries {
                let idx = entry.index() as i64;
                let entry_json = serde_json::to_string(&entry)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

                self.db
                    .inner()
                    .query(format!(
                        "UPSERT _raft_log:\u{27e8}{}_{}\u{27e9} SET hypervisor = {hv}, log_index = {idx}, entry = $entry",
                        self.node_id,
                        entry.index()
                    ))
                    .bind(("entry", entry_json))
                    .await
                    .map_err(db_err)?
                    .check()
                    .map_err(db_err)?;
            }
            callback.io_completed(Ok(()));
            Ok(())
        }

        async fn truncate_after(
            &mut self,
            last_log_id: Option<LogIdOf<C>>,
        ) -> Result<(), io::Error> {
            let start = match last_log_id {
                Some(id) => id.index() as i64 + 1,
                None => 0i64,
            };
            let hv = self.node_id as i64;
            self.db
                .inner()
                .query(format!(
                    "DELETE _raft_log WHERE hypervisor = {hv} AND log_index >= {start}"
                ))
                .await
                .map_err(db_err)?
                .check()
                .map_err(db_err)?;
            Ok(())
        }

        async fn purge(&mut self, log_id: LogIdOf<C>) -> Result<(), io::Error> {
            let hv = self.node_id as i64;
            let idx = log_id.index() as i64;
            self.db
                .inner()
                .query(format!(
                    "DELETE _raft_log WHERE hypervisor = {hv} AND log_index <= {idx}"
                ))
                .await
                .map_err(db_err)?
                .check()
                .map_err(db_err)?;

            let json = serde_json::to_string(&log_id)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            tracing::info!(
                node_id = self.node_id,
                up_to_index = idx,
                "raft: purged log entries"
            );
            self.update_meta_field("purged", &json).await
        }

        async fn get_log_reader(&mut self) -> Self::LogReader {
            self.clone()
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

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

    async fn test_db() -> Arc<Database> {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = Arc::new(Database::open(Some(path.to_str().unwrap())).await.unwrap());
        db.query(crate::SCHEMA).await.unwrap();
        std::mem::forget(dir);
        db
    }

    async fn test_store(db: &Arc<Database>) -> LogStore<TypeConfig> {
        LogStore::<TypeConfig>::open(db.clone(), 42).await.unwrap()
    }

    async fn insert_entries(store: &LogStore<TypeConfig>, entries: &[E]) {
        let hv = store.node_id as i64;
        for e in entries {
            let idx = e.log_id.index() as i64;
            let json = serde_json::to_string(e).unwrap();
            store
                .db
                .inner()
                .query(format!(
                    "UPSERT _raft_log:\u{27e8}{}_{idx}\u{27e9} SET hypervisor = {hv}, log_index = {idx}, entry = $entry",
                    store.node_id
                ))
                .bind(("entry", json))
                .await
                .unwrap()
                .check()
                .unwrap();
        }
    }

    #[tokio::test]
    async fn vote_persists_across_reopen() {
        let db = test_db().await;
        let vote = Vote::new(1u64, 42u64);
        {
            let mut store = test_store(&db).await;
            store.save_vote(&vote).await.unwrap();
        }
        {
            let mut store = test_store(&db).await;
            let loaded = store.read_vote().await.unwrap();
            assert_eq!(loaded, Some(vote));
        }
    }

    #[tokio::test]
    async fn committed_persists_across_reopen() {
        let db = test_db().await;
        let log_id = lid(1, 5);
        {
            let mut store = test_store(&db).await;
            store.save_committed(Some(log_id)).await.unwrap();
        }
        {
            let mut store = test_store(&db).await;
            let loaded = store.read_committed().await.unwrap();
            assert_eq!(loaded, Some(log_id));
        }
    }

    #[tokio::test]
    async fn log_entries_persist() {
        let db = test_db().await;
        let mut store = test_store(&db).await;

        let entries = vec![
            normal_entry(1, 1, "SELECT * FROM t1"),
            normal_entry(1, 2, "SELECT * FROM t2"),
            normal_entry(1, 3, "SELECT * FROM t3"),
        ];
        insert_entries(&store, &entries).await;

        let loaded = store.try_get_log_entries(1..=3).await.unwrap();
        assert_eq!(loaded.len(), 3);
        assert_eq!(loaded[0].log_id, entries[0].log_id);
        assert_eq!(loaded[2].log_id, entries[2].log_id);
    }

    #[tokio::test]
    async fn purge_removes_entries() {
        let db = test_db().await;
        let mut store = test_store(&db).await;

        let entries: Vec<E> = (1..=5).map(|i| blank_entry(1, i)).collect();
        insert_entries(&store, &entries).await;

        store.purge(lid(1, 3)).await.unwrap();

        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id, Some(lid(1, 3)));
        let remaining = store.try_get_log_entries(1..=5).await.unwrap();
        assert_eq!(remaining.len(), 2);
        assert_eq!(remaining[0].log_id.index(), 4);
    }

    #[tokio::test]
    async fn truncate_removes_tail() {
        let db = test_db().await;
        let mut store = test_store(&db).await;

        let entries: Vec<E> = (1..=5).map(|i| blank_entry(1, i)).collect();
        insert_entries(&store, &entries).await;

        store.truncate_after(Some(lid(1, 3))).await.unwrap();

        let remaining = store.try_get_log_entries(1..=5).await.unwrap();
        assert_eq!(remaining.len(), 3);
        assert_eq!(remaining[2].log_id.index(), 3);
    }

    #[tokio::test]
    async fn open_fresh_gives_clean_state() {
        let db = test_db().await;
        let mut store = test_store(&db).await;
        assert_eq!(store.read_vote().await.unwrap(), None);
        assert_eq!(store.read_committed().await.unwrap(), None);
        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id, None);
        assert_eq!(state.last_log_id, None);
    }
}
