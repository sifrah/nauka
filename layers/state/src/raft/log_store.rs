use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io;
use std::ops::RangeBounds;
use std::sync::Arc;

use futures::lock::Mutex;
use openraft::LogState;
use openraft::RaftTypeConfig;
use openraft::alias::{LogIdOf, VoteOf};
use openraft::entry::RaftEntry;
use openraft::storage::IOFlushed;

#[derive(Debug, Clone)]
pub struct LogStore<C: RaftTypeConfig> {
    inner: Arc<Mutex<LogStoreInner<C>>>,
}

impl<C: RaftTypeConfig> Default for LogStore<C> {
    fn default() -> Self {
        Self {
            inner: Arc::new(Mutex::new(LogStoreInner::default())),
        }
    }
}

#[derive(Debug)]
struct LogStoreInner<C: RaftTypeConfig> {
    last_purged_log_id: Option<LogIdOf<C>>,
    log: BTreeMap<u64, C::Entry>,
    committed: Option<LogIdOf<C>>,
    vote: Option<VoteOf<C>>,
}

impl<C: RaftTypeConfig> Default for LogStoreInner<C> {
    fn default() -> Self {
        Self {
            last_purged_log_id: None,
            log: BTreeMap::new(),
            committed: None,
            vote: None,
        }
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
        C::Entry: Clone,
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
            inner.committed = committed;
            Ok(())
        }

        async fn read_committed(&mut self) -> Result<Option<LogIdOf<C>>, io::Error> {
            let inner = self.inner.lock().await;
            Ok(inner.committed.clone())
        }

        async fn save_vote(&mut self, vote: &VoteOf<C>) -> Result<(), io::Error> {
            let mut inner = self.inner.lock().await;
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
            }
            Ok(())
        }

        async fn purge(&mut self, log_id: LogIdOf<C>) -> Result<(), io::Error> {
            let mut inner = self.inner.lock().await;
            inner.last_purged_log_id = Some(log_id.clone());
            let keys: Vec<u64> = inner
                .log
                .range(..=log_id.index())
                .map(|(k, _)| *k)
                .collect();
            for k in keys {
                inner.log.remove(&k);
            }
            Ok(())
        }

        async fn get_log_reader(&mut self) -> Self::LogReader {
            self.clone()
        }
    }
}
