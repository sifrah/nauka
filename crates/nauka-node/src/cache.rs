//! Per-node cache of reconstructed stripes.
//!
//! A stripe reconstructed from REMOTE shards cost one cross-cluster
//! round of transfers; keeping the decoded bytes on local disk makes
//! every subsequent read of it free. The cache key is
//! `(manifest hash, stripe index)` — and because content is addressed
//! by BLAKE3, an entry can never go stale: an overwritten S3 object is
//! a NEW manifest hash, the old entry just stops being asked for. The
//! only cache problem left is eviction, which plain LRU solves.
//!
//! Opt-in via `NAUKA_CACHE_SIZE` (a disk budget). Full downloads keep
//! only remotely reconstructed stripes; Range reads may also cache local
//! stripes because they otherwise re-read and re-hash the same shards.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::Mutex;

use futures::FutureExt as _;

/// Une limite en octets ne borne pas les allocations d'une table remplie de
/// shards minuscules: la clef, l'Arc et le bucket HashMap coûtent alors bien
/// plus que le contenu. Le plafond d'entrées garde ce coût structurel borné.
const MAX_EXTENT_ENTRIES: usize = 4096;
/// Approximation conservatrice du coût hors payload d'une entrée. Elle est
/// facturée au budget en plus de la longueur exacte de la clef.
const EXTENT_ENTRY_OVERHEAD: u64 = 256;
/// Les chargements sont détachés des requêtes afin de survivre à leur
/// annulation. Ils doivent donc avoir leur propre admission, faute de quoi
/// des Range froides sur des clefs distinctes contournent le budget du cache.
const MAX_EXTENT_FLIGHTS: usize = 32;

type SharedLoad = Result<Arc<[u8]>, Arc<str>>;

struct ExtentFlight {
    result: Mutex<Option<SharedLoad>>,
    notify: tokio::sync::Notify,
}

/// Cache RAM borné des unités dont l'intégrité a déjà été vérifiée, avec
/// fusion des lectures froides concurrentes. Les clés sont internes au
/// processus (`shard:<blake3>` ou `stripe:<manifest>:<index>`); elles ne
/// changent ni le manifest ni le protocole sur disque.
///
/// Un sous-bloc arbitraire ne peut pas être authentifié avec les manifests
/// historiques: ils portent un hash par shard, pas un arbre de Merkle. La
/// plus petite unité sûre est donc le shard complet. Une petite Range ne
/// transfère et ne rehache ce shard qu'une fois, puis le découpe en RAM.
pub struct VerifiedExtentCache {
    budget: u64,
    inner: Mutex<ExtentInner>,
    flights: Mutex<HashMap<String, Arc<ExtentFlight>>>,
    load_slots: Arc<tokio::sync::Semaphore>,
}

struct ExtentEntry {
    data: Arc<[u8]>,
    charge: u64,
    last_used: u64,
}

struct ExtentInner {
    entries: HashMap<String, ExtentEntry>,
    payload_total: u64,
    accounted_total: u64,
    clock: u64,
}

impl VerifiedExtentCache {
    pub fn new(budget: u64) -> Arc<Self> {
        let this = Arc::new(Self {
            budget,
            inner: Mutex::new(ExtentInner {
                entries: HashMap::new(),
                payload_total: 0,
                accounted_total: 0,
                clock: 0,
            }),
            flights: Mutex::new(HashMap::new()),
            load_slots: Arc::new(tokio::sync::Semaphore::new(MAX_EXTENT_FLIGHTS)),
        });
        this.publish_levels();
        this
    }

    fn lock_inner(&self) -> std::sync::MutexGuard<'_, ExtentInner> {
        self.inner.lock().unwrap_or_else(|p| p.into_inner())
    }

    fn get(&self, key: &str) -> Option<Arc<[u8]>> {
        let mut inner = self.lock_inner();
        inner.clock += 1;
        let clock = inner.clock;
        let data = inner.entries.get_mut(key).map(|entry| {
            entry.last_used = clock;
            entry.data.clone()
        });
        drop(inner);
        if data.is_some() {
            metrics::counter!("nauka_extent_cache_hits_total").increment(1);
        }
        data
    }

    fn insert(&self, key: String, data: Arc<[u8]>) {
        let len = data.len() as u64;
        let charge = len
            .saturating_add(key.len() as u64)
            .saturating_add(EXTENT_ENTRY_OVERHEAD);
        if self.budget == 0 || charge > self.budget {
            return;
        }
        let mut inner = self.lock_inner();
        inner.clock += 1;
        let clock = inner.clock;
        if let Some(old) = inner.entries.insert(
            key,
            ExtentEntry {
                data,
                charge,
                last_used: clock,
            },
        ) {
            inner.payload_total = inner.payload_total.saturating_sub(old.data.len() as u64);
            inner.accounted_total = inner.accounted_total.saturating_sub(old.charge);
        }
        inner.payload_total = inner.payload_total.saturating_add(len);
        inner.accounted_total = inner.accounted_total.saturating_add(charge);
        while inner.accounted_total > self.budget || inner.entries.len() > MAX_EXTENT_ENTRIES {
            let Some(victim) = inner
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.last_used)
                .map(|(key, _)| key.clone())
            else {
                break;
            };
            if let Some(removed) = inner.entries.remove(&victim) {
                inner.payload_total = inner
                    .payload_total
                    .saturating_sub(removed.data.len() as u64);
                inner.accounted_total = inner.accounted_total.saturating_sub(removed.charge);
                metrics::counter!("nauka_extent_cache_evictions_total").increment(1);
            }
        }
        drop(inner);
        self.publish_levels();
    }

    fn publish_levels(&self) {
        let inner = self.lock_inner();
        metrics::gauge!("nauka_extent_cache_entries").set(inner.entries.len() as f64);
        metrics::gauge!("nauka_extent_cache_bytes").set(inner.payload_total as f64);
        metrics::gauge!("nauka_extent_cache_accounted_bytes").set(inner.accounted_total as f64);
        metrics::gauge!("nauka_extent_cache_budget_bytes").set(self.budget as f64);
    }

    /// Retourne une unité vérifiée déjà chaude ou lance exactement un
    /// chargement partagé. Le chargement est détaché de la requête HTTP:
    /// l'annulation de tous les clients ne laisse ni entrée bloquée ni
    /// travail dupliqué; il se termine sous les timeouts du transport.
    pub async fn get_or_load<F>(self: &Arc<Self>, key: String, load: F) -> SharedLoad
    where
        F: std::future::Future<Output = SharedLoad> + Send + 'static,
    {
        if let Some(data) = self.get(&key) {
            return Ok(data);
        }

        // Rejoindre un vol déjà visible ne consomme pas de slot. Si aucun
        // n'existe, l'admission attend avant de créer une nouvelle tâche
        // détachée: `flights` et la RAM transitoire ont ainsi une borne dure.
        let existing = self
            .flights
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&key)
            .cloned();
        if let Some(cell) = existing {
            metrics::counter!("nauka_extent_singleflight_waiters_total").increment(1);
            return wait_for_flight(&cell).await;
        }
        let permit = self
            .load_slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Arc::<str>::from("extent cache arrêté"))?;

        let (cell, leader) = {
            let mut flights = self.flights.lock().unwrap_or_else(|p| p.into_inner());
            match flights.get(&key) {
                Some(cell) => (cell.clone(), false),
                None => {
                    // Un chargement peut avoir fini entre le premier miss et
                    // l'obtention du slot. Vérifier sous le verrou des vols
                    // ferme cette fenêtre sans pouvoir croiser un ordre de
                    // verrous inverse: `insert` libère `inner` avant flights.
                    if let Some(data) = self.get(&key) {
                        drop(flights);
                        drop(permit);
                        return Ok(data);
                    }
                    let cell = Arc::new(ExtentFlight {
                        result: Mutex::new(None),
                        notify: tokio::sync::Notify::new(),
                    });
                    flights.insert(key.clone(), cell.clone());
                    metrics::gauge!("nauka_extent_inflight").set(flights.len() as f64);
                    (cell, true)
                }
            }
        };
        if leader {
            metrics::counter!("nauka_extent_cache_misses_total").increment(1);
            let cache = self.clone();
            let task_cell = cell.clone();
            let task_key = key.clone();
            tokio::spawn(async move {
                // Un panic du chargeur ne doit jamais laisser les futurs
                // lecteurs suspendus derrière une entrée `flights` orpheline.
                let result = std::panic::AssertUnwindSafe(load)
                    .catch_unwind()
                    .await
                    .unwrap_or_else(|_| Err(Arc::<str>::from("panic du chargeur d'extent")));
                if let Ok(data) = &result {
                    cache.insert(task_key.clone(), data.clone());
                }
                *task_cell.result.lock().unwrap_or_else(|p| p.into_inner()) = Some(result);
                task_cell.notify.notify_waiters();
                let mut flights = cache.flights.lock().unwrap_or_else(|p| p.into_inner());
                if flights
                    .get(&task_key)
                    .is_some_and(|current| Arc::ptr_eq(current, &task_cell))
                {
                    flights.remove(&task_key);
                }
                metrics::gauge!("nauka_extent_inflight").set(flights.len() as f64);
                drop(flights);
                drop(permit);
            });
        } else {
            drop(permit);
            metrics::counter!("nauka_extent_singleflight_waiters_total").increment(1);
        }
        wait_for_flight(&cell).await
    }

    #[cfg(test)]
    fn stats(&self) -> (usize, u64) {
        let inner = self.lock_inner();
        (inner.entries.len(), inner.payload_total)
    }
}

async fn wait_for_flight(cell: &ExtentFlight) -> SharedLoad {
    loop {
        // Construire le waiter AVANT de lire le résultat empêche la perte
        // d'un réveil entre le test et `.await`.
        let notified = cell.notify.notified();
        if let Some(result) = cell
            .result
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
        {
            return result;
        }
        notified.await;
    }
}

/// One cached stripe on disk.
struct Entry {
    len: u64,
    /// Monotonic LRU clock value of the last touch.
    last_used: u64,
}

pub struct StripeCache {
    dir: PathBuf,
    budget: u64,
    inner: Mutex<Inner>,
    /// Lookups served from disk, and lookups that had to go to the
    /// cluster. Occupancy alone cannot tell a warm cache from a useless
    /// one — a cache pinned at its budget serving nothing but misses is
    /// exactly a `--cache-size` too small for the working set. Counted
    /// with atomics off the `Inner` lock, like `EgressMeter`, so the read
    /// path pays nothing for them. Process-lifetime totals: they start at
    /// zero on restart even though the entries on disk are adopted.
    hits: AtomicU64,
    misses: AtomicU64,
}

struct Inner {
    entries: HashMap<String, Entry>,
    total: u64,
    clock: u64,
}

/// File name of an entry: hash of the pair, so arbitrary manifest hashes
/// never form path components.
fn entry_key(file_hash: &str, stripe_idx: usize) -> String {
    blake3::hash(format!("{file_hash}/{stripe_idx}").as_bytes())
        .to_hex()
        .to_string()
}

impl StripeCache {
    /// Opens (or creates) the cache directory and adopts what a previous
    /// run left there, oldest-touched first.
    pub fn open(dir: PathBuf, budget: u64) -> std::io::Result<Self> {
        std::fs::create_dir_all(&dir)?;
        let mut entries = HashMap::new();
        let mut total = 0u64;
        let mut found: Vec<(String, u64, std::time::SystemTime)> = Vec::new();
        for e in std::fs::read_dir(&dir)? {
            let e = e?;
            let meta = e.metadata()?;
            if meta.is_file() {
                if let Some(name) = e.file_name().to_str() {
                    found.push((
                        name.to_string(),
                        meta.len(),
                        meta.modified().unwrap_or(std::time::UNIX_EPOCH),
                    ));
                }
            }
        }
        found.sort_by_key(|(_, _, t)| *t);
        let mut clock = 0;
        for (name, len, _) in found {
            clock += 1;
            total += len;
            entries.insert(
                name,
                Entry {
                    len,
                    last_used: clock,
                },
            );
        }
        let cache = Self {
            dir,
            budget,
            inner: Mutex::new(Inner {
                entries,
                total,
                clock,
            }),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        };
        cache.evict_to_budget();
        Ok(cache)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|p| p.into_inner())
    }

    pub fn get(&self, file_hash: &str, stripe_idx: usize) -> Option<Vec<u8>> {
        let key = entry_key(file_hash, stripe_idx);
        let known = {
            let mut inner = self.lock();
            match inner.entries.contains_key(&key) {
                true => {
                    inner.clock += 1;
                    let clock = inner.clock;
                    inner.entries.get_mut(&key).unwrap().last_used = clock;
                    true
                }
                false => false,
            }
        };
        // Read outside the lock; a racing eviction just means a miss.
        let data = if known {
            std::fs::read(self.dir.join(&key)).ok()
        } else {
            None
        };
        // Counted on the outcome, not on the bookkeeping: an entry the
        // index knows but whose file lost the race is a miss like any
        // other, because the caller pays the same cross-cluster fetch.
        match data.is_some() {
            true => self.hits.fetch_add(1, Ordering::Relaxed),
            false => self.misses.fetch_add(1, Ordering::Relaxed),
        };
        data
    }

    /// Stores a decoded stripe, then evicts least-recently-used entries
    /// until the cache fits its budget. A stripe larger than the whole
    /// budget is simply not cached.
    pub fn put(&self, file_hash: &str, stripe_idx: usize, data: &[u8]) -> bool {
        let len = data.len() as u64;
        if len > self.budget {
            return false;
        }
        let key = entry_key(file_hash, stripe_idx);
        if self.lock().entries.contains_key(&key) {
            return false;
        }
        if std::fs::write(self.dir.join(&key), data).is_err() {
            return false;
        }
        {
            let mut inner = self.lock();
            inner.clock += 1;
            let clock = inner.clock;
            if inner
                .entries
                .insert(
                    key,
                    Entry {
                        len,
                        last_used: clock,
                    },
                )
                .is_none()
            {
                inner.total += len;
            }
        }
        self.evict_to_budget();
        true
    }

    /// Oublie une entrée dont la vérification contre le manifest a échoué.
    /// Le cache est accélérateur, jamais source de vérité: la lecture
    /// retombe ensuite sur les shards BLAKE3/RS.
    pub fn invalidate(&self, file_hash: &str, stripe_idx: usize) {
        let key = entry_key(file_hash, stripe_idx);
        let len = {
            let mut inner = self.lock();
            inner.entries.remove(&key).map(|entry| {
                inner.total = inner.total.saturating_sub(entry.len);
                entry.len
            })
        };
        if len.is_some() {
            let _ = std::fs::remove_file(self.dir.join(key));
        }
    }

    fn evict_to_budget(&self) {
        loop {
            let victim = {
                let inner = self.lock();
                if inner.total <= self.budget {
                    return;
                }
                inner
                    .entries
                    .iter()
                    .min_by_key(|(_, e)| e.last_used)
                    .map(|(k, e)| (k.clone(), e.len))
            };
            let Some((key, len)) = victim else { return };
            let _ = std::fs::remove_file(self.dir.join(&key));
            let mut inner = self.lock();
            if inner.entries.remove(&key).is_some() {
                inner.total -= len;
            }
        }
    }

    /// Drops every entry not derivable from a live manifest — the cache's
    /// analogue of the shard GC. Called from the maintenance tick with
    /// the set of `entry_key`s the registry can still produce; anything
    /// else (deleted or banned content) is purged from disk.
    pub fn sweep(&self, live_keys: &std::collections::HashSet<String>) {
        let stale: Vec<(String, u64)> = {
            let inner = self.lock();
            inner
                .entries
                .iter()
                .filter(|(k, _)| !live_keys.contains(*k))
                .map(|(k, e)| (k.clone(), e.len))
                .collect()
        };
        for (key, len) in stale {
            let _ = std::fs::remove_file(self.dir.join(&key));
            let mut inner = self.lock();
            if inner.entries.remove(&key).is_some() {
                inner.total -= len;
            }
        }
    }

    /// The `entry_key`s a manifest can produce (for `sweep`).
    pub fn keys_of(manifest: &nauka_erasure::FileManifest) -> impl Iterator<Item = String> + '_ {
        (0..manifest.stripes.len()).map(|i| entry_key(&manifest.file_hash, i))
    }

    /// Occupancy: `(entries, bytes on disk)`. Compared against the budget
    /// it says whether `--cache-size` is ever reached at all.
    pub fn stats(&self) -> (usize, u64) {
        let inner = self.lock();
        (inner.entries.len(), inner.total)
    }

    /// The disk budget this cache was opened with — the denominator the
    /// occupancy above is only meaningful against.
    pub fn budget(&self) -> u64 {
        self.budget
    }

    /// `(hits, misses)` since this process started.
    pub fn hit_stats(&self) -> (u64, u64) {
        (
            self.hits.load(Ordering::Relaxed),
            self.misses.load(Ordering::Relaxed),
        )
    }
}

/// Served to cluster peers over the authenticated transport — the
/// cooperative regional cache reads THIS node's entries.
impl nauka_transport::server::CacheView for StripeCache {
    fn stripe(&self, file_hash: &str, stripe_idx: usize) -> Option<Vec<u8>> {
        self.get(file_hash, stripe_idx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp() -> PathBuf {
        let d = std::env::temp_dir().join(format!("nauka-cache-test-{}", rand_suffix()));
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    fn rand_suffix() -> String {
        use rand::Rng;
        let mut b = [0u8; 8];
        rand::thread_rng().fill(&mut b);
        b.iter().map(|x| format!("{x:02x}")).collect()
    }

    #[test]
    fn stores_and_reads_back_by_identity() {
        let c = StripeCache::open(tmp(), 1024 * 1024).unwrap();
        assert!(c.get("h1", 0).is_none());
        c.put("h1", 0, b"stripe zero");
        c.put("h1", 1, b"stripe one");
        assert_eq!(c.get("h1", 0).as_deref(), Some(&b"stripe zero"[..]));
        assert_eq!(c.get("h1", 1).as_deref(), Some(&b"stripe one"[..]));
        assert!(c.get("h2", 0).is_none(), "other content, other entry");
    }

    #[test]
    fn evicts_least_recently_used_beyond_the_budget() {
        // Budget for two 4-byte entries; the third insert evicts the
        // least recently TOUCHED, not the oldest inserted.
        let c = StripeCache::open(tmp(), 8).unwrap();
        c.put("a", 0, b"aaaa");
        c.put("b", 0, b"bbbb");
        assert!(c.get("a", 0).is_some(), "touch a: b becomes the LRU");
        c.put("c", 0, b"cccc");
        assert!(c.get("b", 0).is_none(), "b evicted");
        assert!(c.get("a", 0).is_some());
        assert!(c.get("c", 0).is_some());
        let (n, total) = c.stats();
        assert_eq!((n, total), (2, 8));
    }

    #[test]
    fn counts_hits_and_misses_on_the_read_path() {
        let c = StripeCache::open(tmp(), 1024).unwrap();
        assert_eq!(c.hit_stats(), (0, 0), "no lookup, no opinion");
        assert!(c.get("h", 0).is_none());
        assert_eq!(c.hit_stats(), (0, 1), "cold read: a miss");
        c.put("h", 0, b"decoded stripe");
        assert_eq!(c.hit_stats(), (0, 1), "storing is not a lookup");
        assert!(c.get("h", 0).is_some());
        assert!(c.get("h", 0).is_some());
        assert_eq!(c.hit_stats(), (2, 1));
        // Occupancy says the cache is full of something; only the hit
        // rate says that something is being read.
        assert_eq!(c.stats(), (1, 14));
        assert_eq!(c.budget(), 1024);
    }

    #[test]
    fn an_evicted_entry_reads_as_a_miss_again() {
        let c = StripeCache::open(tmp(), 8).unwrap();
        c.put("a", 0, b"aaaa");
        c.put("b", 0, b"bbbb");
        assert!(c.get("a", 0).is_some());
        c.put("c", 0, b"cccc"); // evicts b
        assert!(c.get("b", 0).is_none());
        assert_eq!(c.hit_stats(), (1, 1));
    }

    #[test]
    fn a_file_lost_under_the_index_counts_as_a_miss() {
        // The index knows the entry, the bytes are gone (a racing sweep,
        // an operator's rm). The caller pays a cluster fetch: a miss.
        let dir = tmp();
        let c = StripeCache::open(dir.clone(), 1024).unwrap();
        c.put("h", 0, b"stripe");
        std::fs::remove_file(dir.join(entry_key("h", 0))).unwrap();
        assert!(c.get("h", 0).is_none());
        assert_eq!(c.hit_stats(), (0, 1));
    }

    #[test]
    fn oversized_entries_are_refused_outright() {
        let c = StripeCache::open(tmp(), 4).unwrap();
        c.put("big", 0, b"way too large");
        assert!(c.get("big", 0).is_none());
        assert_eq!(c.stats(), (0, 0));
    }

    #[test]
    fn survives_a_restart_from_the_directory() {
        let dir = tmp();
        {
            let c = StripeCache::open(dir.clone(), 1024).unwrap();
            c.put("h", 3, b"persisted");
        }
        let c = StripeCache::open(dir, 1024).unwrap();
        assert_eq!(c.get("h", 3).as_deref(), Some(&b"persisted"[..]));
    }

    #[test]
    fn sweep_purges_what_the_registry_no_longer_knows() {
        let c = StripeCache::open(tmp(), 1024).unwrap();
        c.put("kept", 0, b"still live");
        c.put("gone", 0, b"deleted content");
        let mut live = std::collections::HashSet::new();
        live.insert(entry_key("kept", 0));
        c.sweep(&live);
        assert!(c.get("kept", 0).is_some());
        assert!(c.get("gone", 0).is_none());
        assert_eq!(c.stats().0, 1);
    }

    #[tokio::test]
    async fn concurrent_cold_extents_are_loaded_once() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let cache = VerifiedExtentCache::new(1024);
        let loads = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::new();
        for _ in 0..32 {
            let cache = cache.clone();
            let loads = loads.clone();
            tasks.push(tokio::spawn(async move {
                cache
                    .get_or_load("shard:same".into(), async move {
                        loads.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                        Ok(Arc::<[u8]>::from(&b"verified shard"[..]))
                    })
                    .await
                    .unwrap()
            }));
        }
        for task in tasks {
            assert_eq!(task.await.unwrap().as_ref(), b"verified shard");
        }
        assert_eq!(loads.load(Ordering::SeqCst), 1);
        assert_eq!(cache.stats(), (1, 14));
    }

    #[tokio::test]
    async fn cancelled_waiter_does_not_cancel_or_duplicate_the_load() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let cache = VerifiedExtentCache::new(1024);
        let loads = Arc::new(AtomicUsize::new(0));
        let first_cache = cache.clone();
        let first_loads = loads.clone();
        let first = tokio::spawn(async move {
            first_cache
                .get_or_load("shard:slow".into(), async move {
                    first_loads.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(std::time::Duration::from_millis(40)).await;
                    Ok(Arc::<[u8]>::from(&b"complete"[..]))
                })
                .await
        });
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        first.abort();

        let second_loads = loads.clone();
        let data = cache
            .get_or_load("shard:slow".into(), async move {
                second_loads.fetch_add(1, Ordering::SeqCst);
                Ok(Arc::<[u8]>::from(&b"duplicate"[..]))
            })
            .await
            .unwrap();
        assert_eq!(data.as_ref(), b"complete");
        assert_eq!(loads.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn extent_lru_never_exceeds_its_memory_budget() {
        // Deux payloads de 4 octets, leurs clefs et leur coût structurel.
        let cache = VerifiedExtentCache::new(550);
        for (key, value) in [("a", b"aaaa"), ("b", b"bbbb"), ("c", b"cccc")] {
            cache
                .get_or_load(format!("shard:{key}"), async move {
                    Ok(Arc::<[u8]>::from(&value[..]))
                })
                .await
                .unwrap();
        }
        assert_eq!(cache.stats(), (2, 8));
        // a était le moins récent et a été évincé; son prochain accès
        // doit réellement relancer le chargeur.
        let reloaded = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = reloaded.clone();
        cache
            .get_or_load("shard:a".into(), async move {
                flag.store(true, std::sync::atomic::Ordering::SeqCst);
                Ok(Arc::<[u8]>::from(&b"aaaa"[..]))
            })
            .await
            .unwrap();
        assert!(reloaded.load(std::sync::atomic::Ordering::SeqCst));
        assert_eq!(cache.stats(), (2, 8));
    }

    #[tokio::test]
    async fn tiny_extents_cannot_grow_the_entry_table_without_bound() {
        let cache = VerifiedExtentCache::new(64 * 1024 * 1024);
        for i in 0..(MAX_EXTENT_ENTRIES + 17) {
            cache
                .get_or_load(format!("shard:tiny-{i}"), async {
                    Ok(Arc::<[u8]>::from(&b"x"[..]))
                })
                .await
                .unwrap();
        }
        assert_eq!(
            cache.stats(),
            (MAX_EXTENT_ENTRIES, MAX_EXTENT_ENTRIES as u64)
        );
    }

    #[tokio::test]
    async fn distinct_detached_loads_have_a_hard_concurrency_limit() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let cache = VerifiedExtentCache::new(64 * 1024 * 1024);
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::new();
        for i in 0..(MAX_EXTENT_FLIGHTS * 2) {
            let cache = cache.clone();
            let active = active.clone();
            let peak = peak.clone();
            tasks.push(tokio::spawn(async move {
                cache
                    .get_or_load(format!("shard:flight-{i}"), async move {
                        let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                        peak.fetch_max(now, Ordering::SeqCst);
                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                        active.fetch_sub(1, Ordering::SeqCst);
                        Ok(Arc::<[u8]>::from(&b"verified"[..]))
                    })
                    .await
                    .unwrap();
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }
        assert!(peak.load(Ordering::SeqCst) <= MAX_EXTENT_FLIGHTS);
    }

    #[tokio::test]
    async fn a_panicking_loader_releases_the_flight_for_a_retry() {
        let cache = VerifiedExtentCache::new(1024);
        let error = cache
            .get_or_load("shard:panic".into(), async move {
                panic!("boom");
                #[allow(unreachable_code)]
                Ok(Arc::<[u8]>::from(&b"never"[..]))
            })
            .await
            .unwrap_err();
        assert!(error.contains("panic"));

        let data = cache
            .get_or_load("shard:panic".into(), async move {
                Ok(Arc::<[u8]>::from(&b"recovered"[..]))
            })
            .await
            .unwrap();
        assert_eq!(data.as_ref(), b"recovered");
    }

    #[tokio::test]
    async fn cache_is_checked_again_after_waiting_for_load_admission() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let cache = VerifiedExtentCache::new(1024);
        let permits = cache
            .load_slots
            .clone()
            .acquire_many_owned(MAX_EXTENT_FLIGHTS as u32)
            .await
            .unwrap();
        let loader_ran = Arc::new(AtomicBool::new(false));
        let task_cache = cache.clone();
        let task_flag = loader_ran.clone();
        let task = tokio::spawn(async move {
            task_cache
                .get_or_load("shard:late-hit".into(), async move {
                    task_flag.store(true, Ordering::SeqCst);
                    Ok(Arc::<[u8]>::from(&b"duplicate"[..]))
                })
                .await
                .unwrap()
        });
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        assert!(
            !task.is_finished(),
            "le miss doit attendre le slot avant le second check"
        );

        cache.insert(
            "shard:late-hit".into(),
            Arc::<[u8]>::from(&b"already warm"[..]),
        );
        drop(permits);

        assert_eq!(task.await.unwrap().as_ref(), b"already warm");
        assert!(!loader_ran.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn content_versions_never_share_an_extent() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let cache = VerifiedExtentCache::new(1024);
        let loads = Arc::new(AtomicUsize::new(0));
        for version in ["old-manifest", "new-manifest"] {
            let loads = loads.clone();
            cache
                .get_or_load(format!("stripe:{version}:0"), async move {
                    loads.fetch_add(1, Ordering::SeqCst);
                    Ok(Arc::<[u8]>::from(version.as_bytes()))
                })
                .await
                .unwrap();
        }
        assert_eq!(loads.load(Ordering::SeqCst), 2);
        assert_eq!(cache.stats().0, 2);
    }

    #[test]
    fn invalidation_removes_a_corrupt_decoded_stripe() {
        let c = StripeCache::open(tmp(), 1024).unwrap();
        assert!(c.put("manifest", 7, b"decoded bytes"));
        c.invalidate("manifest", 7);
        assert!(c.get("manifest", 7).is_none());
        assert_eq!(c.stats(), (0, 0));
    }
}
