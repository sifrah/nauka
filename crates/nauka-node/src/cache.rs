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
//! Opt-in via `NAUKA_CACHE_SIZE` (a disk budget). Stripes that decode
//! from local shards are NOT cached — they are already free.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

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
    pub fn put(&self, file_hash: &str, stripe_idx: usize, data: &[u8]) {
        let len = data.len() as u64;
        if len > self.budget {
            return;
        }
        let key = entry_key(file_hash, stripe_idx);
        if self.lock().entries.contains_key(&key) {
            return;
        }
        if std::fs::write(self.dir.join(&key), data).is_err() {
            return;
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
}
