//! Elastic ingestion buffer: the amortizing stage between a client pouring
//! bytes in and the encoder draining them stripe by stripe.
//!
//! Shape: a strictly ordered FIFO of segments, each either in RAM or in an
//! append-only spool file. RAM is the hot path; the spool absorbs whatever
//! the drain cannot keep up with. The consumer sees one byte stream and
//! never learns which backing a byte came from.
//!
//! Three sizing rules, all about not lying to the operator:
//!
//! - RAM comes from a **global pool sized once at startup** — a fixed
//!   fraction of the machine, not "whatever is left". Admission never
//!   blocks and never overcommits: an upload that arrives when the pool is
//!   dry gets no RAM window at all and spools from its first byte. Slower,
//!   correct, no OOM — three concurrent 1 GiB uploads on a 4 GiB node must
//!   degrade, not die.
//!
//! - The spool is **bounded by its creator**. When it fills, `push` waits
//!   for the consumer: at that point (disk full behind, peers unreachable
//!   for long) slowing the producer stops being a bug and becomes the only
//!   honest answer.
//!
//! - A RAM segment freed by consumption returns its bytes to the upload's
//!   window immediately, so a drain that keeps up keeps the whole stream
//!   in RAM and the spool file is never even created.
// The streaming encoder (next ticket in this track) is the consumer; until
// it lands, only the pool construction below is referenced from `main`.
// The allow dies with that ticket.
#![allow(dead_code)]

use std::collections::VecDeque;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use bytes::Bytes;
use tokio::sync::Notify;

/// Global admission budget for ingestion RAM windows.
///
/// One per process. `acquire` grants what it can — possibly zero — and the
/// lease returns its bytes on drop. Zero is a valid grant: the buffer then
/// runs spool-only.
pub struct RamPool {
    capacity: u64,
    available: AtomicU64,
}

/// What one upload may hold in RAM at any moment.
pub struct RamLease {
    pool: Arc<RamPool>,
    granted: u64,
}

impl RamPool {
    /// A pool of `fraction_denom`-th of the machine's total RAM, clamped to
    /// [64 MiB, 1 GiB]. Sized once: two uploads must not race for "what is
    /// left of the machine".
    pub fn sized_from_system(fraction_denom: u64) -> Arc<Self> {
        let total = total_system_ram().unwrap_or(4 * 1024 * 1024 * 1024);
        let capacity = (total / fraction_denom.max(1)).clamp(64 << 20, 1 << 30);
        Arc::new(Self {
            capacity,
            available: AtomicU64::new(capacity),
        })
    }

    /// A pool with an explicit byte capacity (tests, tuning).
    pub fn with_capacity(capacity: u64) -> Arc<Self> {
        Arc::new(Self {
            capacity,
            available: AtomicU64::new(capacity),
        })
    }

    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    /// Grant up to `want` bytes, never blocking: whatever is available now,
    /// zero included. The lease gives it back on drop.
    pub fn acquire(self: &Arc<Self>, want: u64) -> RamLease {
        let mut granted = 0;
        let mut cur = self.available.load(Ordering::Relaxed);
        while granted == 0 {
            let take = want.min(cur);
            if take == 0 {
                break;
            }
            match self.available.compare_exchange(
                cur,
                cur - take,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => granted = take,
                Err(now) => cur = now,
            }
        }
        RamLease {
            pool: self.clone(),
            granted,
        }
    }
}

impl Drop for RamLease {
    fn drop(&mut self) {
        self.pool
            .available
            .fetch_add(self.granted, Ordering::AcqRel);
    }
}

impl RamLease {
    pub fn granted(&self) -> u64 {
        self.granted
    }
}

/// One ordered piece of the stream.
enum Seg {
    Ram(Bytes),
    Spool { offset: u64, len: u64 },
}

struct Inner {
    segs: VecDeque<Seg>,
    /// Bytes currently held in RAM segments, bounded by the lease.
    ram_bytes: u64,
    /// Bytes currently parked in the spool and not yet consumed.
    spool_pending: u64,
    /// Next append offset in the spool file.
    spool_write_off: u64,
    /// Lifetime count of bytes that ever hit the spool (telemetry).
    spilled_total: u64,
    /// Producer is done; consumers drain to EOF.
    finished: bool,
    /// Producer side dropped without finishing (client vanished).
    aborted: bool,
}

/// The shared core behind the two handles.
struct Shared {
    inner: Mutex<Inner>,
    /// Wakes the consumer when data or EOF arrives.
    readable: Notify,
    /// Wakes the producer when spool space frees up.
    writable: Notify,
    ram_window: u64,
    spool_bound: u64,
    spool_path: PathBuf,
    /// Lazily created on first spill; a drain that keeps up never touches
    /// the filesystem at all.
    spool: Mutex<Option<std::fs::File>>,
    _lease: RamLease,
}

impl Shared {
    fn with_spool<T>(&self, f: impl FnOnce(&mut std::fs::File) -> std::io::Result<T>) -> Result<T> {
        let mut guard = self.spool.lock().unwrap_or_else(|e| e.into_inner());
        if guard.is_none() {
            let file = std::fs::OpenOptions::new()
                .create(true)
                .truncate(true)
                .read(true)
                .write(true)
                .open(&self.spool_path)
                .with_context(|| format!("creating spool {}", self.spool_path.display()))?;
            *guard = Some(file);
        }
        f(guard.as_mut().expect("spool just created")).context("spool I/O")
    }
}

/// Producer handle. Dropping it without [`IngestWriter::finish`] marks the
/// stream aborted and the reader gets an error rather than a truncated
/// "EOF" it would mistake for a complete object.
pub struct IngestWriter {
    shared: Arc<Shared>,
}

/// Consumer handle: a strictly ordered byte stream.
pub struct IngestReader {
    shared: Arc<Shared>,
    /// Next spool offset the consumer expects; spool segments are consumed
    /// in file order, so one cursor is enough.
    spool_read_off: u64,
}

/// Everything both sides need to know about a finished buffer.
pub struct IngestStats {
    /// Bytes that went through the spool rather than staying in RAM.
    pub spilled_bytes: u64,
    /// Whether a spool file exists on disk (for cleanup accounting).
    pub spooled: bool,
}

/// Build the pair. `spool_path` is where overflow lands if the drain falls
/// behind; the file is only created on first spill and removed when the
/// reader is dropped.
pub fn channel(
    pool: &Arc<RamPool>,
    ram_want: u64,
    spool_path: PathBuf,
    spool_bound: u64,
) -> (IngestWriter, IngestReader) {
    let lease = pool.acquire(ram_want);
    let shared = Arc::new(Shared {
        inner: Mutex::new(Inner {
            segs: VecDeque::new(),
            ram_bytes: 0,
            spool_pending: 0,
            spool_write_off: 0,
            spilled_total: 0,
            finished: false,
            aborted: false,
        }),
        readable: Notify::new(),
        writable: Notify::new(),
        ram_window: lease.granted(),
        spool_bound,
        spool_path,
        spool: Mutex::new(None),
        _lease: lease,
    });
    (
        IngestWriter {
            shared: shared.clone(),
        },
        IngestReader {
            shared,
            spool_read_off: 0,
        },
    )
}

impl IngestWriter {
    /// Append a chunk. RAM while the window allows, spool past it; waits
    /// only when the spool itself is at its bound — the ultimate
    /// backpressure, deliberately surfaced to the producer.
    pub async fn push(&self, chunk: Bytes) -> Result<()> {
        if chunk.is_empty() {
            return Ok(());
        }
        loop {
            let spill: Option<(u64, Bytes)> = {
                let mut inner = self.shared.inner.lock().unwrap_or_else(|e| e.into_inner());
                debug_assert!(!inner.finished, "push after finish");
                if inner.ram_bytes + (chunk.len() as u64) <= self.shared.ram_window {
                    inner.ram_bytes += chunk.len() as u64;
                    inner.segs.push_back(Seg::Ram(chunk));
                    drop(inner);
                    self.shared.readable.notify_one();
                    return Ok(());
                }
                if inner.spool_pending + (chunk.len() as u64) <= self.shared.spool_bound {
                    // Reserve the range under the lock, carry it out, write
                    // outside it. The offset must leave THIS critical
                    // section: re-deriving it from the cursor after
                    // re-locking would race a concurrent push.
                    let offset = inner.spool_write_off;
                    inner.spool_write_off += chunk.len() as u64;
                    inner.spool_pending += chunk.len() as u64;
                    inner.spilled_total += chunk.len() as u64;
                    inner.segs.push_back(Seg::Spool {
                        offset,
                        len: chunk.len() as u64,
                    });
                    Some((offset, chunk.clone()))
                } else {
                    None
                }
            };
            match spill {
                Some((off, bytes)) => {
                    // Sequential append — the one thing disks do best.
                    let this = self.shared.clone();
                    tokio::task::spawn_blocking(move || {
                        this.with_spool(|f| {
                            f.seek(SeekFrom::Start(off))?;
                            f.write_all(&bytes)
                        })
                    })
                    .await
                    .context("spool write task")??;
                    self.shared.readable.notify_one();
                    return Ok(());
                }
                None => {
                    // Spool at its bound: wait for the drain.
                    self.shared.writable.notified().await;
                }
            }
        }
    }

    /// Flush the spool to durable storage. The local-ack mode's whole
    /// defensibility rests on this being a real fsync.
    pub async fn sync(&self) -> Result<()> {
        let this = self.shared.clone();
        let has_spool = {
            let guard = this.spool.lock().unwrap_or_else(|e| e.into_inner());
            guard.is_some()
        };
        if !has_spool {
            return Ok(());
        }
        let this = self.shared.clone();
        tokio::task::spawn_blocking(move || this.with_spool(|f| f.sync_data()))
            .await
            .context("spool sync task")??;
        Ok(())
    }

    /// Declare the stream complete.
    pub fn finish(self) {
        {
            let mut inner = self.shared.inner.lock().unwrap_or_else(|e| e.into_inner());
            inner.finished = true;
        }
        self.shared.readable.notify_one();
        // Bypass the Drop abort marker.
        std::mem::forget(self);
    }
}

impl Drop for IngestWriter {
    fn drop(&mut self) {
        let mut inner = self.shared.inner.lock().unwrap_or_else(|e| e.into_inner());
        if !inner.finished {
            inner.aborted = true;
            drop(inner);
            self.shared.readable.notify_one();
        }
    }
}

impl IngestReader {
    /// The next up-to-`want` bytes, in order. An empty result is true EOF:
    /// the producer called `finish` and everything was drained. A producer
    /// that vanished mid-stream yields an error instead — a truncated
    /// stream must never be mistaken for a complete one.
    pub async fn next(&mut self, want: usize) -> Result<Bytes> {
        loop {
            let action = {
                let mut inner = self.shared.inner.lock().unwrap_or_else(|e| e.into_inner());
                match inner.segs.pop_front() {
                    Some(Seg::Ram(bytes)) => {
                        if bytes.len() > want {
                            let head = bytes.slice(..want);
                            let rest = bytes.slice(want..);
                            inner.segs.push_front(Seg::Ram(rest));
                            inner.ram_bytes -= head.len() as u64;
                            Ok(Some(head))
                        } else {
                            inner.ram_bytes -= bytes.len() as u64;
                            Ok(Some(bytes))
                        }
                    }
                    Some(Seg::Spool { offset, len }) => {
                        let take = (want as u64).min(len);
                        if take < len {
                            inner.segs.push_front(Seg::Spool {
                                offset: offset + take,
                                len: len - take,
                            });
                        }
                        inner.spool_pending -= take;
                        Err((offset, take))
                    }
                    None if inner.aborted => {
                        return Err(anyhow::anyhow!(
                            "producer vanished mid-stream; the buffer is truncated"
                        ))
                    }
                    None if inner.finished => Ok(None),
                    None => {
                        drop(inner);
                        // Nothing buffered yet: wait for the producer.
                        Err((u64::MAX, 0))
                    }
                }
            };
            match action {
                Ok(Some(bytes)) => {
                    // RAM freed: the producer's window just grew back.
                    self.shared.writable.notify_one();
                    return Ok(bytes);
                }
                Ok(None) => return Ok(Bytes::new()),
                Err((u64::MAX, 0)) => {
                    self.shared.readable.notified().await;
                }
                Err((offset, take)) => {
                    debug_assert_eq!(offset, self.spool_read_off);
                    self.spool_read_off = offset + take;
                    let this = self.shared.clone();
                    let bytes = tokio::task::spawn_blocking(move || {
                        this.with_spool(|f| {
                            let mut buf = vec![0u8; take as usize];
                            f.seek(SeekFrom::Start(offset))?;
                            f.read_exact(&mut buf)?;
                            Ok(buf)
                        })
                    })
                    .await
                    .context("spool read task")??;
                    self.shared.writable.notify_one();
                    return Ok(Bytes::from(bytes));
                }
            }
        }
    }

    /// Fill `want` bytes exactly (short only at EOF): the stripe assembler.
    pub async fn next_exact(&mut self, want: usize) -> Result<Bytes> {
        let first = self.next(want).await?;
        if first.len() == want || first.is_empty() {
            return Ok(first);
        }
        let mut buf = Vec::with_capacity(want);
        buf.extend_from_slice(&first);
        while buf.len() < want {
            let more = self.next(want - buf.len()).await?;
            if more.is_empty() {
                break;
            }
            buf.extend_from_slice(&more);
        }
        Ok(Bytes::from(buf))
    }

    pub fn stats(&self) -> IngestStats {
        let inner = self.shared.inner.lock().unwrap_or_else(|e| e.into_inner());
        IngestStats {
            spilled_bytes: inner.spilled_total,
            spooled: inner.spilled_total > 0,
        }
    }
}

impl Drop for IngestReader {
    fn drop(&mut self) {
        // The spool is transient scratch: whoever wants durable spools
        // (the local-ack mode) takes the file over explicitly before
        // dropping the reader.
        let created = {
            let guard = self.shared.spool.lock().unwrap_or_else(|e| e.into_inner());
            guard.is_some()
        };
        if created {
            let _ = std::fs::remove_file(&self.shared.spool_path);
        }
    }
}

/// Free bytes on the filesystem holding `path` — what a spool may still
/// honestly claim. Half of it is a sane spool bound: the node must keep
/// room for the shards the drain is about to write.
pub fn fs_available(path: &std::path::Path) -> u64 {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        if let Ok(c_path) = std::ffi::CString::new(path.as_os_str().as_bytes()) {
            let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
            if unsafe { libc::statvfs(c_path.as_ptr(), &mut stat) } == 0 {
                return (stat.f_bavail as u64).saturating_mul(stat.f_frsize as u64);
            }
        }
    }
    u64::MAX / 2
}

fn total_system_ram() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        let info = std::fs::read_to_string("/proc/meminfo").ok()?;
        let kb: u64 = info
            .lines()
            .find(|l| l.starts_with("MemTotal:"))?
            .split_whitespace()
            .nth(1)?
            .parse()
            .ok()?;
        Some(kb * 1024)
    }
    #[cfg(not(target_os = "linux"))]
    {
        // Non-Linux hosts are dev machines; the clamp floor (64 MiB) is a
        // fine pool there.
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("nauka-ingest-test-{}-{}", std::process::id(), name))
    }

    /// Deterministic chunk stream + its BLAKE3, for integrity checks.
    fn make_chunks(total: usize, seed: u64) -> (Vec<Bytes>, blake3::Hash) {
        let mut chunks = Vec::new();
        let mut hasher = blake3::Hasher::new();
        let mut produced = 0usize;
        let mut state = seed.wrapping_mul(0x9E3779B97F4A7C15) | 1;
        while produced < total {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let len = ((state % 300_000) as usize + 1).min(total - produced);
            let byte = (state >> 32) as u8;
            let chunk = vec![byte ^ (produced as u8); len];
            hasher.update(&chunk);
            chunks.push(Bytes::from(chunk));
            produced += len;
        }
        (chunks, hasher.finalize())
    }

    async fn roundtrip(ram_window: u64, total: usize, read_size: usize, name: &str) {
        let pool = RamPool::with_capacity(ram_window);
        let (tx, mut rx) = channel(&pool, ram_window, tmp(name), u64::MAX);
        let (chunks, want_hash) = make_chunks(total, ram_window ^ total as u64);
        let producer = tokio::spawn(async move {
            for c in chunks {
                tx.push(c).await.unwrap();
            }
            tx.finish();
        });
        let mut hasher = blake3::Hasher::new();
        let mut got = 0usize;
        loop {
            let b = rx.next_exact(read_size).await.unwrap();
            if b.is_empty() {
                break;
            }
            got += b.len();
            hasher.update(&b);
        }
        producer.await.unwrap();
        assert_eq!(got, total, "byte count through {name}");
        assert_eq!(hasher.finalize(), want_hash, "integrity through {name}");
    }

    #[tokio::test]
    async fn ram_only_roundtrip() {
        // Window bigger than the stream: the spool must never be created.
        let pool = RamPool::with_capacity(64 << 20);
        let path = tmp("ram-only");
        let (tx, mut rx) = channel(&pool, 64 << 20, path.clone(), u64::MAX);
        let (chunks, want) = make_chunks(4 << 20, 7);
        for c in chunks {
            tx.push(c).await.unwrap();
        }
        tx.finish();
        let mut hasher = blake3::Hasher::new();
        loop {
            let b = rx.next(1 << 20).await.unwrap();
            if b.is_empty() {
                break;
            }
            hasher.update(&b);
        }
        assert_eq!(hasher.finalize(), want);
        assert!(!rx.stats().spooled, "hot path must not touch the disk");
        assert!(!path.exists(), "no spool file for a keeping-up drain");
    }

    #[tokio::test]
    async fn spool_only_roundtrip() {
        // Zero RAM window: every byte goes through the file.
        roundtrip(0, 6 << 20, 1 << 20, "spool-only").await;
    }

    #[tokio::test]
    async fn mixed_roundtrip_small_window() {
        // Window far smaller than the stream: constant spill and refill,
        // segments alternate backings, order must survive.
        roundtrip(256 << 10, 8 << 20, 700_001, "mixed").await;
    }

    #[tokio::test]
    async fn zero_grant_pool_still_works() {
        let pool = RamPool::with_capacity(1 << 20);
        let _hog = pool.acquire(1 << 20);
        // Pool is dry: this channel gets no RAM at all and must still move
        // bytes correctly through the spool.
        let (tx, mut rx) = channel(&pool, 32 << 20, tmp("dry"), u64::MAX);
        assert_eq!(tx.shared.ram_window, 0);
        tx.push(Bytes::from(vec![42u8; 100_000])).await.unwrap();
        tx.finish();
        let got = rx.next_exact(100_000).await.unwrap();
        assert_eq!(got.len(), 100_000);
        assert!(got.iter().all(|b| *b == 42));
        assert!(rx.stats().spooled);
    }

    #[tokio::test]
    async fn pool_never_overcommits_and_leases_return() {
        let pool = RamPool::with_capacity(100);
        let a = pool.acquire(60);
        let b = pool.acquire(60);
        assert_eq!(a.granted(), 60);
        assert_eq!(b.granted(), 40, "partial grant, never overcommit");
        let c = pool.acquire(10);
        assert_eq!(c.granted(), 0, "dry pool grants zero");
        drop(a);
        let d = pool.acquire(60);
        assert_eq!(d.granted(), 60, "dropped lease returns its bytes");
    }

    #[tokio::test]
    async fn bounded_spool_applies_backpressure() {
        let pool = RamPool::with_capacity(0);
        // No RAM, spool bound = 1 chunk: the second push must wait until
        // the consumer drains the first.
        let (tx, mut rx) = channel(&pool, 0, tmp("backpressure"), 100_000);
        tx.push(Bytes::from(vec![1u8; 100_000])).await.unwrap();
        let push2 = {
            let waited = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let flag = waited.clone();
            let fut = async move {
                tx.push(Bytes::from(vec![2u8; 100_000])).await.unwrap();
                flag.store(true, Ordering::SeqCst);
                tx.finish();
            };
            let handle = tokio::spawn(fut);
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            assert!(
                !waited.load(Ordering::SeqCst),
                "push must block while the spool is at its bound"
            );
            handle
        };
        let first = rx.next_exact(100_000).await.unwrap();
        assert!(first.iter().all(|b| *b == 1));
        push2.await.unwrap();
        let second = rx.next_exact(100_000).await.unwrap();
        assert!(second.iter().all(|b| *b == 2));
    }

    #[tokio::test]
    async fn vanished_producer_is_an_error_not_eof() {
        let pool = RamPool::with_capacity(1 << 20);
        let (tx, mut rx) = channel(&pool, 1 << 20, tmp("abort"), u64::MAX);
        tx.push(Bytes::from_static(b"partial")).await.unwrap();
        drop(tx); // no finish(): the client hung up
        let got = rx.next(7).await.unwrap();
        assert_eq!(&got[..], b"partial");
        let err = rx.next(1).await;
        assert!(
            err.is_err(),
            "truncation must never be mistaken for a complete stream"
        );
    }

    #[tokio::test]
    async fn spool_file_is_removed_on_drop() {
        let pool = RamPool::with_capacity(0);
        let path = tmp("cleanup");
        let (tx, rx) = channel(&pool, 0, path.clone(), u64::MAX);
        tx.push(Bytes::from(vec![9u8; 10_000])).await.unwrap();
        tx.finish();
        assert!(path.exists(), "spill created the file");
        drop(rx);
        assert!(!path.exists(), "reader drop removed the scratch spool");
    }

    #[tokio::test]
    async fn concurrent_producer_consumer_large_stream() {
        // The realistic shape: both sides live, window much smaller than
        // the stream, consumer slower at first. 32 MiB through a 2 MiB
        // window with stripe-sized reads.
        roundtrip(2 << 20, 32 << 20, 4 << 20, "concurrent-large").await;
    }
}
