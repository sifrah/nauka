//! Telemetry: the node's Prometheus exposition.
//!
//! The rest of the codebase records through the `metrics` facade macros
//! (`counter!`, `gauge!`, `histogram!`). Those macros are no-ops until a
//! recorder is installed, and this module is the only place that installs
//! one. `--no-metrics` therefore does more than close a port: it leaves
//! every instrumentation site in the binary inert, at no measurable cost.
//!
//! The module is deliberately named `telemetry` and not `metrics`: a local
//! module called `metrics` would shadow the crate of the same name and make
//! every `metrics::` path here ambiguous.

use std::net::SocketAddr;
use std::time::Duration;

use anyhow::{Context, Result};
use axum::response::IntoResponse;
use axum::{extract::State, routing::get, Router};
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};

/// Histogram buckets for every `*_seconds` metric.
///
/// Explicit buckets rather than the exporter's default summaries. A summary
/// carries quantiles computed on one node, and quantiles do not average: a
/// cluster-wide p99 cannot be recovered from per-node p99s. Buckets add, so
/// they can be summed across nodes and re-quantiled at query time.
///
/// The range spans a served-from-page-cache read (1 ms) to a request that has
/// stalled long enough that the client has probably given up (30 s).
const LATENCY_BUCKETS: &[f64] = &[
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
];

/// Histogram buckets for every `*_bytes` metric, in bytes.
///
/// Roughly powers of eight from 1 KiB to 16 GiB — wide, because object sizes
/// in an S3 workload span that whole range and a narrow ladder would pile
/// everything into `+Inf`.
const SIZE_BUCKETS: &[f64] = &[
    1024.0,        // 1 KiB
    8192.0,        // 8 KiB
    65536.0,       // 64 KiB
    524288.0,      // 512 KiB
    4194304.0,     // 4 MiB
    33554432.0,    // 32 MiB
    268435456.0,   // 256 MiB
    2147483648.0,  // 2 GiB
    17179869184.0, // 16 GiB
];

/// How often histogram data is rotated out of the recorder.
///
/// Must stay below the scrape interval, or a scrape can miss a bucket
/// entirely. Prometheus defaults to 15 s; 5 s leaves room.
const UPKEEP_INTERVAL: Duration = Duration::from_secs(5);

/// Install the global Prometheus recorder.
///
/// Callable once per process — a second call fails rather than silently
/// leaving the first recorder in place.
pub fn install() -> Result<PrometheusHandle> {
    PrometheusBuilder::new()
        .set_buckets_for_metric(Matcher::Suffix("_seconds".to_string()), LATENCY_BUCKETS)
        .context("configuring latency histogram buckets")?
        .set_buckets_for_metric(Matcher::Suffix("_bytes".to_string()), SIZE_BUCKETS)
        .context("configuring size histogram buckets")?
        .install_recorder()
        .context("installing the global Prometheus recorder")
}

/// Record the metrics that describe the process itself.
///
/// `node_addr` is the advertised address, which is what the rest of the
/// cluster knows this node by — the same string used as `self_id` elsewhere,
/// so a metric can be joined against the registry.
pub fn seed(node_addr: &str) {
    metrics::describe_gauge!(
        "nauka_build_info",
        "Always 1. Carries the build version and the advertised address as labels."
    );
    metrics::gauge!(
        "nauka_build_info",
        "version" => env!("CARGO_PKG_VERSION"),
        "node" => node_addr.to_string(),
    )
    .set(1.0);

    // Start time rather than uptime: a constant needs no updater task, and
    // `time() - nauka_start_time_seconds` gives uptime at query time. It is
    // also the convention `process_start_time_seconds` already established,
    // so it reads the way an operator expects.
    metrics::describe_gauge!(
        "nauka_start_time_seconds",
        "Unix timestamp of process start. Uptime is time() - nauka_start_time_seconds."
    );
    let start_unix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    metrics::gauge!("nauka_start_time_seconds").set(start_unix as f64);

    s3::describe();
    crate::api::describe_metrics();
    nauka_cluster::telemetry::describe();
    nauka_cluster::telemetry::describe_maintenance();
    nauka_transport::telemetry::describe();
    nauka_store::ShardStore::describe_metrics();
    describe_node();
}

/// HELP/TYPE for the node-level metrics recorded from `main` and `api`:
/// degraded writes, shard-send retries, the egress ledger and the stripe
/// cache.
fn describe_node() {
    metrics::describe_counter!(
        "nauka_writes_degraded_total",
        "Uploads that completed under-replicated (every stripe reconstructible, but at least one shard undelivered). The scrubber completes them; the counter says how often writes land in that state."
    );
    metrics::describe_counter!(
        "nauka_write_shards_undelivered_total",
        "Shards that could not be delivered during uploads — the repair debt handed to the scrubber."
    );
    metrics::describe_gauge!(
        "nauka_staged_bytes",
        "Bytes of locally-acked uploads not yet dispersed — the live size of the local-ack window. Past a cap, new uploads fall back to full dispersal."
    );
    metrics::describe_counter!(
        "nauka_staged_reads_total",
        "Reads served straight from a staged copy while its upload was still dispersing: local disk, no erasure decode, no cluster round-trip."
    );
    metrics::describe_counter!(
        "nauka_local_ack_uploads_total",
        "Uploads acked after a local fsync, before cluster dispersal. Each one opens a window where the object lives on this node alone."
    );
    metrics::describe_counter!(
        "nauka_native_local_ack_uploads_total",
        "Content-bound native uploads accepted through the durable local-ack path."
    );
    metrics::describe_counter!(
        "nauka_local_ack_drain_failures_total",
        "Locally-acked uploads whose background dispersal failed. The staged copy stays on disk and the next restart retries it; a climbing counter means objects are sitting at single-node redundancy."
    );
    metrics::describe_counter!(
        "nauka_shard_send_retries_total",
        "Retries while sending shards to peers. Climbing retries flag a sick peer well before the liveness map declares it dead."
    );
    metrics::describe_gauge!(
        "nauka_egress_served_bytes",
        "Bytes served this month, per the egress meter. Resets when the month rolls over."
    );
    metrics::describe_gauge!(
        "nauka_egress_quota_bytes",
        "Monthly egress budget. Absent when the node is unmetered — absence means no quota, a value of 0 would mean an exhausted one."
    );
    metrics::describe_gauge!(
        "nauka_cache_entries",
        "Stripes currently held by the stripe cache."
    );
    metrics::describe_gauge!(
        "nauka_cache_size_bytes",
        "Bytes currently held by the stripe cache."
    );
    metrics::describe_gauge!(
        "nauka_cache_budget_bytes",
        "Configured stripe-cache budget (--cache-size)."
    );
    metrics::describe_counter!(
        "nauka_cache_hits_total",
        "Stripe-cache lookups served from local disk instead of the cluster."
    );
    metrics::describe_counter!(
        "nauka_cache_misses_total",
        "Stripe-cache lookups that required shard reads, local or remote. A miss whose backing file lost a race with eviction counts here too."
    );
    metrics::describe_gauge!(
        "nauka_extent_cache_entries",
        "Verified shards and stripes currently resident in the bounded Range RAM cache."
    );
    metrics::describe_gauge!(
        "nauka_extent_cache_bytes",
        "Payload bytes resident in the verified Range RAM cache."
    );
    metrics::describe_gauge!(
        "nauka_extent_cache_accounted_bytes",
        "Payload, key and conservative per-entry overhead charged to the verified Range RAM-cache budget."
    );
    metrics::describe_gauge!(
        "nauka_extent_cache_budget_bytes",
        "Configured Range RAM cache budget (NAUKA_EXTENT_CACHE_SIZE)."
    );
    metrics::describe_gauge!(
        "nauka_extent_inflight",
        "Unique verified extents currently loading; duplicate clients wait on the same load."
    );
    metrics::describe_counter!(
        "nauka_extent_cache_hits_total",
        "Verified extent requests served from RAM without disk, hash, Reed-Solomon, or network work."
    );
    metrics::describe_counter!(
        "nauka_extent_cache_misses_total",
        "Unique verified extent loads started after a RAM miss."
    );
    metrics::describe_counter!(
        "nauka_extent_cache_evictions_total",
        "Verified extents evicted by the bounded LRU."
    );
    metrics::describe_counter!(
        "nauka_extent_singleflight_waiters_total",
        "Duplicate extent requests fused behind an already in-flight load."
    );
    metrics::describe_counter!(
        "nauka_cache_corrupt_entries_total",
        "Decoded disk-cache stripes rejected after re-encoding did not match the manifest shard hashes."
    );
    metrics::describe_counter!(
        "nauka_remote_corrupt_shards_total",
        "Remote shard responses rejected because their BLAKE3 did not match the requested content hash."
    );
}

/// Serve `/metrics` until the listener dies.
///
/// A listener of its own rather than a route on the public HTTP API: the
/// exposition describes cluster topology, node capacities and peer addresses,
/// none of which should be reachable by everyone who can reach the web UI.
pub async fn serve(listen: SocketAddr, handle: PrometheusHandle) -> Result<()> {
    {
        let handle = handle.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(UPKEEP_INTERVAL);
            loop {
                tick.tick().await;
                handle.run_upkeep();
            }
        });
    }

    let router = Router::new()
        .route("/metrics", get(render))
        .with_state(handle);
    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .with_context(|| format!("binding the metrics endpoint on {listen}"))?;
    tracing::info!("metrics on http://{listen}/metrics");
    axum::serve(listener, router).await?;
    Ok(())
}

async fn render(State(handle): State<PrometheusHandle>) -> impl IntoResponse {
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        handle.render(),
    )
}

/// Per-request S3 telemetry.
///
/// The measurement is split across two layers that cannot see each other.
/// The canonical operation name exists only deep inside `s3s`, at the access
/// check; latency and the status the client actually received exist only at
/// the outer hyper service, which knows nothing of S3 routing. A task-local
/// carries the former out to the latter.
///
/// This is sound because `s3s` never spawns: the access check runs in the
/// same task as the outer service call, so the slot written during the check
/// is still the slot belonging to that request when the response is recorded.
// Recording call sites live in the S3 door; without the feature only the
// describe() below runs (keeping the metric catalog stable for dashboards)
// and the recorders are dead weight the compiler may drop.
#[cfg_attr(not(feature = "s3"), allow(dead_code))]
pub mod s3 {
    use std::cell::RefCell;
    use std::future::Future;
    use std::time::Duration;

    tokio::task_local! {
        static CURRENT: RefCell<Option<Op>>;
    }

    /// What the access check learned about the request in flight.
    #[derive(Clone)]
    pub struct Op {
        name: String,
        class: &'static str,
    }

    /// Label for a request that never reached the access check: a bad
    /// signature, an unparseable path, a CDN offload redirect. One extra
    /// series, not an unbounded one — and deliberately not dropped, since
    /// requests rejected before routing are exactly the ones an operator
    /// misses otherwise.
    const UNKNOWN: &str = "unknown";

    /// Register the HELP/TYPE text once, at startup.
    pub fn describe() {
        metrics::describe_counter!(
            "nauka_s3_requests_total",
            "S3 requests served, by operation, status class and read/write class."
        );
        metrics::describe_histogram!(
            "nauka_s3_request_duration_seconds",
            "Wall-clock time to serve an S3 request, measured at the outer HTTP layer."
        );
        metrics::describe_histogram!(
            "nauka_s3_request_bytes",
            "Declared Content-Length of S3 requests and responses. Streaming bodies without a Content-Length are not counted."
        );
        metrics::describe_counter!(
            "nauka_s3_writes_rejected_total",
            "S3 writes refused before or during the Raft commit, by reason."
        );
        metrics::describe_counter!(
            "nauka_s3_reads_total",
            "Object visibility checks on the read path, by the freshness achieved."
        );
    }

    /// Give the request in flight a slot to record its identity in.
    pub async fn scoped<F: Future>(fut: F) -> F::Output {
        CURRENT.scope(RefCell::new(None), fut).await
    }

    /// Name the operation being served, from the one place `s3s` exposes a
    /// canonical name. A no-op outside a [`scoped`] future.
    pub fn set_op(name: &str, class: &'static str) {
        let _ = CURRENT.try_with(|slot| {
            *slot.borrow_mut() = Some(Op {
                name: name.to_owned(),
                class,
            });
        });
    }

    /// Record a finished request. `status` must be read *after* any
    /// post-hoc rewriting of the response, or the metric will disagree with
    /// what the client received.
    pub fn record_request(
        status: u16,
        elapsed: Duration,
        req_bytes: Option<u64>,
        resp_bytes: Option<u64>,
    ) {
        let op = CURRENT
            .try_with(|slot| slot.borrow().clone())
            .ok()
            .flatten();
        let (name, class) = match &op {
            Some(op) => (op.name.as_str(), op.class),
            None => (UNKNOWN, UNKNOWN),
        };
        metrics::counter!(
            "nauka_s3_requests_total",
            "operation" => name.to_owned(),
            "status_class" => status_class(status),
            "class" => class,
        )
        .increment(1);
        metrics::histogram!(
            "nauka_s3_request_duration_seconds",
            "operation" => name.to_owned(),
        )
        .record(elapsed.as_secs_f64());
        if let Some(bytes) = req_bytes {
            metrics::histogram!(
                "nauka_s3_request_bytes",
                "operation" => name.to_owned(),
                "direction" => "in",
            )
            .record(bytes as f64);
        }
        if let Some(bytes) = resp_bytes {
            metrics::histogram!(
                "nauka_s3_request_bytes",
                "operation" => name.to_owned(),
                "direction" => "out",
            )
            .record(bytes as f64);
        }
    }

    /// A write that could not be committed. `reason` is a closed set.
    pub fn record_write_rejected(reason: &'static str) {
        metrics::counter!("nauka_s3_writes_rejected_total", "reason" => reason).increment(1);
    }

    /// The freshness a read-path visibility check achieved.
    pub fn record_read_freshness(freshness: &'static str) {
        metrics::counter!("nauka_s3_reads_total", "freshness" => freshness).increment(1);
    }

    /// Status class rather than the raw code: bounded, and the distinction
    /// an operator actually alerts on.
    fn status_class(status: u16) -> &'static str {
        match status {
            100..=199 => "1xx",
            200..=299 => "2xx",
            300..=399 => "3xx",
            400..=499 => "4xx",
            _ => "5xx",
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn status_classes_cover_the_ranges() {
            assert_eq!(status_class(200), "2xx");
            assert_eq!(status_class(204), "2xx");
            assert_eq!(status_class(302), "3xx");
            assert_eq!(status_class(403), "4xx");
            assert_eq!(status_class(404), "4xx");
            assert_eq!(status_class(500), "5xx");
            assert_eq!(status_class(503), "5xx");
        }

        #[tokio::test]
        async fn op_is_visible_later_in_the_same_scope() {
            scoped(async {
                assert!(CURRENT.with(|slot| slot.borrow().is_none()));
                set_op("GetObject", "read");
                let seen = CURRENT.with(|slot| slot.borrow().clone()).unwrap();
                assert_eq!(seen.name, "GetObject");
                assert_eq!(seen.class, "read");
            })
            .await;
        }

        #[test]
        fn set_op_outside_a_scope_is_a_no_op() {
            // Must not panic: the access check can run on paths that were
            // never wrapped, and telemetry may never break a request.
            set_op("GetObject", "read");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn latency_buckets_are_sorted_and_positive() {
        assert!(LATENCY_BUCKETS[0] > 0.0);
        assert!(LATENCY_BUCKETS.windows(2).all(|w| w[0] < w[1]));
    }

    #[test]
    fn size_buckets_are_sorted_and_positive() {
        assert!(SIZE_BUCKETS[0] > 0.0);
        assert!(SIZE_BUCKETS.windows(2).all(|w| w[0] < w[1]));
    }
}
