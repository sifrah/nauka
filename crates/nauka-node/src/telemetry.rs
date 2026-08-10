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
