//! Logging contract for every nauka crate — see ADR 0005.
//!
//! Call [`init`] once from the process entry point, choosing [`LogMode`]
//! that matches the surface (CLI, daemon, or test). The right
//! `EnvFilter` default, formatter, and panic hook are installed for you.
//! `RUST_LOG` overrides the default in every mode.

use std::fmt::{self, Display};
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use tracing::{Event, Instrument, Level, Subscriber};
use tracing_subscriber::fmt::format::{FormatEvent, FormatFields, Writer};
use tracing_subscriber::fmt::time::{FormatTime, SystemTime};
use tracing_subscriber::fmt::{FmtContext, FormattedFields};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::EnvFilter;

/// `service` field stamped on every event — the product name.
const SERVICE: &str = "nauka";
/// `version` field stamped on every event — the binary's
/// `CARGO_PKG_VERSION` (matches workspace.package.version).
const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Process-wide `node_id`, populated by [`set_node_id`] once the
/// daemon computes it. `0` means "not yet known" — the field is
/// omitted from output in that case, since the CLI doesn't have one
/// until after `hypervisor init`/`join`.
static NODE_ID: AtomicU64 = AtomicU64::new(0);

/// Stamp this process's `node_id` so every subsequent event carries
/// it. Call once at daemon startup after hashing the public key into
/// a node id. Safe to call multiple times — last writer wins.
pub fn set_node_id(id: u64) {
    NODE_ID.store(id, Ordering::Relaxed);
}

/// Which surface the process is — picks the default filter and format.
#[derive(Debug, Clone, Copy)]
pub enum LogMode {
    /// Short-lived CLI invocation. Default filter is `warn` globally —
    /// user-facing output comes from `cli_out` in `bin/nauka`, not from
    /// tracing.
    Cli,
    /// Long-running daemon under systemd. Nauka crates log at `info`
    /// so lifecycle events land in journald; library crates stay at
    /// `warn`.
    Daemon,
    /// Test harness. Nauka crates at `debug` so failing tests have
    /// enough context, library crates at `warn`.
    Test,
}

impl LogMode {
    fn default_filter(self) -> &'static str {
        match self {
            LogMode::Cli => "warn",
            LogMode::Daemon => {
                "warn,nauka=info,nauka_core=info,nauka_state=info,nauka_hypervisor=info"
            }
            LogMode::Test => {
                "warn,nauka=debug,nauka_core=debug,nauka_state=debug,nauka_hypervisor=debug"
            }
        }
    }
}

/// Install the tracing subscriber and the panic hook for this process.
///
/// Safe to call at most once per process. Subsequent calls are
/// no-ops (we use `try_init`) so test harnesses don't panic on reuse.
pub fn init(mode: LogMode) {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(mode.default_filter()));

    let format = match mode {
        // CLI output is ephemeral — drop timestamps and targets so any
        // warning we do emit reads clean next to cli_out output.
        LogMode::Cli => NaukaFormat::new().without_time().without_target(),
        // Daemon output goes to journald; keep timestamps + targets.
        // Tests share the daemon format — full detail for failing tests.
        LogMode::Daemon | LogMode::Test => NaukaFormat::new(),
    };

    let builder = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .event_format(format);

    let _ = match mode {
        LogMode::Cli | LogMode::Daemon => builder.try_init(),
        LogMode::Test => builder.with_test_writer().try_init(),
    };

    install_panic_hook();
}

/// Custom [`FormatEvent`] that replicates tracing-subscriber's full
/// format (time · level · span breadcrumb · target · fields) and
/// injects `service=nauka version=<pkg> node_id=<N>?` immediately
/// before the event's own fields on every line.
///
/// The injection is a FormatEvent concern, not a Layer concern —
/// tracing's Layer API can observe events but not modify them. A
/// custom FormatEvent is the supported seam for "every event gets
/// these fields".
pub struct NaukaFormat {
    with_time: bool,
    with_target: bool,
}

impl NaukaFormat {
    pub const fn new() -> Self {
        Self {
            with_time: true,
            with_target: true,
        }
    }

    pub const fn without_time(mut self) -> Self {
        self.with_time = false;
        self
    }

    pub const fn without_target(mut self) -> Self {
        self.with_target = false;
        self
    }
}

impl Default for NaukaFormat {
    fn default() -> Self {
        Self::new()
    }
}

impl<S, N> FormatEvent<S, N> for NaukaFormat
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'writer> FormatFields<'writer> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        if self.with_time {
            SystemTime.format_time(&mut writer)?;
            write!(writer, "  ")?;
        }

        match *event.metadata().level() {
            Level::TRACE => write!(writer, "TRACE ")?,
            Level::DEBUG => write!(writer, "DEBUG ")?,
            Level::INFO => write!(writer, " INFO ")?,
            Level::WARN => write!(writer, " WARN ")?,
            Level::ERROR => write!(writer, "ERROR ")?,
        }

        // Span breadcrumb, root → leaf, each as `name{fields}`.
        if let Some(scope) = ctx.event_scope() {
            let mut first = true;
            for span in scope.from_root() {
                if !first {
                    write!(writer, ":")?;
                }
                first = false;
                write!(writer, "{}", span.name())?;
                let ext = span.extensions();
                if let Some(fields) = ext.get::<FormattedFields<N>>() {
                    if !fields.fields.is_empty() {
                        write!(writer, "{{{}}}", fields.fields)?;
                    }
                }
            }
            if !first {
                write!(writer, ": ")?;
            }
        }

        if self.with_target {
            write!(writer, "{}: ", event.metadata().target())?;
        }

        // Nauka context fields on every event.
        write!(writer, "service={SERVICE} version={VERSION}")?;
        let node_id = NODE_ID.load(Ordering::Relaxed);
        if node_id != 0 {
            write!(writer, " node_id={node_id}")?;
        }
        write!(writer, " ")?;

        ctx.format_fields(writer.by_ref(), event)?;
        writeln!(writer)
    }
}

/// Route panics through `tracing::error!` with structured fields, then
/// chain to the previously-installed hook (usually the default, which
/// prints the backtrace). Called automatically by [`init`].
pub fn install_panic_hook() {
    let prev = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let thread = std::thread::current();
        let thread_name = thread.name().unwrap_or("<unnamed>");
        let location = info
            .location()
            .map(|l| format!("{}:{}:{}", l.file(), l.line(), l.column()))
            .unwrap_or_else(|| "<unknown>".to_string());
        let payload = payload_str(info.payload());

        tracing::error!(
            event = "panic",
            thread = thread_name,
            location = %location,
            payload = %payload,
            "thread panicked"
        );

        prev(info);
    }));
}

fn payload_str(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "<non-string panic payload>".to_string()
    }
}

/// Helpers for the "log a swallowed error and continue" idiom.
///
/// Use when an error is intentionally not propagated — for example a
/// best-effort reconciler sweep or a bootstrap step that is allowed to
/// fail partially. Errors that propagate via `?` should *not* go
/// through this trait; the caller chooses whether to log.
pub trait LogErr<T, E> {
    /// Emit a `warn!` with `context` and `error` fields if `self` is
    /// `Err`, then return `self` unchanged so the caller can still
    /// branch on it.
    fn warn_if_err(self, context: &'static str) -> Result<T, E>;
    /// Emit a `warn!` with `context` and `error` fields if `self` is
    /// `Err`, then collapse to an `Option`.
    fn ok_or_warn(self, context: &'static str) -> Option<T>;
}

impl<T, E: Display> LogErr<T, E> for Result<T, E> {
    fn warn_if_err(self, context: &'static str) -> Result<T, E> {
        if let Err(ref e) = self {
            tracing::warn!(event = "error.swallowed", context, error = %e, "swallowed error");
        }
        self
    }

    fn ok_or_warn(self, context: &'static str) -> Option<T> {
        match self {
            Ok(v) => Some(v),
            Err(e) => {
                tracing::warn!(event = "error.swallowed", context, error = %e, "swallowed error");
                None
            }
        }
    }
}

/// Errors that carry their own `event` name.
///
/// The name is a property of the error variant, not the call site,
/// so renaming a variant or adding a new one updates every log event
/// that surfaces it — impossible to leave two call sites emitting
/// divergent event strings for the same underlying error.
///
/// Use `"domain.object.cause"` form (`mesh.join.invalid_pin`,
/// `state.raft.write_failed`). Returned string is `&'static str` so
/// it never allocates at log time.
pub trait NaukaError: std::error::Error {
    fn event_name(&self) -> &'static str;
}

/// Log a [`NaukaError`] using its own `event_name` — no caller-
/// supplied context string needed.
///
/// Sister trait to [`LogErr`]: method names differ on purpose
/// (`warn` / `ok_warn` vs `warn_if_err` / `ok_or_warn`) so both
/// traits can be `use`d in the same file without ambiguity.
pub trait LogNaukaErr<T, E> {
    /// Emit a `warn!(event = e.event_name(), error = %e)` if `self`
    /// is `Err`, then return `self` unchanged.
    fn warn(self) -> Result<T, E>;
    /// Emit a `warn!(event = e.event_name(), error = %e)` if `self`
    /// is `Err`, then collapse to an `Option`.
    fn ok_warn(self) -> Option<T>;
}

impl<T, E: NaukaError> LogNaukaErr<T, E> for Result<T, E> {
    fn warn(self) -> Result<T, E> {
        if let Err(ref e) = self {
            tracing::warn!(event = e.event_name(), error = %e, "swallowed error");
        }
        self
    }

    fn ok_warn(self) -> Option<T> {
        match self {
            Ok(v) => Some(v),
            Err(e) => {
                tracing::warn!(event = e.event_name(), error = %e, "swallowed error");
                None
            }
        }
    }
}

/// Emit a `tracing::warn!` with the given fields and
/// `return Err($err.into())` in a single call — the "log then return"
/// idiom in one line so the two concerns can't drift apart.
///
/// The first argument is the error to return (`.into()`-converted so
/// it works with any return type that has a `From` impl for the
/// error — `anyhow::Error`, thiserror enums, etc.). Remaining
/// arguments are passed straight through to `tracing::warn!` —
/// structured fields like `event = "foo.bar"` or `peer = %socket_addr`,
/// optionally followed by a message.
///
/// Error-first, comma-separated: the macro matcher can't reliably
/// disambiguate a `;` separator from a `tt` field, so we put the
/// error in the fixed leading position instead.
///
/// The call-site crate must depend on `tracing` (all nauka crates
/// already do); the macro expands to `::tracing::warn!`.
///
/// # Example
///
/// ```ignore
/// if req.pin != expected {
///     nauka_core::bail_log!(
///         MeshError::Join("invalid pin".into()),
///         event = "peer.join.pin_mismatch",
///         peer = %peer_addr,
///         "invalid pin"
///     );
/// }
/// ```
#[macro_export]
macro_rules! bail_log {
    ($err:expr, $($fields:tt)+) => {{
        ::tracing::warn!($($fields)+);
        return ::std::result::Result::Err(::std::convert::From::from($err));
    }};
}

/// Test-only helpers for asserting on the events an async block
/// emits. See [`test::capture`].
pub mod test {
    use std::collections::BTreeMap;
    use std::future::Future;
    use std::sync::{Arc, Mutex};

    use tracing::field::{Field, Visit};
    use tracing::{Event, Level, Subscriber};
    use tracing_subscriber::layer::{Context, SubscriberExt};
    use tracing_subscriber::Layer;

    /// One captured event with its level, structured fields, and
    /// message. Field values are rendered as they'd appear in a log
    /// line, minus surrounding quotes.
    #[derive(Debug, Clone)]
    pub struct CapturedEvent {
        pub level: Level,
        pub fields: BTreeMap<String, String>,
        pub message: String,
    }

    impl CapturedEvent {
        /// Shortcut for `fields.get("event").map(|s| s.as_str())` —
        /// matches the `event = "domain.object.action"` convention
        /// every nauka event follows.
        pub fn event(&self) -> Option<&str> {
            self.fields.get("event").map(|s| s.as_str())
        }
    }

    /// Run `fut` with a capture subscriber installed as the
    /// thread-local default, then return `(fut's output, every
    /// event emitted)`.
    ///
    /// Intended for `#[tokio::test]` (current-thread runtime) so
    /// events from spawned tasks are captured through the same
    /// thread-local dispatch. Multi-threaded runtimes may see gaps
    /// if tasks migrate off the caller's thread.
    pub async fn capture<F, T>(fut: F) -> (T, Vec<CapturedEvent>)
    where
        F: Future<Output = T>,
    {
        let events: Arc<Mutex<Vec<CapturedEvent>>> = Arc::new(Mutex::new(Vec::new()));
        let subscriber = tracing_subscriber::registry().with(CaptureLayer {
            events: events.clone(),
        });
        let dispatch = tracing::Dispatch::new(subscriber);
        let guard = tracing::dispatcher::set_default(&dispatch);
        let result = fut.await;
        drop(guard);
        let captured = events.lock().expect("poisoned events lock").clone();
        (result, captured)
    }

    struct CaptureLayer {
        events: Arc<Mutex<Vec<CapturedEvent>>>,
    }

    impl<S: Subscriber> Layer<S> for CaptureLayer {
        fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
            let mut fields = BTreeMap::new();
            let mut message = String::new();
            let mut visitor = CaptureVisitor {
                fields: &mut fields,
                message: &mut message,
            };
            event.record(&mut visitor);
            if let Ok(mut vec) = self.events.lock() {
                vec.push(CapturedEvent {
                    level: *event.metadata().level(),
                    fields,
                    message,
                });
            }
        }
    }

    struct CaptureVisitor<'a> {
        fields: &'a mut BTreeMap<String, String>,
        message: &'a mut String,
    }

    impl<'a> CaptureVisitor<'a> {
        fn set(&mut self, field: &Field, value: String) {
            if field.name() == "message" {
                *self.message = value;
            } else {
                self.fields.insert(field.name().to_string(), value);
            }
        }
    }

    impl Visit for CaptureVisitor<'_> {
        fn record_str(&mut self, field: &Field, value: &str) {
            self.set(field, value.to_string());
        }

        fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
            // tracing routes `String`/`Display`-via-`%` values
            // through `record_debug`; strip the surrounding quotes
            // so assertions read as the logical value, not the
            // Debug-format of it.
            let raw = format!("{value:?}");
            let stripped = if raw.starts_with('"') && raw.ends_with('"') && raw.len() >= 2 {
                raw[1..raw.len() - 1].to_string()
            } else {
                raw
            };
            self.set(field, stripped);
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[tokio::test]
        async fn captures_an_info_event_with_fields_and_message() {
            let ((), events) = capture(async {
                tracing::info!(event = "test.capture.smoke", x = 42, "hello");
            })
            .await;

            assert_eq!(events.len(), 1);
            let e = &events[0];
            assert_eq!(e.level, Level::INFO);
            assert_eq!(e.event(), Some("test.capture.smoke"));
            assert_eq!(e.fields.get("x").map(|s| s.as_str()), Some("42"));
            assert_eq!(e.message, "hello");
        }
    }
}

/// Generate a fresh trace_id — a UUID v4 string suitable for
/// `tracing::info_span!("...", trace_id = %new_trace_id())` at any
/// entry point where an operation starts outside of [`instrument_op`]
/// (CLI dispatch, TCP accept loop, Raft RPC handler).
///
/// Operations inside [`instrument_op`] already get their own
/// trace_id; the outer span's trace_id stays in the breadcrumb, so
/// `journalctl … | grep trace_id=<uuid>` picks up everything
/// under it.
pub fn new_trace_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// Wrap an async operation with a span + duration + lifecycle events.
///
/// Emits `<name>.start` at the beginning, `<name>.end` with `elapsed_ms`
/// on success, `<name>.failed` with `elapsed_ms` + `error` on failure.
/// The span carries `name` and a fresh `trace_id` (UUID v4) so every
/// inner event is greppable via `trace_id=<uuid>` in journalctl.
///
/// Use for operations you'd want to measure or correlate in
/// `journalctl` — init, join, snapshot build/install, peer remove,
/// etc. Cheap enough to wrap anywhere an error is worth noting; too
/// verbose for per-tick work (reconciler sweep, raft apply).
pub async fn instrument_op<F, T, E>(name: &'static str, fut: F) -> Result<T, E>
where
    F: Future<Output = Result<T, E>>,
    E: Display,
{
    let trace_id = new_trace_id();
    let span = tracing::info_span!("op", name = name, trace_id = %trace_id);
    async move {
        let start = Instant::now();
        tracing::info!(event = format!("{name}.start"), "op start");
        match fut.await {
            Ok(v) => {
                tracing::info!(
                    event = format!("{name}.end"),
                    elapsed_ms = start.elapsed().as_millis() as u64,
                    "op end"
                );
                Ok(v)
            }
            Err(e) => {
                tracing::warn!(
                    event = format!("{name}.failed"),
                    elapsed_ms = start.elapsed().as_millis() as u64,
                    error = %e,
                    "op failed"
                );
                Err(e)
            }
        }
    }
    .instrument(span)
    .await
}
