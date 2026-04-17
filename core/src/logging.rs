//! Logging contract for every nauka crate — see ADR 0005.
//!
//! Call [`init`] once from the process entry point, choosing [`LogMode`]
//! that matches the surface (CLI, daemon, or test). The right
//! `EnvFilter` default, formatter, and panic hook are installed for you.
//! `RUST_LOG` overrides the default in every mode.

use std::fmt::Display;

use tracing_subscriber::EnvFilter;

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

    let builder = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr);

    let _ = match mode {
        // CLI output is ephemeral — drop timestamps and targets so any
        // warning we do emit reads clean next to cli_out output.
        LogMode::Cli => builder.without_time().with_target(false).try_init(),
        // Daemon output goes to journald; keep timestamps + targets.
        LogMode::Daemon => builder.try_init(),
        // Tests: capture-friendly writer, debug for our crates.
        LogMode::Test => builder.with_test_writer().try_init(),
    };

    install_panic_hook();
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
