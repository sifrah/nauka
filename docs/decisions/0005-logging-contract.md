# ADR 0005 — Logging contract

**Issue:** sifrah/nauka#333
**Status:** Accepted
**Date:** 2026-04-17

## Context

Logging today is inconsistent and noisy:

- `eprintln!`/`println!` calls are scattered across layers (`sm: apply
  query: …`, `reconciler: -peer …`, `+ peer joined: …`), which makes them
  impossible to filter, unstructured, and tangled with user-facing CLI
  output on stdout.
- `bin/nauka/src/main.rs` defaults `EnvFilter` to `info`, which lets
  surrealdb / surrealkv / openraft INFO lines spam every CLI invocation.
  `nauka hypervisor init` buries "mesh created" under ~40 lines of
  library noise.
- There is no convention for event names, structured fields, or spans,
  so every call site reinvents the format.
- The "if `Err(e)` log and continue" idiom is duplicated at ~20 sites
  with slightly different phrasing each time.

We need a single contract every layer, handler, and binary agrees on,
and a small amount of shared infrastructure in `nauka-core` so call
sites stop reimplementing it.

## Decision

**Adopt a single logging contract. All diagnostic output goes through
`tracing`. All CLI user-facing output goes through a thin `cli_out`
helper in `bin/nauka`. Shared behaviour (subscriber setup, panic hook,
swallowed-error helper) lives in `nauka_core::logging`.**

### Rules

1. **No `println!` or `eprintln!` in layer code.** Every layer's
   `lib.rs` carries
   `#![deny(clippy::print_stdout, clippy::print_stderr)]`. The only
   exception is code gated on `#[cfg(test)]`.
2. **User-facing output** happens only in `bin/nauka`, via the
   `cli_out` module (`say`, `pair`, `section`). The module is the one
   place that's allowed to call `println!`.
3. **Diagnostic logging** goes through `tracing`, with levels used as:
   - `error!` — action needed, failure the operator must see
   - `warn!` — a swallowed error or an anomaly that didn't propagate
   - `info!` — lifecycle events (peer joined, snapshot built,
     service up)
   - `debug!` — internal per-tick work (state-machine apply, reconciler
     sweep, raft RPCs)
   - `trace!` — everything else
4. **Structured fields, not string interpolation.** Prefer
   `tracing::info!(peer = %pk, "peer joined")` over
   `info!("peer {pk} joined")`.
5. **Event names are `domain.object.action`** in an `event` field for
   anything worth grepping: `peer.join`, `raft.snapshot.build`,
   `reconciler.peer.add`.
6. **Propagated errors are not logged** (the caller chooses what to
   do). Swallowed errors are logged at `warn!` minimum, with the
   error as a field, via `LogErr::warn_if_err` /
   `LogErr::ok_or_warn`.

### Default filter per mode

`nauka_core::logging::LogMode` picks the `EnvFilter` default:

- **`Cli`** — `warn` globally. CLI invocations are short-lived and
  their user-facing output already comes from `cli_out`; lifecycle
  info from the layers would just be noise.
- **`Daemon`** —
  `warn,nauka=info,nauka_core=info,nauka_state=info,nauka_hypervisor=info`.
  The daemon runs under systemd and writes to journald, so nauka
  crates emit INFO lifecycle lines there while library crates
  (surrealdb, openraft) stay at WARN.
- **`Test`** — `warn,nauka*=debug`. Tests get everything from our
  crates.

`RUST_LOG` overrides the default in every mode.

### Enforcement

- `#![deny(clippy::print_stdout, clippy::print_stderr)]` at the top of
  `layers/state/src/lib.rs` and `layers/hypervisor/src/lib.rs`.
- CI adds `-D clippy::print_stdout -D clippy::print_stderr` to the
  workspace clippy command so new crates and non-lib targets can't
  regress.
- `bin/nauka/src/cli_out.rs` carries local `#[allow]` attributes on
  the three print functions — the only sanctioned `println!` site in
  the tree.

### Shared infrastructure in `nauka_core::logging`

- `pub enum LogMode { Cli, Daemon, Test }`
- `pub fn init(mode: LogMode)` installs the subscriber with the right
  filter / format / writer for that mode, honours `RUST_LOG`, and
  installs the panic hook.
- `pub fn install_panic_hook()` routes `std::panic` through
  `tracing::error!` with structured fields (thread, location, payload)
  before re-invoking the default hook.
- `pub trait LogErr<T, E>` adds `warn_if_err(self, context)` and
  `ok_or_warn(self, context)` to any `Result`, replacing the
  duplicated "if `Err(e)` log + continue" pattern.

## Consequences

- Every existing `eprintln!` in `layers/*` is rewritten as a
  `tracing::{debug,info,warn,error}!` call at the right level, with
  structured fields.
- Every `println!` in layer code that leaked user output to stdout
  from a non-binary path (e.g. the "hypervisor left mesh" line in
  `daemon::leave_hypervisor`) is replaced by a library return value
  that `bin/nauka` prints via `cli_out`.
- `bin/nauka/src/main.rs` drops its inline `EnvFilter` + `fmt()` setup
  and calls `nauka_core::logging::init(LogMode::Cli)`. The daemon
  subcommand calls it with `LogMode::Daemon`.
- `nauka hypervisor init` on a fresh VM prints only the lifecycle
  lines (mesh id, public key, address, raft addr, pin, service
  status). Library noise at INFO is gated to WARN.
- `journalctl -u nauka-hypervisor.service` continues to show INFO
  lifecycle from nauka crates.
- Adding a new crate means adding the `#![deny(...)]` attribute to its
  `lib.rs` and calling `nauka_core::logging::init` from its entry
  point — no per-crate subscriber setup.

## References

- Issue — sifrah/nauka#333
- Depends on — sifrah/nauka#332 (`nauka-core` scaffold)
