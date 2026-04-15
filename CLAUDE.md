# Nauka

Open-source platform that turns bare-metal servers into a programmable cloud.

## Build & Test
- `cargo build` — build all crates
- `cargo test` — run all tests (458+)
- `cargo clippy` — lint
- `cd docs && npx astro build` — build docs site

## Repository Structure
- `layers/core` — `nauka-core`: Resource framework, typed IDs, crypto, addressing, API gen, UI, config (no I/O, no async)
- `layers/state` — `nauka-state`: SurrealDB-backed persistence (`EmbeddedDb` wraps SurrealKV for local bootstrap state, TiKV for cluster state)
- `layers/hypervisor` — `nauka-hypervisor`: WireGuard mesh (fabric), peering protocol, hypervisor daemon, service lifecycle, handlers
- `bin/nauka` — CLI binary that composes all layers (zero logic)
- `docs/` — Starlight documentation site (deployed to GitHub Pages)

## Key Modules (layers/hypervisor/src/)
- `fabric/mesh.rs` — Mesh + hypervisor identity, create_mesh(), create_hypervisor()
- `fabric/peer.rs` — Peer management, PeerList, PeerStatus
- `fabric/ops.rs` — High-level orchestration: init, join, status, start, stop, leave, plus `status_view`/`list_view`/`get_view` JSON views
- `fabric/peering.rs` — TCP peering protocol types (JoinRequest, JoinResponse, PeerAnnounce)
- `fabric/peering_server.rs` — TCP listener for join requests (runs inside the daemon)
- `fabric/peering_client.rs` — TCP client for join flow
- `fabric/announce.rs` — Announce listener + broadcast helpers (runs inside the daemon)
- `fabric/daemon.rs` — `nauka.service` entry point: owns the `EmbeddedDb` handle, spawns peering/announce/health/reconcile tasks, hosts the control socket, handles SIGTERM shutdown
- `fabric/control/` — Unix-socket protocol + server + client (`ControlRequest`/`ControlResponse`, `forward_or_fallback`)
- `fabric/wg.rs` — WireGuard interface management (nauka0)
- `fabric/service.rs` — systemd service management (wg-quick@nauka0)
- `fabric/state.rs` — FabricState persistence + `write_lock` for cross-task serialisation of state mutations
- `handlers.rs` — ResourceDef + thin handlers (forward to daemon via control socket, fall back to direct DB access)

## Architecture
- Every deployed node runs a long-lived `nauka.service` (the hypervisor daemon). The daemon opens `bootstrap.skv` once at startup and holds the handle for its lifetime, hosting the peering TCP listener, the announce listener, the WireGuard health loop, the mesh reconciler, and the operator Unix control socket all in one tokio runtime. SurrealDB's internal concurrency handles multi-reader/multi-writer inside the shared handle; the daemon is the *only* process holding the SurrealKV flock.
- Operator CLI commands (`status`, `list`, `get`, `cp-status`, `drain`, `enable`, `update`) forward through `/run/nauka/ctl.sock` (`~/.nauka/ctl.sock` in CLI mode) when the daemon is up, and fall back to opening `bootstrap.skv` directly when it is not — so bootstrap, recovery, and test harnesses keep working with no daemon installed.
- One-shot CLI commands still run as standalone processes: `init` and `join` open the DB, set state up, release the handle, then `daemon::install_service()` starts the daemon. `leave` sends `ControlRequest::Shutdown`, waits for the daemon to exit, then tears the rest of the state down and uninstalls the unit.
- Every server is a hypervisor. The mesh connects them.
- ResourceDef generates both CLI commands (clap) and REST API routes (axum) from one definition.
- IPv6-native: each mesh gets a ULA /48, each node a /128.

## CLI
- `nauka hypervisor init` — create a new mesh, install `nauka.service`
- `nauka hypervisor join` — join an existing mesh, install `nauka.service`
- `nauka hypervisor status` — show status (forwards to daemon via UDS if up)
- `nauka hypervisor start/stop` — manage all local services (wg, daemon, PD, TiKV, storage)
- `nauka hypervisor leave` — stop daemon, tear down, uninstall unit
- `nauka hypervisor list/get` — list/get hypervisors (forwards to daemon)
- `nauka hypervisor cp-status` — control plane status (forwards for `mesh_ipv6`, then HTTP locally)
- `nauka hypervisor daemon` — `ExecStart` of `nauka.service` (invoked by systemd, not directly)

## Conventions
- serde Serialize/Deserialize on all public types
- thiserror for library errors, anyhow for binaries
- Async runtime: tokio
- Manual peering: no automatic discovery, operator approves join requests
- One layer = one directory in `layers/`, one Rust crate
- Lower layers never depend on higher layers
