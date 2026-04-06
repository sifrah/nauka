# Nauka

Open-source platform that turns bare-metal servers into a programmable cloud.

## Build & Test
- `cargo build` — build all crates
- `cargo test` — run all tests (458+)
- `cargo clippy` — lint
- `cd docs && npx astro build` — build docs site

## Repository Structure
- `layers/core` — `nauka-core`: Resource framework, typed IDs, crypto, addressing, API gen, UI, config (no I/O, no async)
- `layers/state` — `nauka-state`: Embedded persistence (redb), typed tables, TTL, CAS, watch
- `layers/hypervisor` — `nauka-hypervisor`: WireGuard mesh (fabric), peering protocol, service lifecycle, handlers
- `bin/nauka` — CLI binary that composes all layers (zero logic)
- `docs/` — Starlight documentation site (deployed to GitHub Pages)

## Key Modules (layers/hypervisor/src/)
- `fabric/mesh.rs` — Mesh + hypervisor identity, create_mesh(), create_hypervisor()
- `fabric/peer.rs` — Peer management, PeerList, PeerStatus
- `fabric/ops.rs` — High-level orchestration: init, join, status, start, stop, leave
- `fabric/peering.rs` — TCP peering protocol types (JoinRequest, JoinResponse, PeerAnnounce)
- `fabric/peering_server.rs` — TCP listener for join requests
- `fabric/peering_client.rs` — TCP client for join flow
- `fabric/wg.rs` — WireGuard interface management (nauka0)
- `fabric/service.rs` — systemd service management (wg-quick@nauka0)
- `fabric/state.rs` — FabricState persistence (redb)
- `handlers.rs` — ResourceDef + thin handlers (delegate to fabric::ops)

## Architecture
- Nauka is a CLI orchestrator, NOT a daemon. Configures systemd services, then exits.
- Every server is a hypervisor. The mesh connects them.
- ResourceDef generates both CLI commands (clap) and REST API routes (axum) from one definition.
- IPv6-native: each mesh gets a ULA /48, each node a /128.

## CLI
- `nauka hypervisor init` — create a new mesh
- `nauka hypervisor join` — join an existing mesh
- `nauka hypervisor status` — show status
- `nauka hypervisor start/stop` — manage WireGuard service
- `nauka hypervisor leave` — leave the mesh
- `nauka hypervisor peering` — start peering listener
- `nauka hypervisor list/get` — list/get hypervisors

## Conventions
- serde Serialize/Deserialize on all public types
- thiserror for library errors, anyhow for binaries
- Async runtime: tokio
- Manual peering: no automatic discovery, operator approves join requests
- One layer = one directory in `layers/`, one Rust crate
- Lower layers never depend on higher layers
