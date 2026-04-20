# Nauka

Open-source platform that turns bare-metal servers into a programmable cloud.

## Build & Test
- `cargo build` — build all crates
- `cargo test` — run all tests (458+)
- `cargo clippy` — lint
- `cd docs && npx astro build` — build docs site

## Repository Structure
- `core` — `nauka-core`: Shared cross-cutting infrastructure (logging, errors, typed IDs, utilities). Lives at repo root, NOT under `layers/`.
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

## Working Principles

These bias toward caution over speed. For trivial tasks, use judgment.

### 1. Think before coding
Don't assume, don't hide confusion, surface tradeoffs.
- State assumptions explicitly; if uncertain, ask.
- If multiple interpretations exist, present them — don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop, name what's confusing, ask.

### 2. Simplicity first
Minimum code that solves the problem. Nothing speculative.
- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If 200 lines could be 50, rewrite it.

### 3. Surgical changes
Touch only what you must. Clean up only your own mess.
- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it — don't delete it.
- Remove imports/variables/functions that *your* changes made unused; leave pre-existing dead code alone unless asked.
- Every changed line should trace directly to the user's request.

### 4. Goal-driven execution
Define success criteria, loop until verified.
- "Add validation" → write tests for invalid inputs, then make them pass.
- "Fix the bug" → write a test that reproduces it, then make it pass.
- "Refactor X" → ensure tests pass before and after.
- For multi-step work, state a brief plan with a verify step per item.
- Remember the Hetzner rule: real multi-node VM validation is required before any #issue is called done.
