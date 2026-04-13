# nauka-state

State persistence for Nauka.

- `EmbeddedDb` — embedded SurrealDB over the SurrealKV engine, used for
  node-local bootstrap state (mesh identity, peers, WireGuard keys).
- `ClusterDb` — TiKV-backed distributed KV for shared cluster state
  (orgs, VPCs, VMs, ...). Being replaced by `EmbeddedDb<TiKv>` in Phase 2.

See `src/embedded.rs` for the current API surface and `schemas/bootstrap.surql`
for the bootstrap schema that `EmbeddedDb::open` applies at startup.

A full rewrite of this document lands in sifrah/nauka#203 (P1.13).
