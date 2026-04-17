# ADR 0006 — Resource contract

**Issue:** sifrah/nauka#340
**Status:** Accepted
**Date:** 2026-04-17

## Context

Nauka stores its state in SurrealDB. Today there are two resource tables
(`mesh`, `hypervisor`) defined as hand-written `.surql` files. Phase 2+
adds many more (`org`, `project`, `env`, `user`, `vpc`, `subnet`, `vm`,
`workload`, …). Three failure modes are already visible at this small
scale:

1. **Silent divergence.** A new resource forgets a UNIQUE index, a
   timestamp, or a version field — nothing fails until it does in prod.
   Today `hypervisor.node_id` and `hypervisor.raft_addr` are unique by
   design but not by constraint.
2. **Raft-unsafe defaults.** `DEFAULT time::now()` on a replicated table
   silently breaks consensus determinism. `layers/hypervisor/src/definition.surql`
   has a hand-written comment warning the next author. Nothing prevents
   the next table from making the same mistake.
3. **Boilerplate drift.** Every resource implements `CREATE`/`UPDATE`/
   `DELETE` differently. `daemon.rs`, `mesh/join.rs`, `mesh/state.rs`
   all write raw SurrealQL to the same `hypervisor` table with slight
   variations.

Retroactive cleanup at 20 resources is expensive. Locking the contract
in at 2 is cheap.

## Decision

**Adopt a single Resource contract. Every resource is declared as a
Rust struct with `#[derive(Resource)]`; the proc-macro generates the
`SCHEMAFULL` DDL, registers it in a global slice, validates every
invariant at compile time, and emits typed CRUD helpers. The contract
itself lives in `nauka_core::resource`; the macro lives in a separate
`nauka-core-macros` crate.**

A new resource that violates the contract **does not compile**.

### Rules

1. **One source of truth — the Rust struct.** The `.surql` DDL is
   *generated* from the struct, not maintained separately. Hand-written
   `definition.surql` files are deleted.
2. **Record ID is the natural key.** Declared via `#[id]` on a struct
   field. Exactly one `#[id]` per resource. Records are
   `table:⟨id_value⟩`, never auto-generated. Joins, idempotency, and
   `RELATE` all benefit.
3. **Mandatory base fields.** Every resource carries:
   - `created_at: Datetime` — set by the writer at creation time. Never
     `DEFAULT time::now()`.
   - `updated_at: Datetime` — bumped by the generated `update()` helper
     on every save.
   - `version: u64` — `0` on create, `+1` on every update. Enables
     optimistic concurrency, conflict detection, and deterministic
     ordering.
4. **`SCHEMAFULL`** on every resource table. No silent drift.
5. **`UNIQUE` constraints declared via `#[unique]`** on scalar fields.
   Allowed only on scalar types (`String`, `u64`, …). Compile error on
   `Vec<T>`, `Option<T>`, `HashMap`.
6. **Snake-case singular table names.** Enforced at macro-parse time.
7. **Scope is explicit.**
   - `#[resource(scope = "local")]` — per-node SurrealKV (today's `mesh`).
     Schema applied via `local_schemas()` on every node at boot.
   - `#[resource(scope = "cluster")]` — TiKV-shared cluster, replicated
     via Raft (today's `hypervisor`). Schema applied via
     `cluster_schemas()` on the bootstrap node only, per ADR 0004.
8. **No non-deterministic defaults on `cluster` resources.** The macro
   refuses `DEFAULT time::now()`, `DEFAULT rand::*`, and any expression
   that could evaluate differently on two state-machine apply paths.
   This single rule closes the Raft-determinism footgun for good.
9. **All CRUD goes through generated helpers.** `Resource::create`,
   `::update`, `::delete`, `::get`, `::list` — typed, single
   implementation, auto-bumps `updated_at`/`version`, emits lifecycle
   events through `instrument_op` (ADR 0005). Raw `db.query("CREATE
   {table} …")` against a known resource table is rejected by a CI grep
   check.

### Trait surface

```rust
// nauka_core::resource

pub enum Scope { Local, Cluster }

pub trait Resource:
    serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static
{
    const TABLE: &'static str;
    const SCOPE: Scope;
    const DDL: &'static str; // full DEFINE TABLE/FIELD/INDEX, macro-emitted

    type Id: std::fmt::Display + Send + Sync;
    fn id(&self) -> &Self::Id;
    fn created_at(&self) -> &surrealdb::types::Datetime;
    fn updated_at(&self) -> &surrealdb::types::Datetime;
    fn version(&self) -> u64;
}

pub struct ResourceDescriptor {
    pub table: &'static str,
    pub scope: Scope,
    pub ddl: &'static str,
}

#[linkme::distributed_slice]
pub static ALL_RESOURCES: [&'static ResourceDescriptor] = [..];

pub fn local_schemas() -> String;   // concat of all Scope::Local DDLs
pub fn cluster_schemas() -> String; // concat of all Scope::Cluster DDLs
```

### Enforcement

- **`trybuild` compile-fail tests** in `core-macros/tests/compile_fail/`
  cover every invariant: missing `#[id]`, malformed `#[resource]`,
  non-snake-case table, base-field collision, `DEFAULT time::now()` on
  cluster scope, `#[unique]` on `Vec`/`Option`, etc.
- **Integration test** in `nauka-core/tests/schema_roundtrip.rs`:
  apply `local_schemas() + cluster_schemas()` to a fresh embedded DB,
  run `INFO FOR DB`, assert every expected table/field/index is
  present.
- **CI grep check**: no `db.query("CREATE {table}` /
  `"UPDATE {table}` / `"DELETE {table}"` literals in `layers/*` or
  `bin/*` for known resource table names.

## Considered alternatives

### A — Hand-written `.surql` files + convention docs (status quo)

Rejected. Gives no protection against the three failure modes above.
Every new resource is a fresh opportunity to forget a `UNIQUE`
constraint, write a `DEFAULT time::now()`, or hand-roll a slightly
different `CREATE` statement.

### B — Hand-written `.surql` + `build.rs` validator

A build script parses every `.surql` and asserts required fields are
present. Rejected because:

- Two sources of truth (the struct + the DDL) — author still has to
  keep them in sync.
- `build.rs` hides the rule away from the call site; a new author
  encountering a violation reads "build script failed" without
  context.
- Doesn't help with the boilerplate-drift problem (CRUD is still
  hand-rolled).

### C — Runtime check at startup

The bootstrap path queries `INFO FOR DB` after applying schemas and
panics if anything is missing. Rejected because failure is detected at
runtime, not compile time, and the developer's edit-test loop is
slower (have to actually run the binary). Useful as a *complement* to
the compile-time check (covers schema drift in the DB), but not as
primary enforcement.

### D — Attribute macro instead of derive (`#[resource(…)] struct …`)

Considered. An attribute macro can rewrite the struct and *inject* the
base fields, so the author doesn't write `created_at: Datetime` etc.

Chosen, in fact. `#[derive(Resource)]` alone can only generate `impl`
blocks, not modify the struct, so the base fields would have to be
hand-declared and validated. The attribute-macro form
(`#[nauka_core::resource(table = "…", scope = "…")]` decorating the
struct) is the better ergonomic and is what ships.

## Consequences

- **New crate `nauka-core-macros`** at `core-macros/` — proc-macro,
  exports `#[resource]` attribute macro and (privately) the
  `Resource` derive plumbing.
- **`nauka-core` adds dependencies** on `linkme`, `serde`, and
  `surrealdb` (`Datetime` and types live there). nauka-core was
  previously dependency-light; the resource contract justifies the
  growth.
- **`layers/hypervisor/src/definition.surql` and
  `layers/hypervisor/src/mesh/definition.surql` are deleted**, along
  with `nauka_hypervisor::SCHEMA`. Schema text is no longer
  hand-maintained.
- **Every existing raw `CREATE/UPDATE/DELETE` against `hypervisor`
  or `mesh`** in `layers/hypervisor/src/{daemon,mesh/join,mesh/state}.rs`
  is rewritten through the generated helpers.
- **`bin/nauka` switches from `nauka_state::load_schemas(&db, &[…])`**
  to `db.query(&local_schemas()).await?` (and `cluster_schemas()` per
  ADR 0004 once the cluster path lands).
- **`layers/state/tests/raft_cluster.rs`** drops the hand-inlined
  `HYPERVISOR_SCHEMA` const and uses `cluster_schemas()`.
- **CI** gains a grep check that fails the build if a raw
  `CREATE/UPDATE/DELETE {known_table}` appears outside the generated
  CRUD path.

## References

- ADR 0004 — Schema deployment strategy: sifrah/nauka#210
- ADR 0005 — Logging contract: sifrah/nauka#333
- Issue #340 — Resource contract (this ADR)
- `instrument_op` (#334), `NaukaError` (#337) — used by generated CRUD
