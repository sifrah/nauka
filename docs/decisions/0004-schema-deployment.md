# ADR 0004 — Schema deployment strategy

**Issue:** sifrah/nauka#210
**Epic:** sifrah/nauka#183
**Status:** Accepted
**Date:** 2026-04-13

## Context

P2.5 (sifrah/nauka#209) defines SCHEMAFULL `.surql` files for the cluster
resource types (`org`, `project`, `env`, `user`, `vpc`, `subnet`, `vm`, …).
They live under `layers/{org,network,compute}/schemas/` and target the
`(nauka, cluster)` SurrealDB database that runs on top of the shared TiKV
cluster, per ADR 0003 (sifrah/nauka#190).

Before any node can read or write a row of any of those tables, the schema
has to be applied to the database. The question this ADR answers is: **who
applies it, and when?**

There is a useful prior art in Nauka itself. The bootstrap schema
(`layers/state/schemas/bootstrap.surql`) is applied automatically by
`EmbeddedDb::open` on every node, on every boot, via `include_str!` +
`query`. It works fine because:

1. The bootstrap database is backed by **SurrealKV**, which is a per-node
   file (`/var/lib/nauka/bootstrap.skv`). N nodes = N independent files,
   so the "concurrent apply" question doesn't arise — each node only ever
   touches its own file.
2. Every `DEFINE` in the file uses `IF NOT EXISTS`, so re-running the
   script on an already-applied database is a no-op.

The cluster schemas are not the same situation. The TiKV cluster is
**shared** by every node in the mesh — there is exactly one
`(nauka, cluster)` database, and every node connects to it. Reapplying a
SCHEMAFULL schema concurrently from N nodes is technically idempotent (the
`IF NOT EXISTS` clauses gate it), but it has three real downsides:

1. **Wasteful**: every reboot of every node re-runs DDL against the
   cluster, even though nothing has changed. On a 10-node mesh that's 10×
   the SurrealQL parser work and 10× the round trips on each restart.
2. **Racey under partial partitions**: if two nodes apply the same schema
   at the same time and one of them sees a transient TiKV error mid-apply,
   the failure mode is hard to reason about — was it a real schema problem
   or a partition that healed?
3. **Future-hostile**: phase-3+ schema migrations may include data
   backfills (e.g. "rename a field, copy old values into the new field,
   drop the old field"). `IF NOT EXISTS` skips those silently if the
   target already exists, which would make a partial migration look
   successful when it isn't. The "every node applies on boot" model
   can't tell the difference.

We need to commit to who owns this step.

## Considered options

### Option A — Bootstrap node only

`nauka hypervisor init` (the **bootstrap node** that creates the cluster)
applies the schemas exactly once, after PD/TiKV come up and before the
storage region is written. Joining nodes (`nauka hypervisor join`) do
**not** apply them; they assume the cluster already has them and bail with
a clear error if it doesn't.

### Option B — Every node, idempotent at boot

Every node — bootstrap and joiner alike — applies every cluster schema on
every `EmbeddedDb<TiKv>::open`, the same way the bootstrap SurrealKV side
already does. `IF NOT EXISTS` is the only thing standing between us and
duplicate work.

### Option C — Manual `nauka schema apply` CLI

A separate operator-driven command (`nauka schema apply`) that the human
runs once, after `init`, against an already-bootstrapped cluster. Rejected
because Nauka's product position is "no manual setup steps after init" —
adding a mandatory follow-up command would break the single-command
bootstrap promise. It might still have a place as an *opt-in*
re-apply / migration tool in P4+ (see "Consequences" below), but it is
not the default deployment path.

## Decision

**Option A. The bootstrap node applies the cluster schemas during
`nauka hypervisor init`, exactly once, after PD/TiKV are healthy and
before the storage region is written. Joining nodes do not apply them.**

## Rationale

**For Option A:**

- **One source of truth for "what's in the cluster".** A single, ordered,
  log-visible apply event per cluster lifetime. An operator chasing a
  schema bug can point at the init log and say "schema X took effect
  here."
- **No race conditions on join.** Joining nodes don't touch the schema
  at all, so there is no concurrent-DDL window when a node comes up.
- **Future-proof against data backfills.** When a phase-3+ migration
  needs to re-shape existing rows, gating the migration step at the
  bootstrap node makes it safe by default — the migration runs once,
  in a known place, by a known actor.
- **Matches every cluster-management tool that ever existed.**
  Kubernetes CRDs are installed by the cluster admin once and read by
  every controller after that. Terraform state is written by `terraform
  apply` and consumed elsewhere. One party owns the schema, others
  read it.
- **Failure mode is loud and early.** If the schema apply fails at init,
  `init` itself fails. The operator sees the error before any state
  exists to corrupt, and the existing init rollback (P1.x) tears down
  the half-built control plane.

**Against Option A (and mitigations):**

- *The bootstrap node is special — if it dies before applying schemas the
  cluster is half-initialised.* Mitigation: the init handler already
  rolls back the control-plane install on any failure, so a half-init
  state is cleaned up rather than left dangling.
- *Adding a new schema later requires a re-init or a follow-up.*
  Mitigation: P4+ adds a proper schema migration command. For Phase 2
  the schema set is small and rarely changes.

**For Option B:**

- Self-healing: a node joining a cluster with a stale schema would
  auto-upgrade as a side effect of opening the database.
- Simpler to reason about per-node — every node does the same thing.

**Against Option B:**

- N concurrent applies on every reboot — wasteful and racey under
  partial partitions.
- `IF NOT EXISTS` is the *only* duplication guard; future migrations that
  need data backfill have to be re-engineered around that constraint.
- Hides the moment the schema took effect — operators can't point at a
  single event in the logs.

**Against Option C:** breaks the "one command to bootstrap a cluster"
promise.

## Consequences

- `controlplane::ops::bootstrap()` (`layers/hypervisor/src/controlplane/ops.rs`)
  gains a step that connects to the freshly-started TiKV via
  `EmbeddedDb::open_tikv_from_fabric` (P2.3, sifrah/nauka#207) and applies
  each `.surql` file from `layers/{org,network,compute}/schemas/` via
  `client.query(SCHEMA).await?.check()?`. The step runs after
  `wait_tikv_ready` and before the first storage region is written. The
  actual implementation is **out of scope for this ADR** — P2.7
  (sifrah/nauka#211) wires it up.
- A new helper, e.g.
  `nauka_state::schema::apply_cluster_schemas(db: &EmbeddedDb) -> Result<()>`,
  walks the schema files at compile time via `include_str!` so the binary
  is self-contained.
- Joining nodes (`nauka hypervisor join`) do **not** apply schemas. They
  run a quick `INFO FOR DB` sanity check via the `kv-tikv` `EmbeddedDb`
  and bail with a clear error if the schema is missing. Same handler
  ticket: P2.7.
- Adding a new schema in the future requires either (a) a re-init in
  test environments (cheap; the per-ticket workflow already does this on
  every change), or (b) a one-shot `nauka schema apply` command (P4+),
  whose design is deferred until an actual schema migration needs it.
- The bootstrap-side schema (`layers/state/schemas/bootstrap.surql`)
  continues to be applied on every `EmbeddedDb::open` — that is correct
  for SurrealKV (per-node file) and is **not** what this ADR is changing.
  The "apply once" rule applies only to the TiKV-shared cluster schemas.

## References

- ADR 0001 — TiKV API version: sifrah/nauka#188
- ADR 0002 — TiKV migration strategy: sifrah/nauka#189
- ADR 0003 — SurrealDB namespace strategy: sifrah/nauka#190
- P2.5 — Define cluster `.surql` schemas: sifrah/nauka#209
- P2.7 — Implement schema deployment to the cluster (the wire-up that
  follows this ADR): sifrah/nauka#211
- Epic — SurrealDB migration: sifrah/nauka#183
