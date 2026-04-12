# ADR 0003 — SurrealDB namespace / database / multi-tenancy strategy

**Issue:** sifrah/nauka#190
**Epic:** sifrah/nauka#183
**Status:** Accepted
**Date:** 2026-04-12

## Context

SurrealDB organises data in a three-level hierarchy:

```
Namespace (NS) → Database (DB) → Tables → Records
```

A `Namespace` is the top-level container. A `Database` lives inside one
namespace. Tables, records, schemas, indexes, events, accesses (`DEFINE
ACCESS`), users, and field-level permissions are all scoped to a specific
`(NS, DB)` pair, with two exceptions:

- `DEFINE ACCESS … ON NAMESPACE` defines auth that's valid across every
  database in that namespace
- A few admin-level operations (e.g. listing namespaces) require root-level
  auth and traverse all namespaces

Switching the active scope is what `db.use_ns("...").use_db("...")` does on
the SDK side. A given `Surreal<Db>` client can hold one active `(NS, DB)`
pair at a time and must `use_*` again to switch.

For the SurrealDB migration epic (sifrah/nauka#183) we need to commit to a
naming convention for these levels in two distinct contexts:

1. **`EmbeddedDb<SurrealKv>`** — local file-backed instance per node, used
   for bootstrap state (replaces `LocalDb`, P1.x). Scoped to **one node**.
2. **`EmbeddedDb<TiKv>`** — distributed instance backed by the Nauka TiKV
   cluster, used for cluster state (replaces `ClusterDb`, P2.x). Scoped to
   **the entire mesh**.

We also need to decide where the **multi-tenancy boundary** between Nauka
orgs goes: at the SurrealDB namespace level (one NS per org), at the
database level (one DB per org inside one NS), or at the table-row level
(one shared NS/DB with an `org` field on every record enforced by
`DEFINE TABLE … PERMISSIONS`).

These decisions interact: schemas live at the DB level, so the choice of
where the org boundary goes determines whether schemas are shared or
duplicated across tenants.

## Decisions

The full rationale is below. The TL;DR:

| Concern | Decision |
|---|---|
| Local SurrealKV path | `nauka` namespace, `bootstrap` database |
| Distributed TiKV path | `nauka` namespace, `cluster` database |
| Multi-tenancy boundary | **Single shared `(nauka, cluster)` + `org` field on every tenant-scoped table, enforced by `DEFINE TABLE … PERMISSIONS`** |
| Per-tenant data isolation | Row-level permissions (deferred details: P4.3, sifrah/nauka#235) |
| Reserved namespace identifiers | `nauka` (the only namespace Nauka writes to). Future: `nauka_admin` if/when we need cross-tenant ops. Anything else is foreign and should be left alone. |
| Reserved database identifiers (within `nauka` ns) | `bootstrap`, `cluster`. Future: `audit`, `metrics` if we ever store those in SurrealDB. |
| Where `DEFINE ACCESS` lives | `ON DATABASE` (one access definition per `(ns, db)`), not `ON NAMESPACE`. Lets us evolve auth independently for bootstrap vs cluster if needed. (P4.5, sifrah/nauka#237.) |

## The two `EmbeddedDb` instances

### `EmbeddedDb<SurrealKv>` — bootstrap state

- **Backend**: `surrealdb::engine::local::SurrealKv`
- **Location**: `~/.nauka/bootstrap.skv` in CLI mode, `/var/lib/nauka/bootstrap.skv`
  in service mode (per ADR cross-reference: P1.4, sifrah/nauka#194)
- **Scope**: this node only — never replicated
- **NS / DB**: `nauka` / `bootstrap`
- **Tables**: `mesh`, `hypervisor`, `peer`, `wg_key` (defined by P1.6,
  sifrah/nauka#196)

What goes in here is everything that must exist **before** the cluster TiKV
is reachable: WireGuard mesh identity, this node's hypervisor identity, the
list of peer endpoints, the WireGuard private key. It is the moral
equivalent of the `~/.nauka/{layer}.json` files that `LocalDb` writes today.

It does **not** hold any tenant data, any org, or any cluster-shared state.
There is therefore no multi-tenancy concern at this level — the bootstrap
DB is single-tenant by definition (the node owns it).

### `EmbeddedDb<TiKv>` — cluster state

- **Backend**: `surrealdb::engine::local::TiKv`
- **Location**: PD endpoints discovered from `FabricState` (per P2.3,
  sifrah/nauka#207)
- **Scope**: the entire mesh — every node sees the same data
- **NS / DB**: `nauka` / `cluster`
- **Tables**: `org`, `project`, `env`, `vpc`, `subnet`, `vm`, `vpcpeer`,
  `natgw`, `ipam_alloc`, plus whatever Phase 3 schemas land (`.surql` files,
  P3, sifrah/nauka#225+)

What goes in here is everything that today lives in `ClusterDb`'s 9 raw
namespaces (per the inventory in ADR 0002, sifrah/nauka#189), reshaped as
SurrealDB schemafull tables.

This is where the multi-tenancy decision applies.

## Multi-tenancy: the three options

The SurrealDB primitives admit three reasonable ways to partition data
between Nauka orgs:

### Option 1 — One namespace per org

```text
nauka_admin / cluster   (control / billing)
acme        / cluster
globex      / cluster
initech     / cluster
```

- **Hard isolation**: SurrealDB enforces NS-level isolation natively. A
  query against `acme` cannot see `globex` records, period.
- **Schemas duplicated** across every namespace. Adding a field to `vm`
  means re-applying the schema in every NS.
- **Cross-tenant queries** (admin views, billing aggregation, "list all
  VMs across orgs") require fan-out: open one connection per namespace,
  query, merge.
- **Org provisioning is heavyweight**: `CREATE org` ⇒ `DEFINE NAMESPACE` +
  `DEFINE DATABASE` + apply every schema + define every access rule.
- **Auth model**: `DEFINE ACCESS … ON NAMESPACE` per org. A user's auth
  scope is tied to their org's NS.

### Option 2 — One database per org (within a single namespace)

```text
nauka / admin    (control / billing)
nauka / acme
nauka / globex
nauka / initech
```

- **Hard isolation** at the DB level (still SurrealDB-enforced, just one
  level lower than Option 1).
- **Schemas still duplicated** per database — `DEFINE TABLE` is db-scoped,
  so adding a field to `vm` still requires re-applying the schema in
  every db.
- **Cross-tenant queries** are still fan-out, but the fan-out is over
  databases inside one namespace instead of over namespaces. Slightly less
  awkward — `INFO FOR NS nauka` lists the databases.
- **Org provisioning is medium**: `CREATE org` ⇒ `DEFINE DATABASE` +
  re-apply every schema. No new access definition needed if access is at
  the namespace level.
- **Auth model**: `DEFINE ACCESS … ON NAMESPACE` once, with the user's
  database tied to their org via session vars.

### Option 3 — Single shared database, `org` field per row

```text
nauka / cluster
        ├── org
        │     ├── acme    (id, billing_email, ...)
        │     ├── globex
        │     └── initech
        ├── vm
        │     ├── vm-001  (org="acme",   ...)
        │     ├── vm-002  (org="acme",   ...)
        │     └── vm-003  (org="globex", ...)
        ├── vpc
        │     ├── vpc-1   (org="acme",   ...)
        │     ├── vpc-2   (org="globex", ...)
        │     └── ...
        └── ...
```

- **Soft isolation** enforced by `DEFINE TABLE … PERMISSIONS FOR select
  WHERE org = $auth.org` (and similar for write/delete). The DB itself
  is one shared namespace; the boundary is a query predicate that
  SurrealDB applies automatically based on the authenticated user's
  org claim.
- **Schemas defined exactly once** — adding a field to `vm` is one
  `ALTER TABLE` (or, in SurrealQL, an updated `DEFINE FIELD` block).
- **Cross-tenant queries are normal**. An admin query just doesn't
  apply the org filter, or applies it with `OR $auth.role = 'admin'`.
- **Org provisioning is trivial**: `CREATE org SET name = '...'`. A new
  row in the `org` table. Done.
- **Auth model**: `DEFINE ACCESS user ON DATABASE` with `$auth.org`
  pulled from the JWT or session record.
- **Risk**: a permissions bug = a cross-tenant data leak. The boundary
  is a query predicate, not a storage boundary, so it relies on the
  enforcement working correctly on every query path.

## Decision: Option 3

**Nauka uses a single `(nauka, cluster)` SurrealDB database for all org
data, with row-level isolation enforced by `DEFINE TABLE … PERMISSIONS`.**

For symmetry and simplicity, the bootstrap-side `(nauka, bootstrap)` is
also single-tenant by construction (one node = one bootstrap DB).

## Rationale

1. **Nauka is a control plane, not a multi-tenant SaaS database.** The
   tenant count is tens, not thousands. The cross-tenant query patterns
   (admin views, billing aggregation, ownership transfer, "list every VM
   in this cluster") are normal everyday operations, not rare exceptions.
   Fan-out over a namespace per tenant turns those everyday operations
   into per-org loops.

2. **Schemas are uniform across orgs.** A VM in org A and a VM in org B
   have exactly the same shape, the same validation rules, the same
   indexes, and the same lifecycle hooks. Duplicating the schema in
   every namespace (Option 1) or every database (Option 2) just creates
   N copies of the same definition that have to be kept in sync. Phase
   3 (sifrah/nauka#223 ff) ships `.surql` files; with Option 3 there is
   one canonical `vm.surql` and one DEFINE TABLE call against the
   shared database.

3. **Org provisioning becomes a row insert.** `CREATE org SET name = ...`
   is one statement, no DDL, no schema replay, no namespace creation
   ritual. Org deletion is `DELETE org WHERE id = ...` plus a cascade
   on owned resources. An entire tenant lifecycle can be expressed as
   ordinary CRUD against the shared DB, which makes it scriptable, easy
   to audit, and idempotent.

4. **SurrealDB row-level permissions are a real first-class feature**, not
   a userspace bolt-on. `DEFINE TABLE vm PERMISSIONS FOR select WHERE
   org = $auth.org` is checked by the engine on every query, including
   subqueries and graph traversals. The risk surface is "is the
   `PERMISSIONS` clause correct?", which is one place to audit per
   table — much smaller than "is every store call site doing the
   right namespace filtering?". P4.3 (sifrah/nauka#235) is the issue
   that lands these permissions.

5. **Hard isolation is not a current Nauka requirement.** Nauka does not
   yet ship to mutually-untrusting tenants; it ships to a single
   organisation that runs it on its own infrastructure for its own
   internal use. The multi-tenancy story is *organisational* (multiple
   teams within one company, each with their own VMs / VPCs) rather
   than *adversarial* (multiple paying customers who must not see each
   other's bytes under any circumstances). When Nauka eventually does
   need adversarial multi-tenancy (e.g. SaaS hosted Nauka), we can
   re-evaluate — see "Migration paths" below.

6. **Backups remain per-org-tractable.** SurrealDB's `EXPORT` supports
   per-table and `WHERE` filters, so a per-org backup is `EXPORT FOR
   table WHERE org = 'acme'`. This is more work than a per-namespace
   backup but it's not a blocker, and it matches the realistic
   per-org-restore use case (you almost never want to wholesale-restore
   one tenant on top of an active cluster anyway — you want to extract
   specific records to a sandbox).

## Trade-offs accepted

1. **The org boundary is a query predicate, not a storage boundary.** A
   bug in `DEFINE TABLE … PERMISSIONS`, or a code path that uses a
   root-level session and forgets to filter, can leak across tenants.
   Mitigation: every store-touching code path ships under tests that
   set `$auth.org` to a specific org and assert that records from other
   orgs are invisible. This becomes part of the P4.3 acceptance
   criteria.

2. **No per-tenant SurrealDB-level encryption / quotas.** SurrealDB does
   not (today) have per-namespace quotas, per-namespace encryption keys,
   or per-namespace IO accounting. With Option 3 we don't get those
   either, because everything lives in one DB. With Option 1 we
   *could* (eventually) layer them per-NS. We accept this trade-off
   because (a) Nauka doesn't need them today, and (b) the underlying
   storage (TiKV) doesn't really expose those primitives in the
   single-cluster topology either.

3. **Cross-tenant queries work — that is itself a foot-gun.** It is now
   *possible* for an authorised admin to write a query that touches
   every tenant. With per-NS or per-DB isolation, that operation
   requires explicit fan-out. We accept this because (a) the operation
   is normal for Nauka (billing, capacity reporting), (b) it can be
   gated behind `$auth.role = 'admin'`, and (c) the audit story is
   "admin actions are logged in an `audit` table" — orthogonal to the
   isolation model.

4. **An eventual switch to Option 1 / 2 is a real migration**, not a
   config flag. See "Migration paths" below for what it would cost.

## Implications for code

### Connection setup

```rust
// nauka-state, src/embedded.rs (P1.2 + P2.2)
impl EmbeddedDb {
    pub async fn open_local(path: &Path) -> Result<Self> {
        let db = Surreal::new::<SurrealKv>(path.to_str().unwrap()).await?;
        db.use_ns("nauka").use_db("bootstrap").await?;
        // ... apply bootstrap schema (P1.7) ...
        Ok(Self { inner: db, ... })
    }

    pub async fn open_tikv(pd: &str) -> Result<Self> {
        let db = Surreal::new::<TiKv>(pd).await?;
        db.use_ns("nauka").use_db("cluster").await?;
        // ... apply cluster schema (P2.7) ...
        Ok(Self { inner: db, ... })
    }
}
```

The literal strings `"nauka"`, `"bootstrap"`, and `"cluster"` should be
defined as `pub const` in `nauka-state` so that:

```rust
// layers/state/src/lib.rs
pub const NAUKA_NS: &str = "nauka";
pub const BOOTSTRAP_DB: &str = "bootstrap";
pub const CLUSTER_DB: &str = "cluster";
```

and every call site uses the constants.

### Schema files

`.surql` schema files (Phase 3, sifrah/nauka#223 ff) should each carry an
explicit header that documents which `(ns, db)` they target:

```surql
-- layers/state/schemas/bootstrap.surql
-- Target: nauka / bootstrap (EmbeddedDb<SurrealKv>)

DEFINE TABLE mesh SCHEMAFULL;
-- ...
```

```surql
-- layers/org/schemas/org.surql
-- Target: nauka / cluster (EmbeddedDb<TiKv>)

DEFINE TABLE org SCHEMAFULL
    PERMISSIONS
        FOR select WHERE id = $auth.org OR $auth.role = 'admin'
        FOR create, update, delete WHERE $auth.role = 'admin';

DEFINE FIELD name ON org TYPE string ASSERT string::len($value) > 0;
DEFINE FIELD billing_email ON org TYPE option<string>;
-- ...
DEFINE INDEX org_name ON org FIELDS name UNIQUE;
```

The exact `PERMISSIONS` shape on each table is owned by P4.3
(sifrah/nauka#235), not this ADR — but the point is that with Option 3,
**every tenant-scoped table has a `PERMISSIONS` clause**, and the absence
of one on a tenant-scoped table is a review-blocking defect.

### Reserved tables on the cluster side

Within the `cluster` database, the table `org` is special: it is the
**registry of tenants**. Its `PERMISSIONS` clause is unusual because the
"who can read this org row" question is itself the foundation of the
isolation system. The proposed shape (subject to P4.3 refinement):

```surql
DEFINE TABLE org SCHEMAFULL
    PERMISSIONS
        FOR select WHERE id = $auth.org OR $auth.role = 'admin'
        FOR create, update, delete WHERE $auth.role = 'admin';
```

I.e., only the admin role can create/delete orgs; only the row's own org
or an admin can see it.

## Migration paths

If we ever need to switch off Option 3, here are the paths in increasing
order of cost:

### Switching from Option 3 → Option 2 (DB per org)

```text
1. Stand up the new DB topology:
     for each org row, DEFINE DATABASE {org_id} (idempotent)
     re-apply every schema in every new database
2. Replay records:
     for each tenant-scoped table (vm, vpc, subnet, ...):
         for each record where org = X:
             move to nauka/{X}/<table>
3. Update Nauka code:
     use_db("acme")  ⇐  use_db("cluster")
     drop the org-field filter on tenant-scoped tables
     update DEFINE ACCESS / DEFINE USER scopes to NAMESPACE-level
4. Wipe the old shared cluster database
```

Cost: a one-shot migration script (~300–500 LoC), comparable to the
"export-replay" tool from ADR 0002 but easier because both source and
destination speak SurrealQL.

### Switching from Option 3 → Option 1 (NS per org)

Same as above but with `DEFINE NAMESPACE` instead of `DEFINE DATABASE`.
One additional gotcha: namespace switching breaks the `Surreal<Db>` client's
single-active-(ns,db) model — every cross-tenant operation has to either
re-`use_ns` (cheap) or fan out concurrent connections.

Cost: same script, plus invasive changes to every store call site to
deal with the per-org connection model. Realistically a ~2-week project.

## When to revisit this decision

Re-open this ADR (or write a successor) if any of the following happens:

1. **Nauka starts shipping to mutually-untrusting tenants** — i.e., a
   SaaS hosted Nauka where the tenants are paying customers. At that
   point row-level permissions become a regulatory question (SOC 2,
   HIPAA, etc.) and the cost of switching to NS-per-org goes from
   "engineering work" to "compliance prerequisite".

2. **A row-level permissions bug ever leaks tenant data in production.**
   One incident is enough to re-justify the cost of hard isolation.

3. **Per-tenant resource quotas (CPU, IOPS, storage GB) become a
   product requirement** that can be enforced at the storage layer.
   Today's TiKV API V1 (per ADR 0001) doesn't expose those primitives
   per keyspace, but a future TiKV / SurrealDB combo might.

4. **An external user installs Nauka** and asks for "I want to host
   multiple customers on this one Nauka deployment, with hard
   isolation guarantees". At that point Option 3 stops being adequate.

5. **Phase 4.4 (sifrah/nauka#236 — Multi-tenancy via SurrealDB
   namespaces)** explicitly re-asks the question once we've shipped
   the row-level approach and have real operational experience with
   it. That issue should either explicitly defer to this ADR (if
   Option 3 is still right) or supersede it (if we've decided to
   move to Option 1 / 2).

## References

- SurrealQL `DEFINE NAMESPACE` / `DEFINE DATABASE`: see
  `surrealdb/core/src/dbs/executor.rs` and `surrealdb/core/src/kvs/ds.rs`
  in [surrealdb/surrealdb v3.0.5](https://github.com/surrealdb/surrealdb/blob/v3.0.5)
- `DEFINE ACCESS … ON NAMESPACE | DATABASE`: see
  `surrealdb/core/src/key/{namespace,database}/ac.rs`
- ADR 0001 — TiKV API version: sifrah/nauka#188
- ADR 0002 — TiKV migration strategy: sifrah/nauka#189
- P1.6 — bootstrap `.surql` schema: sifrah/nauka#196
- P2.5 — cluster `.surql` schemas: sifrah/nauka#209
- P4.3 — Row-level permissions per resource: sifrah/nauka#235
- P4.4 — Multi-tenancy via SurrealDB namespaces: sifrah/nauka#236
- P4.5 — IAM via `DEFINE ACCESS user`: sifrah/nauka#237
