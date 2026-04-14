-- ============================================================================
-- Nauka — cluster state schema: org
-- ============================================================================
-- Target: nauka / cluster (SurrealDB on top of kv-tikv)
--
-- This file defines the distributed cluster-state representation of the
-- top-level `Org` resource. Today every org is written through
-- `nauka_org::store::OrgStore` into raw TiKV via `ClusterDb` under the
-- `org` / `org-idx` namespaces. Phase 2 (sifrah/nauka#220) migrates that
-- storage onto SurrealDB with a `kv-tikv` backend. Phase 3 codegen
-- (sifrah/nauka#225 ff) will in turn generate the Rust `Org` type, the
-- store, and the API handlers directly from this file — P2.5
-- (sifrah/nauka#209) only ships the schema, no code generation yet.
--
-- Source of truth: `layers/org/src/types.rs` (`struct Org`), which embeds
-- `nauka_core::resource::ResourceMeta` via `#[serde(flatten)]`. The fields
-- below mirror the on-the-wire JSON shape produced by that struct minus
-- the `id` field (see the note at the end of this comment).
--
-- Idempotency: every DEFINE statement uses `IF NOT EXISTS` so applying
-- this file against an already-initialised cluster database is a no-op.
-- New columns added in later phases land as additional
-- `DEFINE FIELD … IF NOT EXISTS` lines without breaking existing data.
--
-- Multi-tenancy: `org` is the root of Nauka's tenant hierarchy — every
-- other cluster resource (project, env, vpc, subnet, vm, …) carries an
-- `org` reference and the per-tenant row-level isolation lives on those
-- child tables. The `org` table itself is the tenant registry, so there
-- is no parent scope to filter by here.
--
-- Foreign keys: P2.5 ships references as plain `string` record-id
-- fields (e.g. `"org:01j…"`) rather than SurrealDB native `record<T>`
-- links. P3 codegen may upgrade them to typed links once the Rust side
-- knows how to read/write both representations.
--
-- Note on the `id` field: `ResourceMeta` has an `id: String` field (the
-- ULID-prefixed resource id, e.g. `org-01J…`). SurrealDB reserves `id` as
-- the record-id slot and defining a separate `id` field collides with it.
-- P3 will persist the ULID as the SurrealDB record id itself — rows will
-- be created with `CREATE org:<ulid> SET name = ..., ...` — so the
-- acceptance criterion ("unique resource id") is still met, but via the
-- record id rather than a standalone field. The bootstrap schema
-- (`layers/state/schemas/bootstrap.surql`) uses the same convention for
-- the same reason.
-- ============================================================================


-- ── org ──────────────────────────────────────────────────────────────────
-- Top-level tenant / organisation. Every other cluster resource is owned
-- by exactly one org. Created via `nauka org create <name>` today; after
-- P3 codegen lands, rows will be created by the generated handler with
-- `CREATE org:<ulid> SET name = $name, status = "active", …`.

DEFINE TABLE IF NOT EXISTS org SCHEMAFULL;

-- Human-readable name. Unique across the whole cluster; enforced both by
-- the `org_name` index below and by the `OrgStore::create` path which
-- refuses to overwrite an existing name.
DEFINE FIELD IF NOT EXISTS name ON org TYPE string
    ASSERT string::len($value) > 0;

-- Resource lifecycle status (e.g. "active", "deleting"). Free-form today;
-- a stricter `$value INSIDE [...]` ASSERT gets added when the resource
-- lifecycle FSM lands.
DEFINE FIELD IF NOT EXISTS status ON org TYPE string
    ASSERT string::len($value) > 0;

-- User-defined key → value labels. Copied over from
-- `ResourceMeta.labels: HashMap<String, String>` — SurrealDB `object`
-- is the natural home for an open-ended string map. Defaults to an empty
-- object so new rows never carry a null here.
DEFINE FIELD IF NOT EXISTS labels ON org TYPE object DEFAULT {};

-- Creation timestamp. `ResourceMeta.created_at` is a Unix-epoch `u64`
-- today; P3 codegen will bridge it to SurrealDB's native `datetime` so
-- the schema-side type is correct from day one.
DEFINE FIELD IF NOT EXISTS created_at ON org TYPE datetime;

-- Last-update timestamp. Same story as `created_at`. Mutated on every
-- write-through the generated handler.
DEFINE FIELD IF NOT EXISTS updated_at ON org TYPE datetime;

-- Unique index on `name` so name → record-id lookups (the fast path for
-- the CLI, which takes `<org_name>` in every command) stay O(1) and so
-- the uniqueness check in `OrgStore::create` is enforced at the schema
-- level — no more "check, then insert" race windows once the migration
-- lands.
DEFINE INDEX IF NOT EXISTS org_name ON org FIELDS name UNIQUE;
