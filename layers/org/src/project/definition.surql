-- ============================================================================
-- Nauka — cluster state schema: project
-- ============================================================================
-- Target: nauka / cluster (SurrealDB on top of kv-tikv)
--
-- Distributed cluster-state representation of a `Project` — the second
-- level of Nauka's tenant hierarchy (`org → project → env`). Today
-- projects are written through `nauka_org::project::store::ProjectStore`
-- into raw TiKV via `ClusterDb`; Phase 2 (sifrah/nauka#220) migrates the
-- storage onto SurrealDB, and Phase 3 codegen (sifrah/nauka#225 ff)
-- will generate the Rust type / store / handlers from this file.
--
-- Source of truth: `layers/org/src/project/types.rs` (`struct Project`),
-- which embeds `ResourceMeta` via `#[serde(flatten)]` and carries an
-- owning `org_id` / `org_name` pair.
--
-- Idempotency: every DEFINE statement uses `IF NOT EXISTS` so this file
-- can be re-applied against an initialised cluster database as a no-op.
--
-- Multi-tenancy: `project` is tenant-scoped. Every row carries the id of
-- its owning `org`, and the natural key is `(org, name)` rather than
-- `name` alone — two different orgs may both have a project called
-- `web`. Row-level authorisation (P3+) filters by the `org` field.
--
-- Foreign keys: `org` is a plain `string` holding a SurrealDB record id
-- like `org:01j…`. See `org.surql` for why P2.5 deliberately does not
-- use SurrealDB's typed `record<org>` links yet.
-- ============================================================================


-- ── project ──────────────────────────────────────────────────────────────
-- A project belongs to exactly one org. The `(org, name)` tuple is the
-- natural key. Created today via `nauka project create <name> --org <org>`
-- and looked up by the same pair.

DEFINE TABLE IF NOT EXISTS project SCHEMAFULL;

-- Human-readable name. Unique per org, not globally — the composite
-- `project_org_name` index below enforces that.
DEFINE FIELD IF NOT EXISTS name ON project TYPE string
    ASSERT string::len($value) > 0;

-- Resource lifecycle status. Same semantics as on `org`; free-form today.
DEFINE FIELD IF NOT EXISTS status ON project TYPE string
    ASSERT string::len($value) > 0;

-- User-defined key → value labels. Defaults to `{}` so the column is
-- never null in practice.
DEFINE FIELD IF NOT EXISTS labels ON project TYPE object DEFAULT {};

-- Owning organisation — SurrealDB record id of the `org` row, stored as
-- a string for now (P3 codegen may upgrade to `record<org>`). Tenant
-- scoping and the `(org, name)` composite index both read this column.
DEFINE FIELD IF NOT EXISTS org ON project TYPE string
    ASSERT string::len($value) > 0;

-- Denormalised org name copied from the parent at write time. Present in
-- today's `Project` struct (`org_name: String`) because the CLI surface
-- wants a human-readable org name without a second fetch. Kept here so
-- the schema round-trips the existing JSON exactly.
DEFINE FIELD IF NOT EXISTS org_name ON project TYPE string
    ASSERT string::len($value) > 0;

-- Creation timestamp. See the `created_at` note in `org.surql`.
DEFINE FIELD IF NOT EXISTS created_at ON project TYPE datetime;

-- Last-update timestamp.
DEFINE FIELD IF NOT EXISTS updated_at ON project TYPE datetime;

-- Composite unique index on `(org, name)`: two different orgs may use
-- the same project name, but within a single org the name is unique.
-- Mirrors the `org_id/name` index key used by today's TiKV-backed
-- `ProjectStore::create`.
DEFINE INDEX IF NOT EXISTS project_org_name ON project
    FIELDS org, name UNIQUE;

-- Secondary (non-unique) index on `org` for fast "list all projects in
-- this org" scans — the most common read pattern.
DEFINE INDEX IF NOT EXISTS project_org ON project FIELDS org;
