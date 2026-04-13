-- ============================================================================
-- Nauka — cluster state schema: env
-- ============================================================================
-- Target: nauka / cluster (SurrealDB on top of kv-tikv)
--
-- Distributed cluster-state representation of an `Environment` — the
-- third level of Nauka's tenant hierarchy (`org → project → env`).
-- Today envs are written through
-- `nauka_org::project::env::store::EnvStore` into raw TiKV via
-- `ClusterDb`; Phase 2 (sifrah/nauka#220) migrates the storage onto
-- SurrealDB, and Phase 3 codegen (sifrah/nauka#225 ff) will generate
-- the Rust type / store / handlers from this file.
--
-- Source of truth: `layers/org/src/project/env/types.rs`
-- (`struct Environment`), which embeds `ResourceMeta` via
-- `#[serde(flatten)]` and carries the full owning path:
-- `project_id`, `project_name`, `org_id`, `org_name`.
--
-- Idempotency: every DEFINE statement uses `IF NOT EXISTS` so re-applying
-- the file against an initialised cluster database is a no-op.
--
-- Multi-tenancy: `env` is tenant-scoped. The primary natural key is
-- `(project, name)` — an env name is unique within a project, not within
-- an org. The `org` column is denormalised for fast row-level authz
-- filtering ("all envs visible to tenant X") without a second JOIN.
--
-- Foreign keys: `project` and `org` are both plain `string` record ids
-- for now. See `org.surql` for the rationale — P3 codegen may upgrade
-- them to typed `record<project>` / `record<org>` links.
-- ============================================================================


-- ── env ──────────────────────────────────────────────────────────────────
-- An environment belongs to exactly one project (and transitively to
-- exactly one org). The `(project, name)` tuple is the natural key.
-- Created today via `nauka env create <name> --project <proj> --org <org>`.

DEFINE TABLE IF NOT EXISTS env SCHEMAFULL;

-- Human-readable name. Unique per project.
DEFINE FIELD IF NOT EXISTS name ON env TYPE string
    ASSERT string::len($value) > 0;

-- Resource lifecycle status.
DEFINE FIELD IF NOT EXISTS status ON env TYPE string
    ASSERT string::len($value) > 0;

-- User-defined labels.
DEFINE FIELD IF NOT EXISTS labels ON env TYPE object DEFAULT {};

-- Owning project — SurrealDB record id of the `project` row (e.g.
-- `project:01j…`), stored as a string for P2.5.
DEFINE FIELD IF NOT EXISTS project ON env TYPE string
    ASSERT string::len($value) > 0;

-- Denormalised project name from `Environment.project_name`.
DEFINE FIELD IF NOT EXISTS project_name ON env TYPE string
    ASSERT string::len($value) > 0;

-- Owning organisation — SurrealDB record id of the `org` row. Carried
-- on every env row so tenant-scoped list queries can filter without
-- walking up through the project table.
DEFINE FIELD IF NOT EXISTS org ON env TYPE string
    ASSERT string::len($value) > 0;

-- Denormalised org name from `Environment.org_name`.
DEFINE FIELD IF NOT EXISTS org_name ON env TYPE string
    ASSERT string::len($value) > 0;

-- Creation timestamp. See the note in `org.surql`.
DEFINE FIELD IF NOT EXISTS created_at ON env TYPE datetime;

-- Last-update timestamp.
DEFINE FIELD IF NOT EXISTS updated_at ON env TYPE datetime;

-- Composite unique index on `(project, name)`. Mirrors the
-- `project_id/name` index key used by today's TiKV-backed
-- `EnvStore::create`.
DEFINE INDEX IF NOT EXISTS env_project_name ON env
    FIELDS project, name UNIQUE;

-- Secondary (non-unique) index on `project` for "list envs in this
-- project" scans.
DEFINE INDEX IF NOT EXISTS env_project ON env FIELDS project;

-- Secondary (non-unique) index on `org` for tenant-scoped "list every
-- env belonging to this tenant" scans.
DEFINE INDEX IF NOT EXISTS env_org ON env FIELDS org;
