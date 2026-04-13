-- ============================================================================
-- Nauka — cluster state schema: vpc
-- ============================================================================
-- Target: nauka / cluster (SurrealDB on top of kv-tikv)
--
-- Distributed cluster-state representation of a `Vpc` — the top-level
-- networking resource. Today VPCs are written through
-- `nauka_network::vpc::store::VpcStore` into raw TiKV via `ClusterDb`
-- under the `vpc` / `vpc-idx` namespaces. Phase 2 (sifrah/nauka#220)
-- migrates the storage onto SurrealDB; Phase 3 codegen
-- (sifrah/nauka#225 ff) will generate the Rust type / store / handlers
-- from this file.
--
-- Source of truth: `layers/network/src/vpc/types.rs` (`struct Vpc`),
-- which embeds `ResourceMeta` via `#[serde(flatten)]` and carries
-- `cidr`, `vni`, the owning `org_id` / `org_name`, and optional
-- `project_id` / `env_id` fields.
--
-- Idempotency: every DEFINE statement uses `IF NOT EXISTS` so re-applying
-- the file against an initialised cluster database is a no-op.
--
-- Multi-tenancy: `vpc` is tenant-scoped on `(org, name)`. A VPC may
-- optionally also be scoped to a specific project or environment
-- within the owning org — those fields are `option<string>` because
-- today's `Vpc` struct marks them `Option<ProjectId>` / `Option<EnvId>`.
--
-- Foreign keys: `org`, `project`, `env` are plain `string` record ids
-- for now. See `org.surql` for the rationale — P3 codegen may upgrade
-- them to typed `record<…>` links.
-- ============================================================================


-- ── vpc ──────────────────────────────────────────────────────────────────
-- A Virtual Private Cloud. Every VPC belongs to exactly one org, and
-- the pair `(org, name)` is the natural key. Created today via
-- `nauka vpc create <name> --org <org> --cidr <cidr>`.

DEFINE TABLE IF NOT EXISTS vpc SCHEMAFULL;

-- Human-readable name. Unique per org.
DEFINE FIELD IF NOT EXISTS name ON vpc TYPE string
    ASSERT string::len($value) > 0;

-- Resource lifecycle status.
DEFINE FIELD IF NOT EXISTS status ON vpc TYPE string
    ASSERT string::len($value) > 0;

-- User-defined labels.
DEFINE FIELD IF NOT EXISTS labels ON vpc TYPE object DEFAULT {};

-- IPv4 CIDR for the VPC's private address space, as a string like
-- "10.0.0.0/16". Format validation lives in the application layer
-- (`nauka_network::validate::private_cidr`) — the schema just asserts
-- non-empty so obviously-wrong inserts are rejected.
DEFINE FIELD IF NOT EXISTS cidr ON vpc TYPE string
    ASSERT string::len($value) > 0;

-- VXLAN Network Identifier (VNI). Allocated monotonically by the
-- `VpcStore::next_vni` counter in today's store, starting at 100. The
-- VNI is a u24 in hardware (valid range 1..=2^24-1); the ASSERT
-- enforces positivity and the u24 ceiling so nonsensical values never
-- reach the data plane.
DEFINE FIELD IF NOT EXISTS vni ON vpc TYPE int
    ASSERT $value > 0 AND $value < 16777216;

-- Owning organisation — SurrealDB record id of the `org` row. Tenant
-- scoping and the `(org, name)` composite index both read this column.
DEFINE FIELD IF NOT EXISTS org ON vpc TYPE string
    ASSERT string::len($value) > 0;

-- Denormalised org name from `Vpc.org_name`.
DEFINE FIELD IF NOT EXISTS org_name ON vpc TYPE string
    ASSERT string::len($value) > 0;

-- Optional owning project. Present in `Vpc` as `project_id:
-- Option<ProjectId>` so some VPCs are org-wide and some are pinned to a
-- single project. Modelled as `option<string>` here; P3 codegen may
-- upgrade to `option<record<project>>`.
DEFINE FIELD IF NOT EXISTS project ON vpc TYPE option<string>;

-- Optional owning environment — same story as `project` above.
DEFINE FIELD IF NOT EXISTS env ON vpc TYPE option<string>;

-- Creation timestamp.
DEFINE FIELD IF NOT EXISTS created_at ON vpc TYPE datetime;

-- Last-update timestamp.
DEFINE FIELD IF NOT EXISTS updated_at ON vpc TYPE datetime;

-- Composite unique index on `(org, name)`. Mirrors the `org_id/name`
-- index key used by today's TiKV-backed `VpcStore::create`.
DEFINE INDEX IF NOT EXISTS vpc_org_name ON vpc
    FIELDS org, name UNIQUE;

-- VNI must be unique cluster-wide — the monotonic counter enforces
-- this at allocation time, but modelling the uniqueness in the schema
-- guards against future races (parallel allocators, backfill scripts,
-- …) and lets the data plane trust the invariant.
DEFINE INDEX IF NOT EXISTS vpc_vni ON vpc FIELDS vni UNIQUE;

-- Secondary (non-unique) index on `org` for tenant-scoped list scans.
DEFINE INDEX IF NOT EXISTS vpc_org ON vpc FIELDS org;
