-- ============================================================================
-- Nauka — cluster state schema: subnet
-- ============================================================================
-- Target: nauka / cluster (SurrealDB on top of kv-tikv)
--
-- Distributed cluster-state representation of a `Subnet` — a contiguous
-- CIDR block carved out of an owning VPC's address space. Today
-- subnets are written through
-- `nauka_network::vpc::subnet::store::SubnetStore` into raw TiKV via
-- `ClusterDb`; Phase 2 (sifrah/nauka#220) migrates the storage onto
-- SurrealDB; Phase 3 codegen (sifrah/nauka#225 ff) will generate the
-- Rust type / store / handlers from this file.
--
-- Source of truth: `layers/network/src/vpc/subnet/types.rs`
-- (`struct Subnet`), which embeds `ResourceMeta` via `#[serde(flatten)]`
-- and carries `cidr`, `gateway`, and the owning `vpc_id` / `vpc_name`
-- pair.
--
-- Idempotency: every DEFINE statement uses `IF NOT EXISTS` so re-applying
-- the file against an initialised cluster database is a no-op.
--
-- Multi-tenancy: subnets inherit their tenant from the owning VPC. The
-- `(vpc, name)` pair is the natural key — a VPC cannot have two
-- subnets with the same name, but two different VPCs may each have a
-- `web` subnet. An `org` column is NOT carried on this table yet
-- (today's `Subnet` struct doesn't denormalise it either); tenant
-- filtering walks up through `vpc`.
--
-- Foreign keys: `vpc` is a plain `string` record id for now. See
-- `org.surql` for the rationale.
-- ============================================================================


-- ── subnet ───────────────────────────────────────────────────────────────
-- A subnet belongs to exactly one VPC. Created today via
-- `nauka subnet create <name> --vpc <vpc> --cidr <cidr>`.

DEFINE TABLE IF NOT EXISTS subnet SCHEMAFULL;

-- Human-readable name. Unique per VPC.
DEFINE FIELD IF NOT EXISTS name ON subnet TYPE string
    ASSERT string::len($value) > 0;

-- Resource lifecycle status.
DEFINE FIELD IF NOT EXISTS status ON subnet TYPE string
    ASSERT string::len($value) > 0;

-- User-defined labels.
DEFINE FIELD IF NOT EXISTS labels ON subnet TYPE object DEFAULT {};

-- IPv4 CIDR, e.g. "10.0.1.0/24". Must be strictly contained in the
-- owning VPC's CIDR; that invariant is enforced at write time by
-- `nauka_network::validate::subnet_within_vpc`, not by this schema.
DEFINE FIELD IF NOT EXISTS cidr ON subnet TYPE string
    ASSERT string::len($value) > 0;

-- Gateway address for this subnet, as a string like "10.0.1.1".
-- Computed by `nauka_network::validate::gateway` from the CIDR at
-- write time. Always populated — there is no scenario today where a
-- subnet exists without a gateway.
DEFINE FIELD IF NOT EXISTS gateway ON subnet TYPE string
    ASSERT string::len($value) > 0;

-- Owning VPC — SurrealDB record id of the `vpc` row. Tenant scoping
-- and the `(vpc, name)` composite index both read this column.
DEFINE FIELD IF NOT EXISTS vpc ON subnet TYPE string
    ASSERT string::len($value) > 0;

-- Denormalised VPC name from `Subnet.vpc_name`.
DEFINE FIELD IF NOT EXISTS vpc_name ON subnet TYPE string
    ASSERT string::len($value) > 0;

-- Creation timestamp.
DEFINE FIELD IF NOT EXISTS created_at ON subnet TYPE datetime;

-- Last-update timestamp.
DEFINE FIELD IF NOT EXISTS updated_at ON subnet TYPE datetime;

-- Composite unique index on `(vpc, name)`. Mirrors the `vpc_id/name`
-- index key used by today's TiKV-backed `SubnetStore::create`.
DEFINE INDEX IF NOT EXISTS subnet_vpc_name ON subnet
    FIELDS vpc, name UNIQUE;

-- Subnet CIDRs must be unique per VPC: two subnets in the same VPC
-- cannot overlap, which includes "cannot share a CIDR". Overlap is
-- enforced at write time by `no_overlap` in the validator, but
-- modelling exact-equality uniqueness at the schema level guards the
-- simplest failure mode (two writers racing to create the same CIDR).
DEFINE INDEX IF NOT EXISTS subnet_vpc_cidr ON subnet
    FIELDS vpc, cidr UNIQUE;

-- Secondary (non-unique) index on `vpc` for "list subnets in this VPC"
-- scans — the most common read pattern.
DEFINE INDEX IF NOT EXISTS subnet_vpc ON subnet FIELDS vpc;
