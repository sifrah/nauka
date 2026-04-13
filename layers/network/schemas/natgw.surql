-- ============================================================================
-- Nauka — cluster state schema: natgw
-- ============================================================================
-- Target: nauka / cluster (SurrealDB on top of kv-tikv)
--
-- Distributed cluster-state representation of a `NatGw` — a NAT
-- gateway that provides outbound IPv4 → IPv6 translation for VMs in a
-- single VPC. Today NAT gateways are written through
-- `nauka_network::vpc::natgw::store::NatGwStore` via the SurrealDB
-- SDK after P2.12 (sifrah/nauka#216); this schema file ships the
-- SCHEMAFULL definition that pair of writes target.
--
-- Source of truth: `layers/network/src/vpc/natgw/types.rs`
-- (`struct NatGw`), which embeds `ResourceMeta` via
-- `#[serde(flatten)]` and carries an owning `vpc_id` / `vpc_name`
-- pair, a public IPv6 address allocated from the hosting hypervisor's
-- `/64` block, the hypervisor's id, and a lifecycle `state`
-- (provisioning → active → error).
--
-- Idempotency: every DEFINE statement uses `IF NOT EXISTS` so this
-- file can be re-applied against an initialised cluster database as a
-- no-op. `nauka_state::apply_cluster_schemas` runs this at bootstrap.
--
-- Multi-tenancy: NAT gateways inherit their tenant from the owning
-- VPC. The `(vpc, name)` tuple is the natural key, same as subnets.
-- There is additionally an "at most one NAT gateway per VPC"
-- invariant enforced by the store layer at create time — the schema
-- does not express that single-row-per-parent constraint because
-- SurrealDB has no "partial unique" index type; the store's "check
-- before create" closes the gap.
--
-- Foreign keys: `vpc` is a plain `string` record id for now. See
-- `org.surql` for the rationale — P3 codegen may upgrade to
-- `record<vpc>`.
-- ============================================================================


-- ── natgw ────────────────────────────────────────────────────────────────
-- A NAT gateway. Exactly one per VPC. Provisioned on a specific
-- hypervisor with an IPv6 address drawn from that hypervisor's /64
-- public block.

DEFINE TABLE IF NOT EXISTS natgw SCHEMAFULL;

-- Human-readable name. Unique within the owning VPC.
DEFINE FIELD IF NOT EXISTS name ON natgw TYPE string
    ASSERT string::len($value) > 0;

-- Resource lifecycle status. Same free-form semantics as every other
-- cluster-state table — NOT the business-logic `state` column below.
DEFINE FIELD IF NOT EXISTS status ON natgw TYPE string
    ASSERT string::len($value) > 0;

-- User-defined labels. Defaults to `{}` so the column is never null.
DEFINE FIELD IF NOT EXISTS labels ON natgw TYPE object DEFAULT {};

-- Owning VPC — SurrealDB record id of the `vpc` row. Tenant scoping
-- and the `(vpc, name)` composite index both read this column.
DEFINE FIELD IF NOT EXISTS vpc ON natgw TYPE string
    ASSERT string::len($value) > 0;

-- Denormalised VPC name copied from `NatGw.vpc_name`.
DEFINE FIELD IF NOT EXISTS vpc_name ON natgw TYPE string
    ASSERT string::len($value) > 0;

-- Dedicated public IPv6 address for this NAT gateway's outbound
-- traffic, as a string like "2a01:4f8:c012:abcd::a1b2:c3d4:e5f6:…".
-- Allocated from the hosting hypervisor's `/64` block by the
-- `ipv6_alloc` helper. SurrealDB has no native IPv6 type, so we
-- store the canonical string form — the application layer parses it
-- back into `std::net::Ipv6Addr` on read.
DEFINE FIELD IF NOT EXISTS public_ipv6 ON natgw TYPE string
    ASSERT string::len($value) > 0;

-- Id of the hypervisor where this NAT gateway is provisioned.
-- Plain `string` today because `NatGw.hypervisor_id` is a `String`,
-- not a typed `HypervisorId`. A future pass may tighten this to
-- `record<hypervisor>` once cluster-side hypervisor rows exist.
DEFINE FIELD IF NOT EXISTS hypervisor ON natgw TYPE string
    ASSERT string::len($value) > 0;

-- Business-logic state, mirrors
-- `nauka_network::vpc::natgw::types::NatGwState`. The enum is
-- rendered as a lowercase string by serde
-- (`#[serde(rename = "provisioning" | "active" | "error")]`); the
-- `ASSERT` limits incoming values to that exact set so a bad client
-- can't write a nonsense state.
DEFINE FIELD IF NOT EXISTS state ON natgw TYPE string
    ASSERT $value INSIDE ["provisioning", "active", "error"];

-- Creation timestamp.
DEFINE FIELD IF NOT EXISTS created_at ON natgw TYPE datetime;

-- Last-update timestamp.
DEFINE FIELD IF NOT EXISTS updated_at ON natgw TYPE datetime;

-- Composite unique index on `(vpc, name)`. Two NAT gateways in
-- different VPCs may share a name; within a single VPC the name is
-- unique.
DEFINE INDEX IF NOT EXISTS natgw_vpc_name ON natgw
    FIELDS vpc, name UNIQUE;

-- Secondary (non-unique) index on `vpc` for "is there a NAT gateway
-- for this VPC yet?" scans, which the store layer uses to enforce
-- the "at most one NAT gateway per VPC" invariant at create time.
DEFINE INDEX IF NOT EXISTS natgw_vpc ON natgw FIELDS vpc;

-- Secondary index on `public_ipv6` to catch address collisions early
-- — the `ipv6_alloc` helper hashes the NAT gateway id into a
-- deterministic /128, so collisions are extremely unlikely but the
-- schema should still guard against a bad migration writing two
-- rows with the same address.
DEFINE INDEX IF NOT EXISTS natgw_public_ipv6 ON natgw
    FIELDS public_ipv6 UNIQUE;
