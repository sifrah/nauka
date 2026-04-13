-- ============================================================================
-- Nauka — cluster state schema: vpc_peering
-- ============================================================================
-- Target: nauka / cluster (SurrealDB on top of kv-tikv)
--
-- Distributed cluster-state representation of a `VpcPeering` — a
-- bidirectional peering relationship between two VPCs in the same
-- cluster. Today peerings are written through
-- `nauka_network::vpc::peering::store::PeeringStore` via the SurrealDB
-- SDK after P2.12 (sifrah/nauka#216); this schema file ships the
-- SCHEMAFULL definition that pair of writes target.
--
-- Source of truth: `layers/network/src/vpc/peering/types.rs`
-- (`struct VpcPeering`), which embeds `ResourceMeta` via
-- `#[serde(flatten)]` and carries an owning `vpc_id` / `vpc_name` pair
-- for the "local" VPC plus a matching `peer_vpc_id` / `peer_vpc_name`
-- pair for the partner. A `state` enum (`active` | `pending`) tracks
-- whether both sides have acknowledged the peering.
--
-- Idempotency: every DEFINE statement uses `IF NOT EXISTS` so this
-- file can be re-applied against an initialised cluster database as a
-- no-op. `nauka_state::apply_cluster_schemas` runs this at bootstrap.
--
-- Multi-tenancy: peerings inherit their tenant from the local VPC —
-- there is no `org` column on this row because a peering is
-- identified cluster-wide and tenant filtering walks through
-- `vpc.org`. A future row-level authz rule may project the owning
-- org down here; for now we keep the schema minimal.
--
-- Uniqueness: the `(vpc, peer_vpc)` pair is enforced unique, and the
-- store side additionally rejects a pair whose reverse `(peer_vpc,
-- vpc)` already exists — a peering is bidirectional, so only one row
-- may exist for each unordered pair of VPCs. The schema index
-- enforces only the ordered-pair case because SurrealDB can't express
-- a "set of 2 values" unique index directly; the store layer closes
-- the reverse-pair gap.
--
-- Foreign keys: `vpc` and `peer_vpc` are both plain `string` record
-- ids for now. See `org.surql` for the rationale — P3 codegen may
-- upgrade them to typed `record<vpc>` links.
-- ============================================================================


-- ── vpc_peering ──────────────────────────────────────────────────────────
-- A bidirectional VPC-to-VPC peering. The two VPCs must be in the
-- same cluster; cross-cluster peerings (Phase 4+) will live in a
-- separate table.

DEFINE TABLE IF NOT EXISTS vpc_peering SCHEMAFULL;

-- Human-readable auto-generated name, e.g. "web-to-db". Present in
-- `ResourceMeta.name`. Non-empty.
DEFINE FIELD IF NOT EXISTS name ON vpc_peering TYPE string
    ASSERT string::len($value) > 0;

-- Resource lifecycle status, same free-form semantics as every other
-- cluster-state table.
DEFINE FIELD IF NOT EXISTS status ON vpc_peering TYPE string
    ASSERT string::len($value) > 0;

-- User-defined labels. Defaults to `{}` so the column is never null.
DEFINE FIELD IF NOT EXISTS labels ON vpc_peering TYPE object DEFAULT {};

-- Local VPC — SurrealDB record id of the `vpc` row that hosts this
-- peering. The "local" side is the one that submitted the create
-- request; the naming is arbitrary because peerings are
-- bidirectional.
DEFINE FIELD IF NOT EXISTS vpc ON vpc_peering TYPE string
    ASSERT string::len($value) > 0;

-- Denormalised local-VPC name copied from `VpcPeering.vpc_name`.
DEFINE FIELD IF NOT EXISTS vpc_name ON vpc_peering TYPE string
    ASSERT string::len($value) > 0;

-- Peer VPC — SurrealDB record id of the other `vpc` row.
DEFINE FIELD IF NOT EXISTS peer_vpc ON vpc_peering TYPE string
    ASSERT string::len($value) > 0;

-- Denormalised peer-VPC name copied from `VpcPeering.peer_vpc_name`.
DEFINE FIELD IF NOT EXISTS peer_vpc_name ON vpc_peering TYPE string
    ASSERT string::len($value) > 0;

-- Peering state, mirrors `nauka_network::vpc::peering::types::PeeringState`.
-- The enum is rendered as a lowercase string by serde
-- (`#[serde(rename = "active" | "pending")]`); the `ASSERT` limits
-- incoming values to that exact set so a bad client can't write a
-- nonsense state.
DEFINE FIELD IF NOT EXISTS state ON vpc_peering TYPE string
    ASSERT $value INSIDE ["active", "pending"];

-- Creation timestamp.
DEFINE FIELD IF NOT EXISTS created_at ON vpc_peering TYPE datetime;

-- Last-update timestamp.
DEFINE FIELD IF NOT EXISTS updated_at ON vpc_peering TYPE datetime;

-- Composite unique index on `(vpc, peer_vpc)`. Same pair cannot be
-- peered twice in the same direction. The reverse-direction check
-- (`(peer_vpc, vpc)`) is enforced by the store layer because
-- SurrealDB can't express "an unordered pair is unique" as a single
-- index.
DEFINE INDEX IF NOT EXISTS vpc_peering_pair ON vpc_peering
    FIELDS vpc, peer_vpc UNIQUE;

-- Secondary (non-unique) index on `vpc` for "list peerings touching
-- this VPC" scans. The list side also walks `peer_vpc` — SurrealQL
-- can `UNION` the two, or the application layer can filter a
-- `SELECT *`; either is fine at the row counts Phase 2 targets.
DEFINE INDEX IF NOT EXISTS vpc_peering_vpc ON vpc_peering FIELDS vpc;

-- Matching secondary index on the other side of the pair.
DEFINE INDEX IF NOT EXISTS vpc_peering_peer_vpc ON vpc_peering
    FIELDS peer_vpc;
