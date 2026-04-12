-- ============================================================================
-- Nauka — bootstrap state schema
-- ============================================================================
-- Target: nauka / bootstrap (EmbeddedDb<SurrealKv>)
--
-- This is the schema for the **local, per-node** SurrealKV-backed database
-- that holds everything Nauka needs to read *before* the cluster TiKV is up:
-- mesh identity, this node's hypervisor identity, peer list, WireGuard keys.
--
-- It is the schema-driven replacement for the JSON store used by the legacy
-- LocalDb. P1.7 (sifrah/nauka#197) wires this file into EmbeddedDb::open
-- via include_str! so every node applies it idempotently at startup.
-- P1.10 (sifrah/nauka#200) is the issue that actually migrates FabricState
-- to read/write through these tables.
--
-- Idempotency: every DEFINE statement uses `IF NOT EXISTS` so re-running the
-- whole script on an existing database is a no-op. SurrealDB SCHEMAFULL +
-- IF NOT EXISTS lets us add fields in later phases without breaking
-- already-applied schemas.
--
-- Field set: this file ships exactly the fields the P1.6 acceptance criteria
-- ask for (sifrah/nauka#196). The full FabricState (regions, zones, ports,
-- endpoints, runtime, ipv6 blocks, etc.) lands in P1.10 when the migration
-- needs them — at that point this file gets extended with `DEFINE FIELD … IF
-- NOT EXISTS` lines for the missing columns.
--
-- Multi-tenancy: this database holds *node-local* state, so there is no
-- tenant boundary at this layer (per ADR 0003, sifrah/nauka#190). The
-- per-org row-level permissions live on the cluster-side schemas (P2.5,
-- sifrah/nauka#209), not here.
-- ============================================================================


-- ── mesh ─────────────────────────────────────────────────────────────────
-- Singleton: there is exactly one mesh per Nauka deployment, so callers
-- always read/write the well-known id `mesh:current`. SurrealDB doesn't
-- have a native "singleton table" type — the convention is enforced in
-- code, not schema.

DEFINE TABLE IF NOT EXISTS mesh SCHEMAFULL;

-- The /48 ULA prefix for this mesh, stored as a string (e.g.
-- "fd54:7725:6b2a::/48"). SurrealDB has no native IPv6 type.
DEFINE FIELD IF NOT EXISTS ipv6_ula ON mesh TYPE string
    ASSERT string::len($value) > 0;

-- Hash of the mesh secret. The raw secret is *never* persisted to disk
-- by P1.x — only the hash is, so peer joins can be verified without
-- giving anyone the ability to forge new joins from a stolen
-- bootstrap.skv file. Hex- or base64-encoded; the encoding choice is
-- the migration's (P1.10) call.
DEFINE FIELD IF NOT EXISTS secret_hash ON mesh TYPE string
    ASSERT string::len($value) > 0;

-- When the mesh was first created.
DEFINE FIELD IF NOT EXISTS created_at ON mesh TYPE datetime;


-- ── hypervisor ───────────────────────────────────────────────────────────
-- Identity of this node within the mesh. Singleton in practice: the local
-- bootstrap database holds exactly one row, keyed by the hypervisor's
-- ULID via SurrealDB's record id (e.g. `hypervisor:hv-01HXXX`).
--
-- Note on `id`: the P1.6 issue (sifrah/nauka#196) lists `id` as a field,
-- but SurrealDB reserves `id` as the record-id slot — defining a separate
-- `id` field collides with the record id and breaks `CREATE hypervisor:foo
-- SET id = ...`. The right thing in SurrealDB is to use the record id
-- *as* the ULID. P1.10 (sifrah/nauka#200) will create rows with
-- `CREATE hypervisor:<ulid> SET name = ..., ...` and read them back via
-- `select(("hypervisor", &ulid_str))`. The acceptance criterion is met
-- functionally: every hypervisor row is uniquely identified by its ULID
-- via the SurrealDB record id.

DEFINE TABLE IF NOT EXISTS hypervisor SCHEMAFULL;

-- Human-readable name (defaults to hostname).
DEFINE FIELD IF NOT EXISTS name ON hypervisor TYPE string
    ASSERT string::len($value) > 0;

-- This node's IPv6 address on the mesh, as a string.
DEFINE FIELD IF NOT EXISTS mesh_ipv6 ON hypervisor TYPE string
    ASSERT string::len($value) > 0;

-- WireGuard public key (base64).
DEFINE FIELD IF NOT EXISTS public_key ON hypervisor TYPE string
    ASSERT string::len($value) > 0;

-- Coarse role within the mesh: e.g. "leader", "candidate", "member".
-- Free-form string for now; a strict enum gets added (probably as an
-- ASSERT $value INSIDE [...]) when the role state machine lands.
DEFINE FIELD IF NOT EXISTS role ON hypervisor TYPE string
    ASSERT string::len($value) > 0;

-- Index on the unique node name so name → id lookups are fast.
DEFINE INDEX IF NOT EXISTS hypervisor_name ON hypervisor FIELDS name UNIQUE;

-- Index on mesh_ipv6 (also unique per mesh).
DEFINE INDEX IF NOT EXISTS hypervisor_mesh_ipv6 ON hypervisor
    FIELDS mesh_ipv6 UNIQUE;


-- ── peer ─────────────────────────────────────────────────────────────────
-- Other nodes that this node knows about. Each row is one peer, keyed by
-- the peer's mesh IPv6.

DEFINE TABLE IF NOT EXISTS peer SCHEMAFULL;

-- The peer's mesh IPv6 address (string form).
DEFINE FIELD IF NOT EXISTS mesh_ipv6 ON peer TYPE string
    ASSERT string::len($value) > 0;

-- The peer's WireGuard public key (base64).
DEFINE FIELD IF NOT EXISTS public_key ON peer TYPE string
    ASSERT string::len($value) > 0;

-- The peer's public WireGuard endpoint (e.g. "1.2.3.4:51820").
-- Optional: peers can be reachable only via the mesh (no public IP) once
-- they've completed an initial handshake.
DEFINE FIELD IF NOT EXISTS endpoint ON peer TYPE option<string>;

-- Last successful WireGuard handshake observed for this peer. Optional:
-- a freshly added peer has no handshake yet.
DEFINE FIELD IF NOT EXISTS last_seen ON peer TYPE option<datetime>;

-- Index for fast peer lookup by mesh address (the natural key).
DEFINE INDEX IF NOT EXISTS peer_mesh_ipv6 ON peer FIELDS mesh_ipv6 UNIQUE;

-- Secondary index on public_key — used by the WireGuard reload path
-- when reconciling actual peer state from `wg show`.
DEFINE INDEX IF NOT EXISTS peer_public_key ON peer FIELDS public_key UNIQUE;


-- ── wg_key ───────────────────────────────────────────────────────────────
-- This node's WireGuard keypair. Singleton — there's exactly one local
-- keypair per node. Keys are sensitive: file permissions on bootstrap.skv
-- are the only thing protecting them, hence the 0o700 dir / 0o600 file
-- requirement enforced by P1.4 (sifrah/nauka#194).

DEFINE TABLE IF NOT EXISTS wg_key SCHEMAFULL;

-- Base64-encoded WireGuard private key.
DEFINE FIELD IF NOT EXISTS private_key ON wg_key TYPE string
    ASSERT string::len($value) > 0;

-- Base64-encoded WireGuard public key (derivable from the private key,
-- but stored explicitly for cheap reads in the hot path).
DEFINE FIELD IF NOT EXISTS public_key ON wg_key TYPE string
    ASSERT string::len($value) > 0;


-- ── fabric ───────────────────────────────────────────────────────────────
-- Transitional SCHEMALESS table used by P1.10 (sifrah/nauka#200) as a
-- single-row blob store for the legacy `FabricState` struct during the
-- migration from `LayerDb` (JSON file) to `EmbeddedDb` (SurrealKV).
--
-- The struct is serialised via serde_json::Value and stored as the
-- single `data` field on the `fabric:state` record. The wider P3
-- (sifrah/nauka#225 ff) codegen will eventually replace this with a
-- proper schemafull split across the existing `mesh` / `hypervisor` /
-- `peer` / `wg_key` tables. Until then this row is the canonical
-- bootstrap state for the embedded path.
--
-- The table is SCHEMALESS rather than SCHEMAFULL because we don't yet
-- want to lock the FabricState shape — Phase 3 will own the strict
-- field-level definitions.

DEFINE TABLE IF NOT EXISTS fabric SCHEMALESS;
