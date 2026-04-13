-- ============================================================================
-- Nauka — cluster state schema: vm
-- ============================================================================
-- Target: nauka / cluster (SurrealDB on top of kv-tikv)
--
-- Distributed cluster-state representation of a `Vm` — the core compute
-- resource. Today VMs are written through
-- `nauka_compute::vm::store::VmStore` into raw TiKV via `ClusterDb`
-- under the `vm` / `vm-idx` namespaces. Phase 2 (sifrah/nauka#220)
-- migrates the storage onto SurrealDB; Phase 3 codegen
-- (sifrah/nauka#225 ff) will generate the Rust type / store / handlers
-- from this file.
--
-- Source of truth: `layers/compute/src/vm/types.rs` (`struct Vm`), which
-- embeds `ResourceMeta` via `#[serde(flatten)]` and carries the full
-- owning scope (`org_id`/`org_name`, `project_id`/`project_name`,
-- `env_id`/`env_name`), network placement (`vpc_id`/`vpc_name`,
-- `subnet_id`/`subnet_name`), hardware specs (`vcpus`, `memory_mb`,
-- `disk_gb`, `image`), physical placement (`region`, `zone`,
-- `hypervisor_id`, `private_ip`), and a lifecycle FSM in `state:
-- VmState`.
--
-- Idempotency: every DEFINE statement uses `IF NOT EXISTS` so re-applying
-- the file against an initialised cluster database is a no-op.
--
-- Multi-tenancy: `vm` is the deepest tenant-scoped resource. Every row
-- carries the full `(org, project, env)` path explicitly so row-level
-- authz filters don't need joins. The natural key is `(env, name)` —
-- the same VM name can exist in different environments.
--
-- Foreign keys: every reference is a plain `string` record id for now.
-- See `org.surql` for the rationale — P3 codegen may upgrade to typed
-- `record<…>` links.
-- ============================================================================


-- ── vm ───────────────────────────────────────────────────────────────────
-- A virtual machine instance, scheduled onto a single hypervisor. The
-- full provisioning path (resolve org/project/env/vpc/subnet → check
-- uniqueness → allocate IP via IPAM → schedule onto a hypervisor →
-- persist the row) lives in today's `VmStore::create`; this schema just
-- describes the persisted shape.

DEFINE TABLE IF NOT EXISTS vm SCHEMAFULL;

-- Human-readable name. Unique within the owning environment.
DEFINE FIELD IF NOT EXISTS name ON vm TYPE string
    ASSERT string::len($value) > 0;

-- Resource lifecycle status — distinct from the VM-specific `state`
-- field below. `status` is Nauka's generic resource status
-- ("active" / "deleting" / …) used for cross-resource list filtering;
-- `state` is the domain-specific VM FSM (pending / running / stopped /
-- …). Both exist today in `Vm` via the flattened `ResourceMeta` plus
-- the standalone `state: VmState` field.
DEFINE FIELD IF NOT EXISTS status ON vm TYPE string
    ASSERT string::len($value) > 0;

-- User-defined labels.
DEFINE FIELD IF NOT EXISTS labels ON vm TYPE object DEFAULT {};

-- ── scope ───────────────────────────────────────────────────────────

-- Owning organisation — record id of the `org` row.
DEFINE FIELD IF NOT EXISTS org ON vm TYPE string
    ASSERT string::len($value) > 0;

-- Denormalised org name.
DEFINE FIELD IF NOT EXISTS org_name ON vm TYPE string
    ASSERT string::len($value) > 0;

-- Owning project — record id of the `project` row.
DEFINE FIELD IF NOT EXISTS project ON vm TYPE string
    ASSERT string::len($value) > 0;

-- Denormalised project name.
DEFINE FIELD IF NOT EXISTS project_name ON vm TYPE string
    ASSERT string::len($value) > 0;

-- Owning environment — record id of the `env` row. The natural key
-- `(env, name)` is enforced by the `vm_env_name` index below.
DEFINE FIELD IF NOT EXISTS env ON vm TYPE string
    ASSERT string::len($value) > 0;

-- Denormalised env name.
DEFINE FIELD IF NOT EXISTS env_name ON vm TYPE string
    ASSERT string::len($value) > 0;

-- ── network ─────────────────────────────────────────────────────────

-- Owning VPC — record id of the `vpc` row.
DEFINE FIELD IF NOT EXISTS vpc ON vm TYPE string
    ASSERT string::len($value) > 0;

-- Denormalised VPC name.
DEFINE FIELD IF NOT EXISTS vpc_name ON vm TYPE string
    ASSERT string::len($value) > 0;

-- Owning subnet — record id of the `subnet` row.
DEFINE FIELD IF NOT EXISTS subnet ON vm TYPE string
    ASSERT string::len($value) > 0;

-- Denormalised subnet name.
DEFINE FIELD IF NOT EXISTS subnet_name ON vm TYPE string
    ASSERT string::len($value) > 0;

-- ── hardware specs ──────────────────────────────────────────────────

-- Virtual CPU count. `> 0` because a 0-vCPU VM is meaningless.
DEFINE FIELD IF NOT EXISTS vcpus ON vm TYPE int
    ASSERT $value > 0;

-- Memory in MiB. `> 0` — zero-memory VMs are meaningless.
DEFINE FIELD IF NOT EXISTS memory_mb ON vm TYPE int
    ASSERT $value > 0;

-- Root disk size in GiB. `> 0` to reject zero-sized disks.
DEFINE FIELD IF NOT EXISTS disk_gb ON vm TYPE int
    ASSERT $value > 0;

-- Image reference. Today this is a free-form identifier resolved by
-- the image registry (`nauka-compute::image`); format validation
-- happens there, not in the schema.
DEFINE FIELD IF NOT EXISTS image ON vm TYPE string
    ASSERT string::len($value) > 0;

-- ── placement ───────────────────────────────────────────────────────

-- Region the VM lives in (e.g. "eu-west"). Non-empty.
DEFINE FIELD IF NOT EXISTS region ON vm TYPE string
    ASSERT string::len($value) > 0;

-- Zone within the region (e.g. "eu-west-1a"). Non-empty.
DEFINE FIELD IF NOT EXISTS zone ON vm TYPE string
    ASSERT string::len($value) > 0;

-- Private IP assigned by the subnet IPAM. Optional because a
-- freshly-created row has no IP until `VmStore::create` allocates one
-- (today both ends happen in the same transaction, but the schema has
-- to allow the intermediate state for migrations and recovery). Stored
-- as a string for now — P3 codegen may upgrade to a native inet type
-- once SurrealDB grows one.
DEFINE FIELD IF NOT EXISTS private_ip ON vm TYPE option<string>;

-- ID of the hypervisor this VM was scheduled onto. Optional — a VM
-- can exist in the `pending` / `creating` state before the scheduler
-- assigns a host. Stored as a plain string: the same `HypervisorId`
-- shape used elsewhere in the tree (e.g. `hv-01J…`).
DEFINE FIELD IF NOT EXISTS hypervisor ON vm TYPE option<string>;

-- ── lifecycle ───────────────────────────────────────────────────────

-- Domain-specific VM state machine. Mirrors the `VmState` enum in
-- `layers/compute/src/vm/types.rs`, serialised with `#[serde(rename_all
-- = "lowercase")]`. The ASSERT pins the allowed values so an
-- unexpected state literal is rejected at insert time rather than
-- surfacing as a deserialisation failure.
DEFINE FIELD IF NOT EXISTS state ON vm TYPE string
    ASSERT $value INSIDE [
        "pending",
        "creating",
        "running",
        "stopped",
        "deleting",
        "deleted"
    ];

-- Creation timestamp.
DEFINE FIELD IF NOT EXISTS created_at ON vm TYPE datetime;

-- Last-update timestamp. Mutated on every state transition today
-- (`VmStore::update_state` rewrites `meta.updated_at`).
DEFINE FIELD IF NOT EXISTS updated_at ON vm TYPE datetime;

-- Composite unique index on `(env, name)`. Mirrors the `env_id/name`
-- index key used by today's TiKV-backed `VmStore::create`.
DEFINE INDEX IF NOT EXISTS vm_env_name ON vm
    FIELDS env, name UNIQUE;

-- Private IP must be unique within a subnet — the IPAM allocator
-- already enforces this at write time, but modelling it at the schema
-- level guards the invariant against future races. The index is
-- *partial in effect* because `private_ip` is `option<string>`: rows
-- in the `pending` state with no IP do not collide with one another.
-- SurrealDB's unique indexes already treat `NONE` values as distinct,
-- so no extra WHERE clause is needed here.
DEFINE INDEX IF NOT EXISTS vm_subnet_private_ip ON vm
    FIELDS subnet, private_ip UNIQUE;

-- Secondary (non-unique) indexes on the common tenant-scoped list
-- paths. The CLI's default `nauka vm list --org <o> --project <p>
-- --env <e>` wants to walk exactly one of these indexes.
DEFINE INDEX IF NOT EXISTS vm_org ON vm FIELDS org;
DEFINE INDEX IF NOT EXISTS vm_project ON vm FIELDS project;
DEFINE INDEX IF NOT EXISTS vm_env ON vm FIELDS env;

-- Scheduler-side index: "which VMs live on this hypervisor?" — the
-- query the fabric reconciler runs on every reconcile cycle.
DEFINE INDEX IF NOT EXISTS vm_hypervisor ON vm FIELDS hypervisor;
