-- Mesh hypervisors (replicated via Raft consensus)
DEFINE TABLE IF NOT EXISTS hypervisor SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS public_key     ON hypervisor TYPE string;
DEFINE FIELD IF NOT EXISTS node_id        ON hypervisor TYPE int;
DEFINE FIELD IF NOT EXISTS address        ON hypervisor TYPE string;
DEFINE FIELD IF NOT EXISTS endpoint       ON hypervisor TYPE option<string>;
DEFINE FIELD IF NOT EXISTS allowed_ips    ON hypervisor TYPE array<string>;
DEFINE FIELD IF NOT EXISTS keepalive      ON hypervisor TYPE option<int>;
DEFINE FIELD IF NOT EXISTS raft_addr      ON hypervisor TYPE string;
-- `joined_at` is set explicitly by the writer (in Rust, before raft.write)
-- rather than via `DEFAULT time::now()`, which would evaluate independently
-- on each node's state machine and break Raft determinism.
DEFINE FIELD IF NOT EXISTS joined_at      ON hypervisor TYPE datetime;

DEFINE INDEX IF NOT EXISTS hypervisor_pubkey ON hypervisor FIELDS public_key UNIQUE;
