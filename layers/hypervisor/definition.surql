-- Mesh identity and configuration (local to this node)
DEFINE TABLE IF NOT EXISTS mesh SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS interface_name ON mesh TYPE string;
DEFINE FIELD IF NOT EXISTS listen_port    ON mesh TYPE int;
DEFINE FIELD IF NOT EXISTS mesh_id        ON mesh TYPE string;
DEFINE FIELD IF NOT EXISTS private_key    ON mesh TYPE string;
DEFINE FIELD IF NOT EXISTS ca_cert        ON mesh TYPE option<string>;
DEFINE FIELD IF NOT EXISTS ca_key         ON mesh TYPE option<string>;
DEFINE FIELD IF NOT EXISTS tls_cert       ON mesh TYPE option<string>;
DEFINE FIELD IF NOT EXISTS tls_key        ON mesh TYPE option<string>;
DEFINE FIELD IF NOT EXISTS peering_pin    ON mesh TYPE option<string>;
DEFINE FIELD IF NOT EXISTS created_at     ON mesh TYPE datetime DEFAULT time::now();

-- Mesh hypervisors (replicated via Raft consensus)
DEFINE TABLE IF NOT EXISTS hypervisor SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS public_key     ON hypervisor TYPE string;
DEFINE FIELD IF NOT EXISTS node_id        ON hypervisor TYPE int;
DEFINE FIELD IF NOT EXISTS address        ON hypervisor TYPE string;
DEFINE FIELD IF NOT EXISTS endpoint       ON hypervisor TYPE option<string>;
DEFINE FIELD IF NOT EXISTS allowed_ips    ON hypervisor TYPE array<string>;
DEFINE FIELD IF NOT EXISTS keepalive      ON hypervisor TYPE option<int>;
DEFINE FIELD IF NOT EXISTS raft_addr      ON hypervisor TYPE string;
DEFINE FIELD IF NOT EXISTS joined_at      ON hypervisor TYPE datetime DEFAULT time::now();

DEFINE INDEX IF NOT EXISTS hypervisor_pubkey ON hypervisor FIELDS public_key UNIQUE;
