-- Mesh identity and configuration (local to this node)
DEFINE TABLE mesh SCHEMAFULL;
DEFINE FIELD interface_name ON mesh TYPE string;
DEFINE FIELD listen_port    ON mesh TYPE int;
DEFINE FIELD mesh_id        ON mesh TYPE string;
DEFINE FIELD private_key    ON mesh TYPE string;
DEFINE FIELD ca_cert        ON mesh TYPE option<string>;
DEFINE FIELD ca_key         ON mesh TYPE option<string>;
DEFINE FIELD tls_cert       ON mesh TYPE option<string>;
DEFINE FIELD tls_key        ON mesh TYPE option<string>;
DEFINE FIELD created_at     ON mesh TYPE datetime DEFAULT time::now();

-- Mesh hypervisors (replicated via Raft consensus)
DEFINE TABLE hypervisor SCHEMAFULL;
DEFINE FIELD public_key     ON hypervisor TYPE string;
DEFINE FIELD node_id        ON hypervisor TYPE int;
DEFINE FIELD address        ON hypervisor TYPE string;
DEFINE FIELD endpoint       ON hypervisor TYPE option<string>;
DEFINE FIELD allowed_ips    ON hypervisor TYPE array<string>;
DEFINE FIELD keepalive      ON hypervisor TYPE option<int>;
DEFINE FIELD raft_addr      ON hypervisor TYPE string;
DEFINE FIELD joined_at      ON hypervisor TYPE datetime DEFAULT time::now();

DEFINE INDEX hypervisor_pubkey ON hypervisor FIELDS public_key UNIQUE;
