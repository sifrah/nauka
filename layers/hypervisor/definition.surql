-- Mesh identity and configuration
DEFINE TABLE mesh SCHEMAFULL;
DEFINE FIELD interface_name ON mesh TYPE string;
DEFINE FIELD listen_port    ON mesh TYPE int;
DEFINE FIELD mesh_id        ON mesh TYPE string;
DEFINE FIELD address        ON mesh TYPE string;
DEFINE FIELD public_key     ON mesh TYPE string;
DEFINE FIELD private_key    ON mesh TYPE string;
DEFINE FIELD created_at     ON mesh TYPE datetime DEFAULT time::now();

-- Mesh peers
DEFINE TABLE peer SCHEMAFULL;
DEFINE FIELD public_key     ON peer TYPE string;
DEFINE FIELD endpoint       ON peer TYPE option<string>;
DEFINE FIELD allowed_ips    ON peer TYPE array<string>;
DEFINE FIELD keepalive      ON peer TYPE option<int>;
DEFINE FIELD mesh           ON peer TYPE option<record<mesh>>;
DEFINE FIELD joined_at      ON peer TYPE datetime DEFAULT time::now();

DEFINE INDEX peer_pubkey ON peer FIELDS public_key UNIQUE;
