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
