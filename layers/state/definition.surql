-- Raft consensus state (local to each node)
DEFINE TABLE _raft_meta SCHEMAFULL;
DEFINE FIELD hypervisor ON _raft_meta TYPE int;
DEFINE FIELD vote       ON _raft_meta TYPE option<string>;
DEFINE FIELD committed  ON _raft_meta TYPE option<string>;
DEFINE FIELD purged     ON _raft_meta TYPE option<string>;

DEFINE TABLE _raft_log SCHEMAFULL;
DEFINE FIELD hypervisor ON _raft_log TYPE int;
DEFINE FIELD log_index  ON _raft_log TYPE int;
DEFINE FIELD entry      ON _raft_log TYPE string;
DEFINE INDEX raft_log_idx ON _raft_log FIELDS hypervisor, log_index UNIQUE;
