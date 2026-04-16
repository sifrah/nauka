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

DEFINE TABLE _raft_snapshot SCHEMAFULL;
DEFINE FIELD hypervisor      ON _raft_snapshot TYPE int;
DEFINE FIELD snapshot_id     ON _raft_snapshot TYPE string;
DEFINE FIELD last_applied    ON _raft_snapshot TYPE option<string>;
DEFINE FIELD last_membership ON _raft_snapshot TYPE string;
DEFINE FIELD data            ON _raft_snapshot TYPE string;
