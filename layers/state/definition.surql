-- Raft consensus state (local to each node)
DEFINE TABLE IF NOT EXISTS _raft_meta SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS hypervisor ON _raft_meta TYPE int;
DEFINE FIELD IF NOT EXISTS vote       ON _raft_meta TYPE option<string>;
DEFINE FIELD IF NOT EXISTS committed  ON _raft_meta TYPE option<string>;
DEFINE FIELD IF NOT EXISTS purged     ON _raft_meta TYPE option<string>;

DEFINE TABLE IF NOT EXISTS _raft_log SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS hypervisor ON _raft_log TYPE int;
DEFINE FIELD IF NOT EXISTS log_index  ON _raft_log TYPE int;
DEFINE FIELD IF NOT EXISTS entry      ON _raft_log TYPE string;
DEFINE INDEX IF NOT EXISTS raft_log_idx ON _raft_log FIELDS hypervisor, log_index UNIQUE;

DEFINE TABLE IF NOT EXISTS _raft_snapshot SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS hypervisor      ON _raft_snapshot TYPE int;
DEFINE FIELD IF NOT EXISTS snapshot_id     ON _raft_snapshot TYPE string;
DEFINE FIELD IF NOT EXISTS last_applied    ON _raft_snapshot TYPE option<string>;
DEFINE FIELD IF NOT EXISTS last_membership ON _raft_snapshot TYPE string;
DEFINE FIELD IF NOT EXISTS data            ON _raft_snapshot TYPE string;
