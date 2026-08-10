//! Consensus telemetry: publishing what openraft already computes.
//!
//! A storage cluster fails in ways that are invisible from the outside: a
//! node serves reads perfectly while the cluster has lost quorum, or while
//! leadership flaps every few seconds. Every number needed to see that is
//! already maintained by the Raft engine — term, role, leader, log indices,
//! membership — and published on a watch channel. This module subscribes to
//! that channel and republishes it as gauges, plus counts the transitions
//! the channel makes visible.
//!
//! Recording goes through the `metrics` facade, whose macros are no-ops
//! until a recorder is installed. `nauka-raft` therefore stays usable (and
//! silent) in tests and in any embedder that never installs one.
//!
//! The module is named `telemetry` and not `metrics` so it does not shadow
//! the crate of the same name.
//!
//! # Cardinality
//!
//! The per-node gauges carry NO labels: the scrape target already identifies
//! the node, and a node id label would only duplicate it. The RPC metrics
//! carry `peer` (bounded by cluster size) and `rpc` (three values). Nothing
//! is ever labelled by term or log index.

use std::sync::Weak;
use std::time::{Duration, Instant};

use openraft::ServerState;

use crate::RaftApp;

/// `nauka_raft_role` encoding. Ordered by how much authority the role
/// carries, so a graph of the gauge reads top-to-bottom the way an operator
/// expects: a node dropping from 4 to 2 lost leadership.
const ROLE_SHUTDOWN: f64 = 0.0;
const ROLE_LEARNER: f64 = 1.0;
const ROLE_FOLLOWER: f64 = 2.0;
const ROLE_CANDIDATE: f64 = 3.0;
const ROLE_LEADER: f64 = 4.0;

/// Failure classes for `nauka_raft_rpc_failures_total`.
///
/// A closed set: three constants, never a formatted string.
pub(crate) const FAIL_TIMEOUT: &str = "timeout";
pub(crate) const FAIL_UNREACHABLE: &str = "unreachable";
pub(crate) const FAIL_REJECTED: &str = "rejected";

/// RPC names for the `rpc` label. Mirrors the three `RaftNetwork` methods.
pub(crate) const RPC_APPEND_ENTRIES: &str = "append_entries";
pub(crate) const RPC_INSTALL_SNAPSHOT: &str = "install_snapshot";
pub(crate) const RPC_VOTE: &str = "vote";

/// How often the committed index is sampled.
///
/// Unlike everything else here, the commit index is not on the metrics watch
/// channel in openraft 0.9: reading it costs one request to the Raft core
/// loop. The watch fires on every applied entry, so sampling on every change
/// would put a message on the core's queue per commit on a busy leader. One
/// sample per heartbeat is plenty for a gauge scraped every 15 s, and costs
/// the core nothing measurable.
const COMMIT_SAMPLE_INTERVAL: Duration = Duration::from_millis(500);

/// Register HELP/TYPE for every consensus metric, once at startup.
///
/// Called from [`RaftApp::start`], so `nauka-node` needs no wiring: any
/// binary that starts a Raft node describes these, and any binary that never
/// installs a recorder discards them.
pub fn describe() {
    metrics::describe_gauge!(
        "nauka_raft_term",
        "Current Raft term as seen by this node. Rises on every election attempt, successful or not."
    );
    metrics::describe_gauge!(
        "nauka_raft_role",
        "Raft role of this node: 0=shutdown, 1=learner, 2=follower, 3=candidate, 4=leader."
    );
    metrics::describe_gauge!(
        "nauka_raft_leader_known",
        "1 when this node knows a current leader, 0 during an election or a partition with no quorum."
    );
    metrics::describe_counter!(
        "nauka_raft_leader_changes_total",
        "Leadership changes observed by this node, counted once per (term, leader) pair — one per genuine election, including the cluster's first."
    );
    metrics::describe_gauge!(
        "nauka_raft_last_log_index",
        "Index of the last log entry this node has appended. 0 when the log is empty."
    );
    metrics::describe_gauge!(
        "nauka_raft_commit_index",
        "Index of the last log entry known to be committed cluster-wide. Read from the Raft core at most twice a second, and never reported below nauka_raft_last_applied — a node applies only committed entries."
    );
    metrics::describe_gauge!(
        "nauka_raft_last_applied",
        "Index of the last log entry applied to this node's state machine. 0 when nothing was applied."
    );
    metrics::describe_gauge!(
        "nauka_raft_apply_lag",
        "Log entries accepted but not yet applied here (last_log_index - last_applied). Sustained non-zero means this node is behind."
    );
    metrics::describe_gauge!(
        "nauka_raft_members",
        "Number of nodes in the current membership config, voters and learners together."
    );
    metrics::describe_histogram!(
        "nauka_raft_rpc_duration_seconds",
        "Round-trip time of an outbound consensus RPC that got an answer, by peer and RPC. Failed calls are not timed here; they are counted in nauka_raft_rpc_failures_total."
    );
    metrics::describe_counter!(
        "nauka_raft_rpc_failures_total",
        "Outbound consensus RPCs that did not succeed, by peer, RPC and kind: unreachable (no connection or a dead one), timeout (no answer within the RPC deadline), rejected (the peer answered and refused — a log conflict or a higher vote)."
    );
    metrics::describe_counter!(
        "nauka_raft_vote_requests_total",
        "Vote requests received from other nodes, by whether this node granted them. Non-zero means elections are being contested."
    );
}

/// Publish the Raft state as gauges, for as long as the node lives.
///
/// Driven by openraft's metrics watch channel: the task sleeps until the
/// engine changes something, so an idle cluster costs one heartbeat-driven
/// wakeup and nothing else. Because it is a watch channel and not a poll,
/// role transitions (follower → candidate → leader) are seen as they
/// happen — which is what makes counting leader changes possible at all.
///
/// Holds a [`Weak`] rather than an `Arc`: an observer must not be the reason
/// the thing it observes stays alive. When the last real owner drops the
/// node, the task notices and exits.
pub(crate) fn spawn(app: Weak<RaftApp>) {
    let Some(strong) = app.upgrade() else { return };
    // A `Raft` handle of its own: cloning it is cheap and it keeps the
    // engine, not the `RaftApp`, reachable — so the Weak above still does
    // its job.
    let raft = strong.raft.clone();
    drop(strong);

    tokio::spawn(async move {
        let mut rx = raft.metrics();
        // The (term, leader) pair last observed. Keyed on the pair and not
        // on the leader id alone: a node re-elected in a higher term won a
        // genuine election, and a leader that merely blinks out of view and
        // comes back did not.
        let mut leader_seen: Option<(u64, crate::types::NodeId)> = None;
        let mut committed_sampled_at: Option<Instant> = None;
        let mut committed = 0u64;

        loop {
            // Sampled before the snapshot is taken, so the value published
            // below is never NEWER than the applied index it is floored
            // against.
            if committed_sampled_at.is_none_or(|t| t.elapsed() >= COMMIT_SAMPLE_INTERVAL) {
                committed_sampled_at = Some(Instant::now());
                match raft
                    .with_raft_state(|st| st.committed.map(|l| l.index))
                    .await
                {
                    Ok(c) => committed = c.unwrap_or(0),
                    // The engine is gone; nothing left to observe.
                    Err(_) => break,
                }
            }

            let Some(node) = app.upgrade() else { break };
            let m = rx.borrow_and_update().clone();

            metrics::gauge!("nauka_raft_term").set(m.current_term as f64);
            metrics::gauge!("nauka_raft_role").set(role_code(m.state));
            metrics::gauge!("nauka_raft_leader_known").set(if node.leader_known() {
                1.0
            } else {
                0.0
            });
            metrics::gauge!("nauka_raft_members").set(node.members().len() as f64);

            let last_log = m.last_log_index.unwrap_or(0);
            let applied = m.last_applied.map(|l| l.index).unwrap_or(0);
            metrics::gauge!("nauka_raft_last_log_index").set(last_log as f64);
            metrics::gauge!("nauka_raft_last_applied").set(applied as f64);
            // Derived from the same snapshot as its two operands, so it can
            // never go negative through a torn read.
            metrics::gauge!("nauka_raft_apply_lag").set(last_log.saturating_sub(applied) as f64);
            // Floored at `applied`: a node applies only committed entries,
            // so a sample that predates the last apply would otherwise
            // publish a commit index BELOW the applied one and break an
            // invariant an operator is entitled to rely on.
            metrics::gauge!("nauka_raft_commit_index").set(committed.max(applied) as f64);

            if let Some(leader) = m.current_leader {
                let now = (m.current_term, leader);
                if leader_seen != Some(now) {
                    leader_seen = Some(now);
                    metrics::counter!("nauka_raft_leader_changes_total").increment(1);
                    tracing::info!(term = m.current_term, leader, "raft leadership changed");
                }
            }
            // No leader: deliberately leave `leader_seen` alone. An election
            // that returns the SAME leader in the SAME term was a blink, not
            // a leadership change, and must not inflate the counter.

            drop(node);

            if rx.changed().await.is_err() {
                break;
            }
        }
    });
}

/// Numeric encoding of a Raft role. See the HELP text on `nauka_raft_role`.
fn role_code(state: ServerState) -> f64 {
    match state {
        ServerState::Shutdown => ROLE_SHUTDOWN,
        ServerState::Learner => ROLE_LEARNER,
        ServerState::Follower => ROLE_FOLLOWER,
        ServerState::Candidate => ROLE_CANDIDATE,
        ServerState::Leader => ROLE_LEADER,
    }
}

/// One outbound consensus RPC that came back with an answer.
///
/// Only answered calls are timed: a latency histogram that also contains
/// every timeout says nothing about how long consensus actually takes, and
/// the failures counter already carries the ones that did not answer.
pub(crate) fn record_rpc(peer: &str, rpc: &'static str, elapsed: Duration) {
    metrics::histogram!(
        "nauka_raft_rpc_duration_seconds",
        "peer" => peer.to_owned(),
        "rpc" => rpc,
    )
    .record(elapsed.as_secs_f64());
}

/// One outbound consensus RPC that did not succeed. `kind` is a closed set:
/// [`FAIL_TIMEOUT`], [`FAIL_UNREACHABLE`] or [`FAIL_REJECTED`].
pub(crate) fn record_rpc_failure(peer: &str, rpc: &'static str, kind: &'static str) {
    metrics::counter!(
        "nauka_raft_rpc_failures_total",
        "peer" => peer.to_owned(),
        "rpc" => rpc,
        "kind" => kind,
    )
    .increment(1);
}

/// One inbound vote request. Two series, and the cheapest possible proof
/// that an election really happened rather than being inferred from a term
/// that moved between two scrapes.
pub(crate) fn record_vote_received(granted: bool) {
    metrics::counter!(
        "nauka_raft_vote_requests_total",
        "granted" => if granted { "true" } else { "false" },
    )
    .increment(1);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn role_codes_are_distinct_and_ordered_by_authority() {
        let codes = [
            role_code(ServerState::Shutdown),
            role_code(ServerState::Learner),
            role_code(ServerState::Follower),
            role_code(ServerState::Candidate),
            role_code(ServerState::Leader),
        ];
        assert!(codes.windows(2).all(|w| w[0] < w[1]));
        assert_eq!(codes[4], ROLE_LEADER);
    }

    #[test]
    fn recording_without_a_recorder_is_a_no_op() {
        // Every call site in this crate must be safe in a binary that never
        // installs a recorder — including the whole test suite.
        describe();
        record_rpc("127.0.0.1:7311", RPC_VOTE, Duration::from_millis(3));
        record_rpc_failure("127.0.0.1:7311", RPC_APPEND_ENTRIES, FAIL_TIMEOUT);
        record_vote_received(true);
    }
}
