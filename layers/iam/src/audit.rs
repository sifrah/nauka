//! Audit log — hash-chained, Rust-instrumented.
//!
//! Every write that passes through `ops.rs` invokes
//! [`audit_write`] after the subject mutation succeeds. The helper:
//!
//! 1. Reads the most-recent [`AuditEvent`]'s `hash` from the DB —
//!    that becomes the new row's `prev_hash`.
//! 2. Builds a `{uid, actor, action, target, outcome, at}`
//!    canonical JSON payload.
//! 3. Computes `sha256(prev_hash || canonical_json)` as hex.
//! 4. Writes the event through the Raft-backed `Writer`, so every
//!    node applies the same record + hash.
//!
//! Concurrent writers can race on step 1 and end up sharing a
//! predecessor — the resulting chain becomes a DAG rather than a
//! strict line. That's acceptable for IAM-5: each individual hash
//! still locks its claimed `prev_hash`, so flipping a bit in any
//! event invalidates that event's hash (and every descendant of
//! the forged value). Full single-writer ordering is an IAM-9
//! governance concern.

use nauka_core::resource::{Datetime, Ref};
use nauka_state::{Database, RaftNode, Writer};
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};
use surrealdb::types::SurrealValue;

use crate::definition::{AuditEvent, Org};
use crate::error::IamError;

/// Placeholder `prev_hash` for the very first event in a database's
/// chain. Same width as a SHA-256 hex digest so everything else in
/// the schema stays uniform.
const GENESIS_PREV_HASH: &str = "0000000000000000000000000000000000000000000000000000000000000000";

/// Best-effort audit emission for callers that don't want an audit
/// failure to roll back the subject mutation. Logs the error via
/// `tracing::warn!` with a stable event name so it surfaces in
/// journalctl even when the caller swallows the `Result`.
pub async fn try_audit(
    db: &Database,
    raft: &RaftNode,
    actor: &str,
    action: &str,
    target: &str,
    org: Option<&str>,
    outcome: &str,
) {
    if let Err(e) = audit_write(db, raft, actor, action, target, org, outcome).await {
        tracing::warn!(
            event = "iam.audit.write_failed",
            actor = %actor,
            action = %action,
            target = %target,
            error = %e,
            "audit event write failed — subject mutation already committed"
        );
    }
}

/// Record an audit event after `subject_write` has already
/// committed. The audit row is Raft-replicated itself, so followers
/// see it alongside the subject mutation.
///
/// `actor` is the full record-id string of the principal
/// (`user:alice@example.com` or `service_account:acme-ci`) —
/// pass in `AuthContext::principal_record_id()` or equivalent.
/// `target` is the record-id string of the subject row, e.g.
/// `org:acme` or `role_binding:<uid>`.
pub async fn audit_write(
    db: &Database,
    raft: &RaftNode,
    actor: &str,
    action: &str,
    target: &str,
    org: Option<&str>,
    outcome: &str,
) -> Result<AuditEvent, IamError> {
    let prev_hash = fetch_latest_hash(db).await?;
    let uid = new_uid();
    let at = Datetime::now();
    let payload = canonical_json(&uid, actor, action, target, outcome, &at.to_string());
    let hash = compute_hash(&prev_hash, &payload);
    let event = AuditEvent {
        uid,
        actor: actor.to_string(),
        action: action.to_string(),
        target: target.to_string(),
        org: org.map(|s| Ref::<Org>::new(s.to_string())),
        outcome: outcome.to_string(),
        prev_hash,
        hash,
        at,
        created_at: at,
        updated_at: at,
        version: 0,
    };
    Writer::new(db)
        .with_raft(raft)
        .create(&event)
        .await
        .map_err(IamError::State)?;
    Ok(event)
}

/// Return the most recent event's `hash`, or the genesis
/// placeholder if the log is empty. Sort is newest-first by `uid`
/// — the ULID-style id prefix is monotonic-enough for this purpose
/// (within the resolution of the timestamp portion).
async fn fetch_latest_hash(db: &Database) -> Result<String, IamError> {
    #[derive(Deserialize, SurrealValue)]
    struct HashRow {
        hash: String,
        // Included only to satisfy SurrealDB 3's rule that fields
        // used by `ORDER BY` must appear in the projection.
        // Deserialised but not read by the caller.
        #[allow(dead_code)]
        uid: String,
    }
    // SurrealDB 3 rejects `ORDER BY <field>` when the projection
    // doesn't include that field — hence `hash, uid` here even
    // though we only need `hash` back.
    let rows: Vec<HashRow> = db
        .query_take("SELECT hash, uid FROM audit_event ORDER BY uid DESC LIMIT 1")
        .await
        .map_err(IamError::State)?;
    Ok(rows
        .into_iter()
        .next()
        .map(|r| r.hash)
        .unwrap_or_else(|| GENESIS_PREV_HASH.to_string()))
}

/// Deterministic JSON of the hashed fields — serde_json sorts keys
/// alphabetically for us when we build from a `serde_json::Value`
/// assembled from an ordered `Map`, but the safer path is to hand
/// a `json!({})` macro the fields in a fixed order and use
/// `serde_json::to_string` which, for object literals, preserves
/// insertion order. That's what [`canonical_json`] does — the
/// specific order is not `Serialize` derived, it is this function's
/// contract.
fn canonical_json(
    uid: &str,
    actor: &str,
    action: &str,
    target: &str,
    outcome: &str,
    at: &str,
) -> String {
    // Fixed key order — changing it would invalidate every stored
    // hash. Keep alphabetical so code readers + auditors don't have
    // to remember a convention.
    let v = json!({
        "action": action,
        "actor": actor,
        "at": at,
        "outcome": outcome,
        "target": target,
        "uid": uid,
    });
    serde_json::to_string(&v).expect("canonical_json must serialize")
}

fn compute_hash(prev_hash: &str, payload: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(prev_hash.as_bytes());
    hasher.update(payload.as_bytes());
    let bytes = hasher.finalize();
    let mut out = String::with_capacity(64);
    for b in bytes {
        out.push_str(&format!("{b:02x}"));
    }
    out
}

/// 24-char id `<12 hex ms-since-epoch><12 hex random>`. Sortable by
/// prefix (newest has the largest ms value), safe for a record-id
/// payload. Not a full ULID — our needs are narrower and the
/// shorter form keeps audit_event:⟨…⟩ literals short in logs.
fn new_uid() -> String {
    use argon2::password_hash::rand_core::{OsRng, RngCore};
    let ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    let mut rand = [0u8; 6];
    OsRng.fill_bytes(&mut rand);
    let mut out = format!("{ms:012x}");
    for b in rand {
        out.push_str(&format!("{b:02x}"));
    }
    out
}

/// Read the chain newest-first. Reading is open to any
/// authenticated caller for IAM-5; IAM-6 narrows visibility.
pub async fn list_audit(db: &Database, limit: usize) -> Result<Vec<AuditEvent>, IamError> {
    let q = format!("SELECT * FROM audit_event ORDER BY uid DESC LIMIT {limit}");
    db.query_take(&q).await.map_err(IamError::State)
}

#[cfg(test)]
pub(crate) fn genesis_prev_hash() -> &'static str {
    GENESIS_PREV_HASH
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_json_is_stable() {
        // Two identical inputs → same JSON byte-for-byte.
        let a = canonical_json("u1", "alice", "create", "org:acme", "success", "2026-04-18");
        let b = canonical_json("u1", "alice", "create", "org:acme", "success", "2026-04-18");
        assert_eq!(a, b);
        // The field order is fixed in the serialized output.
        assert!(a.starts_with("{\"action\":"), "unexpected key order: {a}");
    }

    #[test]
    fn hash_changes_when_any_field_changes() {
        let payload = canonical_json("u1", "alice", "create", "org:acme", "success", "2026-04-18");
        let h = compute_hash(genesis_prev_hash(), &payload);

        // Different actor → different hash.
        let payload2 = canonical_json("u1", "bob", "create", "org:acme", "success", "2026-04-18");
        let h2 = compute_hash(genesis_prev_hash(), &payload2);
        assert_ne!(h, h2);

        // Different prev_hash → different hash, even with identical
        // payload (the chain link is what lets us detect tampering
        // with earlier events).
        let h3 = compute_hash("deadbeef", &payload);
        assert_ne!(h, h3);
    }

    #[test]
    fn hash_is_hex_sha256() {
        let h = compute_hash(genesis_prev_hash(), "{}");
        assert_eq!(h.len(), 64);
        assert!(h.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn uids_are_unique_and_sortable() {
        let a = new_uid();
        std::thread::sleep(std::time::Duration::from_millis(2));
        let b = new_uid();
        assert_ne!(a, b);
        // Newer uid should sort greater than older.
        assert!(b > a, "expected {b} > {a}");
        assert_eq!(a.len(), 24);
    }
}
