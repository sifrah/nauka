//! Shared SurrealDB SDK → Rust bridge helpers used by the org-layer stores.
//!
//! Every store in this crate talks to the cluster database through the
//! `EmbeddedDb::client()` SDK handle and the JSON-bridge pattern
//! established by `nauka_hypervisor::fabric::state::FabricState::save`:
//! write via an explicit `CREATE … SET …` statement, read via
//! `SELECT * FROM …`, and bounce the row through `serde_json::Value` so
//! the Rust side doesn't need a `surrealdb::types::SurrealValue` derive
//! cascade over every resource type in the workspace.
//!
//! The helpers in this module are the small amount of glue every store
//! needs for that pattern:
//!
//! - [`thing_to_id_string`] extracts the bare `<table>-<ulid>` id from
//!   the `Thing` value that SurrealDB hands back on `SELECT`.
//! - [`iso8601_to_epoch`] parses the RFC 3339 `datetime` strings that
//!   SurrealDB emits back into the `u64` Unix-epoch seconds that
//!   [`nauka_core::resource::ResourceMeta`] carries.
//! - [`classify_create_error`] maps a surrealdb-side "unique index
//!   already contains …" error into a human-friendly
//!   `"<kind> '<name>' already exists"` flat `anyhow::Error` so every
//!   store produces the same CLI-facing message shape.
//!
//! P2.9 (sifrah/nauka#213) introduced the first copies of these helpers
//! inline in `crate::store` (the org store). P2.10 (sifrah/nauka#214)
//! extracted them here so the newly-migrated project store can reuse
//! them without drift.

/// Pull the bare id string out of a SurrealDB `Thing` rendered as
/// `serde_json::Value`.
///
/// SurrealDB JSON-encodes a `Thing` as one of several shapes depending
/// on the SDK version and the variant of the inner id:
///
/// 1. `"org:01J…"` or `` "org:`01J…`" `` — a flat string when the id
///    round-tripped through a SurrealQL response that was already
///    strings-only. SurrealDB wraps the id in backticks when the raw
///    id contains any character outside the unquoted-identifier set
///    (the `-` in our `org-<ulid>` form is the usual culprit).
/// 2. `{"tb": "org", "id": "01J…"}` — the structured form for a
///    `String` id.
/// 3. `{"tb": "org", "id": {"String": "01J…"}}` — the structured form
///    when SurrealDB tagged the id variant explicitly.
///
/// `table_prefix` is the `"<table>:"` string used by Shape 1. For the
/// org store pass `"org:"`, for the project store `"project:"`, and so
/// on.
///
/// Always returns the bare `<table>-<ulid>` id (without the `tb:`
/// prefix or any wrapping backticks) because that's what the rest of
/// Nauka has been carrying around as `ResourceMeta::id` since day one.
pub(crate) fn thing_to_id_string(table_prefix: &str, value: &serde_json::Value) -> String {
    let trim_backticks = |s: &str| s.trim_start_matches('`').trim_end_matches('`').to_string();

    if let Some(s) = value.as_str() {
        // Shape 1: flat "org:01J…" string. Strip the "<table>:" prefix
        // if present so the result matches the legacy `<kind>-<ulid>`
        // form, then strip any wrapping backticks SurrealDB added
        // because the id contains a hyphen.
        let without_prefix = s.strip_prefix(table_prefix).unwrap_or(s);
        return trim_backticks(without_prefix);
    }
    if let Some(obj) = value.as_object() {
        // Shapes 2 and 3 carry the id under the `id` key. Shape 3
        // wraps the inner id under `String`.
        if let Some(inner) = obj.get("id") {
            if let Some(s) = inner.as_str() {
                return trim_backticks(s);
            }
            if let Some(inner_obj) = inner.as_object() {
                if let Some(s) = inner_obj.get("String").and_then(|v| v.as_str()) {
                    return trim_backticks(s);
                }
            }
        }
    }
    // Fallback: dump the JSON form so the caller at least sees what
    // SurrealDB actually returned. The `{}` Display for serde_json::Value
    // is the JSON encoding.
    value.to_string()
}

/// Parse an ISO 8601 / RFC 3339 datetime back into Unix-epoch seconds.
///
/// SurrealDB renders `datetime` values as strings like
/// `2024-01-02T03:04:05.123456Z`; we round down to whole seconds
/// because [`nauka_core::resource::ResourceMeta::created_at`] is a
/// `u64` with second granularity. Any parse failure (e.g. an
/// unexpected timezone offset) returns `0` so the caller still gets a
/// value back — the alternative would be to fail a whole list/get path
/// on a single malformed row, which is strictly worse for operators
/// trying to clean up bad data.
pub(crate) fn iso8601_to_epoch(s: &str) -> u64 {
    // Stripped-down parser: YYYY-MM-DDTHH:MM:SS… — accepts a trailing
    // `Z`, an optional fractional seconds part, and an optional
    // timezone offset (which we ignore — the only writers are the
    // org-layer stores, and they always emit `Z`).
    let bytes = s.as_bytes();
    if bytes.len() < 19 {
        return 0;
    }
    let year: i64 = std::str::from_utf8(&bytes[0..4])
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let month: i64 = std::str::from_utf8(&bytes[5..7])
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let day: i64 = std::str::from_utf8(&bytes[8..10])
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let hour: i64 = std::str::from_utf8(&bytes[11..13])
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let minute: i64 = std::str::from_utf8(&bytes[14..16])
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let second: i64 = std::str::from_utf8(&bytes[17..19])
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let days = days_from_civil(year, month, day);
    let total = days * 86400 + hour * 3600 + minute * 60 + second;
    if total < 0 {
        0
    } else {
        total as u64
    }
}

/// Howard Hinnant's "days from civil" algorithm — converts a
/// `(year, month, day)` triple to a count of days since 1970-01-01.
///
/// Lifted directly from the public-domain reference implementation
/// (<https://howardhinnant.github.io/date_algorithms.html#days_from_civil>).
/// We use it instead of a `chrono` / `time` dependency to keep
/// `nauka-org` build-time small and to mirror the existing date math
/// already in `nauka_core::resource::api_response::days_to_date`, which
/// goes the other direction.
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

/// Map a SurrealDB error message from a `CREATE` into a flat
/// `anyhow::Error` with a human-friendly message.
///
/// Each cluster-state store has a unique index on its natural key
/// (e.g. `org_name`, `project_org_name`) that surfaces duplicate
/// inserts as `surrealdb::Error::already_exists`, whose `Display`
/// rendering includes the phrase "already contains" (the substring is
/// stable across surrealdb-types releases and is what the
/// `From<surrealdb::Error>` impl in `nauka_state::lib.rs` documents as
/// the `is_already_exists()` signal). We key on that substring rather
/// than re-introducing a direct `surrealdb` dependency in this crate,
/// so duplicate-name conflicts still surface as the same
/// `<kind> '<name>' already exists` wording every store returned
/// before the SurrealDB migration — CLI error-message tests downstream
/// keep passing without edits.
///
/// `kind` is the resource noun used in the message (`"org"`,
/// `"project"`, `"environment"`, …). `name` is the human-readable name
/// that was rejected. Everything else collapses to the underlying
/// error text verbatim.
pub(crate) fn classify_create_error(kind: &str, name: &str, err_msg: &str) -> anyhow::Error {
    let lowered = err_msg.to_lowercase();
    if lowered.contains("already contains")
        || lowered.contains("already exists")
        || lowered.contains("duplicate")
    {
        anyhow::anyhow!("{kind} '{name}' already exists")
    } else {
        anyhow::anyhow!("{err_msg}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nauka_core::resource::epoch_to_iso8601;

    #[test]
    fn thing_to_id_string_flat_string_with_prefix() {
        let v = serde_json::Value::String("org:org-01J00000000000000000000000".into());
        assert_eq!(
            thing_to_id_string("org:", &v),
            "org-01J00000000000000000000000"
        );
    }

    #[test]
    fn thing_to_id_string_flat_string_with_backticks() {
        let v = serde_json::Value::String("org:`org-01J00000000000000000000000`".into());
        assert_eq!(
            thing_to_id_string("org:", &v),
            "org-01J00000000000000000000000"
        );
    }

    #[test]
    fn thing_to_id_string_structured_string_variant() {
        let v = serde_json::json!({ "tb": "project", "id": "project-01J…" });
        assert_eq!(thing_to_id_string("project:", &v), "project-01J…");
    }

    #[test]
    fn thing_to_id_string_structured_tagged_string() {
        let v = serde_json::json!({
            "tb": "project",
            "id": { "String": "project-01J…" }
        });
        assert_eq!(thing_to_id_string("project:", &v), "project-01J…");
    }

    #[test]
    fn iso8601_round_trip_matches_epoch() {
        for &epoch in &[0u64, 1, 86_399, 86_400, 1_700_000_000, 1_775_665_838] {
            let iso = epoch_to_iso8601(epoch);
            let back = iso8601_to_epoch(&iso);
            assert_eq!(back, epoch, "round trip failed for {epoch}: iso={iso}");
        }
    }

    #[test]
    fn iso8601_too_short_returns_zero() {
        assert_eq!(iso8601_to_epoch("2024"), 0);
        assert_eq!(iso8601_to_epoch(""), 0);
    }

    #[test]
    fn classify_create_error_detects_duplicate() {
        let err = classify_create_error(
            "project",
            "web",
            "database contains already contains an entry for `web`",
        );
        assert_eq!(err.to_string(), "project 'web' already exists");
    }

    #[test]
    fn classify_create_error_passes_through_unrelated() {
        let err = classify_create_error("project", "web", "disk full");
        assert_eq!(err.to_string(), "disk full");
    }
}
