//! Normalized error helpers for resource handlers.
//!
//! All handlers should use these instead of raw `anyhow::anyhow!()` strings
//! to ensure consistent error messages across the CLI and API.

/// Resource not found.
/// Produces: `vm 'web-1' not found`
pub fn not_found(kind: &str, name: &str) -> anyhow::Error {
    anyhow::anyhow!("{kind} '{name}' not found")
}

/// Resource already exists.
/// Produces: `vm 'web-1' already exists`
pub fn already_exists(kind: &str, name: &str) -> anyhow::Error {
    anyhow::anyhow!("{kind} '{name}' already exists")
}

/// Required scope flag is missing.
/// Produces: `--org is required`
pub fn missing_scope(flag: &str) -> anyhow::Error {
    anyhow::anyhow!("{flag} is required")
}

/// Positional name argument is missing.
/// Produces: `missing name`
pub fn missing_name() -> anyhow::Error {
    anyhow::anyhow!("missing name")
}

/// Required field is missing.
/// Produces: `--cpu is required`
pub fn missing_field(flag: &str) -> anyhow::Error {
    anyhow::anyhow!("{flag} is required")
}

/// Field has an invalid value.
/// Produces: `--cpu must be a number`
pub fn invalid_field(flag: &str, expected: &str) -> anyhow::Error {
    anyhow::anyhow!("{flag} must be {expected}")
}

/// Invalid state transition.
/// Produces: `vm cannot transition from Stopped to Deleted`
pub fn invalid_state_transition(kind: &str, from: &str, to: &str) -> anyhow::Error {
    anyhow::anyhow!("{kind} cannot transition from {from} to {to}")
}

/// Resource is in wrong state for the requested operation.
/// Produces: `vm 'web-1' must be Stopped before delete (current state: Running)`
pub fn wrong_state(kind: &str, name: &str, required: &str, actual: &str) -> anyhow::Error {
    anyhow::anyhow!(
        "{kind} '{name}' must be {required} before this operation (current state: {actual})"
    )
}
