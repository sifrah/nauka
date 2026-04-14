-- ============================================================================
-- Nauka — cluster state schema: iam
-- ============================================================================
-- Target: nauka / cluster (SurrealDB on top of kv-tikv)
--
-- Per-org IAM configuration root. Placeholder table for the identity &
-- access management grouping under `layers/org/src/iam/`. Concrete IAM
-- resources (user, future: role, policy, service-account, ...) live
-- underneath this as nested modules with their own `definition.surql`.
--
-- For now this table just tracks per-org IAM-level metadata (timestamps).
-- Future fields will capture IAM features enabled per org: SSO provider,
-- MFA requirements, session TTL, default roles, etc.
--
-- Idempotency: every DEFINE uses `IF NOT EXISTS` so applying this file
-- against an already-initialised cluster is a no-op.
-- ============================================================================

DEFINE TABLE IF NOT EXISTS iam SCHEMAFULL;

-- Org this IAM config scopes to. References `org` records by their
-- ULID-prefixed id string.
DEFINE FIELD IF NOT EXISTS org ON iam TYPE string
    ASSERT string::len($value) > 0;

-- Creation + last-update timestamps.
DEFINE FIELD IF NOT EXISTS created_at ON iam TYPE datetime;
DEFINE FIELD IF NOT EXISTS updated_at ON iam TYPE datetime;

-- One IAM config per org.
DEFINE INDEX IF NOT EXISTS iam_org ON iam FIELDS org UNIQUE;
