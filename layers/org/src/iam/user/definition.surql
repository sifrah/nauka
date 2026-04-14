-- ============================================================================
-- Nauka — cluster state schema: user
-- ============================================================================
-- Target: nauka / cluster (SurrealDB on top of kv-tikv)
--
-- Distributed cluster-state representation of an authenticated Nauka
-- user. Unlike the other schemas in this ticket, P2.5 (sifrah/nauka#209)
-- is the **first** schema for this resource: there is no corresponding
-- Rust type in the tree yet (`grep -r "struct User" layers/` returns
-- nothing as of commit cdffcddf). The acceptance criteria for P2.5
-- still require a user schema, so this file ships a minimal, forward-
-- compatible definition that P3 codegen can grow as the authentication
-- subsystem lands.
--
-- Idempotency: every DEFINE statement uses `IF NOT EXISTS` so re-applying
-- the file against an initialised cluster database is a no-op. New
-- fields added later — password hash, MFA settings, SSH keys, API
-- tokens, tenant memberships, etc. — land as additional
-- `DEFINE FIELD … IF NOT EXISTS` statements without breaking existing
-- rows.
--
-- Multi-tenancy: a user is **not** org-scoped at this layer — users are
-- global identities and their membership in orgs is modelled elsewhere
-- (probably a separate `membership` or `org_user` edge table once the
-- authentication issue lands). If the eventual design makes users
-- tenant-scoped, the schema gains an `org` column and the unique index
-- changes to `(org, email)`; every DEFINE here still uses IF NOT
-- EXISTS, so that migration is forward-compatible.
-- ============================================================================


-- ── user ─────────────────────────────────────────────────────────────────
-- A single identity that can authenticate against Nauka. Keyed by the
-- SurrealDB record id (a ULID-prefixed string like `user:01j…`, the
-- same convention as every other resource in this tree). `email` is the
-- human-visible natural key.

DEFINE TABLE IF NOT EXISTS user SCHEMAFULL;

-- Login email. Unique across the cluster — enforced by `user_email`
-- below. The ASSERT is a minimum length sanity check, not a full RFC
-- 5321 validator; full format validation lives in the application layer
-- where it can emit a useful error message.
DEFINE FIELD IF NOT EXISTS email ON user TYPE string
    ASSERT string::len($value) > 0;

-- Human-readable display name. Optional because early-stage accounts
-- may only carry the email; the generated handler will default it to
-- the local-part of the email when callers omit it.
DEFINE FIELD IF NOT EXISTS display_name ON user TYPE option<string>;

-- Resource lifecycle status ("active", "suspended", "deleting", …).
-- Mirrors the `status` field used by every other cluster resource.
DEFINE FIELD IF NOT EXISTS status ON user TYPE string
    ASSERT string::len($value) > 0;

-- User-defined labels — same semantics as on `org`.
DEFINE FIELD IF NOT EXISTS labels ON user TYPE object DEFAULT {};

-- Creation timestamp.
DEFINE FIELD IF NOT EXISTS created_at ON user TYPE datetime;

-- Last-update timestamp.
DEFINE FIELD IF NOT EXISTS updated_at ON user TYPE datetime;

-- Unique index on `email` so (a) login-by-email is O(1) and (b) two
-- accounts can never share the same email. The uniqueness guarantee is
-- enforced at the schema level so application-side "check then insert"
-- races can't produce duplicate emails.
DEFINE INDEX IF NOT EXISTS user_email ON user FIELDS email UNIQUE;
