//! Daemon-side CRUD for `Org` / `Project` / `Env` — the surface the
//! IPC handlers and integration tests call into.
//!
//! ## Session discipline
//!
//! Every entry point that needs `$auth` takes the full JWT and runs
//! the pattern `authenticate → extract-email → invalidate`. The
//! critical section is serialised by [`IPC_LOCK`] because the
//! embedded `Surreal<Db>` handle shares one session across the whole
//! process — without the lock, two concurrent IPC calls would stomp
//! on each other's authentication state.
//!
//! Writes that have to replicate (cluster-scoped resources) call
//! `Writer::create` *after* invalidating the user session, so the
//! Raft state machine always applies queries as root (no PERMISSIONS
//! enforcement on the apply path). Authorization is instead enforced
//! in Rust against the decoded `$auth.id` before the record is
//! ever proposed.
//!
//! Reads that need PERMISSIONS filtering (Org list, etc.) keep the
//! session authenticated across the query, then invalidate once
//! rows are back. That's the part that actually exercises
//! `fn::iam::can`.

use nauka_core::resource::{Datetime, Ref, ResourceOps};
use nauka_state::{Database, RaftNode, Writer};
use tokio::sync::Mutex;
use tracing::instrument;

use crate::definition::{ApiToken, Env, Org, Project, Role, RoleBinding, ServiceAccount, User};
use crate::error::IamError;

/// Process-wide lock around any operation that mutates the shared
/// `Surreal<Db>::authenticate` / `invalidate` session state. Every
/// signin, signup, authenticate, and PERMISSIONS-filtered read
/// grabs it; every session flip is paired with an invalidate before
/// the lock drops, so the daemon's shared handle is always back to
/// the root (sessionless) state when no IPC is in flight.
///
/// This matters because background tasks (`reconciler`,
/// `refresh_own_endpoint`) query the `hypervisor` table without
/// authenticating. If a leaked signed-in session from an earlier
/// IPC request were still active, those queries would run as that
/// user — and SurrealDB 3.x's default permissions on a SCHEMAFULL
/// table hide rows from record-level sessions, which cascades into
/// the reconciler "seeing" an empty cluster and tearing down
/// WireGuard peers.
///
/// Why not per-request sessions? SurrealDB 3.x embeds a single
/// session per `Surreal<C>` handle, and the embedded SurrealKV
/// engine holds an exclusive file lock so we can't open a second
/// handle. IAM-2 accepts this serialization; later phases will
/// either promote sessions to per-request or move the daemon onto
/// SurrealDB's client SDK for real multi-session support.
pub(crate) static IPC_LOCK: Mutex<()> = Mutex::const_new(());

/// Record holding what the CLI / REST layer needs to know about the
/// caller — derived from the DB's `$auth` after
/// `authenticate(jwt)`. The email doubles as the `User.email` /
/// `#[id]`, so it is the record-id payload for `user:⟨…⟩`.
#[derive(Debug, Clone)]
pub struct AuthContext {
    pub email: String,
}

/// Validate a JWT and recover the caller's email.
///
/// Splits the job: the **signature** is verified by
/// `Surreal::authenticate`, which throws if the token is forged /
/// expired / issued by a different signing key; the **claims** are
/// decoded locally so we don't have to `SELECT ... FROM $auth`.
/// That query would hit the `user` table's default PERMISSIONS,
/// which in SurrealDB 3.x block a record-level session from reading
/// even its own row — the same reason the reconciler saw an empty
/// hypervisor table when a stale session leaked through.
///
/// The session is invalidated before *and* after the authenticate
/// call so the daemon's shared handle never holds onto the caller's
/// identity outside this critical section. See
/// [`IPC_LOCK`]'s docstring for why that matters.
#[instrument(name = "iam.authenticate", skip_all)]
pub async fn authenticate(db: &Database, jwt: &str) -> Result<AuthContext, IamError> {
    // Decode claims first — a malformed JWT fails here with a
    // specific parse error before we ever touch the DB.
    let claims = crate::auth::decode_claims(jwt)?;
    let email = claims.email().ok_or_else(|| {
        IamError::Jwt(format!(
            "JWT has no `ID` claim or it does not name a user record: {:?}",
            claims.id
        ))
    })?;

    let _guard = IPC_LOCK.lock().await;
    let _ = db.inner().invalidate().await;
    db.inner()
        .authenticate(jwt.to_string())
        .await
        .map_err(|_| IamError::InvalidCredentials)?;
    let _ = db.inner().invalidate().await;
    Ok(AuthContext { email })
}

/// Run a PERMISSIONS-aware read against `db` as the caller
/// identified by `jwt`. The session is authenticated for the life
/// of the query only, then invalidated. Serialized by [`IPC_LOCK`].
async fn read_as<T, F, Fut>(db: &Database, jwt: &str, op: F) -> Result<T, IamError>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<T, IamError>>,
{
    let _guard = IPC_LOCK.lock().await;
    let _ = db.inner().invalidate().await;
    db.inner()
        .authenticate(jwt.to_string())
        .await
        .map_err(|_| IamError::InvalidCredentials)?;
    let result = op().await;
    let _ = db.inner().invalidate().await;
    result
}

fn now_pair() -> (Datetime, Datetime) {
    let n = Datetime::now();
    (n, n)
}

// -------- Org --------

/// Create an `Org` owned by the caller (the JWT's user). Replicates
/// via Raft. The `owner` field is forced to `$auth.id` — callers
/// never set it.
#[instrument(name = "iam.org.create", skip_all, fields(slug = %slug))]
pub async fn create_org(
    db: &Database,
    raft: &RaftNode,
    jwt: &str,
    slug: &str,
    display_name: &str,
) -> Result<Org, IamError> {
    let auth = authenticate(db, jwt).await?;
    validate_slug(slug)?;
    let (c, u) = now_pair();
    let org = Org {
        slug: slug.to_string(),
        display_name: display_name.to_string(),
        owner: Ref::<User>::new(auth.email),
        created_at: c,
        updated_at: u,
        version: 0,
    };
    match Writer::new(db).with_raft(raft).create(&org).await {
        Ok(()) => Ok(org),
        Err(e) => {
            let s = e.to_string();
            if s.contains("already exists") || s.contains("Database record") {
                Err(IamError::AlreadyExists(format!("org:{slug}")))
            } else {
                Err(IamError::State(e))
            }
        }
    }
}

/// List orgs the caller can see. Goes through the PERMISSIONS path,
/// so the visibility rule is whatever the Org table's `permissions`
/// clause says — here: `$this.owner = $auth.id`.
#[instrument(name = "iam.org.list", skip_all)]
pub async fn list_orgs(db: &Database, jwt: &str) -> Result<Vec<Org>, IamError> {
    read_as(db, jwt, || async {
        db.query_take(&Org::list_query())
            .await
            .map_err(IamError::State)
    })
    .await
}

// -------- Project --------

#[instrument(name = "iam.project.create", skip_all, fields(org = %org_slug, slug = %slug))]
pub async fn create_project(
    db: &Database,
    raft: &RaftNode,
    jwt: &str,
    org_slug: &str,
    slug: &str,
    display_name: &str,
) -> Result<Project, IamError> {
    let _auth = authenticate(db, jwt).await?;
    validate_slug(slug)?;
    // Scoped uid so we can live with a flat `#[id]` column while
    // the (org, slug) pair is what humans care about. `project:`
    // prefix kept out of the stored value — SurrealDB adds the
    // table prefix itself at the record-id layer.
    let uid = format!("{org_slug}-{slug}");
    let (c, u) = now_pair();
    let project = Project {
        uid: uid.clone(),
        slug: slug.to_string(),
        org: Ref::<Org>::new(org_slug.to_string()),
        display_name: display_name.to_string(),
        created_at: c,
        updated_at: u,
        version: 0,
    };
    match Writer::new(db).with_raft(raft).create(&project).await {
        Ok(()) => Ok(project),
        Err(e) => {
            let s = e.to_string();
            if s.contains("already exists") || s.contains("Database record") {
                Err(IamError::AlreadyExists(format!("project:{uid}")))
            } else {
                Err(IamError::State(e))
            }
        }
    }
}

#[instrument(name = "iam.project.list", skip_all)]
pub async fn list_projects(db: &Database, jwt: &str) -> Result<Vec<Project>, IamError> {
    read_as(db, jwt, || async {
        db.query_take(&Project::list_query())
            .await
            .map_err(IamError::State)
    })
    .await
}

// -------- Env --------

#[instrument(name = "iam.env.create", skip_all, fields(project = %project_uid, slug = %slug))]
pub async fn create_env(
    db: &Database,
    raft: &RaftNode,
    jwt: &str,
    project_uid: &str,
    slug: &str,
    display_name: &str,
) -> Result<Env, IamError> {
    let _auth = authenticate(db, jwt).await?;
    validate_slug(slug)?;
    let uid = format!("{project_uid}-{slug}");
    let (c, u) = now_pair();
    let env = Env {
        uid: uid.clone(),
        slug: slug.to_string(),
        project: Ref::<Project>::new(project_uid.to_string()),
        display_name: display_name.to_string(),
        created_at: c,
        updated_at: u,
        version: 0,
    };
    match Writer::new(db).with_raft(raft).create(&env).await {
        Ok(()) => Ok(env),
        Err(e) => {
            let s = e.to_string();
            if s.contains("already exists") || s.contains("Database record") {
                Err(IamError::AlreadyExists(format!("env:{uid}")))
            } else {
                Err(IamError::State(e))
            }
        }
    }
}

#[instrument(name = "iam.env.list", skip_all)]
pub async fn list_envs(db: &Database, jwt: &str) -> Result<Vec<Env>, IamError> {
    read_as(db, jwt, || async {
        db.query_take(&Env::list_query())
            .await
            .map_err(IamError::State)
    })
    .await
}

// -------- Role / RoleBinding (IAM-3) --------

/// List every role visible to the caller. Primitive / predefined
/// roles are globally readable; custom roles are filtered by the
/// table's PERMISSIONS clause (owner of the role's org only).
#[instrument(name = "iam.role.list", skip_all)]
pub async fn list_roles(db: &Database, jwt: &str) -> Result<Vec<Role>, IamError> {
    read_as(db, jwt, || async {
        db.query_take(&Role::list_query())
            .await
            .map_err(IamError::State)
    })
    .await
}

/// Attach an existing role to a principal at an Org scope.
/// Authorization: only the Org's owner (as enforced by the
/// `role_binding` table's `scope_by = "org"` PERMISSIONS) can
/// establish new bindings.
#[instrument(
    name = "iam.role.bind",
    skip_all,
    fields(principal = %principal_email, role = %role_slug, org = %org_slug)
)]
pub async fn bind_role(
    db: &Database,
    raft: &RaftNode,
    jwt: &str,
    principal_email: &str,
    role_slug: &str,
    org_slug: &str,
) -> Result<RoleBinding, IamError> {
    let _auth = authenticate(db, jwt).await?;
    // Synthetic uid keeps `(org, principal, role)` unique and makes
    // the record id greppable in SurrealDB record form
    // (`role_binding:⟨acme-bob@example.com-viewer⟩`).
    let uid = format!("{org_slug}-{principal_email}-{role_slug}");
    let (c, u) = now_pair();
    let binding = RoleBinding {
        uid: uid.clone(),
        principal: Ref::<User>::new(principal_email.to_string()),
        role: Ref::<Role>::new(role_slug.to_string()),
        org: Ref::<Org>::new(org_slug.to_string()),
        created_at: c,
        updated_at: u,
        version: 0,
    };
    match Writer::new(db).with_raft(raft).create(&binding).await {
        Ok(()) => Ok(binding),
        Err(e) => {
            let s = e.to_string();
            if s.contains("already exists") || s.contains("Database record") {
                Err(IamError::AlreadyExists(format!("role_binding:{uid}")))
            } else {
                Err(IamError::State(e))
            }
        }
    }
}

/// Remove a role binding previously created with [`bind_role`].
/// Authorization follows the same `scope_by = "org"` rule as
/// creation.
#[instrument(
    name = "iam.role.unbind",
    skip_all,
    fields(principal = %principal_email, role = %role_slug, org = %org_slug)
)]
pub async fn unbind_role(
    db: &Database,
    raft: &RaftNode,
    jwt: &str,
    principal_email: &str,
    role_slug: &str,
    org_slug: &str,
) -> Result<(), IamError> {
    let _auth = authenticate(db, jwt).await?;
    let uid = format!("{org_slug}-{principal_email}-{role_slug}");
    Writer::new(db)
        .with_raft(raft)
        .delete::<RoleBinding>(&uid)
        .await
        .map_err(IamError::State)?;
    Ok(())
}

#[instrument(name = "iam.role.list_bindings", skip_all)]
pub async fn list_bindings(db: &Database, jwt: &str) -> Result<Vec<RoleBinding>, IamError> {
    read_as(db, jwt, || async {
        db.query_take(&RoleBinding::list_query())
            .await
            .map_err(IamError::State)
    })
    .await
}

// -------- Service accounts + API tokens (IAM-4) --------

#[instrument(name = "iam.sa.create", skip_all, fields(slug = %slug, org = %org_slug))]
pub async fn create_service_account(
    db: &Database,
    raft: &RaftNode,
    jwt: &str,
    org_slug: &str,
    slug: &str,
    display_name: &str,
) -> Result<ServiceAccount, IamError> {
    let _auth = authenticate(db, jwt).await?;
    validate_slug(slug)?;
    // `<org>-<slug>` keeps SA record ids globally unique while the
    // operator-facing slug stays per-org. Same pattern Project and
    // Env use.
    let scoped = format!("{org_slug}-{slug}");
    let (c, u) = now_pair();
    let sa = ServiceAccount {
        slug: scoped.clone(),
        display_name: display_name.to_string(),
        org: Ref::<Org>::new(org_slug.to_string()),
        created_at: c,
        updated_at: u,
        version: 0,
    };
    match Writer::new(db).with_raft(raft).create(&sa).await {
        Ok(()) => Ok(sa),
        Err(e) => {
            let s = e.to_string();
            if s.contains("already exists") || s.contains("Database record") {
                Err(IamError::AlreadyExists(format!("service_account:{scoped}")))
            } else {
                Err(IamError::State(e))
            }
        }
    }
}

#[instrument(name = "iam.sa.list", skip_all)]
pub async fn list_service_accounts(
    db: &Database,
    jwt: &str,
) -> Result<Vec<ServiceAccount>, IamError> {
    read_as(db, jwt, || async {
        db.query_take(&ServiceAccount::list_query())
            .await
            .map_err(IamError::State)
    })
    .await
}

/// Result of minting a new API token. The `plaintext` field is the
/// only place the secret ever appears — the daemon does not store
/// it and the CLI is expected to display it once and discard.
pub struct MintedToken {
    pub plaintext: String,
    pub record: ApiToken,
}

/// Create a new API token for a service account. The plaintext
/// token follows the Stripe-inspired `nk_live_<id>_<secret>` shape:
/// `id` is a 16-char URL-safe random string stored as the record's
/// primary key, `secret` is 48 URL-safe chars hashed via Argon2id
/// and stored as `hash`. The full plaintext is returned *once* via
/// [`MintedToken::plaintext`]; anyone wanting a second copy must
/// revoke and re-mint.
#[instrument(
    name = "iam.token.create",
    skip_all,
    fields(service_account = %sa_scoped_slug, name = %name)
)]
pub async fn create_api_token(
    db: &Database,
    raft: &RaftNode,
    jwt: &str,
    sa_scoped_slug: &str,
    name: &str,
) -> Result<MintedToken, IamError> {
    let _auth = authenticate(db, jwt).await?;
    if name.trim().is_empty() {
        return Err(IamError::InvalidSlug("token name cannot be empty".into()));
    }

    let token_id = random_token_chunk(16);
    let secret = random_token_chunk(48);
    // Separator is `.` because the URL-safe base64 alphabet
    // includes `_` — using `_` would make `parse_api_token`
    // ambiguous when either the id or secret happened to contain
    // one. `.` is outside the alphabet, so split is unique.
    let plaintext = format!("nk_live_{token_id}.{secret}");
    let hash = crate::auth::hash_password(&secret)?;
    let (c, u) = now_pair();
    let record = ApiToken {
        token_id: token_id.clone(),
        name: name.to_string(),
        service_account: Ref::<ServiceAccount>::new(sa_scoped_slug.to_string()),
        hash,
        created_at: c,
        updated_at: u,
        version: 0,
    };
    match Writer::new(db).with_raft(raft).create(&record).await {
        Ok(()) => Ok(MintedToken { plaintext, record }),
        Err(e) => {
            let s = e.to_string();
            if s.contains("already exists") || s.contains("Database record") {
                // 16 URL-safe chars = 96 bits of entropy; a collision
                // is astronomically unlikely. If we hit one anyway,
                // surface it rather than silently retrying — easier
                // to spot corruption of the RNG.
                Err(IamError::AlreadyExists(format!("api_token:{token_id}")))
            } else {
                Err(IamError::State(e))
            }
        }
    }
}

#[instrument(name = "iam.token.list", skip_all)]
pub async fn list_api_tokens(db: &Database, jwt: &str) -> Result<Vec<ApiToken>, IamError> {
    read_as(db, jwt, || async {
        db.query_take(&ApiToken::list_query())
            .await
            .map_err(IamError::State)
    })
    .await
}

#[instrument(name = "iam.token.revoke", skip_all, fields(token_id = %token_id))]
pub async fn revoke_api_token(
    db: &Database,
    raft: &RaftNode,
    jwt: &str,
    token_id: &str,
) -> Result<(), IamError> {
    let _auth = authenticate(db, jwt).await?;
    Writer::new(db)
        .with_raft(raft)
        .delete::<ApiToken>(&token_id.to_string())
        .await
        .map_err(IamError::State)?;
    Ok(())
}

/// Return `n` URL-safe-base64 characters drawn from the OS RNG.
/// Collected to a `String` directly rather than going through a
/// crate-provided helper — the alphabet is narrow enough to inline,
/// and staying dependency-free here keeps the token format owned by
/// nauka-iam.
fn random_token_chunk(n: usize) -> String {
    use argon2::password_hash::rand_core::{OsRng, RngCore};
    // URL-safe base64 alphabet minus padding — 64 entries, so one
    // byte per char fits cleanly via `% 64`.
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut bytes = vec![0u8; n];
    OsRng.fill_bytes(&mut bytes);
    bytes
        .iter()
        .map(|b| ALPHABET[(*b as usize) % ALPHABET.len()] as char)
        .collect()
}

/// Parse an `nk_live_<token_id>.<secret>` string. Fails with
/// `InvalidCredentials` if the prefix / shape is wrong — callers
/// should surface that to the caller verbatim (same error as a
/// wrong secret) so an attacker can't distinguish "malformed" from
/// "unknown".
pub fn parse_api_token(plaintext: &str) -> Result<(String, String), IamError> {
    let rest = plaintext
        .strip_prefix("nk_live_")
        .ok_or(IamError::InvalidCredentials)?;
    let (token_id, secret) = rest.split_once('.').ok_or(IamError::InvalidCredentials)?;
    if token_id.is_empty() || secret.is_empty() {
        return Err(IamError::InvalidCredentials);
    }
    Ok((token_id.to_string(), secret.to_string()))
}

#[cfg(test)]
mod api_token_tests {
    use super::*;

    #[test]
    fn random_chunks_are_url_safe() {
        let s = random_token_chunk(32);
        assert_eq!(s.len(), 32);
        for c in s.chars() {
            assert!(
                c.is_ascii_alphanumeric() || c == '-' || c == '_',
                "unexpected char `{c}` in token chunk"
            );
        }
    }

    #[test]
    fn parse_accepts_well_formed() {
        let (id, secret) = parse_api_token("nk_live_abc.def").unwrap();
        assert_eq!(id, "abc");
        assert_eq!(secret, "def");
    }

    #[test]
    fn parse_preserves_underscores_in_id_and_secret() {
        // The URL-safe base64 alphabet includes `_`; the `.`
        // separator guarantees split_once() never miscounts.
        let (id, secret) = parse_api_token("nk_live_a_b_c.d_e_f").unwrap();
        assert_eq!(id, "a_b_c");
        assert_eq!(secret, "d_e_f");
    }

    #[test]
    fn parse_rejects_garbage() {
        for bad in ["", "abc", "nk_live_", "nk_live_only", "nk_test_a.b"] {
            assert!(parse_api_token(bad).is_err(), "should reject `{bad}`");
        }
    }
}

/// Constrain slugs to ASCII lowercase + digits + `-`. Spliced into
/// record ids and CLI argv, so the safe set is intentionally narrow.
fn validate_slug(slug: &str) -> Result<(), IamError> {
    if slug.is_empty() {
        return Err(IamError::InvalidSlug("slug cannot be empty".into()));
    }
    if slug.len() > 63 {
        return Err(IamError::InvalidSlug(format!(
            "slug `{slug}` longer than 63 chars"
        )));
    }
    let bytes = slug.as_bytes();
    if !bytes[0].is_ascii_lowercase() {
        return Err(IamError::InvalidSlug(format!(
            "slug `{slug}` must start with an ASCII lowercase letter"
        )));
    }
    for &b in bytes {
        let ok = b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-';
        if !ok {
            return Err(IamError::InvalidSlug(format!(
                "slug `{slug}` must match [a-z0-9-]"
            )));
        }
    }
    if slug.ends_with('-') || slug.contains("--") {
        return Err(IamError::InvalidSlug(format!(
            "slug `{slug}` has a trailing or doubled `-`"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slug_accepts_canonical_forms() {
        for s in ["acme", "web-platform", "acme2", "a-b-c"] {
            assert!(validate_slug(s).is_ok(), "should accept `{s}`");
        }
    }

    #[test]
    fn slug_rejects_garbage() {
        for s in ["", "-foo", "foo-", "Foo", "foo--bar", "foo_bar", "foo.bar"] {
            assert!(validate_slug(s).is_err(), "should reject `{s}`");
        }
    }
}
