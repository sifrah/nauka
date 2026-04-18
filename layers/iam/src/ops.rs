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

use crate::definition::{Env, Org, Project, Role, RoleBinding, User};
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
