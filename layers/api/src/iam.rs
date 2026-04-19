//! IAM resources routing — 342-C2.
//!
//! Every IAM table that opts into the API (every one except
//! `PasswordResetToken`, which has `api_verbs = ""`) is mounted
//! here through the generic [`crate::crud::mount_crud`]. No
//! per-resource handler bodies — the struct-level
//! `#[serde(skip)]` on `User.password_hash` and `ApiToken.hash`
//! handles secret masking, and the per-resource `api_verbs`
//! attributes decide which verbs appear.
//!
//! GraphQL resolvers for these resources are **deferred** to a
//! later commit (342-C3 once the CLI migration is in): each
//! resource needs concrete `Field::new` wiring against its
//! struct shape, and batching them with the REST switchover
//! would inflate this diff without adding coverage that the
//! generic CRUD tests don't already give.

use axum::Router;
use nauka_iam::{
    ActiveSession, ApiToken, AuditEvent, Env, Permission, Project, Role, RoleBinding,
    ServiceAccount, User,
};

use crate::crud::{self, Verb};
use crate::Deps;

/// Full CRUD — the default shape.
const FULL: &[Verb] = &[
    Verb::Create,
    Verb::Get,
    Verb::List,
    Verb::Update,
    Verb::Delete,
];

/// Read-only resources — the catalog tables where writes are
/// seeded or chain-appended and the API only exposes reads.
const READ_ONLY: &[Verb] = &[Verb::Get, Verb::List];

pub fn routes() -> Router<Deps> {
    let mut r = Router::new();

    // Scope tree.
    r = crud::mount_crud::<Project>(r, "/v1/projects", FULL);
    r = crud::mount_crud::<Env>(r, "/v1/envs", FULL);

    // RBAC.
    r = crud::mount_crud::<Role>(r, "/v1/roles", FULL);
    r = crud::mount_crud::<RoleBinding>(r, "/v1/role-bindings", FULL);
    r = crud::mount_crud::<Permission>(r, "/v1/permissions", READ_ONLY);

    // Machine identity. `ApiToken` drops `create` because the
    // plaintext-reveal flow is a TTY-only CLI surface — see the
    // attr comment on the resource.
    r = crud::mount_crud::<ServiceAccount>(r, "/v1/service-accounts", FULL);
    r = crud::mount_crud::<ApiToken>(
        r,
        "/v1/api-tokens",
        &[Verb::Get, Verb::List, Verb::Delete],
    );

    // Observability / lifecycle.
    r = crud::mount_crud::<ActiveSession>(
        r,
        "/v1/sessions",
        &[Verb::Get, Verb::List, Verb::Delete],
    );
    r = crud::mount_crud::<AuditEvent>(r, "/v1/audit-events", READ_ONLY);

    // User — `create` stays on the bespoke signup path to keep
    // Argon2id hashing on the leader. Everything else is vanilla.
    r = crud::mount_crud::<User>(
        r,
        "/v1/users",
        &[Verb::Get, Verb::List, Verb::Update, Verb::Delete],
    );

    r
}
