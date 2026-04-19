//! IAM resources routing — 342-C2 + 342-D1.
//!
//! Every IAM table that opts into the API (every one except
//! `PasswordResetToken`, which has `api_verbs = ""`) is mounted
//! through [`crate::crud::mount_crud`]. The path for each resource
//! comes from the `#[resource(api_path = …)]` attribute (with
//! `/v1/{table}s` as the default) — no manual path strings here,
//! so the OpenAPI descriptors and the real REST routes stay in
//! lockstep by construction.
//!
//! GraphQL resolvers for these resources are deferred; see the
//! 342-C2 commit note.

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
    r = crud::mount_crud::<Project>(r, FULL);
    r = crud::mount_crud::<Env>(r, FULL);

    // RBAC.
    r = crud::mount_crud::<Role>(r, FULL);
    r = crud::mount_crud::<RoleBinding>(r, FULL);
    r = crud::mount_crud::<Permission>(r, READ_ONLY);

    // Machine identity. `ApiToken` drops `create` because the
    // plaintext-reveal flow is a TTY-only CLI surface.
    r = crud::mount_crud::<ServiceAccount>(r, FULL);
    r = crud::mount_crud::<ApiToken>(r, &[Verb::Get, Verb::List, Verb::Delete]);

    // Observability / lifecycle.
    r = crud::mount_crud::<ActiveSession>(r, &[Verb::Get, Verb::List, Verb::Delete]);
    r = crud::mount_crud::<AuditEvent>(r, READ_ONLY);

    // User — `create` stays on the bespoke signup path.
    r = crud::mount_crud::<User>(
        r,
        &[Verb::Get, Verb::List, Verb::Update, Verb::Delete],
    );

    r
}
