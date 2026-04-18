//! IAM bootstrap — seed `Permission` + primitive `Role` records.
//!
//! Runs once per cluster lifetime, from `init_hypervisor` after the
//! Raft cluster has a leader and the self-Hypervisor record exists.
//! Every write goes through `Writer::create` so the permission /
//! role records replicate to every future joiner via the Raft log.
//!
//! ## Idempotency
//!
//! Each create is wrapped to ignore SurrealDB's "already exists"
//! error, so re-running the seeder (e.g. the same node calling
//! `init_hypervisor` twice after a failed bootstrap) is safe. We do
//! NOT auto-add permissions when a new resource lands — that
//! currently needs a manual migration. The epic tracks this as
//! tech debt; IAM-5 audit will probably expose it when the
//! operator-facing permission catalog matters.
//!
//! ## Which roles get seeded?
//!
//! - `owner` is *not* a stored role. `fn::iam::can` keeps the
//!   `$scope.owner = $auth.id` shortcut from IAM-2; whoever owns the
//!   org has implicit full access, no binding required. That leaves
//!   IAM-1 and IAM-2 flows untouched.
//! - `editor` — all non-select verbs across all resources.
//! - `viewer` — all select verbs across all resources.
//!
//! Both are `kind = "primitive"`, globally visible, and intended to
//! be attached to principals via `RoleBinding`.

use nauka_core::resource::{Datetime, Ref, ResourceDescriptor, ALL_RESOURCES};
use nauka_state::{Database, RaftNode, Writer};

use crate::definition::{Permission, Role};
use crate::error::IamError;

/// Seed the permission catalog + primitive roles. Called from
/// `init_hypervisor` on cluster birth. Idempotent — "already
/// exists" errors are swallowed so a restart during bootstrap
/// finishes the job rather than aborting.
pub async fn bootstrap(db: &Database, raft: &RaftNode) -> Result<(), IamError> {
    seed_permissions(db, raft).await?;
    seed_primitive_roles(db, raft).await?;
    Ok(())
}

async fn seed_permissions(db: &Database, raft: &RaftNode) -> Result<(), IamError> {
    for desc in ALL_RESOURCES.iter() {
        for verb in permission_verbs_for(desc) {
            let now = Datetime::default();
            let perm = Permission {
                name: format!("{}.{verb}", desc.table),
                table: desc.table.to_string(),
                verb: verb.to_string(),
                created_at: now,
                updated_at: now,
                version: 0,
            };
            create_if_absent::<Permission>(db, raft, &perm).await?;
        }
    }
    Ok(())
}

async fn seed_primitive_roles(db: &Database, raft: &RaftNode) -> Result<(), IamError> {
    let editor = Role {
        slug: "editor".to_string(),
        kind: "primitive".to_string(),
        org: None,
        permissions: all_permission_refs(|verb| {
            // editor covers the mutating verbs; viewer owns `select`.
            verb != "select"
        }),
        created_at: Datetime::default(),
        updated_at: Datetime::default(),
        version: 0,
    };
    create_if_absent::<Role>(db, raft, &editor).await?;

    let viewer = Role {
        slug: "viewer".to_string(),
        kind: "primitive".to_string(),
        org: None,
        permissions: all_permission_refs(|verb| verb == "select"),
        created_at: Datetime::default(),
        updated_at: Datetime::default(),
        version: 0,
    };
    create_if_absent::<Role>(db, raft, &viewer).await?;

    Ok(())
}

fn permission_verbs_for(desc: &ResourceDescriptor) -> Vec<&'static str> {
    let mut verbs: Vec<&'static str> = vec!["select", "create", "update", "delete"];
    for a in desc.custom_actions.iter() {
        verbs.push(*a);
    }
    verbs
}

fn all_permission_refs<F>(keep: F) -> Vec<Ref<Permission>>
where
    F: Fn(&str) -> bool,
{
    let mut out = Vec::new();
    for desc in ALL_RESOURCES.iter() {
        for verb in permission_verbs_for(desc) {
            if !keep(verb) {
                continue;
            }
            out.push(Ref::<Permission>::new(format!("{}.{verb}", desc.table)));
        }
    }
    out
}

async fn create_if_absent<R>(db: &Database, raft: &RaftNode, value: &R) -> Result<(), IamError>
where
    R: nauka_core::resource::ResourceOps,
{
    match Writer::new(db).with_raft(raft).create(value).await {
        Ok(()) => Ok(()),
        Err(e) => {
            // SurrealDB reports a record-id clash as "Database record
            // ... already exists". Anything else is a real failure.
            let s = e.to_string();
            if s.contains("already exists") || s.contains("Database record") {
                Ok(())
            } else {
                Err(IamError::State(e))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn primitive_verbs_include_base_crud() {
        // Confirms the slice we seed over always starts with the
        // four CRUD verbs. Regression guard in case someone trims
        // the list thinking it's redundant with `ResourceDescriptor`.
        let desc = ResourceDescriptor {
            table: "x",
            scope: nauka_core::resource::Scope::Cluster,
            ddl: "",
            custom_actions: &[],
        };
        assert_eq!(
            permission_verbs_for(&desc),
            vec!["select", "create", "update", "delete"]
        );
    }

    #[test]
    fn custom_actions_appended_to_crud() {
        let desc = ResourceDescriptor {
            table: "vm",
            scope: nauka_core::resource::Scope::Cluster,
            ddl: "",
            custom_actions: &["start", "stop"],
        };
        assert_eq!(
            permission_verbs_for(&desc),
            vec!["select", "create", "update", "delete", "start", "stop"]
        );
    }
}
