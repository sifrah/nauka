//! Integration test for IAM-3 (#347) — RoleBinding-driven
//! authorization end-to-end.
//!
//! Alice creates an org + project + env. Bob is a stranger until
//! alice binds him to the `viewer` role scoped to her org. After
//! the binding, `fn::iam::can` lets bob see every row under the org
//! via the scope chain; without it he sees nothing (confirmed by
//! the IAM-2 integration test). This file locks in the positive
//! case.
//!
//! As in `scope_tree.rs`, we run single-node without Raft and
//! bypass the Writer for direct inserts. Replication semantics are
//! covered by the Hetzner script; this test is about
//! permission-matching correctness.

use nauka_core::resource::{Datetime, Ref, ResourceOps};
use nauka_iam::{hash_password, Env, Org, Permission, Project, Role, RoleBinding, User};
use nauka_state::Database;
use surrealdb::opt::auth::Record;
use surrealdb::types::SurrealValue;

async fn fresh_db() -> (Database, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("iam3-test.db");
    let db = Database::open(Some(path.to_str().unwrap())).await.unwrap();

    let functions = nauka_core::function_definitions();
    let cluster = nauka_core::cluster_schemas();
    let local = nauka_core::local_schemas();
    let access = nauka_core::access_definitions();
    nauka_state::load_schemas(
        &db,
        &[nauka_state::SCHEMA, &functions, &cluster, &local, &access],
    )
    .await
    .unwrap();

    (db, dir)
}

async fn seed_permission(db: &Database, name: &str, table: &str, verb: &str) {
    let p = Permission {
        name: name.to_string(),
        table: table.to_string(),
        verb: verb.to_string(),
        created_at: Datetime::default(),
        updated_at: Datetime::default(),
        version: 0,
    };
    db.query(&p.create_query())
        .await
        .expect("create permission");
}

async fn seed_viewer_role(db: &Database) {
    // Minimal viewer: select on org + project + env.
    for table in ["org", "project", "env"] {
        let name = format!("{table}.select");
        seed_permission(db, &name, table, "select").await;
    }
    let role = Role {
        slug: "viewer".to_string(),
        kind: "primitive".to_string(),
        org: None,
        permissions: vec![
            Ref::<Permission>::new("org.select"),
            Ref::<Permission>::new("project.select"),
            Ref::<Permission>::new("env.select"),
        ],
        created_at: Datetime::default(),
        updated_at: Datetime::default(),
        version: 0,
    };
    db.query(&role.create_query()).await.expect("create role");
}

async fn create_user(db: &Database, email: &str, display_name: &str) {
    let hash = hash_password("test-pass").unwrap();
    let user = User {
        email: email.to_string(),
        password_hash: hash,
        display_name: display_name.to_string(),
        email_verified_at: None,
        active: true,
        created_at: Datetime::default(),
        updated_at: Datetime::default(),
        version: 0,
    };
    db.query(&user.create_query()).await.expect("create user");
}

async fn sign_in_as(db: &Database, email: &str) {
    #[derive(SurrealValue)]
    struct Params {
        email: String,
        password: String,
    }
    db.inner()
        .signin(Record {
            namespace: nauka_state::DEFAULT_NAMESPACE.to_string(),
            database: nauka_state::DEFAULT_DATABASE.to_string(),
            access: "user".to_string(),
            params: Params {
                email: email.to_string(),
                password: "test-pass".to_string(),
            },
        })
        .await
        .expect("signin");
}

async fn invalidate(db: &Database) {
    db.inner().invalidate().await.expect("invalidate");
}

#[tokio::test]
async fn role_resources_register_with_permissions_clauses() {
    let cluster = nauka_core::cluster_schemas();
    assert!(cluster.contains("DEFINE TABLE IF NOT EXISTS permission SCHEMAFULL"));
    assert!(cluster.contains("DEFINE TABLE IF NOT EXISTS role SCHEMAFULL"));
    assert!(cluster.contains("DEFINE TABLE IF NOT EXISTS role_binding SCHEMAFULL"));
    // role_binding uses an explicit `permissions` clause (not
    // `scope_by`) to avoid an infinite recursion through
    // `fn::iam::can`. Owner of the org OR the principal themselves
    // can see their binding.
    assert!(
        cluster.contains(
            "WHERE $auth = NONE OR $this.principal = $auth.id OR $this.org.owner = $auth.id"
        ),
        "role_binding permissions wrong: {cluster}"
    );
}

#[tokio::test]
async fn fn_iam_can_ddl_references_role_binding() {
    let defs = nauka_core::function_definitions();
    assert!(defs.contains("role_binding"));
    assert!(defs.contains("role.permissions CONTAINS $needed"));
}

#[tokio::test]
async fn viewer_binding_grants_select_across_scope_tree() {
    let (db, _dir) = fresh_db().await;
    create_user(&db, "alice@example.com", "Alice").await;
    create_user(&db, "bob@example.com", "Bob").await;
    seed_viewer_role(&db).await;

    // Alice's scope tree.
    db.query(
        &Org {
            slug: "acme".into(),
            display_name: "Acme".into(),
            owner: Ref::new("alice@example.com"),
            created_at: Datetime::default(),
            updated_at: Datetime::default(),
            version: 0,
        }
        .create_query(),
    )
    .await
    .unwrap();
    db.query(
        &Project {
            uid: "p-web".into(),
            slug: "web".into(),
            org: Ref::new("acme"),
            display_name: "Web".into(),
            created_at: Datetime::default(),
            updated_at: Datetime::default(),
            version: 0,
        }
        .create_query(),
    )
    .await
    .unwrap();
    db.query(
        &Env {
            uid: "e-prod".into(),
            slug: "production".into(),
            project: Ref::new("p-web"),
            display_name: "Production".into(),
            created_at: Datetime::default(),
            updated_at: Datetime::default(),
            version: 0,
        }
        .create_query(),
    )
    .await
    .unwrap();

    // Bob sees nothing at first.
    sign_in_as(&db, "bob@example.com").await;
    let orgs: Vec<Org> = db.query_take(&Org::list_query()).await.unwrap();
    assert_eq!(orgs.len(), 0, "bob must not see acme without a binding");
    invalidate(&db).await;

    // Alice binds bob to `viewer` at acme.
    let binding = RoleBinding {
        uid: "acme-bob@example.com-viewer".into(),
        principal: Ref::new("bob@example.com"),
        role: Ref::new("viewer"),
        org: Ref::new("acme"),
        reason: "test-seed".to_string(),
        created_at: Datetime::default(),
        updated_at: Datetime::default(),
        version: 0,
    };
    db.query(&binding.create_query()).await.unwrap();

    // Now bob sees the org, project, and env — full scope chain —
    // via his viewer binding.
    sign_in_as(&db, "bob@example.com").await;
    let orgs: Vec<Org> = db.query_take(&Org::list_query()).await.unwrap();
    assert_eq!(orgs.len(), 1, "bob must see acme after binding");
    let projects: Vec<Project> = db.query_take(&Project::list_query()).await.unwrap();
    assert_eq!(projects.len(), 1, "bob must see the project under acme");
    let envs: Vec<Env> = db.query_take(&Env::list_query()).await.unwrap();
    assert_eq!(envs.len(), 1, "bob must see the env under the project");
}

#[tokio::test]
async fn owner_still_sees_everything_without_explicit_binding() {
    // The IAM-2 owner shortcut is preserved — alice owns acme and
    // should keep her full view even though IAM-3 now routes
    // through `fn::iam::can`'s role-binding branch for non-owners.
    let (db, _dir) = fresh_db().await;
    create_user(&db, "alice@example.com", "Alice").await;

    db.query(
        &Org {
            slug: "acme".into(),
            display_name: "Acme".into(),
            owner: Ref::new("alice@example.com"),
            created_at: Datetime::default(),
            updated_at: Datetime::default(),
            version: 0,
        }
        .create_query(),
    )
    .await
    .unwrap();
    db.query(
        &Project {
            uid: "p-web".into(),
            slug: "web".into(),
            org: Ref::new("acme"),
            display_name: "Web".into(),
            created_at: Datetime::default(),
            updated_at: Datetime::default(),
            version: 0,
        }
        .create_query(),
    )
    .await
    .unwrap();

    sign_in_as(&db, "alice@example.com").await;
    let orgs: Vec<Org> = db.query_take(&Org::list_query()).await.unwrap();
    let projects: Vec<Project> = db.query_take(&Project::list_query()).await.unwrap();
    assert_eq!(orgs.len(), 1);
    assert_eq!(projects.len(), 1);
}
