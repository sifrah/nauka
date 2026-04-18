//! Integration test for IAM-2 (#346) — exercise the Org / Project /
//! Env scope tree with real SurrealDB PERMISSIONS enforcement.
//!
//! The test loads the full Nauka schema (functions, then tables,
//! then access), creates two users, and signs in as each in turn to
//! confirm that `fn::iam::can` + the generated PERMISSIONS clause
//! filter rows correctly.
//!
//! Writes in this test go directly through `db.query` rather than
//! `Writer::create` because we're running single-node without Raft.
//! The hash / replication path is already covered by
//! `tests/auth_flow.rs` from IAM-1; here we focus on authorization
//! semantics.

use nauka_core::resource::{Datetime, ResourceOps};
use nauka_iam::{hash_password, Env, Org, Project, User};
use nauka_state::Database;
use surrealdb::opt::auth::Record;
use surrealdb::types::SurrealValue;

async fn fresh_db() -> (Database, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("iam2-test.db");
    let db = Database::open(Some(path.to_str().unwrap())).await.unwrap();

    // Same load order as `bin/nauka::open_db`: functions first
    // (required by PERMISSIONS clauses), then tables, then access.
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

async fn create_user(db: &Database, email: &str, display_name: &str) {
    let hash = hash_password("test-pass").unwrap();
    let user = User {
        email: email.to_string(),
        password_hash: hash,
        display_name: display_name.to_string(),
        created_at: Datetime::default(),
        updated_at: Datetime::default(),
        version: 0,
    };
    db.query(&user.create_query())
        .await
        .expect("create user row");
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
        .expect("signin sets session");
}

async fn invalidate_session(db: &Database) {
    // `invalidate` wipes the session so the next query runs with no
    // `$auth` — used to confirm PERMISSIONS reject unauthenticated
    // reads.
    db.inner().invalidate().await.expect("invalidate session");
}

#[tokio::test]
async fn function_definitions_includes_iam_can() {
    let defs = nauka_core::function_definitions();
    assert!(
        defs.contains("DEFINE FUNCTION IF NOT EXISTS fn::iam::can"),
        "iam::can not registered: {defs}"
    );
}

#[tokio::test]
async fn permissions_clause_in_table_ddl() {
    let cluster = nauka_core::cluster_schemas();
    // Org uses `permissions = "..."` — one FOR clause for all verbs.
    assert!(
        cluster.contains(
            "FOR select, create, update, delete WHERE \
             $auth = NONE OR $this.owner = $auth.id"
        ),
        "org permissions missing: {cluster}"
    );
    // Project uses `scope_by = "org"` — four per-verb FOR clauses.
    for verb in ["select", "create", "update", "delete"] {
        let expected = format!("FOR {verb} WHERE fn::iam::can('{verb}', $this.org)");
        assert!(
            cluster.contains(&expected),
            "project missing `{expected}`: {cluster}"
        );
    }
    // Env uses `scope_by = "project"`.
    assert!(
        cluster.contains("fn::iam::can('select', $this.project)"),
        "env missing scope_by clause: {cluster}"
    );
}

#[tokio::test]
async fn org_owner_can_select_their_org() {
    let (db, _dir) = fresh_db().await;
    create_user(&db, "alice@example.com", "Alice").await;

    // Create as a system-level connection (no $auth), so the
    // PERMISSIONS FOR create check is not in force — this models the
    // production daemon writing via `Writer::create` which runs
    // elevated.
    let org = Org {
        slug: "acme".into(),
        display_name: "Acme Corp".into(),
        owner: nauka_core::resource::Ref::new("alice@example.com"),
        created_at: Datetime::default(),
        updated_at: Datetime::default(),
        version: 0,
    };
    db.query(&org.create_query()).await.expect("create org");

    sign_in_as(&db, "alice@example.com").await;
    let rows: Vec<Org> = db
        .query_take(&Org::list_query())
        .await
        .expect("list orgs as alice");
    assert_eq!(rows.len(), 1, "alice should see her own org");
    assert_eq!(rows[0].slug, "acme");
}

#[tokio::test]
async fn non_owner_cannot_see_org() {
    let (db, _dir) = fresh_db().await;
    create_user(&db, "alice@example.com", "Alice").await;
    create_user(&db, "bob@example.com", "Bob").await;

    let org = Org {
        slug: "acme".into(),
        display_name: "Acme Corp".into(),
        owner: nauka_core::resource::Ref::new("alice@example.com"),
        created_at: Datetime::default(),
        updated_at: Datetime::default(),
        version: 0,
    };
    db.query(&org.create_query()).await.expect("create org");

    sign_in_as(&db, "bob@example.com").await;
    let rows: Vec<Org> = db
        .query_take(&Org::list_query())
        .await
        .expect("list orgs as bob");
    assert!(
        rows.is_empty(),
        "PERMISSIONS should filter bob out; got {} rows",
        rows.len()
    );
}

#[tokio::test]
async fn project_visibility_follows_org_ownership() {
    let (db, _dir) = fresh_db().await;
    create_user(&db, "alice@example.com", "Alice").await;
    create_user(&db, "bob@example.com", "Bob").await;

    let org = Org {
        slug: "acme".into(),
        display_name: "Acme".into(),
        owner: nauka_core::resource::Ref::new("alice@example.com"),
        created_at: Datetime::default(),
        updated_at: Datetime::default(),
        version: 0,
    };
    db.query(&org.create_query()).await.unwrap();

    let proj = Project {
        uid: "p-web".into(),
        slug: "web".into(),
        org: nauka_core::resource::Ref::new("acme"),
        display_name: "Web Platform".into(),
        created_at: Datetime::default(),
        updated_at: Datetime::default(),
        version: 0,
    };
    db.query(&proj.create_query()).await.unwrap();

    // alice (owner of the org) can see the project via the scope chain.
    sign_in_as(&db, "alice@example.com").await;
    let rows: Vec<Project> = db.query_take(&Project::list_query()).await.unwrap();
    assert_eq!(rows.len(), 1, "alice should see the project in her org");

    // bob (unrelated) should not see it.
    invalidate_session(&db).await;
    sign_in_as(&db, "bob@example.com").await;
    let rows: Vec<Project> = db.query_take(&Project::list_query()).await.unwrap();
    assert!(
        rows.is_empty(),
        "bob should not see alice's project; got {} rows",
        rows.len()
    );
}

#[tokio::test]
async fn env_visibility_follows_project_chain() {
    let (db, _dir) = fresh_db().await;
    create_user(&db, "alice@example.com", "Alice").await;
    create_user(&db, "bob@example.com", "Bob").await;

    db.query(
        &Org {
            slug: "acme".into(),
            display_name: "Acme".into(),
            owner: nauka_core::resource::Ref::new("alice@example.com"),
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
            org: nauka_core::resource::Ref::new("acme"),
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
            project: nauka_core::resource::Ref::new("p-web"),
            display_name: "Production".into(),
            created_at: Datetime::default(),
            updated_at: Datetime::default(),
            version: 0,
        }
        .create_query(),
    )
    .await
    .unwrap();

    // Two-hop scope traversal — env -> project -> org -> owner.
    sign_in_as(&db, "alice@example.com").await;
    let rows: Vec<Env> = db.query_take(&Env::list_query()).await.unwrap();
    assert_eq!(rows.len(), 1);

    invalidate_session(&db).await;
    sign_in_as(&db, "bob@example.com").await;
    let rows: Vec<Env> = db.query_take(&Env::list_query()).await.unwrap();
    assert!(
        rows.is_empty(),
        "bob should not reach envs in alice's project"
    );
}
