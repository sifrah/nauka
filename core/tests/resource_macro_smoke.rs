//! End-to-end smoke test for `#[resource]`. Verifies a well-formed
//! resource declaration:
//!
//! 1. Compiles.
//! 2. Implements `nauka_core::resource::Resource` correctly.
//! 3. Registers itself into `ALL_RESOURCES`.
//! 4. Generates DDL with all the expected `DEFINE` statements,
//!    including the injected base fields and UNIQUE indexes.
//! 5. Is included in the appropriate `local_schemas()` /
//!    `cluster_schemas()` output.

use nauka_core::resource::{
    cluster_schemas, local_schemas, Datetime, Resource, ResourceOps, Scope, SurrealValue,
};
use nauka_core_macros::resource;
use serde::{Deserialize, Serialize};

#[resource(table = "test_widget", scope = "cluster")]
#[derive(Serialize, Deserialize, SurrealValue)]
pub struct TestWidget {
    #[id]
    pub key: String,
    #[unique]
    pub serial: u64,
    pub label: String,
    pub tags: Vec<String>,
    pub note: Option<String>,
}

#[resource(table = "test_local_thing", scope = "local")]
#[derive(Serialize, Deserialize, SurrealValue)]
pub struct TestLocalThing {
    #[id]
    pub name: String,
    pub value: i64,
}

#[test]
fn trait_constants_match_attribute() {
    assert_eq!(TestWidget::TABLE, "test_widget");
    assert_eq!(TestWidget::SCOPE, Scope::Cluster);
    assert_eq!(TestLocalThing::TABLE, "test_local_thing");
    assert_eq!(TestLocalThing::SCOPE, Scope::Local);
}

#[test]
fn ddl_contains_table_and_user_fields() {
    let ddl = TestWidget::DDL;
    assert!(ddl.contains("DEFINE TABLE IF NOT EXISTS test_widget SCHEMAFULL"));
    assert!(ddl.contains("DEFINE FIELD IF NOT EXISTS key ON test_widget TYPE string"));
    assert!(ddl.contains("DEFINE FIELD IF NOT EXISTS serial ON test_widget TYPE int"));
    assert!(ddl.contains("DEFINE FIELD IF NOT EXISTS label ON test_widget TYPE string"));
    assert!(ddl.contains("DEFINE FIELD IF NOT EXISTS tags ON test_widget TYPE array<string>"));
    assert!(ddl.contains("DEFINE FIELD IF NOT EXISTS note ON test_widget TYPE option<string>"));
}

#[test]
fn ddl_contains_injected_base_fields() {
    let ddl = TestWidget::DDL;
    assert!(ddl.contains("DEFINE FIELD IF NOT EXISTS created_at ON test_widget TYPE datetime"));
    assert!(ddl.contains("DEFINE FIELD IF NOT EXISTS updated_at ON test_widget TYPE datetime"));
    assert!(ddl.contains("DEFINE FIELD IF NOT EXISTS version    ON test_widget TYPE int"));
}

#[test]
fn ddl_contains_unique_index_for_marked_field() {
    let ddl = TestWidget::DDL;
    assert!(ddl.contains(
        "DEFINE INDEX IF NOT EXISTS test_widget_serial ON test_widget FIELDS serial UNIQUE"
    ));
    // No UNIQUE on `label` (not marked).
    assert!(!ddl.contains("test_widget_label"));
}

#[test]
fn no_default_time_now_on_cluster_resource() {
    // Base fields must not carry `DEFAULT time::now()` for cluster
    // scope — would break Raft determinism. Verify the macro never
    // emits it.
    let ddl = TestWidget::DDL;
    assert!(!ddl.contains("DEFAULT time::now()"));
    assert!(!ddl.contains("DEFAULT time::now"));
}

#[test]
fn instance_methods_return_struct_state() {
    let created = Datetime::default();
    let updated = Datetime::default();
    let widget = TestWidget {
        key: "alpha".into(),
        serial: 42,
        label: "label".into(),
        tags: vec!["a".into(), "b".into()],
        note: None,
        created_at: created,
        updated_at: updated,
        version: 7,
    };

    assert_eq!(widget.id(), "alpha");
    assert_eq!(widget.version(), 7);
    assert_eq!(widget.created_at(), &created);
    assert_eq!(widget.updated_at(), &updated);
}

#[test]
fn registered_in_global_slice_under_correct_scope() {
    let cluster = cluster_schemas();
    let local = local_schemas();

    assert!(cluster.contains("DEFINE TABLE IF NOT EXISTS test_widget"));
    assert!(local.contains("DEFINE TABLE IF NOT EXISTS test_local_thing"));

    // Cross-scope leakage check.
    assert!(!cluster.contains("test_local_thing"));
    assert!(!local.contains("test_widget"));
}

#[test]
fn create_query_emits_record_id_syntax_and_all_set_clauses() {
    let widget = TestWidget {
        key: "alpha".into(),
        serial: 42,
        label: r#"label with "quotes" and \backslash"#.into(),
        tags: vec!["a".into(), "b".into()],
        note: None,
        created_at: Datetime::default(),
        updated_at: Datetime::default(),
        version: 0,
    };

    let q = widget.create_query();

    assert!(q.starts_with("CREATE test_widget:\u{27E8}alpha\u{27E9} SET "));
    assert!(q.contains(r#"key = "alpha""#));
    assert!(q.contains("serial = 42"));
    assert!(q.contains(r#"label = "label with \"quotes\" and \\backslash""#));
    assert!(q.contains(r#"tags = ["a","b"]"#));
    assert!(q.contains("note = NONE"));
    assert!(q.contains("version = 0"));
    assert!(q.contains("created_at = <datetime>\""));
    assert!(q.contains("updated_at = <datetime>\""));
}

#[test]
fn update_query_uses_update_verb_on_same_record() {
    let widget = TestWidget {
        key: "beta".into(),
        serial: 7,
        label: "x".into(),
        tags: vec![],
        note: Some("hi".into()),
        created_at: Datetime::default(),
        updated_at: Datetime::default(),
        version: 3,
    };

    let q = widget.update_query();
    assert!(q.starts_with("UPDATE test_widget:\u{27E8}beta\u{27E9} SET "));
    assert!(q.contains(r#"note = "hi""#));
    assert!(q.contains("version = 3"));
}

#[test]
fn delete_get_list_queries_match_convention() {
    assert_eq!(
        <TestWidget as ResourceOps>::delete_query(&"gamma".to_string()),
        "DELETE test_widget:\u{27E8}gamma\u{27E9}"
    );
    assert_eq!(
        <TestWidget as ResourceOps>::get_query(&"delta".to_string()),
        "SELECT * FROM test_widget:\u{27E8}delta\u{27E9}"
    );
    assert_eq!(TestWidget::list_query(), "SELECT * FROM test_widget");
}

#[test]
fn record_id_escaping_blocks_injection() {
    // A hostile id that tries to close the ⟨…⟩ and append a second
    // statement must be escaped inside the record-id payload so
    // SurrealDB parses the whole prefix as one record literal.
    let injected = TestWidget {
        key: "x\u{27E9}; DELETE test_widget;".into(),
        serial: 1,
        label: "l".into(),
        tags: vec![],
        note: None,
        created_at: Datetime::default(),
        updated_at: Datetime::default(),
        version: 0,
    };

    let q = injected.create_query();

    // Expect the id payload to carry an escaped `\⟩` and end with a
    // single unescaped `⟩ SET ` — that single unescaped bracket is
    // the real record-id terminator.
    let expected_prefix =
        "CREATE test_widget:\u{27E8}x\\\u{27E9}; DELETE test_widget;\u{27E9} SET ";
    assert!(
        q.starts_with(expected_prefix),
        "query did not escape ⟩ in record id payload: {q}"
    );
}
