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
    cluster_schemas, local_schemas, Datetime, Ref, Resource, ResourceOps, Scope, SurrealValue,
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

// --- Cross-resource references ---

#[resource(table = "test_parent", scope = "cluster")]
#[derive(Serialize, Deserialize, SurrealValue)]
pub struct TestParent {
    #[id]
    pub name: String,
    pub note: String,
}

#[resource(table = "test_child", scope = "cluster")]
#[derive(Serialize, Deserialize, SurrealValue)]
pub struct TestChild {
    #[id]
    pub slug: String,
    pub parent: Ref<TestParent>,
    pub siblings: Vec<Ref<TestParent>>,
    pub preferred: Option<Ref<TestParent>>,
}

#[test]
fn reference_fields_become_record_types_in_ddl() {
    let ddl = TestChild::DDL;
    assert!(
        ddl.contains("parent ON test_child TYPE record<test_parent>"),
        "missing record<test_parent> for `parent`: {ddl}"
    );
    assert!(
        ddl.contains("siblings ON test_child TYPE array<record<test_parent>>"),
        "missing array<record<test_parent>> for `siblings`: {ddl}"
    );
    assert!(
        ddl.contains("preferred ON test_child TYPE option<record<test_parent>>"),
        "missing option<record<test_parent>> for `preferred`: {ddl}"
    );
}

#[test]
fn create_query_emits_bare_record_literals_for_refs() {
    let child = TestChild {
        slug: "c1".into(),
        parent: Ref::new("p1"),
        siblings: vec![Ref::new("p2"), Ref::new("p3")],
        preferred: None,
        created_at: Datetime::default(),
        updated_at: Datetime::default(),
        version: 0,
    };

    let q = child.create_query();

    // Record refs must NOT be emitted as quoted strings — the DDL
    // says `record<test_parent>` so the value side must produce a
    // bare record literal.
    assert!(
        q.contains("parent = test_parent:\u{27E8}p1\u{27E9}"),
        "parent ref not a bare record literal: {q}"
    );
    assert!(
        q.contains("siblings = [test_parent:\u{27E8}p2\u{27E9},test_parent:\u{27E8}p3\u{27E9}]"),
        "siblings array wrong: {q}"
    );
    assert!(q.contains("preferred = NONE"));
}

// --- Cascade / restrict / set_null / assert ---

#[resource(
    table = "test_vm",
    scope = "cluster",
    cascade_delete = "attached",
    restrict_delete = "test_snapshot:vm",
    set_null_on_delete = "test_policy:preferred_vm"
)]
#[derive(Serialize, Deserialize, SurrealValue)]
pub struct TestVm {
    #[id]
    pub name: String,
    #[assert("$value BETWEEN 1 AND 128")]
    pub cpu: u32,
    pub attached: Vec<Ref<TestParent>>,
}

#[test]
fn on_delete_event_includes_cascade_restrict_and_set_null() {
    let ddl = TestVm::DDL;
    assert!(
        ddl.contains("DEFINE EVENT IF NOT EXISTS test_vm_on_delete ON test_vm"),
        "missing DEFINE EVENT: {ddl}"
    );
    assert!(ddl.contains("WHEN $event = \"DELETE\""), "{ddl}");
    assert!(
        ddl.contains("DELETE $before.attached"),
        "missing cascade delete: {ddl}"
    );
    assert!(
        ddl.contains("count() FROM test_snapshot WHERE vm = $before.id"),
        "missing restrict check: {ddl}"
    );
    assert!(
        ddl.contains("THROW \"cannot delete test_vm: still referenced by test_snapshot.vm\""),
        "missing restrict THROW: {ddl}"
    );
    assert!(
        ddl.contains("UPDATE test_policy SET preferred_vm = NONE"),
        "missing set_null UPDATE: {ddl}"
    );
}

#[test]
fn assert_attribute_appends_to_define_field() {
    let ddl = TestVm::DDL;
    assert!(
        ddl.contains("cpu ON test_vm TYPE int ASSERT $value BETWEEN 1 AND 128"),
        "cpu missing ASSERT clause: {ddl}"
    );
    // Fields without #[assert] must NOT carry a trailing ASSERT.
    assert!(
        ddl.contains("name ON test_vm TYPE string;"),
        "name field has unexpected trailing clause: {ddl}"
    );
}

#[test]
fn resource_without_on_delete_emits_no_event() {
    // TestWidget / TestParent / TestChild above don't use on_delete.
    assert!(
        !TestWidget::DDL.contains("DEFINE EVENT"),
        "unexpected DEFINE EVENT for TestWidget: {}",
        TestWidget::DDL
    );
    assert!(
        !TestParent::DDL.contains("DEFINE EVENT"),
        "unexpected DEFINE EVENT for TestParent"
    );
}

#[test]
fn ref_is_type_safe_across_resources() {
    // A `Ref<TestParent>` cannot be used where a `Ref<TestWidget>`
    // is expected — compile-time guarantee, no runtime check.
    // Verify the Ref value carries the right table in its Display.
    let r: Ref<TestParent> = Ref::new("foo");
    assert_eq!(r.id(), "foo");
    assert_eq!(format!("{r}"), "test_parent:foo");
}

// --- scope_by / permissions (IAM-2) ---

#[resource(
    table = "test_scoped_parent",
    scope = "cluster",
    permissions = "$auth != NONE AND $this.owner = $auth.id"
)]
#[derive(Serialize, Deserialize, SurrealValue)]
pub struct TestScopedParent {
    #[id]
    pub slug: String,
    pub owner: String,
}

#[resource(table = "test_scoped_child", scope = "cluster", scope_by = "parent")]
#[derive(Serialize, Deserialize, SurrealValue)]
pub struct TestScopedChild {
    #[id]
    pub id: String,
    pub parent: Ref<TestScopedParent>,
    pub label: String,
}

#[test]
fn permissions_attr_emits_single_where_clause() {
    let ddl = TestScopedParent::DDL;
    assert!(
        ddl.contains("DEFINE TABLE IF NOT EXISTS test_scoped_parent SCHEMAFULL"),
        "table def missing: {ddl}"
    );
    assert!(
        ddl.contains(
            "PERMISSIONS FOR select, create, update, delete WHERE \
             $auth != NONE AND $this.owner = $auth.id"
        ),
        "permissions clause wrong: {ddl}"
    );
}

#[test]
fn scope_by_emits_one_clause_per_verb() {
    let ddl = TestScopedChild::DDL;
    assert!(
        ddl.contains("DEFINE TABLE IF NOT EXISTS test_scoped_child SCHEMAFULL"),
        "table def missing: {ddl}"
    );
    for verb in ["select", "create", "update", "delete"] {
        let expected = format!("FOR {verb} WHERE fn::iam::can('{verb}', $this.parent)");
        assert!(ddl.contains(&expected), "missing `{expected}`: {ddl}");
    }
}

#[resource(
    table = "test_vm_with_actions",
    scope = "cluster",
    custom_actions = "start, stop, reboot"
)]
#[derive(Serialize, Deserialize, SurrealValue)]
pub struct TestVmWithActions {
    #[id]
    pub name: String,
    pub cpu: u32,
}

#[test]
fn custom_actions_register_in_descriptor() {
    use nauka_core::resource::ALL_RESOURCES;
    let desc = ALL_RESOURCES
        .iter()
        .find(|d| d.table == "test_vm_with_actions")
        .expect("test_vm_with_actions descriptor registered");
    assert_eq!(desc.custom_actions, &["start", "stop", "reboot"]);
}

#[test]
fn resource_without_custom_actions_has_empty_slice() {
    use nauka_core::resource::ALL_RESOURCES;
    let desc = ALL_RESOURCES
        .iter()
        .find(|d| d.table == "test_widget")
        .expect("test_widget descriptor registered");
    assert!(desc.custom_actions.is_empty());
}

#[test]
fn resource_without_scope_by_or_permissions_omits_clause() {
    // TestParent / TestWidget above don't set either key — they must
    // not emit a PERMISSIONS clause (SurrealDB applies its default).
    assert!(
        !TestParent::DDL.contains("PERMISSIONS"),
        "TestParent should not carry PERMISSIONS: {}",
        TestParent::DDL
    );
    assert!(
        !TestWidget::DDL.contains("PERMISSIONS"),
        "TestWidget should not carry PERMISSIONS: {}",
        TestWidget::DDL
    );
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
