//! Integration tests for the centralized validation pipeline.
//!
//! These tests verify that the framework rejects bad input BEFORE calling handlers.
//! Each validation function is tested through the public re-exports in
//! `nauka_core::resource::*` (the module itself is `pub(crate)` but all items
//! are re-exported via `pub use validation::*`).

use nauka_core::resource::*;
use std::collections::HashMap;

// ── Test fixtures ────────────────────────────────────────

/// A "widget" resource scoped to an org, with a mix of field types:
/// - `color`: required string (CreateOnly, no default)
/// - `count`: integer with default "1"
/// - `size`: enum {small, medium, large}
/// - `id`: ReadOnly / Hidden (should be stripped)
fn widget_def() -> ResourceDef {
    ResourceDef {
        identity: ResourceIdentity {
            kind: "widget",
            cli_name: "widget",
            plural: "widgets",
            description: "Test widget",
            aliases: &[],
        },
        scope: ScopeDef::within("org", "--org", "Organization"),
        schema: ResourceSchema {
            fields: vec![
                FieldDef::string("color", "Widget color"),
                FieldDef::integer("count", "Number of items").with_default("1"),
                FieldDef::enum_field("size", "Widget size", &["small", "medium", "large"]),
                FieldDef {
                    name: "id",
                    description: "Resource ID",
                    field_type: FieldType::String,
                    mutability: Mutability::ReadOnly,
                    short: None,
                    default: None,
                    env_var: None,
                    visibility: CliVisibility::Hidden,
                },
            ],
        },
        operations: vec![
            OperationDef::create().with_arg(OperationArg::required(
                "material",
                FieldDef::string("material", "Material type"),
            )),
            OperationDef::list(),
            OperationDef::get(),
            OperationDef::delete(),
        ],
        presentation: PresentationDef::none(),
    }
}

fn create_op(def: &ResourceDef) -> &OperationDef {
    def.operations.iter().find(|o| o.name == "create").unwrap()
}

fn list_op(def: &ResourceDef) -> &OperationDef {
    def.operations.iter().find(|o| o.name == "list").unwrap()
}

fn get_op(def: &ResourceDef) -> &OperationDef {
    def.operations.iter().find(|o| o.name == "get").unwrap()
}

fn delete_op(def: &ResourceDef) -> &OperationDef {
    def.operations.iter().find(|o| o.name == "delete").unwrap()
}

// ── validate_name ────────────────────────────────────────

#[test]
fn rejects_missing_name_on_create() {
    let result = validate_name(&None, &OperationSemantics::Create);
    assert!(result.is_err());
    assert!(result.unwrap_err().message.contains("name is required"));
}

#[test]
fn rejects_invalid_name_uppercase() {
    let result = validate_name(
        &Some("MY_WIDGET".to_string()),
        &OperationSemantics::Create,
    );
    assert!(result.is_err());
    let msg = result.unwrap_err().message;
    assert!(
        msg.contains("invalid") || msg.contains("lowercase"),
        "expected name-validation error, got: {msg}"
    );
}

#[test]
fn rejects_name_too_short() {
    let result = validate_name(
        &Some("ab".to_string()),
        &OperationSemantics::Create,
    );
    assert!(result.is_err());
    let msg = result.unwrap_err().message;
    assert!(
        msg.contains("at least 3"),
        "expected too-short error, got: {msg}"
    );
}

#[test]
fn rejects_name_consecutive_hyphens() {
    let result = validate_name(
        &Some("my--widget".to_string()),
        &OperationSemantics::Create,
    );
    assert!(result.is_err());
    let msg = result.unwrap_err().message;
    assert!(
        msg.contains("consecutive hyphens"),
        "expected consecutive-hyphens error, got: {msg}"
    );
}

#[test]
fn rejects_name_starting_with_digit() {
    let result = validate_name(
        &Some("1widget".to_string()),
        &OperationSemantics::Create,
    );
    assert!(result.is_err());
    let msg = result.unwrap_err().message;
    assert!(
        msg.contains("start with a lowercase letter"),
        "expected start-with-letter error, got: {msg}"
    );
}

#[test]
fn rejects_name_ending_with_hyphen() {
    let result = validate_name(
        &Some("widget-".to_string()),
        &OperationSemantics::Create,
    );
    assert!(result.is_err());
    let msg = result.unwrap_err().message;
    assert!(
        msg.contains("end with"),
        "expected end-with error, got: {msg}"
    );
}

#[test]
fn accepts_valid_name_on_create() {
    let result = validate_name(
        &Some("my-widget-1".to_string()),
        &OperationSemantics::Create,
    );
    assert!(result.is_ok());
}

#[test]
fn skips_name_on_list() {
    // List operations never have a name — should pass even with None.
    let result = validate_name(&None, &OperationSemantics::List);
    assert!(result.is_ok());
}

#[test]
fn skips_name_on_action() {
    let result = validate_name(&None, &OperationSemantics::Action);
    assert!(result.is_ok());
}

#[test]
fn requires_name_on_get() {
    let result = validate_name(&None, &OperationSemantics::Get);
    assert!(result.is_err());
}

#[test]
fn requires_name_on_delete() {
    let result = validate_name(&None, &OperationSemantics::Delete);
    assert!(result.is_err());
}

#[test]
fn requires_name_on_update() {
    let result = validate_name(&None, &OperationSemantics::Update { patch: false });
    assert!(result.is_err());
}

// ── validate_scope ───────────────────────────────────────

#[test]
fn rejects_missing_scope_on_create() {
    let def = widget_def();
    let op = create_op(&def);
    let scope = ScopeValues::default();
    let result = validate_scope(&def, op, &scope);
    assert!(result.is_err());
    let msg = result.unwrap_err().message;
    assert!(msg.contains("--org"), "expected --org error, got: {msg}");
}

#[test]
fn accepts_scope_on_create() {
    let def = widget_def();
    let op = create_op(&def);
    let mut scope = ScopeValues::default();
    scope.set("org", "acme");
    let result = validate_scope(&def, op, &scope);
    assert!(result.is_ok());
}

#[test]
fn scope_optional_on_list() {
    let def = widget_def();
    let op = list_op(&def);
    let scope = ScopeValues::default();
    let result = validate_scope(&def, op, &scope);
    assert!(result.is_ok());
}

#[test]
fn scope_optional_on_get() {
    let def = widget_def();
    let op = get_op(&def);
    let scope = ScopeValues::default();
    let result = validate_scope(&def, op, &scope);
    assert!(result.is_ok());
}

#[test]
fn rejects_missing_scope_on_delete() {
    let def = widget_def();
    let op = delete_op(&def);
    let scope = ScopeValues::default();
    let result = validate_scope(&def, op, &scope);
    assert!(result.is_err());
    let msg = result.unwrap_err().message;
    assert!(msg.contains("--org"), "expected --org error, got: {msg}");
}

#[test]
fn global_scope_never_requires_parent() {
    let def = ResourceDef {
        identity: ResourceIdentity {
            kind: "org",
            cli_name: "org",
            plural: "orgs",
            description: "Organization",
            aliases: &[],
        },
        scope: ScopeDef::global(),
        schema: ResourceSchema { fields: vec![] },
        operations: vec![OperationDef::create()],
        presentation: PresentationDef::none(),
    };
    let op = &def.operations[0];
    let scope = ScopeValues::default();
    let result = validate_scope(&def, op, &scope);
    assert!(result.is_ok());
}

// ── validate_required_fields ─────────────────────────────

#[test]
fn rejects_missing_required_op_arg() {
    let def = widget_def();
    let op = create_op(&def);
    // Provide schema fields but NOT the required op arg "material".
    let mut fields = HashMap::new();
    fields.insert("color".to_string(), "red".to_string());
    fields.insert("size".to_string(), "small".to_string());
    let result = validate_required_fields(&def, op, &fields);
    assert!(result.is_err());
    let msg = result.unwrap_err().message;
    assert!(
        msg.contains("material"),
        "expected 'material' in error, got: {msg}"
    );
}

#[test]
fn rejects_missing_schema_field_on_create() {
    let def = widget_def();
    let op = create_op(&def);
    // Provide the required op arg "material" but miss required schema field "color".
    let mut fields = HashMap::new();
    fields.insert("material".to_string(), "steel".to_string());
    fields.insert("size".to_string(), "small".to_string());
    let result = validate_required_fields(&def, op, &fields);
    assert!(result.is_err());
    let msg = result.unwrap_err().message;
    assert!(
        msg.contains("color"),
        "expected 'color' in error, got: {msg}"
    );
}

#[test]
fn accepts_all_required_fields() {
    let def = widget_def();
    let op = create_op(&def);
    let mut fields = HashMap::new();
    fields.insert("material".to_string(), "steel".to_string());
    fields.insert("color".to_string(), "red".to_string());
    fields.insert("size".to_string(), "small".to_string());
    let result = validate_required_fields(&def, op, &fields);
    assert!(result.is_ok());
}

#[test]
fn required_fields_not_checked_on_list() {
    let def = widget_def();
    let op = list_op(&def);
    let fields = HashMap::new(); // empty — should be fine for list
    let result = validate_required_fields(&def, op, &fields);
    assert!(result.is_ok());
}

#[test]
fn readonly_field_not_required_on_create() {
    // The "id" field is ReadOnly — it should NOT be required on create.
    let def = widget_def();
    let op = create_op(&def);
    let mut fields = HashMap::new();
    fields.insert("material".to_string(), "steel".to_string());
    fields.insert("color".to_string(), "red".to_string());
    fields.insert("size".to_string(), "small".to_string());
    // "id" is NOT provided — should still pass.
    let result = validate_required_fields(&def, op, &fields);
    assert!(result.is_ok());
}

#[test]
fn field_with_default_not_required_on_create() {
    // "count" has default "1" — should NOT be required on create.
    let def = widget_def();
    let op = create_op(&def);
    let mut fields = HashMap::new();
    fields.insert("material".to_string(), "steel".to_string());
    fields.insert("color".to_string(), "red".to_string());
    fields.insert("size".to_string(), "small".to_string());
    // "count" is NOT provided — should still pass because it has a default.
    let result = validate_required_fields(&def, op, &fields);
    assert!(result.is_ok());
}

// ── validate_field_types ─────────────────────────────────

#[test]
fn rejects_invalid_enum_value() {
    let def = widget_def();
    let op = create_op(&def);
    let mut fields = HashMap::new();
    fields.insert("size".to_string(), "xxl".to_string());
    let result = validate_field_types(&def, op, &fields);
    assert!(result.is_err());
    let msg = result.unwrap_err().message;
    assert!(msg.contains("--size"), "expected --size in error, got: {msg}");
    assert!(msg.contains("xxl"), "expected 'xxl' in error, got: {msg}");
    assert!(
        msg.contains("small") && msg.contains("medium") && msg.contains("large"),
        "expected allowed values in error, got: {msg}"
    );
}

#[test]
fn rejects_non_integer() {
    let def = widget_def();
    let op = create_op(&def);
    let mut fields = HashMap::new();
    fields.insert("count".to_string(), "abc".to_string());
    let result = validate_field_types(&def, op, &fields);
    assert!(result.is_err());
    let msg = result.unwrap_err().message;
    assert!(
        msg.contains("--count"),
        "expected --count in error, got: {msg}"
    );
    assert!(
        msg.contains("integer"),
        "expected 'integer' in error, got: {msg}"
    );
}

#[test]
fn accepts_valid_types() {
    let def = widget_def();
    let op = create_op(&def);
    let mut fields = HashMap::new();
    fields.insert("color".to_string(), "red".to_string());
    fields.insert("count".to_string(), "42".to_string());
    fields.insert("size".to_string(), "medium".to_string());
    let result = validate_field_types(&def, op, &fields);
    assert!(result.is_ok());
}

#[test]
fn accepts_negative_integer() {
    let def = widget_def();
    let op = create_op(&def);
    let mut fields = HashMap::new();
    fields.insert("count".to_string(), "-5".to_string());
    let result = validate_field_types(&def, op, &fields);
    assert!(result.is_ok());
}

#[test]
fn unknown_field_silently_skipped() {
    let def = widget_def();
    let op = create_op(&def);
    let mut fields = HashMap::new();
    fields.insert("not-in-schema".to_string(), "whatever".to_string());
    let result = validate_field_types(&def, op, &fields);
    assert!(result.is_ok());
}

#[test]
fn validates_op_arg_custom_field_type() {
    // "material" is a Custom ArgSource with String type — any value should pass.
    let def = widget_def();
    let op = create_op(&def);
    let mut fields = HashMap::new();
    fields.insert("material".to_string(), "anything-goes".to_string());
    let result = validate_field_types(&def, op, &fields);
    assert!(result.is_ok());
}

#[test]
fn validates_each_enum_variant() {
    let def = widget_def();
    let op = create_op(&def);
    for variant in &["small", "medium", "large"] {
        let mut fields = HashMap::new();
        fields.insert("size".to_string(), variant.to_string());
        let result = validate_field_types(&def, op, &fields);
        assert!(result.is_ok(), "enum variant '{variant}' should be accepted");
    }
}

// ── apply_defaults ───────────────────────────────────────

#[test]
fn applies_schema_defaults() {
    let def = widget_def();
    let op = create_op(&def);
    let mut fields = HashMap::new();
    apply_defaults(&def, op, &mut fields);
    assert_eq!(fields.get("count").map(|s| s.as_str()), Some("1"));
}

#[test]
fn does_not_overwrite_provided_values() {
    let def = widget_def();
    let op = create_op(&def);
    let mut fields = HashMap::new();
    fields.insert("count".to_string(), "99".to_string());
    apply_defaults(&def, op, &mut fields);
    assert_eq!(
        fields.get("count").map(|s| s.as_str()),
        Some("99"),
        "apply_defaults should not overwrite user-provided value"
    );
}

#[test]
fn applies_op_arg_defaults() {
    // Build a resource with an operation arg that has a default.
    let def = ResourceDef {
        identity: ResourceIdentity {
            kind: "gizmo",
            cli_name: "gizmo",
            plural: "gizmos",
            description: "Test gizmo",
            aliases: &[],
        },
        scope: ScopeDef::global(),
        schema: ResourceSchema { fields: vec![] },
        operations: vec![
            OperationDef::create().with_arg(OperationArg::optional(
                "mode",
                FieldDef::string("mode", "Operating mode").with_default("auto"),
            )),
        ],
        presentation: PresentationDef::none(),
    };
    let op = &def.operations[0];
    let mut fields = HashMap::new();
    apply_defaults(&def, op, &mut fields);
    assert_eq!(
        fields.get("mode").map(|s| s.as_str()),
        Some("auto"),
        "operation arg default should be applied"
    );
}

#[test]
fn op_arg_default_does_not_overwrite() {
    let def = ResourceDef {
        identity: ResourceIdentity {
            kind: "gizmo",
            cli_name: "gizmo",
            plural: "gizmos",
            description: "Test gizmo",
            aliases: &[],
        },
        scope: ScopeDef::global(),
        schema: ResourceSchema { fields: vec![] },
        operations: vec![
            OperationDef::create().with_arg(OperationArg::optional(
                "mode",
                FieldDef::string("mode", "Operating mode").with_default("auto"),
            )),
        ],
        presentation: PresentationDef::none(),
    };
    let op = &def.operations[0];
    let mut fields = HashMap::new();
    fields.insert("mode".to_string(), "manual".to_string());
    apply_defaults(&def, op, &mut fields);
    assert_eq!(fields.get("mode").map(|s| s.as_str()), Some("manual"));
}

// ── filter_readonly_fields ───────────────────────────────

#[test]
fn strips_readonly_from_input() {
    let def = widget_def();
    let mut fields = HashMap::new();
    fields.insert("id".to_string(), "injected-id".to_string());
    fields.insert("color".to_string(), "red".to_string());
    filter_readonly_fields(&def, &mut fields);
    assert!(
        !fields.contains_key("id"),
        "ReadOnly field 'id' should be stripped"
    );
}

#[test]
fn keeps_normal_fields() {
    let def = widget_def();
    let mut fields = HashMap::new();
    fields.insert("color".to_string(), "red".to_string());
    fields.insert("count".to_string(), "5".to_string());
    fields.insert("size".to_string(), "large".to_string());
    filter_readonly_fields(&def, &mut fields);
    assert!(fields.contains_key("color"));
    assert!(fields.contains_key("count"));
    assert!(fields.contains_key("size"));
}

#[test]
fn keeps_non_schema_fields() {
    // Fields not in the schema (e.g. operation-specific args) should be kept.
    let def = widget_def();
    let mut fields = HashMap::new();
    fields.insert("material".to_string(), "steel".to_string());
    filter_readonly_fields(&def, &mut fields);
    assert!(
        fields.contains_key("material"),
        "non-schema field should survive filter_readonly_fields"
    );
}

#[test]
fn strips_internal_fields() {
    // Build a def with an Internal field.
    let def = ResourceDef {
        identity: ResourceIdentity {
            kind: "secret",
            cli_name: "secret",
            plural: "secrets",
            description: "Test secret",
            aliases: &[],
        },
        scope: ScopeDef::global(),
        schema: ResourceSchema {
            fields: vec![
                FieldDef::string("name", "Secret name"),
                FieldDef {
                    name: "internal-token",
                    description: "Internal use only",
                    field_type: FieldType::String,
                    mutability: Mutability::Internal,
                    short: None,
                    default: None,
                    env_var: None,
                    visibility: CliVisibility::Hidden,
                },
            ],
        },
        operations: vec![],
        presentation: PresentationDef::none(),
    };
    let mut fields = HashMap::new();
    fields.insert("name".to_string(), "my-secret".to_string());
    fields.insert("internal-token".to_string(), "tok_abc".to_string());
    filter_readonly_fields(&def, &mut fields);
    assert!(!fields.contains_key("internal-token"));
    assert!(fields.contains_key("name"));
}

// ── Full pipeline integration ────────────────────────────
// Simulate what dispatch does: validate_name -> validate_scope ->
// validate_required_fields -> validate_field_types -> apply_defaults ->
// filter_readonly_fields

#[test]
fn full_pipeline_valid_create() {
    let def = widget_def();
    let op = create_op(&def);
    let name = Some("my-widget".to_string());
    let mut scope = ScopeValues::default();
    scope.set("org", "acme");
    let mut fields = HashMap::new();
    fields.insert("material".to_string(), "steel".to_string());
    fields.insert("color".to_string(), "red".to_string());
    fields.insert("size".to_string(), "medium".to_string());
    fields.insert("id".to_string(), "should-be-stripped".to_string());

    // Step 1: validate_name
    validate_name(&name, &op.semantics).expect("name should be valid");

    // Step 2: validate_scope
    validate_scope(&def, op, &scope).expect("scope should be valid");

    // Step 3: validate_required_fields
    validate_required_fields(&def, op, &fields).expect("required fields present");

    // Step 4: validate_field_types
    validate_field_types(&def, op, &fields).expect("field types valid");

    // Step 5: apply_defaults
    apply_defaults(&def, op, &mut fields);
    assert_eq!(
        fields.get("count").map(|s| s.as_str()),
        Some("1"),
        "count should get default"
    );

    // Step 6: filter_readonly_fields
    filter_readonly_fields(&def, &mut fields);
    assert!(!fields.contains_key("id"), "id should be stripped");

    // Final state: fields should have color, count, size, material
    assert!(fields.contains_key("color"));
    assert!(fields.contains_key("count"));
    assert!(fields.contains_key("size"));
    assert!(fields.contains_key("material"));
}

#[test]
fn full_pipeline_rejects_early_on_bad_name() {
    let def = widget_def();
    let op = create_op(&def);
    let name = Some("BAD".to_string());

    // Should fail at step 1 — no need to check later steps.
    let result = validate_name(&name, &op.semantics);
    assert!(result.is_err());
}

#[test]
fn full_pipeline_rejects_on_missing_scope() {
    let def = widget_def();
    let op = create_op(&def);
    let name = Some("my-widget".to_string());
    let scope = ScopeValues::default(); // missing --org

    // Step 1 passes.
    validate_name(&name, &op.semantics).expect("name ok");

    // Step 2 fails.
    let result = validate_scope(&def, op, &scope);
    assert!(result.is_err());
}

// ── Error classification coverage (T4) ───────────────────
// classify_anyhow is private to api::route_gen, so we test via the
// public NaukaError constructors and ErrorCode mappings.

#[test]
fn error_classification_coverage() {
    use nauka_core::error::{ErrorCode, NaukaError};

    // ValidationError -> 400
    let err = NaukaError::validation("bad input");
    assert_eq!(err.code, ErrorCode::ValidationError);
    assert_eq!(err.http_status(), 400);

    // InvalidName -> 400
    let err = NaukaError::invalid_name("BAD", "uppercase");
    assert_eq!(err.code, ErrorCode::InvalidName);
    assert_eq!(err.http_status(), 400);

    // NotFound -> 404
    let err = NaukaError::not_found("vpc", "web");
    assert_eq!(err.code, ErrorCode::ResourceNotFound);
    assert_eq!(err.http_status(), 404);

    // AlreadyExists -> 409
    let err = NaukaError::already_exists("vpc", "web");
    assert_eq!(err.code, ErrorCode::ResourceAlreadyExists);
    assert_eq!(err.http_status(), 409);

    // PermissionDenied -> 403
    let err = NaukaError::permission_denied("not allowed");
    assert_eq!(err.code, ErrorCode::PermissionDenied);
    assert_eq!(err.http_status(), 403);

    // NotImplemented -> 501
    let err = NaukaError::not_implemented("resize");
    assert_eq!(err.code, ErrorCode::NotImplemented);
    assert_eq!(err.http_status(), 501);
}

#[test]
fn validation_errors_are_not_retryable() {
    use nauka_core::error::NaukaError;

    let err = NaukaError::validation("bad");
    assert!(!err.is_retryable());

    let err = NaukaError::invalid_name("x", "too short");
    assert!(!err.is_retryable());

    let err = NaukaError::not_found("vpc", "web");
    assert!(!err.is_retryable());
}
