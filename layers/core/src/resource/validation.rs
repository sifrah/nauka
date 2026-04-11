//! Centralized pre-handler validation pipeline.
//!
//! Called from both CLI dispatch and API route_gen BEFORE the handler.
//! Handlers can assume all inputs are validated.

use super::constraint::FieldMap;
use super::operation::{ArgSource, OperationDef, OperationSemantics};
use super::registry::ScopeValues;
use super::schema::{FieldDef, FieldType, Mutability};
use super::ResourceDef;
use crate::error::NaukaError;

/// Validate the resource name based on operation semantics.
///
/// Create, Get, Delete, and Update operations require a name that passes
/// `crate::validate::name()`. List and Action operations skip name validation
/// (List never has a name; Action may or may not).
pub fn validate_name(
    name: &Option<String>,
    semantics: &OperationSemantics,
) -> Result<(), NaukaError> {
    match semantics {
        OperationSemantics::Create
        | OperationSemantics::Get
        | OperationSemantics::Delete
        | OperationSemantics::Update { .. } => {
            let n = name
                .as_deref()
                .ok_or_else(|| NaukaError::validation("name is required for this operation"))?;
            crate::validate::name(n)
        }
        OperationSemantics::List | OperationSemantics::Action => Ok(()),
    }
}

/// Validate that required scope parents are present.
///
/// For Create and Delete operations, every parent with `required_on_create == true`
/// must be present in the scope values. Delete requires the same scope as Create
/// to prevent accidental deletion of the wrong resource.
pub fn validate_scope(
    def: &ResourceDef,
    op: &OperationDef,
    scope: &ScopeValues,
) -> Result<(), NaukaError> {
    for parent in &def.scope.parents {
        let required = match &op.semantics {
            OperationSemantics::Create | OperationSemantics::Delete => parent.required_on_create,
            _ => false,
        };

        if required && scope.get(parent.kind).is_none() {
            let flag_name = parent.flag.trim_start_matches('-');
            return Err(NaukaError::validation(format!(
                "--{} is required",
                flag_name,
            )));
        }
    }
    Ok(())
}

/// Validate that all required fields are present.
///
/// Checks two categories:
/// 1. Operation args marked as `required: true` must be in the field map.
/// 2. For Create operations, schema fields with `CreateOnly` or `Mutable` mutability
///    and no default value must also be present.
pub fn validate_required_fields(
    def: &ResourceDef,
    op: &OperationDef,
    fields: &FieldMap,
) -> Result<(), NaukaError> {
    // Check required operation args.
    for arg in &op.args {
        if arg.required && !fields.contains_key(arg.name) {
            return Err(NaukaError::validation(format!(
                "missing required field: {}",
                arg.name,
            )));
        }
    }

    // For Create, schema fields that are settable and have no default must be present.
    if op.semantics == OperationSemantics::Create {
        for field in &def.schema.fields {
            let settable = matches!(
                field.mutability,
                Mutability::CreateOnly | Mutability::Mutable
            );
            if settable && field.default.is_none() && !fields.contains_key(field.name) {
                return Err(NaukaError::validation(format!(
                    "missing required field: {}",
                    field.name,
                )));
            }
        }
    }

    Ok(())
}

/// Validate field values match their declared types.
///
/// Looks up the `FieldDef` for each field (from schema or operation args) and
/// validates the value against its `FieldType`. Fields without a known definition
/// are silently skipped (they may be internal or framework-managed).
pub fn validate_field_types(
    def: &ResourceDef,
    op: &OperationDef,
    fields: &FieldMap,
) -> Result<(), NaukaError> {
    for (name, value) in fields {
        let field_def = match find_field_def(def, op, name) {
            Some(fd) => fd,
            None => continue,
        };
        validate_single_field(name, value, &field_def.field_type)?;
    }
    Ok(())
}

/// Apply default values for fields not already present.
///
/// Fills in defaults from two sources:
/// 1. Schema fields with `default: Some(val)`.
/// 2. Operation args with `ArgSource::Custom(field_def)` where the field def
///    has a default.
pub fn apply_defaults(def: &ResourceDef, op: &OperationDef, fields: &mut FieldMap) {
    // Schema-level defaults.
    for field in &def.schema.fields {
        if let Some(default) = field.default {
            fields
                .entry(field.name.to_string())
                .or_insert_with(|| default.to_string());
        }
    }

    // Operation arg defaults.
    for arg in &op.args {
        if let ArgSource::Custom(ref field_def) = arg.source {
            if let Some(default) = field_def.default {
                fields
                    .entry(arg.name.to_string())
                    .or_insert_with(|| default.to_string());
            }
        }
    }
}

/// Remove fields that clients should never set.
///
/// Strips any field whose `FieldDef` has `Mutability::ReadOnly` or
/// `Mutability::Internal`. This prevents API clients from injecting system
/// fields like `id` or `created_at`.
pub fn filter_readonly_fields(def: &ResourceDef, fields: &mut FieldMap) {
    fields.retain(|name, _| {
        let Some(field) = def.schema.fields.iter().find(|f| f.name == name) else {
            // Not a schema field — keep it (might be an operation-specific arg).
            return true;
        };
        !matches!(
            field.mutability,
            Mutability::ReadOnly | Mutability::Internal
        )
    });
}

// ── Helpers ──────────────────────────────────────────────

/// Find the `FieldDef` for a field by name, searching both the resource schema
/// and the operation's args.
fn find_field_def<'a>(
    def: &'a ResourceDef,
    op: &'a OperationDef,
    name: &str,
) -> Option<&'a FieldDef> {
    // Check schema fields first.
    if let Some(field) = def.schema.fields.iter().find(|f| f.name == name) {
        return Some(field);
    }
    // Then check operation args with custom field defs.
    for arg in &op.args {
        if arg.name == name {
            if let ArgSource::Custom(ref field_def) = arg.source {
                return Some(field_def);
            }
            // ArgSource::FromSchema — already checked in schema above.
        }
    }
    None
}

/// Validate a single field value against its declared type.
fn validate_single_field(
    name: &str,
    value: &str,
    field_type: &FieldType,
) -> Result<(), NaukaError> {
    match field_type {
        FieldType::Enum(enum_def) => {
            if !enum_def.values.contains(&value) {
                return Err(NaukaError::validation(format!(
                    "--{}: must be one of [{}], got '{}'",
                    name,
                    enum_def.values.join(", "),
                    value,
                )));
            }
        }
        FieldType::Integer => {
            value.parse::<i64>().map_err(|_| {
                NaukaError::validation(format!("--{}: expected an integer, got '{}'", name, value,))
            })?;
        }
        FieldType::Port => {
            value.parse::<u16>().map_err(|_| {
                NaukaError::validation(format!(
                    "--{}: expected a port number (0-65535), got '{}'",
                    name, value,
                ))
            })?;
        }
        FieldType::SizeGb | FieldType::SizeMb => {
            value.parse::<u64>().map_err(|_| {
                NaukaError::validation(format!(
                    "--{}: expected a positive number, got '{}'",
                    name, value,
                ))
            })?;
        }
        FieldType::Cidr => {
            crate::validate::cidr(value).map_err(|_| {
                NaukaError::validation(format!("--{}: invalid CIDR notation '{}'", name, value,))
            })?;
        }
        FieldType::IpAddr => {
            value.parse::<std::net::IpAddr>().map_err(|_| {
                NaukaError::validation(format!("--{}: invalid IP address '{}'", name, value,))
            })?;
        }
        FieldType::Flag => {
            if value != "true" && value != "false" {
                return Err(NaukaError::validation(format!(
                    "--{}: expected 'true' or 'false', got '{}'",
                    name, value,
                )));
            }
        }
        // String, Path, Secret, Duration, KeyValue, ResourceRef: accept any string.
        FieldType::String
        | FieldType::Path
        | FieldType::Secret
        | FieldType::Duration
        | FieldType::KeyValue
        | FieldType::ResourceRef(_) => {}
    }
    Ok(())
}

/// Validate the response contract: Resource responses must have `id` and `name`.
///
/// Called after handler returns, in both CLI and API paths.
/// Returns an error if the contract is violated (replaces the old debug_assert).
pub fn validate_response_contract(
    kind: &str,
    response: &super::registry::OperationResponse,
) -> Result<(), NaukaError> {
    match response {
        super::registry::OperationResponse::Resource(v) => {
            if v.get("id").is_none() {
                return Err(NaukaError::internal(format!(
                    "[E080] {kind}: response JSON missing 'id'"
                )));
            }
            if v.get("name").is_none() {
                return Err(NaukaError::internal(format!(
                    "[E081] {kind}: response JSON missing 'name'"
                )));
            }
        }
        super::registry::OperationResponse::ResourceList(items) => {
            for (i, item) in items.iter().enumerate() {
                if item.get("id").is_none() {
                    return Err(NaukaError::internal(format!(
                        "[E080] {kind}: list item [{i}] missing 'id'"
                    )));
                }
                if item.get("name").is_none() {
                    return Err(NaukaError::internal(format!(
                        "[E081] {kind}: list item [{i}] missing 'name'"
                    )));
                }
            }
        }
        _ => {}
    }
    Ok(())
}

/// Filter secret fields from response JSON.
///
/// Replaces any field marked as `FieldType::Secret` in the schema with `"****"`.
/// Prevents accidental exposure of passwords, API keys, etc.
pub fn filter_response_secrets(
    def: &ResourceDef,
    response: &mut super::registry::OperationResponse,
) {
    let secret_fields: Vec<&str> = def
        .schema
        .fields
        .iter()
        .filter(|f| matches!(f.field_type, FieldType::Secret))
        .map(|f| f.name)
        .collect();

    if secret_fields.is_empty() {
        return;
    }

    match response {
        super::registry::OperationResponse::Resource(v) => {
            mask_secrets(v, &secret_fields);
        }
        super::registry::OperationResponse::ResourceList(items) => {
            for item in items.iter_mut() {
                mask_secrets(item, &secret_fields);
            }
        }
        _ => {}
    }
}

fn mask_secrets(value: &mut serde_json::Value, secret_fields: &[&str]) {
    if let Some(obj) = value.as_object_mut() {
        for field in secret_fields {
            if obj.contains_key(*field) {
                obj.insert(field.to_string(), serde_json::json!("****"));
            }
        }
    }
}

/// Normalize timestamp fields in API responses to ISO 8601.
///
/// For any field ending in `_at` whose value is a number (epoch seconds),
/// converts it to ISO 8601 format (e.g., `"2026-04-11T14:30:00Z"`).
pub fn normalize_timestamps(response: &mut super::registry::OperationResponse) {
    match response {
        super::registry::OperationResponse::Resource(v) => {
            normalize_value_timestamps(v);
        }
        super::registry::OperationResponse::ResourceList(items) => {
            for item in items.iter_mut() {
                normalize_value_timestamps(item);
            }
        }
        _ => {}
    }
}

fn normalize_value_timestamps(value: &mut serde_json::Value) {
    if let Some(obj) = value.as_object_mut() {
        let keys: Vec<String> = obj.keys().filter(|k| k.ends_with("_at")).cloned().collect();
        for key in keys {
            if let Some(serde_json::Value::Number(n)) = obj.get(&key) {
                if let Some(epoch) = n.as_u64() {
                    let iso = crate::resource::api_response::epoch_to_iso8601(epoch);
                    obj.insert(key, serde_json::json!(iso));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resource::identity::ResourceIdentity;
    use crate::resource::operation::{OperationArg, OutputDef, OutputKind, ProgressHint};
    use crate::resource::presentation::PresentationDef;
    use crate::resource::schema::{FieldDef, ResourceSchema};
    use crate::resource::scope::ScopeDef;

    fn test_resource_def() -> ResourceDef {
        ResourceDef {
            identity: ResourceIdentity {
                kind: "vm",
                cli_name: "vm",
                description: "Virtual machine",
                plural: "VMs",
                aliases: &[],
            },
            scope: ScopeDef::within("vpc", "--vpc", "Parent VPC"),
            schema: ResourceSchema {
                fields: vec![
                    FieldDef::string("region", "Region").with_default("fsn1"),
                    FieldDef::string("type", "Server type"),
                    FieldDef {
                        name: "id",
                        description: "Resource ID",
                        field_type: FieldType::String,
                        mutability: Mutability::ReadOnly,
                        short: None,
                        default: None,
                        env_var: None,
                        visibility: crate::resource::schema::CliVisibility::Hidden,
                    },
                    FieldDef::integer("vcpus", "Number of vCPUs").with_default("2"),
                    FieldDef::enum_field("size", "Disk size", &["small", "medium", "large"]),
                ],
            },
            operations: Vec::new(),
            presentation: PresentationDef::none(),
        }
    }

    fn create_op() -> OperationDef {
        OperationDef {
            name: "create",
            description: "Create a VM",
            semantics: OperationSemantics::Create,
            args: vec![OperationArg {
                name: "image",
                description: "OS image",
                required: true,
                source: ArgSource::Custom(FieldDef::string("image", "OS image")),
            }],
            constraints: Vec::new(),
            confirmable: false,
            output: OutputDef {
                kind: OutputKind::Resource,
                success_message: None,
            },
            examples: Vec::new(),
            progress: ProgressHint::None,
        }
    }

    fn list_op() -> OperationDef {
        OperationDef {
            name: "list",
            description: "List VMs",
            semantics: OperationSemantics::List,
            args: Vec::new(),
            constraints: Vec::new(),
            confirmable: false,
            output: OutputDef {
                kind: OutputKind::ResourceList,
                success_message: None,
            },
            examples: Vec::new(),
            progress: ProgressHint::None,
        }
    }

    // ── validate_name ──

    #[test]
    fn name_required_for_create() {
        let result = validate_name(&None, &OperationSemantics::Create);
        assert!(result.is_err());
    }

    #[test]
    fn name_valid_for_create() {
        let result = validate_name(&Some("my-vm".to_string()), &OperationSemantics::Create);
        assert!(result.is_ok());
    }

    #[test]
    fn name_skipped_for_list() {
        let result = validate_name(&None, &OperationSemantics::List);
        assert!(result.is_ok());
    }

    #[test]
    fn name_skipped_for_action() {
        let result = validate_name(&None, &OperationSemantics::Action);
        assert!(result.is_ok());
    }

    #[test]
    fn name_invalid_characters() {
        let result = validate_name(&Some("MY_VM".to_string()), &OperationSemantics::Get);
        assert!(result.is_err());
    }

    // ── validate_scope ──

    #[test]
    fn scope_required_on_create() {
        let def = test_resource_def();
        let op = create_op();
        let scope = ScopeValues::default();
        let result = validate_scope(&def, &op, &scope);
        assert!(result.is_err());
        assert!(result.unwrap_err().message.contains("--vpc"));
    }

    #[test]
    fn scope_present_on_create() {
        let def = test_resource_def();
        let op = create_op();
        let mut scope = ScopeValues::default();
        scope.set("vpc", "my-vpc");
        let result = validate_scope(&def, &op, &scope);
        assert!(result.is_ok());
    }

    #[test]
    fn scope_optional_on_list() {
        let def = test_resource_def();
        let op = list_op();
        let scope = ScopeValues::default();
        let result = validate_scope(&def, &op, &scope);
        assert!(result.is_ok());
    }

    // ── validate_required_fields ──

    #[test]
    fn required_op_arg_missing() {
        let def = test_resource_def();
        let op = create_op();
        let fields = FieldMap::new();
        let result = validate_required_fields(&def, &op, &fields);
        assert!(result.is_err());
        assert!(result.unwrap_err().message.contains("image"));
    }

    #[test]
    fn required_schema_field_missing_on_create() {
        let def = test_resource_def();
        let op = create_op();
        // Provide the required op arg but not the schema field "type" (no default).
        let mut fields = FieldMap::new();
        fields.insert("image".to_string(), "ubuntu-22.04".to_string());
        let result = validate_required_fields(&def, &op, &fields);
        assert!(result.is_err());
        assert!(result.unwrap_err().message.contains("type"));
    }

    #[test]
    fn all_required_fields_present() {
        let def = test_resource_def();
        let op = create_op();
        let mut fields = FieldMap::new();
        fields.insert("image".to_string(), "ubuntu-22.04".to_string());
        fields.insert("type".to_string(), "cx21".to_string());
        fields.insert("size".to_string(), "small".to_string());
        let result = validate_required_fields(&def, &op, &fields);
        assert!(result.is_ok());
    }

    // ── validate_field_types ──

    #[test]
    fn valid_enum_field() {
        let def = test_resource_def();
        let op = create_op();
        let mut fields = FieldMap::new();
        fields.insert("size".to_string(), "medium".to_string());
        let result = validate_field_types(&def, &op, &fields);
        assert!(result.is_ok());
    }

    #[test]
    fn invalid_enum_field() {
        let def = test_resource_def();
        let op = create_op();
        let mut fields = FieldMap::new();
        fields.insert("size".to_string(), "xxl".to_string());
        let result = validate_field_types(&def, &op, &fields);
        assert!(result.is_err());
        let msg = result.unwrap_err().message;
        assert!(msg.contains("--size"));
        assert!(msg.contains("xxl"));
    }

    #[test]
    fn valid_integer_field() {
        let def = test_resource_def();
        let op = create_op();
        let mut fields = FieldMap::new();
        fields.insert("vcpus".to_string(), "4".to_string());
        let result = validate_field_types(&def, &op, &fields);
        assert!(result.is_ok());
    }

    #[test]
    fn invalid_integer_field() {
        let def = test_resource_def();
        let op = create_op();
        let mut fields = FieldMap::new();
        fields.insert("vcpus".to_string(), "abc".to_string());
        let result = validate_field_types(&def, &op, &fields);
        assert!(result.is_err());
        assert!(result.unwrap_err().message.contains("--vcpus"));
    }

    #[test]
    fn unknown_field_skipped() {
        let def = test_resource_def();
        let op = create_op();
        let mut fields = FieldMap::new();
        fields.insert("unknown-field".to_string(), "whatever".to_string());
        let result = validate_field_types(&def, &op, &fields);
        assert!(result.is_ok());
    }

    // ── apply_defaults ──

    #[test]
    fn defaults_applied() {
        let def = test_resource_def();
        let op = create_op();
        let mut fields = FieldMap::new();
        apply_defaults(&def, &op, &mut fields);
        assert_eq!(fields.get("region").map(|s| s.as_str()), Some("fsn1"));
        assert_eq!(fields.get("vcpus").map(|s| s.as_str()), Some("2"));
    }

    #[test]
    fn defaults_do_not_overwrite() {
        let def = test_resource_def();
        let op = create_op();
        let mut fields = FieldMap::new();
        fields.insert("region".to_string(), "nbg1".to_string());
        apply_defaults(&def, &op, &mut fields);
        assert_eq!(fields.get("region").map(|s| s.as_str()), Some("nbg1"));
    }

    // ── filter_readonly_fields ──

    #[test]
    fn readonly_fields_removed() {
        let def = test_resource_def();
        let mut fields = FieldMap::new();
        fields.insert("id".to_string(), "injected-id".to_string());
        fields.insert("region".to_string(), "fsn1".to_string());
        filter_readonly_fields(&def, &mut fields);
        assert!(!fields.contains_key("id"));
        assert!(fields.contains_key("region"));
    }

    #[test]
    fn non_schema_fields_kept() {
        let def = test_resource_def();
        let mut fields = FieldMap::new();
        fields.insert("custom-op-arg".to_string(), "value".to_string());
        filter_readonly_fields(&def, &mut fields);
        assert!(fields.contains_key("custom-op-arg"));
    }

    // ── validate_response_contract ──

    #[test]
    fn response_contract_valid() {
        use crate::resource::registry::OperationResponse;
        let resp = OperationResponse::Resource(serde_json::json!({"id": "x", "name": "y"}));
        assert!(validate_response_contract("test", &resp).is_ok());
    }

    #[test]
    fn response_contract_missing_id() {
        use crate::resource::registry::OperationResponse;
        let resp = OperationResponse::Resource(serde_json::json!({"name": "y"}));
        assert!(validate_response_contract("test", &resp).is_err());
    }

    #[test]
    fn response_contract_list_missing_name() {
        use crate::resource::registry::OperationResponse;
        let resp = OperationResponse::ResourceList(vec![serde_json::json!({"id": "x"})]);
        assert!(validate_response_contract("test", &resp).is_err());
    }

    // ── filter_response_secrets ──

    #[test]
    fn secrets_masked() {
        use crate::resource::registry::OperationResponse;
        let mut def = test_resource_def();
        def.schema
            .fields
            .push(FieldDef::secret("api-key", "API key"));
        let mut resp =
            OperationResponse::Resource(serde_json::json!({"api-key": "sk-123", "name": "x"}));
        filter_response_secrets(&def, &mut resp);
        if let OperationResponse::Resource(v) = &resp {
            assert_eq!(v["api-key"], "****");
            assert_eq!(v["name"], "x");
        }
    }

    // ── normalize_timestamps ──

    #[test]
    fn timestamps_normalized() {
        use crate::resource::registry::OperationResponse;
        let mut resp = OperationResponse::Resource(serde_json::json!({
            "name": "x",
            "created_at": 1712847000u64,
            "status": "active"
        }));
        normalize_timestamps(&mut resp);
        if let OperationResponse::Resource(v) = &resp {
            let ts = v["created_at"].as_str().unwrap();
            assert!(ts.ends_with('Z'), "expected ISO 8601, got: {ts}");
            assert!(ts.contains('T'), "expected ISO 8601, got: {ts}");
            // status should not be touched
            assert_eq!(v["status"], "active");
        }
    }
}
