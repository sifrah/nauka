//! ResourceDef lint engine.
//!
//! Enforces structural, behavioral, and UX consistency across all resource
//! definitions.  Rules are checked at two points:
//!
//! 1. **`lint_def()`** — called from `.done()`, validates a single ResourceDef.
//!    Errors panic immediately (binary won't start with a broken def).
//! 2. **`lint_registry()`** — called from tests, validates cross-resource rules.
//!
//! Severity levels:
//! - `Error`   — invariant violation, panics in `.done()`.
//! - `Warning` — convention violation, fails CI test.

use super::operation::{OperationSemantics, OutputKind, ProgressHint};
use super::presentation::DisplayFormat;
use super::ResourceDef;

/// Lint severity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity {
    Error,
    Warning,
}

/// A single lint violation.
#[derive(Debug, Clone)]
pub struct Violation {
    pub rule: &'static str,
    pub resource: &'static str,
    pub message: String,
    pub severity: Severity,
}

impl std::fmt::Display for Violation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let level = match self.severity {
            Severity::Error => "ERROR",
            Severity::Warning => "WARN",
        };
        write!(
            f,
            "[{}] {} ({}): {}",
            self.rule, self.resource, level, self.message
        )
    }
}

// ── Public API ────────────────────────────────────────────────

/// Validate a single ResourceDef for internal consistency.
pub fn lint_def(def: &ResourceDef) -> Vec<Violation> {
    let mut v = Vec::new();
    let kind = def.identity.kind;

    structural_rules(def, kind, &mut v);
    ux_rules(def, kind, &mut v);
    output_rules(def, kind, &mut v);
    scope_rules(def, kind, &mut v);
    description_rules(def, kind, &mut v);
    presentation_alignment_rules(def, kind, &mut v);
    state_machine_rules(def, kind, &mut v);
    progress_rules(def, kind, &mut v);
    naming_rules(def, kind, &mut v);

    v
}

/// Validate a flat list of ResourceDefs for cross-resource consistency.
pub fn lint_registry(defs: &[&ResourceDef]) -> Vec<Violation> {
    let mut v = Vec::new();
    registry_rules(defs, &mut v);
    v
}

/// Format a list of violations for display.
pub fn format_violations(violations: &[Violation]) -> String {
    violations
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join("\n")
}

// ── Rule implementations ──────────────────────────────────────

fn has_op(def: &ResourceDef, name: &str) -> bool {
    def.operations.iter().any(|op| op.name == name)
}

fn is_crud(def: &ResourceDef) -> bool {
    has_op(def, "create") && has_op(def, "list") && has_op(def, "get") && has_op(def, "delete")
}

fn has_column(def: &ResourceDef, field: &str) -> bool {
    def.presentation
        .table
        .as_ref()
        .map(|t| t.columns.iter().any(|c| c.field == field))
        .unwrap_or(false)
}

fn has_column_with_format(def: &ResourceDef, field: &str, fmt: &DisplayFormat) -> bool {
    def.presentation
        .table
        .as_ref()
        .map(|t| {
            t.columns.iter().any(|c| {
                c.field == field && std::mem::discriminant(&c.format) == std::mem::discriminant(fmt)
            })
        })
        .unwrap_or(false)
}

fn has_detail_field(def: &ResourceDef, field: &str) -> bool {
    def.presentation
        .detail
        .as_ref()
        .map(|d| {
            d.sections
                .iter()
                .any(|s| s.fields.iter().any(|f| f.field == field))
        })
        .unwrap_or(false)
}

fn has_detail_field_with_format(def: &ResourceDef, field: &str, fmt: &DisplayFormat) -> bool {
    def.presentation
        .detail
        .as_ref()
        .map(|d| {
            d.sections.iter().any(|s| {
                s.fields.iter().any(|f| {
                    f.field == field
                        && std::mem::discriminant(&f.format) == std::mem::discriminant(fmt)
                })
            })
        })
        .unwrap_or(false)
}

fn is_snake_case(s: &str) -> bool {
    !s.is_empty()
        && s.chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

fn is_kebab_lower(s: &str) -> bool {
    !s.is_empty()
        && s.chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
}

// ── Structural (E001–E004, E030–E031, E090, E100) ────────────

fn structural_rules(def: &ResourceDef, kind: &'static str, v: &mut Vec<Violation>) {
    // E001: list requires table
    if has_op(def, "list") && def.presentation.table.is_none() {
        v.push(Violation {
            rule: "E001",
            resource: kind,
            message: "has 'list' operation but no table columns defined".into(),
            severity: Severity::Error,
        });
    }

    // E002: get requires detail
    if has_op(def, "get") && def.presentation.detail.is_none() {
        v.push(Violation {
            rule: "E002",
            resource: kind,
            message: "has 'get' operation but no detail section defined".into(),
            severity: Severity::Error,
        });
    }

    // E003: no duplicate operation names
    let mut seen_ops = std::collections::HashSet::new();
    for op in &def.operations {
        if !seen_ops.insert(op.name) {
            v.push(Violation {
                rule: "E003",
                resource: kind,
                message: format!("duplicate operation name '{}'", op.name),
                severity: Severity::Error,
            });
        }
    }

    // E004: no duplicate column headers
    if let Some(table) = &def.presentation.table {
        let mut seen_cols = std::collections::HashSet::new();
        for col in &table.columns {
            if !seen_cols.insert(col.header) {
                v.push(Violation {
                    rule: "E004",
                    resource: kind,
                    message: format!("duplicate column header '{}'", col.header),
                    severity: Severity::Error,
                });
            }
        }
    }

    // E030: kind must be lowercase kebab
    if !is_kebab_lower(kind) {
        v.push(Violation {
            rule: "E030",
            resource: kind,
            message: format!("kind '{}' must be lowercase [a-z0-9-]+", kind),
            severity: Severity::Error,
        });
    }

    // E031: plural must differ from kind
    if def.identity.plural == kind {
        v.push(Violation {
            rule: "E031",
            resource: kind,
            message: format!(
                "plural '{}' must differ from kind '{}'",
                def.identity.plural, kind
            ),
            severity: Severity::Error,
        });
    }

    // E090: column and detail fields must be snake_case
    if let Some(table) = &def.presentation.table {
        for col in &table.columns {
            if !is_snake_case(col.field) {
                v.push(Violation {
                    rule: "E090",
                    resource: kind,
                    message: format!("column field '{}' is not snake_case", col.field),
                    severity: Severity::Error,
                });
            }
        }
    }
    if let Some(detail) = &def.presentation.detail {
        for section in &detail.sections {
            for field in &section.fields {
                if !is_snake_case(field.field) {
                    v.push(Violation {
                        rule: "E090",
                        resource: kind,
                        message: format!("detail field '{}' is not snake_case", field.field),
                        severity: Severity::Error,
                    });
                }
            }
        }
    }

    // E100: delete must resolve unambiguously
    if has_op(def, "delete") && !def.scope.parents.is_empty() {
        let create_required: std::collections::HashSet<&str> = def
            .scope
            .parents
            .iter()
            .filter(|p| p.required_on_create)
            .map(|p| p.kind)
            .collect();
        let delete_resolvable: std::collections::HashSet<&str> = def
            .scope
            .parents
            .iter()
            .filter(|p| p.required_on_resolve)
            .map(|p| p.kind)
            .collect();

        // If uniqueness is within parent but delete doesn't require the scope parent,
        // and resolve is not required, it's ambiguous
        if let super::scope::UniquenessScope::WithinParent(parent) = &def.scope.uniqueness {
            if create_required.contains(parent) && !delete_resolvable.contains(parent) {
                // This is OK — name can be resolved by scanning.
                // Only flag if there's no way to filter.
                // We allow this pattern (optional scope on resolve) — it's the common case.
            }
        }
    }

    // E040: FromSchema references must resolve to existing schema fields
    for op in &def.operations {
        for arg in &op.args {
            if let super::operation::ArgSource::FromSchema(field_name) = &arg.source {
                let exists = def.schema.fields.iter().any(|f| f.name == *field_name);
                if !exists {
                    v.push(Violation {
                        rule: "E040",
                        resource: kind,
                        message: format!(
                            "operation '{}' arg '{}' references schema field '{}' which does not exist",
                            op.name, arg.name, field_name
                        ),
                        severity: Severity::Error,
                    });
                }
            }
        }
    }
}

// ── UX (W001–W007) ───────────────────────────────────────────

fn ux_rules(def: &ResourceDef, kind: &'static str, v: &mut Vec<Violation>) {
    // W001: list requires empty_message
    if has_op(def, "list") {
        if let Some(table) = &def.presentation.table {
            if table.empty_message.is_none() {
                v.push(Violation {
                    rule: "W001",
                    resource: kind,
                    message: "list operation has no empty_message".into(),
                    severity: Severity::Warning,
                });
            }
        }
    }

    // W002: CRUD resource should have created_at column with Timestamp
    if is_crud(def) && !has_column_with_format(def, "created_at", &DisplayFormat::Timestamp) {
        v.push(Violation {
            rule: "W002",
            resource: kind,
            message: "CRUD resource missing 'created_at' column with Timestamp format".into(),
            severity: Severity::Warning,
        });
    }

    // W003: CRUD resource should have id column
    if is_crud(def) && !has_column(def, "id") {
        v.push(Violation {
            rule: "W003",
            resource: kind,
            message: "CRUD resource missing 'id' column".into(),
            severity: Severity::Warning,
        });
    }

    // W004: detail section should include name and id
    if def.presentation.detail.is_some() {
        if !has_detail_field(def, "name") {
            v.push(Violation {
                rule: "W004",
                resource: kind,
                message: "detail section missing 'name' field".into(),
                severity: Severity::Warning,
            });
        }
        if !has_detail_field(def, "id") {
            v.push(Violation {
                rule: "W004",
                resource: kind,
                message: "detail section missing 'id' field".into(),
                severity: Severity::Warning,
            });
        }
    }

    // W005: detail should have created_at with Timestamp
    if def.presentation.detail.is_some()
        && !has_detail_field_with_format(def, "created_at", &DisplayFormat::Timestamp)
    {
        v.push(Violation {
            rule: "W005",
            resource: kind,
            message: "detail section missing 'created_at' field with Timestamp format".into(),
            severity: Severity::Warning,
        });
    }

    // W006: parent should appear in columns or detail
    for parent in &def.scope.parents {
        let parent_field = format!("{}_name", parent.kind);
        let has_in_table = has_column(def, &parent_field) || has_column(def, parent.kind);
        let has_in_detail =
            has_detail_field(def, &parent_field) || has_detail_field(def, parent.kind);
        if !has_in_table && !has_in_detail {
            v.push(Violation {
                rule: "W006",
                resource: kind,
                message: format!(
                    "parent '{}' not visible in table or detail (expected field '{}')",
                    parent.kind, parent_field
                ),
                severity: Severity::Warning,
            });
        }
    }

    // W007: action with required arg "name" — should be positional
    for op in &def.operations {
        if matches!(op.semantics, OperationSemantics::Action) {
            for arg in &op.args {
                if arg.name == "name" && arg.required {
                    v.push(Violation {
                        rule: "W007",
                        resource: kind,
                        message: format!(
                            "action '{}' has required arg 'name' — should use positional semantics (Get/Delete) instead of --name flag",
                            op.name
                        ),
                        severity: Severity::Warning,
                    });
                }
            }
        }
    }
}

// ── Output (W020–W026) ───────────────────────────────────────

fn output_rules(def: &ResourceDef, kind: &'static str, v: &mut Vec<Violation>) {
    for op in &def.operations {
        // W020: create should have examples
        if op.semantics == OperationSemantics::Create && op.examples.is_empty() {
            v.push(Violation {
                rule: "W020",
                resource: kind,
                message: "create operation has no examples".into(),
                severity: Severity::Warning,
            });
        }

        // W021: delete should be confirmable
        if op.semantics == OperationSemantics::Delete && !op.confirmable {
            v.push(Violation {
                rule: "W021",
                resource: kind,
                message: "delete operation is not confirmable (missing --yes)".into(),
                severity: Severity::Warning,
            });
        }

        // W022: create output should be Resource
        if op.semantics == OperationSemantics::Create && op.output.kind != OutputKind::Resource {
            v.push(Violation {
                rule: "W022",
                resource: kind,
                message: format!(
                    "create operation output is {:?}, expected Resource",
                    op.output.kind
                ),
                severity: Severity::Warning,
            });
        }

        // W023: delete output should be Message
        if op.semantics == OperationSemantics::Delete && op.output.kind != OutputKind::Message {
            v.push(Violation {
                rule: "W023",
                resource: kind,
                message: format!(
                    "delete operation output is {:?}, expected Message",
                    op.output.kind
                ),
                severity: Severity::Warning,
            });
        }
    }

    // W024: NAME should be first column
    if let Some(table) = &def.presentation.table {
        if let Some(first) = table.columns.first() {
            if first.header != "NAME" {
                v.push(Violation {
                    rule: "W024",
                    resource: kind,
                    message: format!("first column is '{}', expected 'NAME'", first.header),
                    severity: Severity::Warning,
                });
            }
        }
    }

    // W025: state/status column should use DisplayFormat::Status
    if let Some(table) = &def.presentation.table {
        for col in &table.columns {
            if (col.field == "state" || col.field == "status")
                && !matches!(col.format, DisplayFormat::Status)
            {
                v.push(Violation {
                    rule: "W025",
                    resource: kind,
                    message: format!("column '{}' should use DisplayFormat::Status", col.header),
                    severity: Severity::Warning,
                });
            }
        }
    }

    // W026: list should have default_sort
    if has_op(def, "list") {
        if let Some(table) = &def.presentation.table {
            if table.default_sort.is_none() {
                v.push(Violation {
                    rule: "W026",
                    resource: kind,
                    message: "list table has no default_sort defined".into(),
                    severity: Severity::Warning,
                });
            }
        }
    }
}

// ── Scope (E020, W030, W031) ─────────────────────────────────

fn scope_rules(def: &ResourceDef, kind: &'static str, v: &mut Vec<Violation>) {
    // E020: create with parent → parent must be required_on_create
    if has_op(def, "create") {
        for parent in &def.scope.parents {
            if !parent.required_on_create {
                v.push(Violation {
                    rule: "E020",
                    resource: kind,
                    message: format!(
                        "create operation exists but parent '{}' is not required_on_create",
                        parent.kind
                    ),
                    severity: Severity::Error,
                });
            }
        }
    }

    // W030: delete should require same scope as create
    // Check if delete exists and any parent required_on_create is not usable for delete
    // (This is a heuristic — we check if the handler could resolve via the scope)
    // Skip if no parents
    if has_op(def, "delete") && has_op(def, "create") && !def.scope.parents.is_empty() {
        // We only warn if there are multiple parents (ambiguity risk)
        let create_parents: Vec<&str> = def
            .scope
            .parents
            .iter()
            .filter(|p| p.required_on_create)
            .map(|p| p.kind)
            .collect();
        if create_parents.len() > 1 {
            // Multiple parents on create — delete should also accept them
            // (We can't enforce this perfectly at the def level since handlers
            // can do their own resolution, but it's a good heuristic)
        }
    }

    // W031: if list accepts optional parent filters, get/delete should too
    // This checks operation args — if list has optional parent-like args that
    // get/delete don't have. Hard to check generically without knowing which
    // args are parent filters vs data filters. Skip for now — covered by
    // handler review.

    // W080: delete with multiple parents should clarify scope
    if has_op(def, "delete") && def.scope.parents.len() > 1 {
        let all_resolve = def.scope.parents.iter().all(|p| p.required_on_resolve);
        if !all_resolve {
            v.push(Violation {
                rule: "W080",
                resource: kind,
                message: "delete operation with multiple parents: consider setting required_on_resolve=true for unambiguous deletion".into(),
                severity: Severity::Warning,
            });
        }
    }
}

// ── Description (W040, W041) ─────────────────────────────────

fn description_rules(def: &ResourceDef, kind: &'static str, v: &mut Vec<Violation>) {
    let desc = def.identity.description;

    // W040: description should start with uppercase
    if let Some(first) = desc.chars().next() {
        if !first.is_uppercase() {
            v.push(Violation {
                rule: "W040",
                resource: kind,
                message: format!("description '{}' should start with uppercase", desc),
                severity: Severity::Warning,
            });
        }
    }

    // W041: description should not end with period
    if desc.ends_with('.') {
        v.push(Violation {
            rule: "W041",
            resource: kind,
            message: format!("description '{}' should not end with period", desc),
            severity: Severity::Warning,
        });
    }
}

// ── Presentation alignment (W050, W051) ──────────────────────

fn presentation_alignment_rules(def: &ResourceDef, kind: &'static str, v: &mut Vec<Violation>) {
    // W050: every table column should have a corresponding detail field
    if let (Some(table), Some(_detail)) = (&def.presentation.table, &def.presentation.detail) {
        for col in &table.columns {
            if !has_detail_field(def, col.field) {
                v.push(Violation {
                    rule: "W050",
                    resource: kind,
                    message: format!(
                        "table column '{}' (field '{}') has no corresponding detail field",
                        col.header, col.field
                    ),
                    severity: Severity::Warning,
                });
            }
        }
    }

    // W051: timestamp fields in detail must use DisplayFormat::Timestamp
    if let Some(detail) = &def.presentation.detail {
        for section in &detail.sections {
            for field in &section.fields {
                if field.field.ends_with("_at") && !matches!(field.format, DisplayFormat::Timestamp)
                {
                    v.push(Violation {
                        rule: "W051",
                        resource: kind,
                        message: format!(
                            "detail field '{}' ends with '_at' but does not use Timestamp format",
                            field.field
                        ),
                        severity: Severity::Warning,
                    });
                }
            }
        }
    }
}

// ── State machine (W060, W061) ───────────────────────────────

fn state_machine_rules(def: &ResourceDef, kind: &'static str, v: &mut Vec<Violation>) {
    let has_start = has_op(def, "start");
    let has_stop = has_op(def, "stop");

    // W060: resource with start/stop should have STATE column
    if (has_start || has_stop) && !has_column(def, "state") && !has_column(def, "status") {
        v.push(Violation {
            rule: "W060",
            resource: kind,
            message: "has start/stop actions but no 'state' or 'status' column".into(),
            severity: Severity::Warning,
        });
    }

    // W061: update action should output Resource
    for op in &def.operations {
        if matches!(op.name, "update") && op.output.kind != OutputKind::Resource {
            v.push(Violation {
                rule: "W061",
                resource: kind,
                message: format!(
                    "action '{}' mutates state but output is {:?}, expected Resource",
                    op.name, op.output.kind
                ),
                severity: Severity::Warning,
            });
        }
    }
}

// ── Progress (W070–W073, E070) ───────────────────────────────

fn progress_rules(def: &ResourceDef, kind: &'static str, v: &mut Vec<Violation>) {
    for op in &def.operations {
        match op.semantics {
            // W070: create should have progress
            OperationSemantics::Create => {
                if matches!(op.progress, ProgressHint::None) {
                    v.push(Violation {
                        rule: "W070",
                        resource: kind,
                        message: "create operation has no ProgressHint".into(),
                        severity: Severity::Warning,
                    });
                }
            }
            // W071: delete should have progress
            OperationSemantics::Delete => {
                if matches!(op.progress, ProgressHint::None) {
                    v.push(Violation {
                        rule: "W071",
                        resource: kind,
                        message: "delete operation has no ProgressHint".into(),
                        severity: Severity::Warning,
                    });
                }
            }
            _ => {}
        }

        // W072: state-mutating actions should have progress
        if matches!(op.semantics, OperationSemantics::Action)
            && matches!(
                op.name,
                "start" | "stop" | "update" | "init" | "join" | "leave"
            )
            && matches!(op.progress, ProgressHint::None)
        {
            v.push(Violation {
                rule: "W072",
                resource: kind,
                message: format!("action '{}' mutates state but has no ProgressHint", op.name),
                severity: Severity::Warning,
            });
        }

        // W073: list/get should not have progress
        if matches!(
            op.semantics,
            OperationSemantics::List | OperationSemantics::Get
        ) && !matches!(op.progress, ProgressHint::None)
        {
            v.push(Violation {
                rule: "W073",
                resource: kind,
                message: format!("operation '{}' is a read but has ProgressHint set", op.name),
                severity: Severity::Warning,
            });
        }

        // E070: Spinner message must end with "..."
        if let ProgressHint::Spinner(msg) = op.progress {
            if !msg.ends_with("...") {
                v.push(Violation {
                    rule: "E070",
                    resource: kind,
                    message: format!(
                        "ProgressHint::Spinner message '{}' must end with '...'",
                        msg
                    ),
                    severity: Severity::Error,
                });
            }
        }
    }
}

// ── Naming (E030 already handled above) ──────────────────────

fn naming_rules(_def: &ResourceDef, _kind: &'static str, _v: &mut Vec<Violation>) {
    // Additional naming rules can be added here.
    // E030 and E031 are handled in structural_rules.
}

// ── Registry (E010, E011, W010) ──────────────────────────────

fn registry_rules(defs: &[&ResourceDef], v: &mut Vec<Violation>) {
    // E010: no duplicate kinds
    let mut seen = std::collections::HashSet::new();
    for def in defs {
        if !seen.insert(def.identity.kind) {
            v.push(Violation {
                rule: "E010",
                resource: def.identity.kind,
                message: format!("duplicate resource kind '{}'", def.identity.kind),
                severity: Severity::Error,
            });
        }
    }

    // E011: every parent kind must exist in registry
    let all_kinds: std::collections::HashSet<&str> = defs.iter().map(|d| d.identity.kind).collect();
    for def in defs {
        for parent in &def.scope.parents {
            if !all_kinds.contains(parent.kind) {
                v.push(Violation {
                    rule: "E011",
                    resource: def.identity.kind,
                    message: format!("parent kind '{}' not found in registry", parent.kind),
                    severity: Severity::Error,
                });
            }
        }
    }

    // W010: child resource should have at least list + one other op
    for def in defs {
        if !def.scope.parents.is_empty() && def.operations.len() < 2 {
            v.push(Violation {
                rule: "W010",
                resource: def.identity.kind,
                message: "child resource should have at least 2 operations".into(),
                severity: Severity::Warning,
            });
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resource::*;

    fn minimal_crud_def() -> ResourceDef {
        ResourceDef::build("widget", "Manage widgets")
            .plural("widgets")
            .scope_global()
            .crud()
            .column("NAME", "name")
            .column("ID", "id")
            .column_def(
                ColumnDef::new("CREATED", "created_at").with_format(DisplayFormat::Timestamp),
            )
            .empty_message("No widgets found.")
            .sort_by("name")
            .detail_section(
                None,
                vec![
                    DetailField::new("Name", "name"),
                    DetailField::new("ID", "id"),
                    DetailField::new("Created", "created_at").with_format(DisplayFormat::Timestamp),
                ],
            )
            .done()
    }

    #[test]
    fn clean_def_has_no_violations() {
        let def = minimal_crud_def();
        let violations = lint_def(&def);
        let errors: Vec<_> = violations
            .iter()
            .filter(|v| v.severity == Severity::Error)
            .collect();
        assert!(
            errors.is_empty(),
            "unexpected errors: {}",
            format_violations(&violations)
        );
    }

    #[test]
    #[should_panic(expected = "E001")]
    fn e001_list_without_table() {
        ResourceDef::build("bad", "Bad resource")
            .plural("bads")
            .list()
            .done();
    }

    #[test]
    #[should_panic(expected = "E002")]
    fn e002_get_without_detail() {
        ResourceDef::build("bad", "Bad resource")
            .plural("bads")
            .get()
            .column("NAME", "name")
            .done();
    }

    #[test]
    #[should_panic(expected = "E003")]
    fn e003_duplicate_operations() {
        ResourceDef::build("bad", "Bad resource")
            .plural("bads")
            .list()
            .list()
            .column("NAME", "name")
            .empty_message("none")
            .done();
    }

    #[test]
    fn w007_action_with_name_arg() {
        let def = ResourceDef::build("thing", "Manage things")
            .plural("things")
            .action("start", "Start a thing")
            .op(|op| {
                op.with_arg(OperationArg::required(
                    "name",
                    FieldDef::string("name", "Thing name"),
                ))
            })
            .column("NAME", "name")
            .empty_message("none")
            .done();
        let violations = lint_def(&def);
        assert!(
            violations.iter().any(|v| v.rule == "W007"),
            "expected W007, got: {}",
            format_violations(&violations)
        );
    }

    #[test]
    fn e010_duplicate_kinds_in_registry() {
        let def1 = minimal_crud_def();
        let def2 = minimal_crud_def();
        let violations = lint_registry(&[&def1, &def2]);
        assert!(violations.iter().any(|v| v.rule == "E010"), "expected E010");
    }

    #[test]
    fn e011_missing_parent_in_registry() {
        let def = ResourceDef::build("child", "A child")
            .plural("children")
            .parent("nonexistent", "--nonexistent", "Does not exist")
            .list()
            .column("NAME", "name")
            .empty_message("none")
            .done();
        let violations = lint_registry(&[&def]);
        assert!(violations.iter().any(|v| v.rule == "E011"), "expected E011");
    }

    #[test]
    fn e040_from_schema_references_nonexistent_field() {
        use crate::resource::operation::{OperationArg, ArgSource};

        let def = ResourceDef {
            identity: ResourceIdentity {
                kind: "thing",
                cli_name: "thing",
                plural: "things",
                description: "Test thing",
                aliases: &[],
            },
            scope: ScopeDef::global(),
            schema: ResourceSchema::new(),
            operations: vec![
                OperationDef::action("filter", "Filter things")
                    .with_arg(OperationArg {
                        name: "missing-field",
                        description: "A field that doesn't exist",
                        required: false,
                        source: ArgSource::FromSchema("nonexistent"),
                    }),
            ],
            presentation: PresentationDef::none(),
        };
        let violations = lint_def(&def);
        assert!(
            violations.iter().any(|v| v.rule == "E040"),
            "expected E040, got: {}",
            format_violations(&violations)
        );
    }
}
