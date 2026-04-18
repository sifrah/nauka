//! `fn::iam::can` — the authorization decision function every
//! `#[resource(..., scope_by = "...")]` PERMISSIONS clause calls.
//!
//! IAM-2 (#346) ships an owner-only implementation: given a scope
//! record (`Org`, `Project`, or `Env`), the function walks up the
//! hierarchy to the root `Org` and checks whether the org's `owner`
//! is the authenticated user. Any other principal — including the
//! same person signed in via a different record-id — is denied.
//!
//! The `$action` parameter (`"select"` / `"create"` / `"update"` /
//! `"delete"`) is accepted and ignored here; IAM-3 will branch on it
//! once `RoleBinding` records exist.
//!
//! SurrealDB auto-dereferences record links in dot-notation
//! (`$scope.org.owner` walks from a `Project` to its `Org.owner`),
//! so the walk fits in one SurrealQL expression without explicit
//! `SELECT` statements.

use linkme::distributed_slice;
use nauka_core::resource::{FunctionDescriptor, ALL_DB_FUNCTIONS};

/// Full DDL for `fn::iam::can`. Exposed as a const so tests can
/// assert the shape; `bin/nauka` reads it transparently via
/// [`nauka_core::function_definitions`].
pub const IAM_CAN_DDL: &str = r#"DEFINE FUNCTION IF NOT EXISTS fn::iam::can($action: string, $scope: record) {
    -- Root / state-machine / background-task path: no `$auth`, so
    -- the query is coming from the daemon itself (Raft apply,
    -- reconciler, endpoint refresh). Those contexts are trusted by
    -- design — authorization for CLI-originated writes runs in Rust
    -- before the record reaches the state machine. Returning `true`
    -- here unblocks the `PERMISSIONS FOR create` check when
    -- `Writer::create` routes through Raft after the handler has
    -- already cleared its session (see ops::create_org).
    IF $auth = NONE {
        RETURN true;
    };
    LET $tb = meta::tb($scope);
    LET $owner = IF $tb = 'org' {
        $scope.owner
    } ELSE IF $tb = 'project' {
        $scope.org.owner
    } ELSE IF $tb = 'env' {
        $scope.project.org.owner
    } ELSE {
        NONE
    };
    RETURN $owner = $auth.id;
} PERMISSIONS FULL;
"#;

#[distributed_slice(ALL_DB_FUNCTIONS)]
static IAM_CAN: &FunctionDescriptor = &FunctionDescriptor {
    name: "fn::iam::can",
    ddl: IAM_CAN_DDL,
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ddl_contains_signature_and_body_structure() {
        assert!(IAM_CAN_DDL.contains(
            "DEFINE FUNCTION IF NOT EXISTS fn::iam::can($action: string, $scope: record)"
        ));
        // Sanity — each level of the Org/Project/Env walk appears.
        for tb in ["org", "project", "env"] {
            assert!(
                IAM_CAN_DDL.contains(&format!("$tb = '{tb}'")),
                "missing branch for table `{tb}`: {IAM_CAN_DDL}"
            );
        }
        // RoleBinding lookup hasn't landed — the action param is
        // accepted but the body must not reference it yet.
        assert!(!IAM_CAN_DDL.contains("role_binding"));
    }

    #[test]
    fn registered_in_function_slice() {
        let defs = nauka_core::function_definitions();
        assert!(
            defs.contains("fn::iam::can"),
            "fn::iam::can not found in function_definitions(): {defs}"
        );
    }
}
