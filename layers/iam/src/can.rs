//! `fn::iam::can` — the authorization decision function every
//! `#[resource(..., scope_by = "...")]` PERMISSIONS clause calls.
//!
//! IAM-3 (#347) extends IAM-2's owner-only check with RoleBinding
//! lookups:
//!
//! 1. If `$auth = NONE`, return true — this is the Raft-state-machine
//!    / background-task path. CLI-originated writes are authorized
//!    in Rust before they reach the log.
//! 2. Walk the scope chain to the root `Org` (same as IAM-2).
//! 3. If `$auth.id = org.owner`, return true. The owner shortcut
//!    avoids needing a binding for the person who created the org.
//! 4. Otherwise, look for a `RoleBinding` where
//!    `principal = $auth.id`, `org = <resolved org>`, and the role's
//!    `permissions` list contains a record named
//!    `<table-of-\$scope>.<\$action>`.
//!
//! IAM-3 only binds at Org scope. Project/Env bindings are
//! tech-debt tracked in the epic follow-up.

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
    LET $org = IF $tb = 'org' {
        $scope
    } ELSE IF $tb = 'project' {
        $scope.org
    } ELSE IF $tb = 'env' {
        $scope.project.org
    } ELSE IF $tb = 'service_account' {
        $scope.org
    } ELSE {
        NONE
    };
    IF $org = NONE {
        RETURN false;
    };
    -- Owner shortcut: the org's creator has full access without
    -- needing an explicit RoleBinding. Carried over from IAM-2.
    IF $org.owner = $auth.id {
        RETURN true;
    };
    -- Service-account shortcut (IAM-6): any SA authenticated via
    -- its API token can see resources scoped to the SA's own org.
    -- IAM-3b will promote this to a polymorphic RoleBinding so SAs
    -- can have narrower roles than "all permissions on own org".
    IF meta::tb($auth) = 'service_account' AND $auth.org = $org.id {
        RETURN true;
    };
    -- Role-binding path: any binding tying $auth to a role that
    -- includes `<table>.<action>` grants the permission. The
    -- permission record's id follows that exact name
    -- (`permission:⟨org.select⟩`), so we can compare by record id
    -- rather than querying the permission table first.
    LET $needed = type::record('permission', $tb + '.' + $action);
    LET $matched = (SELECT VALUE id FROM role_binding
        WHERE principal = $auth.id
          AND org = $org.id
          AND role.permissions CONTAINS $needed);
    RETURN array::len($matched) > 0;
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
        for tb in ["org", "project", "env"] {
            assert!(
                IAM_CAN_DDL.contains(&format!("$tb = '{tb}'")),
                "missing branch for table `{tb}`: {IAM_CAN_DDL}"
            );
        }
        // IAM-3 — RoleBinding lookup, owner shortcut preserved.
        assert!(IAM_CAN_DDL.contains("role_binding"));
        assert!(IAM_CAN_DDL.contains("$org.owner = $auth.id"));
        assert!(IAM_CAN_DDL.contains("role.permissions CONTAINS $needed"));
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
