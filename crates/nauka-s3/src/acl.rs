//! S3 ACLs: the grant lists on buckets and objects.
//!
//! A canned ACL (`public-read`, …) is shorthand for a grant list, and GET
//! always answers the expanded form. Grants are stored as JSON on the
//! bucket/version (`None` = the private default: owner FULL_CONTROL).
//! Display names are NOT stored — they are looked up from the credential
//! registry at read time, so renaming a key renames its grants.
//!
//! Order matters to the conformance suite: group grants come before
//! canonical-user grants in every listing (its comparison only sorts when
//! the first grant has a display name, and a group grant has none).

use serde::{Deserialize, Serialize};

pub const ALL_USERS: &str = "http://acs.amazonaws.com/groups/global/AllUsers";
pub const AUTH_USERS: &str = "http://acs.amazonaws.com/groups/global/AuthenticatedUsers";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AclGrantee {
    /// A user, by canonical id.
    Canonical { id: String },
    /// A predefined group, by URI (AllUsers, AuthenticatedUsers).
    Group { uri: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AclGrant {
    pub grantee: AclGrantee,
    /// READ, WRITE, READ_ACP, WRITE_ACP or FULL_CONTROL.
    pub permission: String,
}

impl AclGrant {
    pub fn canonical(id: &str, permission: &str) -> Self {
        Self {
            grantee: AclGrantee::Canonical { id: id.into() },
            permission: permission.into(),
        }
    }

    pub fn group(uri: &str, permission: &str) -> Self {
        Self {
            grantee: AclGrantee::Group { uri: uri.into() },
            permission: permission.into(),
        }
    }
}

/// Expands a canned ACL into its grant list. `bucket_owner` is only
/// needed by the object-level `bucket-owner-*` values; those return
/// `None` when it is absent, as does an unknown canned name.
pub fn canned_grants(
    canned: &str,
    owner_id: &str,
    bucket_owner: Option<&str>,
) -> Option<Vec<AclGrant>> {
    let fc = AclGrant::canonical(owner_id, "FULL_CONTROL");
    Some(match canned {
        "private" => vec![fc],
        "public-read" => vec![AclGrant::group(ALL_USERS, "READ"), fc],
        "public-read-write" => vec![
            AclGrant::group(ALL_USERS, "READ"),
            AclGrant::group(ALL_USERS, "WRITE"),
            fc,
        ],
        "authenticated-read" => vec![AclGrant::group(AUTH_USERS, "READ"), fc],
        "bucket-owner-read" => vec![fc, AclGrant::canonical(bucket_owner?, "READ")],
        "bucket-owner-full-control" => {
            vec![fc, AclGrant::canonical(bucket_owner?, "FULL_CONTROL")]
        }
        _ => return None,
    })
}

/// Whether a canned ACL opens the resource to a public group — the ones
/// `BlockPublicAcls` refuses.
pub fn canned_is_public(canned: &str) -> bool {
    matches!(
        canned,
        "public-read" | "public-read-write" | "authenticated-read"
    )
}

/// Whether a grant list reaches a public group.
pub fn grants_are_public(grants: &[AclGrant]) -> bool {
    grants.iter().any(|g| {
        matches!(&g.grantee, AclGrantee::Group { uri } if uri == ALL_USERS || uri == AUTH_USERS)
    })
}

/// Whether `requester` (a canonical id; `None` = anonymous) holds
/// `permission` in this grant list. FULL_CONTROL implies everything.
/// `ignore_public` is the bucket's IgnorePublicAcls: group grants stop
/// counting, exactly as if they were not there.
pub fn grants_allow(
    grants: &[AclGrant],
    requester: Option<&str>,
    permission: &str,
    ignore_public: bool,
) -> bool {
    grants.iter().any(|g| {
        if g.permission != permission && g.permission != "FULL_CONTROL" {
            return false;
        }
        match &g.grantee {
            AclGrantee::Canonical { id } => requester == Some(id.as_str()),
            AclGrantee::Group { uri } if ignore_public => {
                let _ = uri;
                false
            }
            AclGrantee::Group { uri } if uri == ALL_USERS => true,
            AclGrantee::Group { uri } if uri == AUTH_USERS => requester.is_some(),
            AclGrantee::Group { .. } => false,
        }
    })
}

/// The JSON form stored on a bucket or object version.
pub fn to_json(grants: &[AclGrant]) -> String {
    serde_json::to_string(grants).unwrap_or_else(|_| "[]".into())
}

pub fn from_json(raw: &str) -> Option<Vec<AclGrant>> {
    serde_json::from_str(raw).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canned_acls_expand_with_groups_before_the_owner() {
        let g = canned_grants("public-read", "me", None).unwrap();
        assert_eq!(
            g,
            vec![
                AclGrant::group(ALL_USERS, "READ"),
                AclGrant::canonical("me", "FULL_CONTROL"),
            ]
        );
        let g = canned_grants("public-read-write", "me", None).unwrap();
        assert_eq!(g.len(), 3, "READ + WRITE + owner");
        assert_eq!(g[2], AclGrant::canonical("me", "FULL_CONTROL"));

        assert_eq!(canned_grants("private", "me", None).unwrap().len(), 1);
        assert!(canned_grants("bucket-owner-read", "me", None).is_none());
        let g = canned_grants("bucket-owner-read", "me", Some("boss")).unwrap();
        assert_eq!(g[1], AclGrant::canonical("boss", "READ"));
        assert!(canned_grants("no-such-acl", "me", None).is_none());
    }

    #[test]
    fn full_control_implies_every_permission() {
        let g = vec![AclGrant::canonical("me", "FULL_CONTROL")];
        for p in ["READ", "WRITE", "READ_ACP", "WRITE_ACP", "FULL_CONTROL"] {
            assert!(grants_allow(&g, Some("me"), p, false));
            assert!(!grants_allow(&g, Some("you"), p, false));
            assert!(!grants_allow(&g, None, p, false));
        }
    }

    #[test]
    fn groups_reach_anonymous_and_authenticated_callers() {
        let g = canned_grants("public-read", "me", None).unwrap();
        assert!(grants_allow(&g, None, "READ", false), "AllUsers = anyone");
        assert!(grants_allow(&g, Some("you"), "READ", false));
        assert!(!grants_allow(&g, Some("you"), "WRITE", false));

        let g = canned_grants("authenticated-read", "me", None).unwrap();
        assert!(
            !grants_allow(&g, None, "READ", false),
            "anonymous is not authenticated"
        );
        assert!(grants_allow(&g, Some("you"), "READ", false));
    }

    #[test]
    fn ignore_public_acls_silences_group_grants_only() {
        let g = canned_grants("public-read", "me", None).unwrap();
        assert!(!grants_allow(&g, Some("you"), "READ", true));
        assert!(!grants_allow(&g, None, "READ", true));
        // The owner's own canonical grant still counts.
        assert!(grants_allow(&g, Some("me"), "READ", true));
    }

    #[test]
    fn publicness_is_a_group_grant() {
        assert!(grants_are_public(
            &canned_grants("public-read", "me", None).unwrap()
        ));
        assert!(grants_are_public(
            &canned_grants("authenticated-read", "me", None).unwrap()
        ));
        assert!(!grants_are_public(
            &canned_grants("private", "me", None).unwrap()
        ));
        assert!(canned_is_public("public-read-write"));
        assert!(!canned_is_public("private"));
        assert!(!canned_is_public("bucket-owner-read"));
    }

    #[test]
    fn json_round_trip() {
        let g = canned_grants("public-read-write", "me", None).unwrap();
        assert_eq!(from_json(&to_json(&g)).unwrap(), g);
        assert!(from_json("[]").unwrap().is_empty());
        assert!(from_json("garbage").is_none());
    }
}
