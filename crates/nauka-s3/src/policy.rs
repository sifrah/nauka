//! Bucket policies: the IAM-style JSON documents S3 evaluates per request.
//!
//! The document is stored on the bucket as the raw string the client sent
//! (GET must round-trip it byte-for-byte) and parsed again at evaluation
//! time. Parsing is lenient where AWS is lenient (string-or-array fields,
//! one statement or many) and strict where the conformance suite checks
//! (`NotPrincipal` with `Allow` is refused, unknown effects are malformed).
//!
//! Evaluation follows IAM's order: an explicit `Deny` beats everything,
//! then an `Allow` grants, and no match at all decides nothing — the
//! caller falls back to ownership and credential grants. Conditions we do
//! not understand fail closed: the statement simply never matches.

use std::collections::BTreeMap;

use serde::Deserialize;

/// One string or a list of them — IAM accepts both everywhere.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum OneOrMany {
    One(String),
    Many(Vec<String>),
}

impl OneOrMany {
    fn iter(&self) -> impl Iterator<Item = &str> {
        match self {
            OneOrMany::One(s) => std::slice::from_ref(s).iter().map(String::as_str),
            OneOrMany::Many(v) => v[..].iter().map(String::as_str),
        }
    }
}

/// `"Principal": "*"` or `{"AWS": "..."|[...], "Service": ...}`.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum PrincipalSpec {
    Wildcard(String),
    Map(BTreeMap<String, OneOrMany>),
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Statement {
    #[serde(rename = "Sid")]
    pub sid: Option<String>,
    #[serde(rename = "Effect")]
    pub effect: String,
    #[serde(rename = "Principal")]
    pub principal: Option<PrincipalSpec>,
    #[serde(rename = "NotPrincipal")]
    pub not_principal: Option<PrincipalSpec>,
    #[serde(rename = "Action")]
    pub action: Option<OneOrMany>,
    #[serde(rename = "NotAction")]
    pub not_action: Option<OneOrMany>,
    #[serde(rename = "Resource")]
    pub resource: Option<OneOrMany>,
    #[serde(rename = "NotResource")]
    pub not_resource: Option<OneOrMany>,
    #[serde(rename = "Condition")]
    pub condition: Option<BTreeMap<String, BTreeMap<String, OneOrMany>>>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum Statements {
    One(Box<Statement>),
    Many(Vec<Statement>),
}

#[derive(Debug, Clone, Deserialize)]
pub struct Policy {
    #[serde(rename = "Version")]
    pub version: Option<String>,
    #[serde(rename = "Id")]
    pub id: Option<String>,
    #[serde(rename = "Statement")]
    statement: Statements,
}

/// Why a policy document was refused at PUT time.
#[derive(Debug, PartialEq, Eq)]
pub enum PolicyError {
    /// Not JSON, or not the shape of a policy at all.
    Malformed(String),
    /// Parsed fine but breaks a rule AWS reports as InvalidArgument
    /// (currently: `NotPrincipal` with `Effect: Allow`).
    InvalidArgument(String),
}

/// The identity a request runs as, for principal matching.
#[derive(Debug, Clone, Copy)]
pub enum Requester<'a> {
    Anonymous,
    /// An authenticated access key (and its canonical user id, if distinct).
    Key {
        access_key: &'a str,
        user_id: &'a str,
    },
}

/// What the policy decided — `NoMatch` defers to ownership and grants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    Allow,
    Deny,
    NoMatch,
}

impl Policy {
    /// Parses and validates a policy document for PutBucketPolicy.
    pub fn parse(raw: &str) -> Result<Self, PolicyError> {
        let policy: Policy = serde_json::from_str(raw)
            .map_err(|e| PolicyError::Malformed(format!("invalid policy JSON: {e}")))?;
        for s in policy.statements() {
            match s.effect.as_str() {
                "Allow" | "Deny" => {}
                other => {
                    return Err(PolicyError::Malformed(format!(
                        "unknown Effect \"{other}\""
                    )))
                }
            }
            if s.not_principal.is_some() && s.effect == "Allow" {
                // AWS refuses Allow+NotPrincipal outright: it would grant to
                // everyone except someone, which is never what was meant.
                return Err(PolicyError::InvalidArgument(
                    "NotPrincipal with Effect: Allow is not allowed".into(),
                ));
            }
            if s.resource.is_none() && s.not_resource.is_none() {
                return Err(PolicyError::Malformed(
                    "a statement needs a Resource".into(),
                ));
            }
            if s.action.is_none() && s.not_action.is_none() {
                return Err(PolicyError::Malformed("a statement needs an Action".into()));
            }
        }
        Ok(policy)
    }

    pub fn statements(&self) -> &[Statement] {
        match &self.statement {
            Statements::One(s) => std::slice::from_ref(s.as_ref()),
            Statements::Many(v) => v,
        }
    }

    /// Evaluates the policy for one request. `action` is the S3 action
    /// (`s3:GetObject`), `resource` the full ARN of what is touched, and
    /// `ctx` the condition context (`s3:prefix`, `aws:Referer`, …) — only
    /// keys that have a value in this request appear in the map.
    pub fn evaluate(
        &self,
        who: Requester<'_>,
        action: &str,
        resource: &str,
        ctx: &BTreeMap<String, String>,
    ) -> Decision {
        let mut allowed = false;
        for s in self.statements() {
            if !s.matches_principal(who)
                || !s.matches_action(action)
                || !s.matches_resource(resource)
                || !s.matches_conditions(ctx)
            {
                continue;
            }
            match s.effect.as_str() {
                // An explicit Deny is final, whatever else allows.
                "Deny" => return Decision::Deny,
                _ => allowed = true,
            }
        }
        if allowed {
            Decision::Allow
        } else {
            Decision::NoMatch
        }
    }

    /// Whether the policy makes the bucket public: some `Allow` statement
    /// grants to everyone (`Principal: "*"`) without a condition narrowing
    /// it down. This drives both GetBucketPolicyStatus and the
    /// BlockPublicPolicy refusal.
    pub fn is_public(&self) -> bool {
        self.statements().iter().any(|s| {
            s.effect == "Allow"
                && s.principal.as_ref().is_some_and(principal_is_wildcard)
                && s.condition.as_ref().is_none_or(|c| c.is_empty())
        })
    }
}

fn principal_is_wildcard(p: &PrincipalSpec) -> bool {
    match p {
        PrincipalSpec::Wildcard(s) => s == "*",
        PrincipalSpec::Map(m) => m.values().any(|v| v.iter().any(|s| s == "*")),
    }
}

fn principal_matches(p: &PrincipalSpec, who: Requester<'_>) -> bool {
    let entries: Vec<&str> = match p {
        PrincipalSpec::Wildcard(s) => vec![s.as_str()],
        PrincipalSpec::Map(m) => m
            .iter()
            // Only AWS principals can name our users; a Service or
            // Federated principal never matches a signed S3 request here.
            .filter(|(k, _)| k.as_str() == "AWS" || k.as_str() == "*")
            .flat_map(|(_, v)| v.iter())
            .collect(),
    };
    entries.iter().any(|e| match who {
        // "*" includes the anonymous principal, as on AWS.
        Requester::Anonymous => *e == "*",
        Requester::Key {
            access_key,
            user_id,
        } => {
            *e == "*"
                || *e == access_key
                || *e == user_id
                // The ARN forms RGW/AWS use for a user principal.
                || e.ends_with(&format!(":user/{user_id}"))
                || e.ends_with(&format!(":user/{access_key}"))
        }
    })
}

impl Statement {
    fn matches_principal(&self, who: Requester<'_>) -> bool {
        match (&self.principal, &self.not_principal) {
            (Some(p), _) => principal_matches(p, who),
            (None, Some(np)) => !principal_matches(np, who),
            // A statement without a principal grants to nobody in a
            // resource policy.
            (None, None) => false,
        }
    }

    fn matches_action(&self, action: &str) -> bool {
        match (&self.action, &self.not_action) {
            (Some(a), _) => a.iter().any(|pat| wildcard_match_ci(pat, action)),
            (None, Some(na)) => !na.iter().any(|pat| wildcard_match_ci(pat, action)),
            (None, None) => false,
        }
    }

    fn matches_resource(&self, resource: &str) -> bool {
        match (&self.resource, &self.not_resource) {
            (Some(r), _) => r.iter().any(|pat| wildcard_match(pat, resource)),
            (None, Some(nr)) => !nr.iter().any(|pat| wildcard_match(pat, resource)),
            (None, None) => false,
        }
    }

    fn matches_conditions(&self, ctx: &BTreeMap<String, String>) -> bool {
        let Some(cond) = &self.condition else {
            return true;
        };
        cond.iter().all(|(op, keys)| {
            keys.iter().all(|(key, values)| {
                // Null is its own operator: "true" asserts the key is
                // ABSENT from the request, "false" that it is present.
                if op == "Null" {
                    let want_absent = values.iter().any(|v| v == "true");
                    return ctx.contains_key(key) != want_absent;
                }
                let (op, if_exists) = match op.strip_suffix("IfExists") {
                    Some(op) => (op, true),
                    None => (op.as_str(), false),
                };
                let actual = ctx.get(key);
                match actual {
                    // ...IfExists is satisfied by an absent key; a plain
                    // operator is not.
                    None => if_exists,
                    Some(actual) => match op {
                        "StringEquals" => values.iter().any(|v| v == actual),
                        "StringNotEquals" => !values.iter().any(|v| v == actual),
                        "StringLike" => values.iter().any(|v| wildcard_match(v, actual)),
                        "StringNotLike" => !values.iter().any(|v| wildcard_match(v, actual)),
                        // An operator we do not implement (IpAddress, date
                        // and numeric ops…) fails closed: the statement
                        // never matches, so it can neither grant nor deny.
                        _ => false,
                    },
                }
            })
        })
    }
}

/// IAM wildcard matching: `*` spans any run, `?` one character. Iterative
/// greedy-with-backtrack, so a pathological pattern in a policy cannot
/// blow the stack or go exponential.
fn wildcard_match(pattern: &str, value: &str) -> bool {
    let (p, v) = (pattern.as_bytes(), value.as_bytes());
    let (mut pi, mut vi) = (0usize, 0usize);
    let mut star: Option<(usize, usize)> = None;
    while vi < v.len() {
        if pi < p.len() && (p[pi] == b'?' || p[pi] == v[vi]) {
            pi += 1;
            vi += 1;
        } else if pi < p.len() && p[pi] == b'*' {
            star = Some((pi, vi));
            pi += 1;
        } else if let Some((sp, sv)) = star {
            pi = sp + 1;
            vi = sv + 1;
            star = Some((sp, sv + 1));
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == b'*' {
        pi += 1;
    }
    pi == p.len()
}

/// Action names compare case-insensitively (`s3:getobject` matches).
fn wildcard_match_ci(pattern: &str, value: &str) -> bool {
    wildcard_match(&pattern.to_ascii_lowercase(), &value.to_ascii_lowercase())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn anon() -> Requester<'static> {
        Requester::Anonymous
    }

    fn key(ak: &'static str) -> Requester<'static> {
        Requester::Key {
            access_key: ak,
            user_id: ak,
        }
    }

    fn ctx(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    const LIST_ALL: &str = r#"{
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"AWS": "*"},
            "Action": "s3:ListBucket",
            "Resource": ["arn:aws:s3:::mybucket", "arn:aws:s3:::mybucket/*"]
        }]
    }"#;

    #[test]
    fn a_star_grant_allows_any_signed_or_anonymous_caller() {
        let p = Policy::parse(LIST_ALL).unwrap();
        let c = ctx(&[]);
        for who in [anon(), key("NOPQRSTUVWXYZABCDEFG")] {
            assert_eq!(
                p.evaluate(who, "s3:ListBucket", "arn:aws:s3:::mybucket", &c),
                Decision::Allow
            );
        }
        // A different action or bucket decides nothing.
        assert_eq!(
            p.evaluate(anon(), "s3:GetObject", "arn:aws:s3:::mybucket", &c),
            Decision::NoMatch
        );
        assert_eq!(
            p.evaluate(anon(), "s3:ListBucket", "arn:aws:s3:::other", &c),
            Decision::NoMatch
        );
    }

    #[test]
    fn bucket_arn_grant_does_not_cover_objects() {
        // The multipart conformance test hangs on exactly this: PutObject
        // granted on the bucket ARN must NOT allow writing a key.
        let p = Policy::parse(
            r#"{"Version":"2012-10-17","Statement":[{
                "Effect":"Allow","Principal":{"AWS":"*"},
                "Action":"s3:PutObject","Resource":"arn:aws:s3:::b"}]}"#,
        )
        .unwrap();
        let c = ctx(&[]);
        assert_eq!(
            p.evaluate(anon(), "s3:PutObject", "arn:aws:s3:::b/mpobj", &c),
            Decision::NoMatch
        );
        let p2 = Policy::parse(
            r#"{"Version":"2012-10-17","Statement":[{
                "Effect":"Allow","Principal":{"AWS":"*"},
                "Action":"s3:PutObject","Resource":"arn:aws:s3:::b/mpobj"}]}"#,
        )
        .unwrap();
        assert_eq!(
            p2.evaluate(anon(), "s3:PutObject", "arn:aws:s3:::b/mpobj", &c),
            Decision::Allow
        );
    }

    #[test]
    fn deny_beats_allow() {
        let p = Policy::parse(
            r#"{"Statement":[
                {"Effect":"Allow","Principal":"*","Action":"*","Resource":"arn:aws:s3:::b/*"},
                {"Effect":"Deny","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::b/secret"}
            ]}"#,
        )
        .unwrap();
        let c = ctx(&[]);
        assert_eq!(
            p.evaluate(anon(), "s3:GetObject", "arn:aws:s3:::b/secret", &c),
            Decision::Deny
        );
        assert_eq!(
            p.evaluate(anon(), "s3:GetObject", "arn:aws:s3:::b/open", &c),
            Decision::Allow
        );
    }

    #[test]
    fn string_conditions_gate_the_grant() {
        let p = Policy::parse(
            r#"{"Statement":[{
                "Effect":"Allow","Principal":{"AWS":"*"},
                "Action":"s3:ListBucket","Resource":"arn:aws:s3:::b",
                "Condition":{"StringLike":{"s3:prefix":"public/*"}}}]}"#,
        )
        .unwrap();
        assert_eq!(
            p.evaluate(
                anon(),
                "s3:ListBucket",
                "arn:aws:s3:::b",
                &ctx(&[("s3:prefix", "public/object")])
            ),
            Decision::Allow
        );
        assert_eq!(
            p.evaluate(
                anon(),
                "s3:ListBucket",
                "arn:aws:s3:::b",
                &ctx(&[("s3:prefix", "private/object")])
            ),
            Decision::NoMatch
        );
        // A plain operator with the key absent from the request: no match.
        assert_eq!(
            p.evaluate(anon(), "s3:ListBucket", "arn:aws:s3:::b", &ctx(&[])),
            Decision::NoMatch
        );
    }

    #[test]
    fn if_exists_is_satisfied_by_absence() {
        let p = Policy::parse(
            r#"{"Statement":[{
                "Effect":"Allow","Principal":"*",
                "Action":"s3:GetObject","Resource":"arn:aws:s3:::b/*",
                "Condition":{"StringLikeIfExists":{"aws:Referer":"http://www.example.com/*"}}}]}"#,
        )
        .unwrap();
        assert_eq!(
            p.evaluate(anon(), "s3:GetObject", "arn:aws:s3:::b/k", &ctx(&[])),
            Decision::Allow
        );
        assert_eq!(
            p.evaluate(
                anon(),
                "s3:GetObject",
                "arn:aws:s3:::b/k",
                &ctx(&[("aws:Referer", "http://www.example.com/index.html")])
            ),
            Decision::Allow
        );
        assert_eq!(
            p.evaluate(
                anon(),
                "s3:GetObject",
                "arn:aws:s3:::b/k",
                &ctx(&[("aws:Referer", "http://evil.example.org/")])
            ),
            Decision::NoMatch
        );
    }

    #[test]
    fn null_condition_tests_key_absence() {
        // The suite's "deny unencrypted uploads" pattern: Deny when the
        // sse header is absent (Null: true), Deny when it is not AES256.
        let p = Policy::parse(
            r#"{"Statement":[
                {"Effect":"Deny","Principal":"*","Action":"s3:PutObject","Resource":"arn:aws:s3:::b/*",
                 "Condition":{"Null":{"s3:x-amz-server-side-encryption":"true"}}},
                {"Effect":"Deny","Principal":"*","Action":"s3:PutObject","Resource":"arn:aws:s3:::b/*",
                 "Condition":{"StringNotEquals":{"s3:x-amz-server-side-encryption":"AES256"}}}
            ]}"#,
        )
        .unwrap();
        let none = ctx(&[]);
        let aes = ctx(&[("s3:x-amz-server-side-encryption", "AES256")]);
        let kms = ctx(&[("s3:x-amz-server-side-encryption", "aws:kms")]);
        assert_eq!(
            p.evaluate(anon(), "s3:PutObject", "arn:aws:s3:::b/k", &none),
            Decision::Deny,
            "unencrypted: denied by the Null condition"
        );
        assert_eq!(
            p.evaluate(anon(), "s3:PutObject", "arn:aws:s3:::b/k", &kms),
            Decision::Deny,
            "wrong algorithm: denied by StringNotEquals"
        );
        assert_eq!(
            p.evaluate(anon(), "s3:PutObject", "arn:aws:s3:::b/k", &aes),
            Decision::NoMatch,
            "AES256 satisfies neither Deny"
        );
    }

    #[test]
    fn unknown_condition_operators_fail_closed() {
        let p = Policy::parse(
            r#"{"Statement":[{
                "Effect":"Allow","Principal":{"AWS":"*"},
                "Action":"s3:ListBucket","Resource":"arn:aws:s3:::b",
                "Condition":{"IpAddress":{"aws:SourceIp":"10.0.0.0/32"}}}]}"#,
        )
        .unwrap();
        assert_eq!(
            p.evaluate(
                anon(),
                "s3:ListBucket",
                "arn:aws:s3:::b",
                &ctx(&[("aws:SourceIp", "10.0.0.1")])
            ),
            Decision::NoMatch
        );
    }

    #[test]
    fn is_public_needs_a_star_principal_and_no_condition() {
        assert!(Policy::parse(LIST_ALL).unwrap().is_public());
        // A named principal is not public.
        let named = Policy::parse(
            r#"{"Statement":[{
                "Effect":"Allow","Principal":{"AWS":"arn:aws:iam::s3tenant1:root"},
                "Action":"s3:ListBucket","Resource":"arn:aws:s3:::b"}]}"#,
        )
        .unwrap();
        assert!(!named.is_public());
        // A condition narrows the grant below "public".
        let conditional = Policy::parse(
            r#"{"Statement":[{
                "Effect":"Allow","Principal":{"AWS":"*"},
                "Action":"s3:ListBucket","Resource":"arn:aws:s3:::b",
                "Condition":{"IpAddress":{"aws:SourceIp":"10.0.0.0/32"}}}]}"#,
        )
        .unwrap();
        assert!(!conditional.is_public());
    }

    #[test]
    fn not_principal_with_allow_is_refused() {
        let e = Policy::parse(
            r#"{"Statement":[{
                "Effect":"Allow","NotPrincipal":{"AWS":"arn:aws:iam::s3tenant1:root"},
                "Action":"s3:ListBucket","Resource":"arn:aws:s3:::b"}]}"#,
        )
        .unwrap_err();
        assert!(matches!(e, PolicyError::InvalidArgument(_)));
    }

    #[test]
    fn garbage_is_malformed() {
        assert!(matches!(
            Policy::parse("not json").unwrap_err(),
            PolicyError::Malformed(_)
        ));
        assert!(matches!(
            Policy::parse(
                r#"{"Statement":[{"Effect":"Maybe","Action":"*","Principal":"*","Resource":"*"}]}"#
            )
            .unwrap_err(),
            PolicyError::Malformed(_)
        ));
        assert!(matches!(
            Policy::parse(
                r#"{"Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject"}]}"#
            )
            .unwrap_err(),
            PolicyError::Malformed(_)
        ));
    }

    #[test]
    fn the_wildcard_matcher_behaves_like_iam() {
        assert!(wildcard_match("arn:aws:s3:::*", "arn:aws:s3:::any-bucket"));
        assert!(wildcard_match(
            "arn:aws:s3:::*/*",
            "arn:aws:s3:::b/deep/key"
        ));
        assert!(!wildcard_match("arn:aws:s3:::b", "arn:aws:s3:::b/k"));
        assert!(wildcard_match("public/*", "public/x"));
        assert!(!wildcard_match("public/*", "private/x"));
        assert!(wildcard_match("a?c", "abc"));
        assert!(!wildcard_match("a?c", "ac"));
        assert!(wildcard_match_ci("s3:getobject", "s3:GetObject"));
        assert!(wildcard_match_ci("s3:*", "s3:PutObject"));
    }
}
