//! Credential generation and authorization decisions.
//!
//! SigV4 itself is handled by `s3s`: it asks us for the secret matching an
//! access key and verifies the signature. What belongs here is minting
//! credentials that look like AWS ones (so every client and every test
//! suite accepts them) and deciding what a key is allowed to do.

use std::collections::BTreeMap;

use rand::Rng;

use crate::model::{BucketPermission, Credential};

/// Alphabet of an AWS access key id: uppercase letters and digits.
const AK_ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";

/// Mints a credential pair shaped like AWS's: a 20-character access key id
/// starting with `AKIA`, and a 40-character secret. The shape matters —
/// clients and test suites validate it, and some refuse to send a request
/// with a malformed key rather than let the server answer.
pub fn generate_credential(name: Option<String>, now: u64) -> Credential {
    let mut rng = rand::thread_rng();
    let suffix: String = (0..16)
        .map(|_| AK_ALPHABET[rng.gen_range(0..AK_ALPHABET.len())] as char)
        .collect();
    let mut secret_bytes = [0u8; 30];
    rng.fill(&mut secret_bytes);
    Credential {
        access_key_id: format!("AKIA{suffix}"),
        // 30 bytes of base64 give exactly the 40 characters AWS uses.
        secret_access_key: data_encoding::BASE64.encode(&secret_bytes),
        name,
        user_id: None,
        created_at: now,
        buckets: None,
    }
}

/// What a request wants to do with a bucket.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    Read,
    Write,
    /// Bucket lifecycle and configuration: create, delete, set policy.
    Own,
}

impl Credential {
    /// Whether an EXPLICIT grant lets this credential perform `action` on
    /// `bucket`. Ownership is not consulted here — a key always has full
    /// access to buckets it created, and the endpoint checks that (plus
    /// the bucket policy) separately. Absence of a grant is denial, never
    /// a default-allow: each key is its own account, like on AWS.
    pub fn allows(&self, bucket: &str, action: Action) -> bool {
        let Some(p) = self.buckets.as_ref().and_then(|g| g.get(bucket)) else {
            return false;
        };
        match action {
            Action::Read => p.read,
            Action::Write => p.write,
            Action::Own => p.owner,
        }
    }

    /// The buckets this credential can see in a ListBuckets response:
    /// the ones it owns, plus any it holds an explicit grant on.
    pub fn visible_buckets<'a>(
        &self,
        all: impl Iterator<Item = (&'a String, &'a str)>,
    ) -> Vec<String> {
        all.filter(|(name, owner)| {
            *owner == self.access_key_id
                || self
                    .buckets
                    .as_ref()
                    .and_then(|g| g.get(*name))
                    .is_some_and(|p| p.read || p.write)
        })
        .map(|(name, _)| name.clone())
        .collect()
    }

    /// Adds explicit per-bucket grants to this credential.
    pub fn with_grants(mut self, grants: BTreeMap<String, BucketPermission>) -> Self {
        self.buckets = Some(grants);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_credentials_have_the_aws_shape() {
        let c = generate_credential(Some("backup".into()), 42);
        assert_eq!(c.access_key_id.len(), 20, "AWS access keys are 20 chars");
        assert!(c.access_key_id.starts_with("AKIA"));
        assert!(c
            .access_key_id
            .bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit()));
        assert_eq!(c.secret_access_key.len(), 40, "AWS secrets are 40 chars");
        assert_eq!(c.name.as_deref(), Some("backup"));
        assert_eq!(c.created_at, 42);
    }

    #[test]
    fn two_credentials_never_collide() {
        let a = generate_credential(None, 0);
        let b = generate_credential(None, 0);
        assert_ne!(a.access_key_id, b.access_key_id);
        assert_ne!(a.secret_access_key, b.secret_access_key);
    }

    #[test]
    fn a_credential_without_grants_is_an_ordinary_account() {
        // No grant map does NOT mean cluster-wide access: each key is its
        // own account, and only ownership (checked by the endpoint) or an
        // explicit grant opens a bucket.
        let c = generate_credential(None, 0);
        assert!(!c.allows("anything", Action::Read));
        assert!(!c.allows("anything", Action::Write));
        assert!(!c.allows("anything", Action::Own));
    }

    #[test]
    fn a_restricted_credential_is_denied_by_default() {
        let mut grants = BTreeMap::new();
        grants.insert(
            "photos".to_string(),
            BucketPermission {
                read: true,
                write: true,
                owner: false,
            },
        );
        grants.insert(
            "archive".to_string(),
            BucketPermission {
                read: true,
                write: false,
                owner: false,
            },
        );
        let c = generate_credential(None, 0).with_grants(grants);

        assert!(c.allows("photos", Action::Read));
        assert!(c.allows("photos", Action::Write));
        assert!(!c.allows("photos", Action::Own), "not the bucket owner");

        assert!(c.allows("archive", Action::Read));
        assert!(!c.allows("archive", Action::Write), "read-only grant");

        // A bucket with no grant at all: denied, never defaulted to allow.
        assert!(!c.allows("secrets", Action::Read));
        assert!(!c.allows("secrets", Action::Write));
    }

    #[test]
    fn list_buckets_shows_owned_and_granted_buckets_only() {
        let me = generate_credential(None, 0);
        let names = [
            "photos".to_string(),
            "archive".to_string(),
            "secrets".to_string(),
        ];
        // (bucket, owner access key): I own photos, someone else the rest.
        let my_key = me.access_key_id.clone();
        let world = || {
            vec![
                (&names[0], my_key.as_str()),
                (&names[1], "SOMEONE-ELSE"),
                (&names[2], "SOMEONE-ELSE"),
            ]
        };
        assert_eq!(me.visible_buckets(world().into_iter()), vec!["photos"]);

        // An explicit read grant makes a foreign bucket visible too.
        let mut grants = BTreeMap::new();
        grants.insert(
            "archive".to_string(),
            BucketPermission {
                read: true,
                write: false,
                owner: false,
            },
        );
        let granted = me.clone().with_grants(grants);
        assert_eq!(
            granted.visible_buckets(world().into_iter()),
            vec!["photos", "archive"]
        );
    }
}
