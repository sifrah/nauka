//! Daemon-side authentication — hash, sign up, sign in, decode JWT.
//!
//! These helpers run on the node holding the SurrealDB handle (the
//! hypervisor daemon). The CLI talks to the daemon over the loopback
//! join-port TCP channel; it never opens the DB directly, so it
//! never sees plaintext credentials long enough to matter.

use argon2::password_hash::rand_core::OsRng;
use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use argon2::Argon2;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use nauka_core::resource::Datetime;
use nauka_state::{Database, RaftNode, Writer};
use serde::{Deserialize, Serialize};
use surrealdb::opt::auth::Record;
use surrealdb::types::SurrealValue;
use tracing::instrument;

use crate::definition::User;
use crate::error::IamError;

/// Opaque wrapper over the JWT string returned by SurrealDB. Stored
/// verbatim in `~/.config/nauka/token` and forwarded to the daemon on
/// every request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Jwt(pub String);

impl Jwt {
    pub fn as_str(&self) -> &str {
        &self.0
    }
    pub fn into_string(self) -> String {
        self.0
    }
}

/// Subset of SurrealDB's JWT claims that the CLI cares about for
/// `whoami`. Decoded without signature verification — the token came
/// from our own DB, and verification requires the daemon's signing
/// key which the CLI does not hold.
///
/// SurrealDB 3.x emits claim names in upper-case (`NS`, `DB`, `AC`,
/// `ID`) and aliases the lower-case forms for backward compat.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Claims {
    #[serde(default)]
    pub iss: Option<String>,
    #[serde(default)]
    pub iat: Option<i64>,
    #[serde(default)]
    pub exp: Option<i64>,
    #[serde(default, alias = "ns", rename = "NS")]
    pub ns: Option<String>,
    #[serde(default, alias = "db", rename = "DB")]
    pub db: Option<String>,
    /// Access name (`DEFINE ACCESS <name>`). For user logins this is
    /// `"user"`.
    #[serde(default, alias = "ac", rename = "AC")]
    pub access: Option<String>,
    /// Record id as a SurrealQL literal — e.g. `` user:`foo@bar.com` ``
    /// or `user:⟨foo@bar.com⟩` depending on the version.
    #[serde(default, alias = "id", rename = "ID")]
    pub id: Option<String>,
}

impl Claims {
    /// Recover the user's email from the `ID` claim by stripping the
    /// `user:<…>` record-id wrapper. Handles both the Unicode angle
    /// bracket (`⟨…⟩`) form and the backtick-quoted form SurrealDB 3
    /// uses for identifiers containing special chars like `@`.
    /// Returns `None` if the claim is missing or in an unexpected
    /// shape.
    pub fn email(&self) -> Option<String> {
        let id = self.id.as_deref()?;
        let after_colon = id.strip_prefix("user:")?;
        let unwrapped = after_colon
            .strip_prefix('\u{27E8}')
            .and_then(|s| s.strip_suffix('\u{27E9}'))
            .or_else(|| {
                after_colon
                    .strip_prefix('`')
                    .and_then(|s| s.strip_suffix('`'))
            })
            .unwrap_or(after_colon);
        Some(unwrapped.to_string())
    }
}

/// Argon2id-hash a password into a PHC string. Uses the default
/// parameters of the `argon2` crate for IAM-1; IAM-6 will swap in
/// vetted production parameters (64MB memory, 3 iterations).
///
/// The output is compatible with SurrealDB's
/// `crypto::argon2::compare`, which the DEFINE ACCESS SIGNIN clause
/// uses to verify the password server-side.
pub fn hash_password(password: &str) -> Result<String, IamError> {
    let salt = SaltString::generate(&mut OsRng);
    Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map(|h| h.to_string())
        .map_err(|e| IamError::Password(e.to_string()))
}

/// Verify a plaintext password against a PHC-encoded hash. Used by
/// IAM-1 only in tests; the production signin path goes through
/// SurrealDB's `crypto::argon2::compare` via the DEFINE ACCESS SIGNIN
/// query.
pub fn verify_password(hash: &str, password: &str) -> Result<bool, IamError> {
    let parsed = PasswordHash::new(hash).map_err(|e| IamError::Password(e.to_string()))?;
    Ok(Argon2::default()
        .verify_password(password.as_bytes(), &parsed)
        .is_ok())
}

fn validate_email(email: &str) -> Result<(), IamError> {
    // Intentionally permissive — we rely on SurrealDB's record-id
    // encoding and the `email` type assertion we'll add in IAM-6.
    // Block only the two shapes that break SurrealQL or make the
    // record-id unusable as a login identifier.
    if email.is_empty() {
        return Err(IamError::InvalidEmail("empty".into()));
    }
    if !email.contains('@') {
        return Err(IamError::InvalidEmail(format!("missing `@`: `{email}`")));
    }
    if email.chars().any(|c| c.is_whitespace()) {
        return Err(IamError::InvalidEmail(format!(
            "whitespace not allowed: `{email}`"
        )));
    }
    Ok(())
}

/// Create a user and mint a JWT. Runs on the Raft leader (or forwards
/// there): hashes the password with Argon2id locally so the resulting
/// PHC string is byte-identical on every replica, routes
/// `Writer::create(&user)` through Raft for cluster-wide replication,
/// then calls `db.signin(...)` against the local engine to produce a
/// fresh JWT.
///
/// Why not `db.signup(...)`? The DEFINE ACCESS SIGNUP clause runs the
/// `crypto::argon2::generate` call on whatever node the client hits,
/// producing a different PHC string per node. That write would
/// bypass Raft entirely and the cluster state would diverge. Hashing
/// here, then replicating the literal hash via the Writer, keeps the
/// state machine deterministic (see ADR 0006 rule #7).
#[instrument(name = "iam.signup", skip_all, fields(email = %email))]
pub async fn signup(
    db: &Database,
    raft: &RaftNode,
    email: &str,
    password: &str,
    display_name: &str,
) -> Result<Jwt, IamError> {
    validate_email(email)?;
    if password.is_empty() {
        return Err(IamError::Password("password cannot be empty".into()));
    }

    let password_hash = hash_password(password)?;
    let now = Datetime::now();
    let user = User {
        email: email.to_string(),
        password_hash,
        display_name: display_name.to_string(),
        created_at: now,
        updated_at: now,
        version: 0,
    };

    match Writer::new(db).with_raft(raft).create(&user).await {
        Ok(()) => {}
        Err(e) => {
            // SurrealDB returns a record-already-exists error when the
            // id `user:⟨email⟩` is taken. Surface that as
            // `UserExists` so the CLI can print a friendly message;
            // bubble anything else up as `State`.
            let s = e.to_string();
            if s.contains("already exists") || s.contains("Database record") {
                return Err(IamError::UserExists(email.to_string()));
            }
            return Err(IamError::State(e));
        }
    }

    sign_in_via_db(db, email, password).await
}

/// Authenticate an existing user by running the DEFINE ACCESS SIGNIN
/// query via the embedded SurrealDB engine. SurrealDB evaluates
/// `crypto::argon2::compare(password_hash, $password)` and, on
/// success, mints a JWT signed with the database's signing key.
#[instrument(name = "iam.signin", skip_all, fields(email = %email))]
pub async fn signin(db: &Database, email: &str, password: &str) -> Result<Jwt, IamError> {
    validate_email(email)?;
    sign_in_via_db(db, email, password).await
}

async fn sign_in_via_db(db: &Database, email: &str, password: &str) -> Result<Jwt, IamError> {
    // Derive `SurrealValue` so the SDK's `Record<P>` credential type
    // accepts this struct directly — the derive flattens the struct
    // into the parameter object the SIGNIN clause consumes.
    #[derive(SurrealValue)]
    struct Params {
        email: String,
        password: String,
    }

    let token = db
        .inner()
        .signin(Record {
            namespace: nauka_state::DEFAULT_NAMESPACE.to_string(),
            database: nauka_state::DEFAULT_DATABASE.to_string(),
            access: "user".to_string(),
            params: Params {
                email: email.to_string(),
                password: password.to_string(),
            },
        })
        .await
        .map_err(|e| {
            // SurrealDB returns a generic auth error on bad creds;
            // collapse into `InvalidCredentials` so a caller can't
            // distinguish "wrong password" from "no such user" —
            // that distinction is itself an enumeration oracle.
            let s = e.to_string().to_ascii_lowercase();
            if s.contains("invalid") || s.contains("authenticat") || s.contains("no record") {
                IamError::InvalidCredentials
            } else {
                IamError::Db(e)
            }
        })?;

    // `AccessToken::into_insecure_token` is named that way to
    // discourage logging — we only hand it over loopback to the CLI,
    // which writes it to a 0600 file. Safe in context.
    Ok(Jwt(token.access.into_insecure_token()))
}

/// Parse the payload of a SurrealDB-issued JWT. Signature is NOT
/// verified — the function is meant for local introspection
/// (`nauka whoami`), not authorisation. Anything that relies on
/// claims for access control must call `Surreal::authenticate(jwt)`
/// on the daemon, which re-validates against the signing key.
pub fn decode_claims(jwt: &str) -> Result<Claims, IamError> {
    let mut parts = jwt.split('.');
    let _header = parts
        .next()
        .ok_or_else(|| IamError::Jwt("missing header".into()))?;
    let payload = parts
        .next()
        .ok_or_else(|| IamError::Jwt("missing payload".into()))?;
    if parts.next().is_none() {
        return Err(IamError::Jwt("missing signature".into()));
    }
    let bytes = URL_SAFE_NO_PAD
        .decode(payload.as_bytes())
        .map_err(|e| IamError::Jwt(format!("payload base64: {e}")))?;
    serde_json::from_slice::<Claims>(&bytes)
        .map_err(|e| IamError::Jwt(format!("payload json: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_round_trips() {
        let h = hash_password("hunter2").unwrap();
        assert!(h.starts_with("$argon2id$"), "PHC format expected: {h}");
        assert!(verify_password(&h, "hunter2").unwrap());
        assert!(!verify_password(&h, "hunter3").unwrap());
    }

    #[test]
    fn email_validation_rejects_obvious_garbage() {
        assert!(validate_email("").is_err());
        assert!(validate_email("nope").is_err());
        assert!(validate_email("foo@ bar.com").is_err());
        assert!(validate_email("foo@bar.com").is_ok());
    }

    #[test]
    fn claims_recover_email_from_record_id_angle_brackets() {
        let c = Claims {
            id: Some("user:\u{27E8}alice@example.com\u{27E9}".into()),
            ..Default::default()
        };
        assert_eq!(c.email().as_deref(), Some("alice@example.com"));
    }

    #[test]
    fn claims_recover_email_from_backtick_form() {
        let c = Claims {
            id: Some("user:`alice@example.com`".into()),
            ..Default::default()
        };
        assert_eq!(c.email().as_deref(), Some("alice@example.com"));
    }

    #[test]
    fn claims_handles_missing_id() {
        let c = Claims::default();
        assert!(c.email().is_none());
    }
}
