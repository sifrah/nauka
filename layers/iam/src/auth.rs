//! Daemon-side authentication — hash, sign up, sign in, decode JWT.
//!
//! These helpers run on the node holding the SurrealDB handle (the
//! hypervisor daemon). The CLI talks to the daemon over the loopback
//! join-port TCP channel; it never opens the DB directly, so it
//! never sees plaintext credentials long enough to matter.

use argon2::password_hash::rand_core::OsRng;
use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use argon2::{Algorithm, Argon2, Params, Version};
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

/// Argon2id parameters — IAM-6 vetted values.
///
/// - `m_cost` = 65 536 KiB (64 MiB).
/// - `t_cost` = 3 iterations.
/// - `p_cost` = 1 lane.
///
/// These match the OWASP 2026 cheat-sheet recommendation for
/// password hashing on server hardware. The constants live here
/// rather than in `Argon2::default()` because the crate's default
/// is weaker (intended for low-resource contexts) and because
/// subsequent phases may want to bump memory / iterations as CPUs
/// get faster — changing a single constant is cheaper than walking
/// every call site.
///
/// Verification reads the parameters out of the stored PHC string,
/// so existing hashes keep working after a parameter bump: only
/// freshly-minted passwords pay the new cost.
const ARGON2_M_COST_KIB: u32 = 64 * 1024;
const ARGON2_T_COST: u32 = 3;
const ARGON2_P_COST: u32 = 1;

fn argon2_hasher() -> Argon2<'static> {
    // `Params::new(...)` only fails for out-of-range values; the
    // constants above are static and hand-checked, so we panic on
    // the unreachable error path to keep `argon2_hasher` infallible
    // for callers.
    let params =
        Params::new(ARGON2_M_COST_KIB, ARGON2_T_COST, ARGON2_P_COST, None).expect("valid params");
    Argon2::new(Algorithm::Argon2id, Version::V0x13, params)
}

/// Argon2id-hash a password into a PHC string using the IAM-6
/// vetted parameters (64 MiB memory, 3 iterations, 1 lane). Output
/// stays compatible with SurrealDB's `crypto::argon2::compare`,
/// which reads the parameters out of the PHC string itself — so
/// hashes minted at the old (weaker) defaults continue to verify.
pub fn hash_password(password: &str) -> Result<String, IamError> {
    let salt = SaltString::generate(&mut OsRng);
    argon2_hasher()
        .hash_password(password.as_bytes(), &salt)
        .map(|h| h.to_string())
        .map_err(|e| IamError::Password(e.to_string()))
}

/// IAM-7 password-complexity gate.
///
/// Lives in Rust because plaintext is never stored — a SurrealDB
/// `ASSERT` couldn't see the raw password. The rule is deliberately
/// narrow: at least 10 characters, must contain a letter and a
/// non-letter (symbol or digit). Anything stricter (history, max
/// age) lands in follow-up issues where the storage design is
/// worth resolving on its own.
///
/// Callers: `signup`, `consume_password_reset`. Surface the
/// returned message to operators so they can fix the password
/// before retrying — wrapping this in `InvalidCredentials` would
/// leak the reason up through the auth error type.
pub fn validate_password_complexity(password: &str) -> Result<(), IamError> {
    if password.chars().count() < 10 {
        return Err(IamError::Password(
            "password must be at least 10 characters long".into(),
        ));
    }
    let has_letter = password.chars().any(|c| c.is_ascii_alphabetic());
    let has_non_letter = password.chars().any(|c| !c.is_ascii_alphabetic());
    if !has_letter || !has_non_letter {
        return Err(IamError::Password(
            "password must contain at least one letter and one non-letter (digit or symbol)".into(),
        ));
    }
    Ok(())
}

/// Verify a plaintext password against a PHC-encoded hash. The
/// verifier reads the parameters from the PHC string, so this works
/// regardless of which parameter set produced the stored hash —
/// legacy hashes keep working across parameter bumps.
pub fn verify_password(hash: &str, password: &str) -> Result<bool, IamError> {
    let parsed = PasswordHash::new(hash).map_err(|e| IamError::Password(e.to_string()))?;
    Ok(argon2_hasher()
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
#[instrument(name = "iam.signup", skip_all, fields(email = %email, peer_ip = %peer_ip))]
pub async fn signup(
    db: &Database,
    raft: &RaftNode,
    email: &str,
    password: &str,
    display_name: &str,
    peer_ip: &str,
) -> Result<Jwt, IamError> {
    validate_email(email)?;
    validate_password_complexity(password)?;

    let password_hash = hash_password(password)?;
    let now = Datetime::now();
    let user = User {
        email: email.to_string(),
        password_hash,
        display_name: display_name.to_string(),
        email_verified_at: None,
        active: true,
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

    let jwt = sign_in_via_db(db, email, password).await?;
    record_active_session(db, raft, email, peer_ip).await;
    Ok(jwt)
}

/// Authenticate an existing user by running the DEFINE ACCESS SIGNIN
/// query via the embedded SurrealDB engine. SurrealDB evaluates
/// `crypto::argon2::compare(password_hash, $password)` and, on
/// success, mints a JWT signed with the database's signing key.
#[instrument(name = "iam.signin", skip_all, fields(email = %email, peer_ip = %peer_ip))]
pub async fn signin(
    db: &Database,
    raft: &RaftNode,
    email: &str,
    password: &str,
    peer_ip: &str,
) -> Result<Jwt, IamError> {
    validate_email(email)?;
    let jwt = sign_in_via_db(db, email, password).await?;
    record_active_session(db, raft, email, peer_ip).await;
    Ok(jwt)
}

/// Write an `ActiveSession` row on every successful signin. Runs
/// outside the `IPC_LOCK` critical section held in `sign_in_via_db`
/// — the JWT has already been minted and the session state is back
/// to NONE, so the Raft-routed write sees the root path and the
/// PERMISSIONS rule's `$auth = NONE` arm.
///
/// Best-effort: a write failure emits a warning but does not fail
/// the signin, same policy as audit. Losing an observability row
/// should never lock a user out.
async fn record_active_session(db: &Database, raft: &RaftNode, email: &str, peer_ip: &str) {
    let now = Datetime::now();
    let uid = new_session_uid();
    let session = crate::ActiveSession {
        uid: uid.clone(),
        user: nauka_core::resource::Ref::<User>::new(email.to_string()),
        ip: peer_ip.to_string(),
        user_agent: "cli".to_string(),
        last_active_at: now,
        created_at: now,
        updated_at: now,
        version: 0,
    };
    if let Err(e) = Writer::new(db).with_raft(raft).create(&session).await {
        tracing::warn!(
            event = "iam.session.write_failed",
            email = %email,
            peer_ip = %peer_ip,
            uid = %uid,
            error = %e,
            "active_session write failed — signin already succeeded"
        );
    }
}

/// 24-char `<12 hex ms><12 hex random>` id, same shape audit
/// events use. Sortable by prefix; doesn't need a full ULID.
fn new_session_uid() -> String {
    use argon2::password_hash::rand_core::{OsRng, RngCore};
    let ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    let mut rand = [0u8; 6];
    OsRng.fill_bytes(&mut rand);
    let mut out = format!("{ms:012x}");
    for b in rand {
        out.push_str(&format!("{b:02x}"));
    }
    out
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

    // Hold the shared IPC lock across the whole signin + invalidate
    // so the daemon's session never leaks between concurrent IPC
    // handlers or between the handler and a background task (see
    // `ops::IPC_LOCK`'s docstring for the full reasoning — tl;dr: a
    // leaked session breaks the reconciler).
    let _guard = crate::ops::IPC_LOCK.lock().await;
    // Drop any stale session that a previous panicked request may
    // have left behind before the new signin mutates it.
    let _ = db.inner().invalidate().await;

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

    // The JWT is what the caller keeps; the server-side session is
    // throwaway. Invalidate before returning so the daemon goes back
    // to root-level for any background task that runs next.
    let _ = db.inner().invalidate().await;

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
