//! Space authentication: Ed25519 request signatures for writes.
//!
//! A space's client signs its HTTP writes with a private key it generated
//! itself and never transmitted; every node verifies with the public keys
//! replicated in the Raft state. No shared secrets anywhere: a compromised
//! node can check signatures, not mint them.
//!
//! The canonical string a write signature covers:
//!
//! ```text
//! {method}\n{path}\n{space}\n{timestamp}\n{content_hash or "-"}
//! ```
//!
//! `timestamp` is unix seconds and must be within [`MAX_CLOCK_SKEW`] of
//! the serving node's clock — a captured signature dies in minutes. When
//! the client pre-computes the BLAKE3 of its upload it includes it, and
//! the signature then binds the exact bytes; "-" leaves the body unbound
//! within the window (documented trade-off for plain-curl clients — the
//! signed-link format of the read path is separate and always fully
//! bound).

use anyhow::{bail, Context, Result};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};

/// Accepted clock difference between signer and verifier, seconds.
pub const MAX_CLOCK_SKEW: u64 = 300;

/// Prefix of the textual private-key form (`nsk_<hex seed>`): printed
/// once at generation, recognized by `nauka space sign`.
pub const SECRET_PREFIX: &str = "nsk_";

/// The canonical string a WRITE signature covers. The `nauka-write-v1`
/// prefix is domain separation: a write signature can never be replayed
/// as a read link nor vice versa, whatever creative values a signing
/// oracle is fed — the two canonicals disagree on their first bytes by
/// construction.
pub fn canonical_write(
    method: &str,
    path: &str,
    space: &str,
    timestamp: u64,
    content_hash: Option<&str>,
) -> String {
    format!(
        "nauka-write-v1\n{method}\n{path}\n{space}\n{timestamp}\n{}",
        content_hash.unwrap_or("-")
    )
}

/// Canonical v2 réservé aux uploads délégués. Contrairement au format
/// historique, il lie la signature aux octets, à leur longueur et à la
/// query exacte (`name`/`ttl`). Rejouer le grant ne peut donc produire
/// que le même objet et la même référence, ce qui est idempotent.
pub fn canonical_upload(
    path: &str,
    query: &str,
    space: &str,
    timestamp: u64,
    content_hash: &str,
    content_length: u64,
) -> String {
    format!(
        "nauka-upload-v2\nPUT\n{path}\n{query}\n{space}\n{timestamp}\n{content_hash}\n{content_length}"
    )
}

/// The canonical string a READ LINK signature covers (see
/// `?space=&exp=&sig=&rate=&conc=` on `GET /f/<hash>`). `exp` is the
/// unix second the link dies at — unlike writes there is no clock-skew
/// window: the signer chooses the exact lifetime. `rate` is the
/// per-connection ceiling in bytes/s ("-" = none) and `conc` the
/// ceiling on SIMULTANEOUS connections: both live inside the signed
/// string, so the recipient of a throttled link can neither
/// un-throttle it nor multiply it by editing the URL.
pub fn canonical_link(
    file_hash: &str,
    space: &str,
    exp: u64,
    rate: Option<u64>,
    conc: Option<u32>,
    ct: Option<&str>,
) -> String {
    let mut canonical = match rate {
        Some(r) => format!("nauka-link-v1\n{file_hash}\n{space}\n{exp}\n{r}"),
        None => format!("nauka-link-v1\n{file_hash}\n{space}\n{exp}\n-"),
    };
    // `conc` (max simultaneous connections) joined the format after
    // links were already in the wild: it is appended as a sixth line
    // ONLY when set, so every link signed before it existed keeps
    // verifying. No ambiguity is introduced — the query parameters
    // determine the canonical exactly, and stripping `conc=` from a
    // URL changes the string the signature was computed over.
    //
    // `ct` (the inline content type) came later still, as a seventh
    // line. A seventh line cannot exist without a sixth, so a link that
    // carries `ct` without `conc` spends a "-" on line six exactly like
    // an unthrottled `rate` does: without it, (conc=None, ct=X) and
    // (conc=X, ct=None) would collapse onto the same string.
    if conc.is_some() || ct.is_some() {
        match conc {
            Some(c) => canonical.push_str(&format!("\n{c}")),
            None => canonical.push_str("\n-"),
        }
    }
    if let Some(t) = ct {
        canonical.push_str(&format!("\n{t}"));
    }
    canonical
}

/// The content types a signed link may ask to be served INLINE, mapped
/// to the exact header value the node emits.
///
/// Two rules make this table the whole of the policy. First, the node
/// never echoes the caller's string — it serves the constant it looked
/// up — so no header injection and no smuggled `charset` survive the
/// round trip. Second, nothing that a browser executes in the origin's
/// context is in the table: no `text/html`, no `image/svg+xml`, no
/// XML. A tenant that could name those types would own a stored XSS on
/// whatever domain serves the file, which on a shared node is every
/// other tenant's domain too.
///
/// Everything absent from this table keeps being served as an
/// `application/octet-stream` attachment, which is the safe default the
/// node has always had.
pub fn inline_content_type(ct: &str) -> Option<&'static str> {
    Some(match ct {
        "image/jpeg" => "image/jpeg",
        "image/png" => "image/png",
        "image/gif" => "image/gif",
        "image/webp" => "image/webp",
        "image/avif" => "image/avif",
        "video/mp4" => "video/mp4",
        "video/webm" => "video/webm",
        "audio/mpeg" => "audio/mpeg",
        "audio/mp4" => "audio/mp4",
        "audio/ogg" => "audio/ogg",
        "audio/wav" => "audio/wav",
        "application/pdf" => "application/pdf",
        // Served with an explicit charset: a .txt full of markup is
        // then displayed as the text it is, never sniffed into a
        // document.
        "text/plain" => "text/plain; charset=utf-8",
        _ => return None,
    })
}

/// Parses an `nsk_…` private key into a signing key.
pub fn parse_secret(secret: &str) -> Result<SigningKey> {
    let hex_part = secret
        .strip_prefix(SECRET_PREFIX)
        .context("a private key starts with nsk_")?;
    let bytes: [u8; 32] = hex::decode(hex_part)
        .ok()
        .and_then(|v| v.try_into().ok())
        .context("a private key is nsk_ followed by 64 hex chars")?;
    Ok(SigningKey::from_bytes(&bytes))
}

/// Generates a fresh keypair; returns `(nsk_… secret, public key bytes)`.
pub fn generate() -> (String, [u8; 32]) {
    let signing = SigningKey::generate(&mut rand::rngs::OsRng);
    let secret = format!("{SECRET_PREFIX}{}", hex::encode(signing.to_bytes()));
    (secret, signing.verifying_key().to_bytes())
}

/// Signs a canonical string; returns the signature as hex (128 chars).
pub fn sign(secret: &SigningKey, canonical: &str) -> String {
    hex::encode(secret.sign(canonical.as_bytes()).to_bytes())
}

/// Verifies a hex signature over a canonical string against a raw public
/// key. Any malformed input is simply "not verified".
pub fn verify(public_key: &[u8; 32], canonical: &str, signature_hex: &str) -> bool {
    let Ok(vk) = VerifyingKey::from_bytes(public_key) else {
        return false;
    };
    let Ok(sig_bytes) = hex::decode(signature_hex) else {
        return false;
    };
    let Ok(sig_arr) = <[u8; 64]>::try_from(sig_bytes) else {
        return false;
    };
    vk.verify(canonical.as_bytes(), &Signature::from_bytes(&sig_arr))
        .is_ok()
}

/// `now` for the signing side.
pub fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Is `timestamp` within the accepted window of `now`?
pub fn timestamp_fresh(timestamp: u64, now: u64) -> bool {
    now.abs_diff(timestamp) <= MAX_CLOCK_SKEW
}

/// Parses a hex public key (64 chars) into raw bytes.
pub fn parse_public_hex(s: &str) -> Result<[u8; 32]> {
    let bytes: [u8; 32] = hex::decode(s)
        .ok()
        .and_then(|v| v.try_into().ok())
        .context("a public key is 64 hex chars")?;
    // Reject values that are not valid curve points outright: they could
    // never verify anything, so registering one is always a mistake.
    if VerifyingKey::from_bytes(&bytes).is_err() {
        bail!("not a valid Ed25519 public key");
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sign_verify_round_trip() {
        let (secret, public) = generate();
        let sk = parse_secret(&secret).unwrap();
        let c = canonical_write("PUT", "/api/upload", "yogfile/uploads", 1_755_000_000, None);
        let sig = sign(&sk, &c);
        assert!(verify(&public, &c, &sig));
        // Any field change breaks the signature.
        let c2 = canonical_write("PUT", "/api/upload", "yogfile/other", 1_755_000_000, None);
        assert!(!verify(&public, &c2, &sig));
        // Binding the content hash changes the canonical string.
        let c3 = canonical_write(
            "PUT",
            "/api/upload",
            "yogfile/uploads",
            1_755_000_000,
            Some("abcd"),
        );
        assert!(!verify(&public, &c3, &sig));
    }

    #[test]
    fn delegated_upload_binds_hash_size_and_query() {
        let (secret, public) = generate();
        let sk = parse_secret(&secret).unwrap();
        let canonical = canonical_upload(
            "/api/upload",
            "name=report.pdf&ttl=3600",
            "yogfile/files",
            1_755_000_000,
            "abcd",
            42,
        );
        let signature = sign(&sk, &canonical);
        assert!(verify(&public, &canonical, &signature));
        for changed in [
            canonical_upload(
                "/api/upload",
                "name=other.pdf&ttl=3600",
                "yogfile/files",
                1_755_000_000,
                "abcd",
                42,
            ),
            canonical_upload(
                "/api/upload",
                "name=report.pdf&ttl=3600",
                "yogfile/files",
                1_755_000_000,
                "abcd",
                43,
            ),
            canonical_upload(
                "/api/upload",
                "name=report.pdf&ttl=3600",
                "yogfile/files",
                1_755_000_000,
                "dcba",
                42,
            ),
        ] {
            assert!(!verify(&public, &changed, &signature));
        }
    }

    #[test]
    fn foreign_key_never_verifies() {
        let (secret, _) = generate();
        let (_, other_public) = generate();
        let sk = parse_secret(&secret).unwrap();
        let c = canonical_write("PUT", "/api/upload", "yogfile/uploads", 1, None);
        assert!(!verify(&other_public, &c, &sign(&sk, &c)));
    }

    #[test]
    fn timestamp_window() {
        assert!(timestamp_fresh(1000, 1000 + MAX_CLOCK_SKEW));
        assert!(timestamp_fresh(1000 + MAX_CLOCK_SKEW, 1000));
        assert!(!timestamp_fresh(1000, 1001 + MAX_CLOCK_SKEW));
    }

    #[test]
    fn write_and_link_domains_never_cross() {
        let (secret, public) = generate();
        let sk = parse_secret(&secret).unwrap();
        // A link signature must not validate as any write, even one
        // crafted to mimic the link's field layout.
        let link_sig = sign(
            &sk,
            &canonical_link("abcd", "yogfile/uploads", 999, None, None, None),
        );
        let mimic = canonical_write("abcd", "yogfile/uploads", "999", 0, None);
        assert!(!verify(&public, &mimic, &link_sig));
        // And a write signature must not open a link.
        let write_sig = sign(
            &sk,
            &canonical_write("GET", "/f/abcd", "yogfile/uploads", 999, None),
        );
        assert!(!verify(
            &public,
            &canonical_link("abcd", "yogfile/uploads", 999, None, None, None),
            &write_sig
        ));
    }

    #[test]
    fn link_rate_is_bound_by_the_signature() {
        let (secret, public) = generate();
        let sk = parse_secret(&secret).unwrap();
        let throttled = canonical_link("abcd", "yogfile/uploads", 999, Some(1_000_000), None, None);
        let sig = sign(&sk, &throttled);
        assert!(verify(&public, &throttled, &sig));
        // Stripping or editing the rate invalidates the signature.
        assert!(!verify(
            &public,
            &canonical_link("abcd", "yogfile/uploads", 999, None, None, None),
            &sig
        ));
        assert!(!verify(
            &public,
            &canonical_link("abcd", "yogfile/uploads", 999, Some(8_000_000), None, None),
            &sig
        ));
    }

    #[test]
    fn link_conc_is_bound_by_the_signature() {
        let (secret, public) = generate();
        let sk = parse_secret(&secret).unwrap();
        let capped = canonical_link("abcd", "yogfile/uploads", 999, Some(500_000), Some(2), None);
        let sig = sign(&sk, &capped);
        assert!(verify(&public, &capped, &sig));
        // Stripping conc (the aria2 gambit), raising it, or keeping it
        // while dropping the rate all invalidate the signature.
        assert!(!verify(
            &public,
            &canonical_link("abcd", "yogfile/uploads", 999, Some(500_000), None, None),
            &sig
        ));
        assert!(!verify(
            &public,
            &canonical_link(
                "abcd",
                "yogfile/uploads",
                999,
                Some(500_000),
                Some(16),
                None
            ),
            &sig
        ));
        assert!(!verify(
            &public,
            &canonical_link("abcd", "yogfile/uploads", 999, None, Some(2), None),
            &sig
        ));
        // And a pre-conc link still verifies: the canonical without conc
        // is byte-identical to what v0.6.3 signed.
        let legacy = canonical_link("abcd", "yogfile/uploads", 999, None, None, None);
        assert_eq!(legacy.matches('\n').count(), 4, "five lines, no sixth");
        let legacy_sig = sign(&sk, &legacy);
        assert!(verify(&public, &legacy, &legacy_sig));
    }

    #[test]
    fn link_content_type_is_bound_by_the_signature() {
        let (secret, public) = generate();
        let sk = parse_secret(&secret).unwrap();
        let inline = canonical_link(
            "abcd",
            "yogfile/files",
            999,
            Some(500_000),
            Some(2),
            Some("video/mp4"),
        );
        let sig = sign(&sk, &inline);
        assert!(verify(&public, &inline, &sig));
        // Dropping the type, or swapping it for another allowlisted
        // one, breaks the signature: the holder of a link to a video
        // cannot re-present the same bytes as a PDF.
        assert!(!verify(
            &public,
            &canonical_link("abcd", "yogfile/files", 999, Some(500_000), Some(2), None),
            &sig
        ));
        assert!(!verify(
            &public,
            &canonical_link(
                "abcd",
                "yogfile/files",
                999,
                Some(500_000),
                Some(2),
                Some("application/pdf")
            ),
            &sig
        ));
    }

    #[test]
    fn content_type_without_conc_still_spends_the_sixth_line() {
        // The trap this guards: if `ct` slid up into the sixth line
        // whenever `conc` was absent, then (conc=None, ct=X) and
        // (conc=X, ct=None) would sign the same bytes. A placeholder
        // keeps every field on the line it was born on.
        let ct_only = canonical_link("abcd", "yogfile/files", 999, None, None, Some("video/mp4"));
        let lines: Vec<&str> = ct_only.split('\n').collect();
        assert_eq!(lines.len(), 7, "ct always lands on line seven");
        assert_eq!(lines[4], "-", "no rate");
        assert_eq!(lines[5], "-", "no conc, but the line is spent");
        assert_eq!(lines[6], "video/mp4");
        // And a link with neither is byte-identical to a pre-ct one.
        let plain = canonical_link("abcd", "yogfile/files", 999, None, None, None);
        assert_eq!(plain.matches('\n').count(), 4);
    }

    #[test]
    fn only_inert_types_are_servable_inline() {
        assert_eq!(inline_content_type("video/mp4"), Some("video/mp4"));
        assert_eq!(inline_content_type("image/png"), Some("image/png"));
        // Text is pinned to a charset so a .txt full of markup stays
        // text on every browser.
        assert_eq!(
            inline_content_type("text/plain"),
            Some("text/plain; charset=utf-8")
        );
        // Anything a browser would execute in the origin's context is
        // refused — this is the whole security argument of the feature.
        for executable in [
            "text/html",
            "image/svg+xml",
            "application/xml",
            "text/xml",
            "application/xhtml+xml",
            "application/javascript",
        ] {
            assert_eq!(inline_content_type(executable), None, "{executable}");
        }
        // No smuggling parameters past the table: the node serves the
        // constant it looked up, and lookups are exact.
        assert_eq!(inline_content_type("video/mp4; boundary=x"), None);
        assert_eq!(inline_content_type("VIDEO/MP4"), None);
        assert_eq!(inline_content_type("text/plain\r\nX-Evil: 1"), None);
    }

    #[test]
    fn secret_format_round_trips() {
        let (secret, public) = generate();
        assert!(secret.starts_with(SECRET_PREFIX));
        assert_eq!(
            parse_secret(&secret).unwrap().verifying_key().to_bytes(),
            public
        );
        assert!(parse_secret("nsk_zz").is_err());
        assert!(parse_secret("deadbeef").is_err());
    }
}
