//! Bucket and key naming rules, and the ETag construction.
//!
//! These look like trivia and are not: `s3-tests` checks them precisely,
//! and real clients depend on them. A bucket name that we accept but AWS
//! rejects (or the reverse) is a conformance failure, and an ETag that is
//! not the MD5 clients expect breaks tools that verify integrity offline.

use md5::{Digest, Md5};

/// Validates a bucket name against the general-purpose rules AWS applies
/// in every region since 2018.
///
/// Rejected: shorter than 3 or longer than 63 characters, anything but
/// lowercase letters, digits, dots and hyphens, a name that does not start
/// and end alphanumeric, adjacent dots, a dotted-quad that would be
/// ambiguous with an IP address, and the reserved prefixes and suffixes.
pub fn valid_bucket_name(name: &str) -> bool {
    if !(3..=63).contains(&name.len()) {
        return false;
    }
    if !name
        .bytes()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'.')
    {
        return false;
    }
    let first = name.as_bytes()[0];
    let last = name.as_bytes()[name.len() - 1];
    if !(first.is_ascii_lowercase() || first.is_ascii_digit()) {
        return false;
    }
    if !(last.is_ascii_lowercase() || last.is_ascii_digit()) {
        return false;
    }
    if name.contains("..") {
        return false;
    }
    // A name shaped like an IPv4 address is refused: it would be ambiguous
    // in virtual-hosted style URLs.
    if name.split('.').count() == 4 && name.split('.').all(|p| p.parse::<u8>().is_ok()) {
        return false;
    }
    const BAD_PREFIXES: [&str; 4] = ["xn--", "sthree-", "amzn-s3-demo-", "sthree-configurator"];
    if BAD_PREFIXES.iter().any(|p| name.starts_with(p)) {
        return false;
    }
    const BAD_SUFFIXES: [&str; 3] = ["-s3alias", "--ol-s3", "--x-s3"];
    if BAD_SUFFIXES.iter().any(|s| name.ends_with(s)) {
        return false;
    }
    true
}

/// Validates an object key: 1 to 1024 **bytes** of UTF-8. S3 accepts
/// essentially anything else, including newlines and control characters —
/// being stricter than AWS is itself a conformance bug.
pub fn valid_key(key: &str) -> bool {
    !key.is_empty() && key.len() <= 1024
}

/// ETag of a single-part object: the quoted MD5 of its bytes. Quotes are
/// part of the value, not decoration — clients compare the header verbatim.
pub fn etag_single(md5: &[u8; 16]) -> String {
    format!("\"{}\"", hex(md5))
}

/// ETag of a multipart object: MD5 over the concatenated **binary** part
/// digests, then `-<part count>`. Reproducing this exactly is what lets a
/// client tell a multipart object from a plain one, and check it offline.
pub fn etag_multipart(part_md5s: &[[u8; 16]]) -> String {
    let mut hasher = Md5::new();
    for digest in part_md5s {
        hasher.update(digest);
    }
    let combined: [u8; 16] = hasher.finalize().into();
    format!("\"{}-{}\"", hex(&combined), part_md5s.len())
}

/// Parses the hex digest out of a quoted ETag, single-part only.
pub fn md5_from_etag(etag: &str) -> Option<[u8; 16]> {
    let inner = etag.trim().trim_matches('"');
    if inner.len() != 32 || inner.contains('-') {
        return None;
    }
    let mut out = [0u8; 16];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(inner.get(i * 2..i * 2 + 2)?, 16).ok()?;
    }
    Some(out)
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bucket_names_follow_the_aws_rules() {
        // Lengths are computed, never hand-counted: an off-by-one in the
        // fixture reads exactly like a bug in the validator.
        let max_len = "a".repeat(63);
        let too_long = "a".repeat(64);
        let min_len = "abc";
        for ok in ["my-bucket", "a.b.c", "1234-5678", min_len, &max_len] {
            assert!(valid_bucket_name(ok), "{ok} should be accepted");
        }
        for bad in [
            "ab",                // too short
            "",                  // empty
            "UPPERCASE",         // capitals
            "under_score",       // underscore
            "-leading-hyphen",   // must start alphanumeric
            "trailing-hyphen-",  // must end alphanumeric
            "double..dot",       // adjacent dots
            "192.168.0.1",       // looks like an IPv4
            "xn--punycode",      // reserved prefix
            "something-s3alias", // reserved suffix
            &too_long,
            "ab", // one below the minimum
        ] {
            assert!(!valid_bucket_name(bad), "{bad} should be refused");
        }
    }

    #[test]
    fn keys_are_bounded_by_bytes_not_characters() {
        assert!(valid_key("a"));
        assert!(valid_key("dir/sub/file.txt"));
        // S3 is permissive on content: refusing these would be the bug.
        assert!(valid_key("with spaces and é★"));
        assert!(valid_key("with\nnewline"));
        assert!(!valid_key(""), "empty key");
        assert!(valid_key(&"a".repeat(1024)));
        assert!(!valid_key(&"a".repeat(1025)));
        // 1024 multi-byte characters exceed 1024 bytes.
        assert!(!valid_key(&"é".repeat(1024)));
    }

    #[test]
    fn single_part_etag_is_the_quoted_md5() {
        // MD5("") — the canonical empty-object ETag S3 returns.
        let digest: [u8; 16] = Md5::digest(b"").into();
        assert_eq!(etag_single(&digest), "\"d41d8cd98f00b204e9800998ecf8427e\"");
        let hello: [u8; 16] = Md5::digest(b"hello").into();
        assert_eq!(etag_single(&hello), "\"5d41402abc4b2a76b9719d911017c592\"");
    }

    #[test]
    fn multipart_etag_is_md5_of_binary_digests_with_a_suffix() {
        let a: [u8; 16] = Md5::digest(b"part one").into();
        let b: [u8; 16] = Md5::digest(b"part two").into();
        let etag = etag_multipart(&[a, b]);
        assert!(etag.ends_with("-2\""), "must carry the part count: {etag}");

        // The digests are concatenated as bytes, NOT as hex text — the
        // classic mistake, and one that only shows up against real clients.
        let mut hasher = Md5::new();
        hasher.update(a);
        hasher.update(b);
        let expected: [u8; 16] = hasher.finalize().into();
        assert_eq!(etag, format!("\"{}-2\"", hex(&expected)));

        // A single-part upload done through multipart still carries "-1".
        assert!(etag_multipart(&[a]).ends_with("-1\""));
    }

    #[test]
    fn etags_round_trip_for_single_part_only() {
        let digest: [u8; 16] = Md5::digest(b"payload").into();
        assert_eq!(md5_from_etag(&etag_single(&digest)), Some(digest));
        // A multipart ETag is not an MD5 of anything the client holds.
        assert_eq!(md5_from_etag(&etag_multipart(&[digest, digest])), None);
        assert_eq!(md5_from_etag("\"not-hex\""), None);
    }
}
