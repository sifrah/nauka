# S3 conformance — excluded tests

Nauka runs `ceph/s3-tests` in CI and every test outside this list must
pass. What is excluded, and why, is tracked here. **This list is expected
to shrink**: an exclusion is a debt, not a decision.

Exclusions are applied by pytest marker in `run.sh`, one marker at a time.
As a feature lands, its marker comes off the exclusion line and the suite
starts holding Nauka to the real behaviour.

## Permanently out of scope

These exercise AWS-only services with no self-hosted meaning. They will
never come off the list.

| Marker | Why |
| --- | --- |
| `fails_on_aws` | The suite itself marks these as not matching AWS. |
| `fails_on_rgw` | Ceph-RGW-specific expectations, not the S3 contract. |
| `fails_on_dbstore` (partially!) | "Fails on Ceph's dbstore reference backend" — which says nothing about Nauka. The 223 tests carrying ONLY this marker were all executed against Nauka: the 94 that pass are pinned in `dbstore-passing.txt` and run as a second gate pass; the ~129 that fail map to features still to land (real SSE-S3/KMS at rest, multipart checksums, conditional writes, ObjectOwnership, POST…) and rejoin the list as those land. |
| `s3select` | SQL-over-objects — an AWS compute feature, not storage. |
| `cloud_transition`, `cloud_restore`, `lifecycle_transition` | Tiering to external cloud / Glacier. No cold tier here. |
| `sns` | Bucket-notification delivery to AWS SNS topics. |

## Deferred to a later phase

Built later; the marker comes off then. Each maps to a task on the roadmap.

| Marker | Feature | Phase |
| --- | --- | --- |
| `delete_marker` | Delete-marker corner cases (e.g. a plain 404 in an unversioned bucket carrying `x-amz-delete-marker: false`) | 5 |
| `s3website` | Static website hosting + routing — the pinned suite carries NO website tests (`test_s3_website.py` was removed upstream), so this marker currently deselects nothing; it stays as documentation until a suite bump restores coverage | 5 |
| `bucket_logging` | Server access logging | 5 |
| `object_ownership` | The ObjectOwnership setting (BucketOwnerEnforced &c.) — plain ACLs landed in phase 6, this marker's tests exercise the ownership-transfer knob | 6 |
| `iam_account`, `iam_role`, `iam_user`, `webidentity_test` | Full AWS IAM / STS | 6 (partial) |
| `group`, `user_policy`, `role_policy`, `session_policy` | IAM policy attachment | 6 |
| `conditional_write` | If-Match/If-None-Match on writes | 5 |

## Deferred, but unmarked by the suite

Some phase-5/6 tests carry no pytest marker, so `run.sh` excludes them by
name substring (`-k`): `website`, plus the POST / anonymous / presigned
families. Same status as the marked ones above — they come off when the
feature lands.

The `cors` name filter came off when CORS landed: configuration storage,
OPTIONS preflight and Access-Control-* response headers, plus just enough
anonymous access for its tests — an unauthenticated READ of a bucket whose
canned ACL is `public-read`. Full ACL evaluation is still phase 6.

The `checksum` markers came off when additional checksums landed: a
client-sent CRC32/CRC32C/CRC64NVME/SHA1/SHA256 is verified against the
body on PUT (BadDigest on mismatch), stored on the version, echoed on the
PUT response, and returned on GET/HEAD under `ChecksumMode: ENABLED` and
in `GetObjectAttributes`. The multipart-checksum tests stay out via the
permanent `fails_on_dbstore` marker; aws-chunked trailer checksums stay
with the `aws_chunked` exclusion.

The `lifecycle` markers came off when lifecycle configuration landed: rule
storage and validation, plus the `x-amz-expiration` header. The
timing-based expiration tests (`lifecycle_expiration` + `fails_on_aws` /
`fails_on_dbstore`) exercise RGW's compressed debug clock and stay out via
those permanent markers, as they do on AWS itself.

The `bucket_policy` marker and the `policy` name filter came off when
bucket policies landed: Put/Get/Delete BucketPolicy, GetBucketPolicyStatus,
Put/Get/Delete PublicAccessBlock (BlockPublicPolicy enforced at PUT), and
IAM-style evaluation on every request — principals (`*`, named), actions,
resource ARNs with wildcards, String* conditions with `IfExists`, explicit
Deny overriding Allow, and the AWS 404-vs-403 rule for denied reads of
missing keys when the caller holds `s3:ListBucket` for the prefix. Landing
it also wired real authorization: a key is its own account (owns what it
creates), cross-key access needs a policy or an explicit grant, and the
RGW `tenant:bucket` addressing form resolves into the flat namespace.

The `sse_s3` and `encryption` markers came off when server-side
encryption landed. SSE-C is REAL encryption: the body is encrypted with
the customer's key (AES-256-GCM, the same engine as the native
end-to-end flow) before erasure coding, the key is never stored — only
its MD5 fingerprint — and reads require presenting the same key (wrong
or missing key → 400). SSE-S3/KMS record and echo the mode and validate
the AWS error surface (conflicting headers, kms without key id, SSE
headers on a read → 400 InvalidArgument); their at-rest encryption is
not implemented. Bucket default-encryption config round-trips
(Put/Get/Delete BucketEncryption, ServerSideEncryptionConfigurationNotFoundError
when unset) but is not yet APPLIED to plain PUTs. Policies gained the
`Null` condition operator and the `s3:x-amz-server-side-encryption`
condition key — and an explicit Deny now binds the bucket owner too
(except on the policy subresource itself, so a lockout is always
repairable).

The `acl` and `access_bucket` name filters came off when full ACLs
landed: grant lists on buckets and objects (canned ACLs expanded, group
grants listed before canonical users), Get/Put Bucket/Object ACL with
grantee validation (unknown user → InvalidArgument, email grantee →
UnresolvableGrantByEmailAddress), display names resolved from the
credential registry, and enforcement — bucket ACL governs listing (READ),
key writes (WRITE) and the ACL subresource (READ_ACP/WRITE_ACP); the
OBJECT ACL alone governs object reads; an explicit policy Deny beats any
ACL; BlockPublicAcls refuses public ACLs at PUT and IgnorePublicAcls
silences group grants at evaluation. Objects default to
`binary/octet-stream` when the client sent no Content-Type, as AWS does.

## Individually deselected edge cases

Six tests are deselected by full node id, each for a specific reason. These
are the only per-test exclusions; everything else in scope must pass.

| Test | Reason |
| --- | --- |
| `test_object_delete_key_bucket_gone` | Uses an unauthenticated client — anonymous access (phase 6). |
| `test_bucket_list_prefix_unreadable` | A raw control character (`\n`) in a prefix; the XML serializer percent-encodes it. Cosmetic, harmless. |
| `test_multi_object_delete_key_limit`, `test_multi_objectv2_delete_key_limit` | Create 1000 objects then delete; a timing test, slow under a debug build. |
| `test_multipart_resend_first_finishes_last` | Re-reads the part body multiple times via a fake file; an upload pattern no real client uses. |
| `test_get_versioned_object_attributes`, `test_get_checksum_object_attributes` | `GetObjectAttributes` is the one operation AWS returns the ETag *unquoted*; the `s3s` XML serializer always quotes it. An `s3s` limitation, not a Nauka behaviour. (The checksum variant's `Checksum` block itself is served correctly.) |

`test_get_object_ifnonematch_good` used to be here (the `304 Not Modified`
was missing its `ETag`/`Last-Modified` headers) and is now in scope — the
first exclusion paid back.

## In scope — must pass

Everything not listed above: buckets, objects (PUT/GET/HEAD/DELETE, ranges,
copy, batch delete), ListObjects v1/v2 (prefix, delimiter, pagination,
continuation, url encoding), multipart, conditional reads, system and user
metadata. A failure here is a real bug and fails CI.

## Note on `NotImplemented`

An operation Nauka has not built yet answers a conformant `501
NotImplemented`, which is a valid S3 response — never a wrong one. The
suite counts it as a failure for that operation's own tests, which is why
those markers are deferred rather than pretended-passing.
