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
| `fails_on_rgw`, `fails_on_dbstore` | Ceph-backend-specific expectations, not the S3 contract. |
| `s3select` | SQL-over-objects — an AWS compute feature, not storage. |
| `cloud_transition`, `cloud_restore`, `lifecycle_transition` | Tiering to external cloud / Glacier. No cold tier here. |
| `sns` | Bucket-notification delivery to AWS SNS topics. |

## Deferred to a later phase

Built later; the marker comes off then. Each maps to a task on the roadmap.

| Marker | Feature | Phase |
| --- | --- | --- |
| `versioning`, `delete_marker` | Object versioning + delete markers | 5 |
| `object_lock` | Object Lock (retention, legal hold) | 5 |
| `tagging` | Object and bucket tagging | 5 |
| `lifecycle`, `lifecycle_expiration` | Lifecycle expiration rules | 5 |
| `cors` | CORS configuration | 5 |
| `s3website` | Static website hosting + routing | 5 |
| `checksum` | Additional checksums (CRC32/C, SHA1/256) | 5 |
| `bucket_logging` | Server access logging | 5 |
| `bucket_policy` | Bucket policies (IAM-style evaluation) | 6 |
| `object_ownership` | Object ownership / bucket & object ACLs | 6 |
| `sse_s3`, `encryption` | Server-side encryption (SSE-S3/C/KMS) | 6 |
| `iam_account`, `iam_role`, `iam_user`, `webidentity_test` | Full AWS IAM / STS | 6 (partial) |
| `group`, `user_policy`, `role_policy`, `session_policy` | IAM policy attachment | 6 |
| `conditional_write` | If-Match/If-None-Match on writes | 5 |

## Deferred, but unmarked by the suite

Some phase-5/6 tests carry no pytest marker, so `run.sh` excludes them by
name substring (`-k`): `versioning`, `versioned`, `object_lock`, `cors`,
`checksum`, plus the ACL / POST / anonymous / presigned families. Same
status as the marked ones above — they come off when the feature lands.

## Individually deselected edge cases

Six tests are deselected by full node id, each for a specific reason. These
are the only per-test exclusions; everything else in scope must pass.

| Test | Reason |
| --- | --- |
| `test_object_delete_key_bucket_gone` | Uses an unauthenticated client — anonymous access (phase 6). |
| `test_bucket_list_prefix_unreadable` | A raw control character (`\n`) in a prefix; the XML serializer percent-encodes it. Cosmetic, harmless. |
| `test_multi_object_delete_key_limit`, `test_multi_objectv2_delete_key_limit` | Create 1000 objects then delete; a timing test, slow under a debug build. |
| `test_multipart_resend_first_finishes_last` | Re-reads the part body multiple times via a fake file; an upload pattern no real client uses. |

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
