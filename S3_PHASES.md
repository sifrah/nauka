# Nauka S3 — phase tracker (resume doc)

Working state for the S3-compatibility effort, so work can resume after a
context clear. Goal: **the full AWS S3 surface**, measured — not asserted —
by the `ceph/s3-tests` conformance suite running in CI.

## Where we are

In-scope conformance tests passing, per feature landed (each a separate
commit on `main`, each CI-green on a release build):

| Milestone | In-scope tests | Status |
| --- | --- | --- |
| Phase 4 baseline (objects, listing, multipart, ranges, conditional) | 130 | ✅ CI |
| + Versioning | 147 | ✅ CI |
| + Tagging | 154 | ✅ CI |
| + Object Lock | 161 | ✅ CI |
| + Lifecycle | 178 | ✅ CI |
| + CORS | 186 | ✅ CI |
| + Checksums | 188 | ✅ CI |
| + Bucket policies (phase 6 start) | 204 | ✅ CI |
| + Full ACLs (buckets + objects) | 250 | ✅ CI |
| + SSE (SSE-C real crypto, S3/KMS surface) | 273 | ✅ CI |
| + dbstore triage: 94 upstream-excluded tests measured green and pinned | 367 | ✅ CI |
| + POST-object + presigned/anonymous | 422 | ✅ CI |

The suite has ~838 collectable tests; the rest are **excluded on purpose**
and tracked in `conformance/EXCLUSIONS.md` (each exclusion is debt meant to
shrink). Excluded = AWS-only services (S3 Select, Glacier tiering, SNS) or
features not built yet (phases 5–6 below), plus a handful of per-test edge
cases deselected by name with a reason.

## Phase status

- **Phase 1–3 — DONE.** Native S3 endpoint on port 8333 built on the `s3s`
  crate (SigV4, XML, routing, the 99-op service trait). Credentials,
  buckets, objects (PUT/GET/HEAD/DELETE, ranges, copy, batch delete),
  ListObjects v1/v2, multipart, conditional reads, system+user metadata.
- **Phase 4 — DONE.** `ceph/s3-tests` in CI (`.github/workflows/conformance.yml`
  + `conformance/run.sh`), in-scope subset a hard gate.
- **Phase 5 — IN PROGRESS.**
  - Versioning — DONE (147).
  - Tagging — DONE (154).
  - Object Lock — DONE (161).
  - Lifecycle — DONE (178). Rule config stored as its XML wire form on
    the bucket (exact GET round-trip), validation (ids, status, Days>0,
    midnight dates), x-amz-expiration on PUT/GET/HEAD. The timing-based
    expiration tests stay excluded via the permanent `fails_on_aws` /
    `fails_on_dbstore` markers (RGW debug-clock tests); actual background
    expiry is NOT implemented — wire it to the engine TTL when needed.
  - CORS — DONE (186). Config ops (XML round-trip on Bucket.cors) plus an
    HTTP layer in our own hyper wrapper (`ServiceRef`): OPTIONS preflight
    answered there (400 no Origin/ACRM, 403 no rule, 200 match), and
    Access-Control-* headers decorated onto every response whose Origin +
    (ACRM || method) matches a rule. The behavioural tests also needed
    anonymous READ: `Bucket.acl` stores the canned ACL from create, and a
    `NaukaAccess` (s3s S3Access hook) allows unauthenticated
    GetObject/HeadObject/HeadBucket/ListObjects(V2) on a `public-read`
    bucket — everything else still 403s. Full ACL grants remain phase 6.
  - Checksums — DONE (188). Client-sent CRC32/CRC32C/CRC64NVME/SHA1/SHA256
    verified on PUT (BadDigest), stored per version, echoed on PUT,
    returned under ChecksumMode: ENABLED and in GetObjectAttributes.
    Multipart part checksums NOT wired (their tests are fails_on_dbstore).
    test_get_checksum_object_attributes deselected — the same unquoted-
    ETag s3s quirk as test_get_versioned_object_attributes.
  - **Website — no tests at the pin.** The pinned s3-tests commit removed
    test_s3_website.py, so website hosting has NOTHING to gate against;
    the s3website marker is dead weight kept as documentation. Implement
    only if/when the suite pin is bumped to one with coverage (or skip).
  - **bucket_logging — moved to phase 6.** Its 6 candidate tests are
    built on put_bucket_policy + policy EVALUATION (service principals,
    SourceArn conditions, AccessDenied for alt users, log-delivery
    permission checks) — implement after the bucket-policy engine.
  - delete_marker and conditional_write markers deselect ZERO in-scope
    tests at this pin (all their tests are fails_on_dbstore/aws);
    removing them is free but pointless until a suite bump.
  - **Phase 5 is therefore COMPLETE at the current suite pin (188).**
- **Phase 6 — IN PROGRESS.**
  - **Bucket policies — DONE (204).** `nauka-s3::policy` parses (lenient
    string-or-array, strict on Effect / Allow+NotPrincipal / missing
    Action-Resource) and evaluates IAM-style: Deny > Allow > NoMatch,
    wildcard actions/resources, String* conditions with `IfExists`
    (unknown operators fail closed). Ops: Put/Get/Delete BucketPolicy,
    GetBucketPolicyStatus (IsPublic), Put/Get/Delete PublicAccessBlock
    (BlockPublicPolicy refuses a public policy at PUT). Policy stored as
    its raw string on `Bucket.policy` (exact GET round-trip).
  - **Real authorization landed with it** (`NaukaAccess::check`): a key
    is its own account — full access to buckets it created (by
    `Bucket.owner`), everything else needs an explicit credential grant
    (`Credential::allows`, wired at last) or a policy Allow; explicit
    Deny is final; anonymous keeps the `public-read` canned-ACL reads.
    **Semantics change:** `buckets: None` on a credential no longer means
    cluster-wide access. Also: AWS's 404-vs-403 rule (denied GET/HEAD of
    a missing key answers NoSuchKey when the caller holds s3:ListBucket
    for that prefix), and `tenant:bucket` / `:bucket` RGW addressing
    resolves into the flat namespace (custom s3s NameValidation +
    `tenant_suffix`).
  - **Credentials got a canonical user id** (`--user-id` on
    s3-key-create; falls back to the access key). ACL grantee ids and
    policy principals match it — CI + local registration now pass the
    suite's `user_id` values.
  - **Minimal object ownership**: `ObjectVersion.owner` = uploader's
    canonical id, or the `id=` of `x-amz-grant-full-control` at PUT.
  - **Full ACLs — DONE (250).** `nauka-s3::acl`: grant lists stored as
    JSON on `Bucket.acl_grants` / `ObjectVersion.acl` (None = private:
    owner FULL_CONTROL), canned ACLs expanded (groups before the owner —
    the suite's comparison depends on that order), display names looked
    up from credential `name` at read time (registration now uses the
    conf display names: 'M. Tester', 'john.doe', …). Ops: Get/Put
    Bucket/Object ACL (canned + AccessControlPolicy, grantee validation:
    unknown canonical id → InvalidArgument, email →
    UnresolvableGrantByEmailAddress), object ACL versioned via the
    SetObjectAcl Raft command. Enforcement in the authorizer ladder:
    owner → policy (explicit Deny is FINAL, before ACLs — the
    policy_acl tests check this) → credential grants → ACLs (bucket ACL
    = listing/writes/ACL subresource, OBJECT ACL alone = object reads)
    → policy allow → the 404-vs-403 rule (now also satisfied by bucket
    ACL READ, which keeps the anonymous CORS 404s working).
    BlockPublicAcls refuses public ACLs at PUT (bucket + object canned),
    IgnorePublicAcls mutes group grants at evaluation. PUTs without a
    Content-Type now store `binary/octet-stream`, as AWS does.
  - **SSE — DONE (273).** SSE-C is REAL encryption: the body is
    encrypted with the customer's 32-byte key via `nauka-crypto`
    (AES-256-GCM, same engine as the native E2E flow) BEFORE erasure
    coding — the cluster stores and content-addresses ciphertext, the
    key is never kept (only its MD5 fingerprint on `ObjectVersion.sse`
    as JSON `SseInfo`), and reads must present the same key (missing /
    wrong / mismatched-MD5 → 400 InvalidArgument). Multipart: each part
    is an independent cipher stream (`UploadedPart.plain_size` vs
    stored size; segment lengths on the completed version let GET
    decrypt in order). `version.size` is ALWAYS plaintext size;
    manifest sizes are ciphertext. Copying an SSE-C source → 501.
    SSE-S3/aws:kms record+echo the mode and validate the error surface
    (conflicts, kms-without-key-id, `aes:kms`, SSE headers on read →
    400); at-rest crypto for those is NOT implemented, and bucket
    default encryption (Put/Get/Delete BucketEncryption round-trip,
    ServerSideEncryptionConfigurationNotFoundError) is stored but not
    yet applied to plain PUTs. Policy engine gained `Null` conditions
    and `s3:x-amz-server-side-encryption`; explicit Deny now binds the
    OWNER too (except the policy subresource — lockout stays
    repairable).
  - **dbstore triage — DONE (367).** The 223 tests carrying only the
    `fails_on_dbstore` marker (Ceph's reference backend's failures, not
    ours) were all run against Nauka: 94 pass — object_lock 32/32,
    object_copy 11, sse_kms 10, multipart 5… — and are now PINNED in
    `conformance/dbstore-passing.txt`, run as a second pytest pass in
    run.sh (a regression there fails CI like any in-scope test). The
    ~129 that fail cluster by missing feature: ~60 real SSE-S3/KMS at
    rest, 14 multipart (part checksums), 12 listing edge cases, 9
    policy extras, 5 conditional writes (If-Match on PUT), 5
    ObjectOwnership ops, GetBucketLocation, range-read trailing
    checksum handling. Each future feature moves its wins into the
    pinned list.
  - **POST-object + presigned/anonymous — DONE (422).** `s3s` owns the
    POST protocol (form parsing, policy document expiration+conditions,
    the form signature — V2 and V4); the `post_object` handler is a
    simplified PutObject fed from form fields (canned ACL, XML tagging
    field, metadata, default content-type, owner). Anonymous requests
    ride the phase-6 ACL layer (`PostObject`/`PutObject` on a bucket
    path = bucket WRITE). The HTTP wrapper (ServiceRef) patches three
    AWS-vs-s3s gaps: out-of-range/negative `X-Amz-Expires` → 403 before
    signature checking; unmet post-policy condition → 403 AccessDenied
    (bad policy structure stays 400 — told apart by the s3s message);
    quoted ETag restored in the success_action_redirect Location. Four
    POST tests deselected with reasons (s3s drops `${filename}`,
    accepts lenient expiration dates, refuses out-of-policy
    `x-amz-checksum-*` fields, treats unsigned POST as anonymous).
  - TODO next: object_ownership (the BucketOwnerEnforced knob),
    bucket_logging (needs policy-evaluation extras: service principals,
    SourceArn conditions), event notifications, real at-rest SSE-S3/KMS
    (~60 more pool tests), conditional writes, GetBucketLocation.

## How to run the gate locally

```sh
# 1. build
cargo build -p nauka-node

# 2. start a single-node cluster with the S3 endpoint on :8000
BIN=target/debug/nauka
TOKEN=$("$BIN" token)
NAUKA_TOKEN="$TOKEN" "$BIN" --data-dir ./nd \
  serve --listen 127.0.0.1:7311 --http 127.0.0.1:8080 --s3 127.0.0.1:8000 --no-discover &
ID=$("$BIN" --data-dir ./nd --token "$TOKEN" node-info | head -1 | awk '{print $3}')
"$BIN" --data-dir ./nd --token "$TOKEN" cluster-init "$ID@127.0.0.1:7311"

# 3. register the four fixed credentials the suite config expects
#    (--user-id = the conf's user_id: ACL grantee ids must match it;
#     --name = the conf's display_name: ACL responses show it)
reg() { "$BIN" --data-dir ./nd --token "$TOKEN" s3-key-create --name "$1" --user-id "$2" --access-key "$3" --secret-key "$4" --peer 127.0.0.1:7311; }
reg 'M. Tester'          testid 0555b35654ad1656d804 'h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q=='
reg 'john.doe'           56789abcdef0123456789abcdef0123456789abcdef0123456789abcdef01234 NOPQRSTUVWXYZABCDEFG 'nopqrstuvwxyzabcdefghijklmnopqrstuvwxyz1'
reg 'testx$tenanteduser' 9876543210abcdef0123456789abcdef0123456789abcdef0123456789abcdef HIJKLMNOPQRSTUVWXYZA 'opqrstuvwxyzabcdefghijklmnopqrstuvwxyzab'
reg 'youruseridhere'     9876543210abcdef0123456789abcdef0123456789abcdef0123456789abcdef ABCDEFGHIJKLMNOPQRST 'abcdefghijklmnopqrstuvwxyzabcdefghijklmn'

# 4. run the gate (clones/pins ceph/s3-tests, applies the exclusion filters)
./conformance/run.sh
# expect: 328 passed on pass 1, then 94 passed on the dbstore pass (422 total)
```

To iterate on ONE feature's family fast, run pytest with `-k "<marker or
substring> and not acl"` against a running node (S3TEST_CONF pointing at
`conformance/s3tests.conf`).

## Bringing a new feature in-scope (the loop)

1. Implement the ops in `crates/nauka-node/src/s3.rs` (+ any Raft command
   in `crates/nauka-raft/src/types.rs` & `store.rs`, + model fields in
   `crates/nauka-s3/src/model.rs`).
2. Run its test family with `-k`, read failures, fix. Many "failures" are
   really per-test edge cases or cross-feature markers.
3. Remove the feature's marker from the `EXCLUDE`/`KEYWORDS` lines in
   `conformance/run.sh` (they must stay SINGLE-LINE — a backslash-newline
   inside the single quotes is literal and pytest rejects the -k/-m expr;
   this bit us once, green locally, red in CI).
4. Move the marker from "deferred" to "Resolved" in `conformance/EXCLUSIONS.md`.
5. Validate: `cargo test --workspace` (71), clippy 0, `cargo fmt --check`,
   then `./conformance/run.sh` = 0 failed. Commit, push, confirm CI green.

## Key design decisions (so they aren't relitigated)

- **`s3s` owns the protocol.** We implement the service trait; unimplemented
  ops inherit its default → a conformant `501 NotImplemented`, never a
  silent wrong answer.
- **Content vs naming.** Objects point at BLAKE3 manifest hashes, so dedup
  survives S3's mutable bucket/key naming. Manifest **refcount is DERIVED**
  from the index (`S3State::refcount` / `live_content`), never incremented —
  a drift means leaked disk or lost data.
- **Empty object ≠ delete marker.** `ObjectVersion.content == None` is
  ambiguous, so an explicit `delete_marker: bool` flag distinguishes a
  zero-byte object from a deletion. (This bug hid empty objects from
  listings; the fix is load-bearing.)
- **ETags are real MD5.** Single-part = quoted MD5; multipart =
  `md5(concat of BINARY part digests)-N`. GetObjectAttributes is the ONE
  op AWS returns the ETag unquoted — `s3s` always quotes, so that single
  test is deselected (documented).
- **Retention** stored as JSON `{mode, until}` on the version; legal hold a
  bool. Lock enforcement lives in the shared `delete_one`. Custom error
  code `InvalidRetentionPeriod` via `custom_error()` (s3s has no variant).
- **Response headers `s3s` DTOs lack** (x-amz-tagging-count, x-amz-object-
  lock-*, ETag on 304) are set directly on `S3Response::headers` /
  `S3Error::set_headers`.
- **Concurrency belongs in the state machine.** Bucket-exists check, part
  merge (`PutUploadPart`), tag/retention/legal-hold sets are Raft commands
  so the log serializes them — clients hit these concurrently (boto3 uses
  8 threads for multipart).

## Cluster / infra state (unrelated to S3, don't disturb)

- 5-node WAN cluster (Paris×2, Warsaw, Milan, Amsterdam) running v0.3.0
  under systemd, with a soak timer (`nauka-soak`) uploading+verifying every
  5 min, logging to `/var/log/nauka-soak.csv`. Untouched during S3 work.
- The working checkout is `~/Documents/nauka` on `main` (origin =
  `sifrah/nauka`; the old platform history lives on `basics`). The
  `~/Documents/yogfile` checkout is the same history via its `nauka`
  remote — don't work in both at once.
  Releases are tagged `vX.Y.Z`; the engine is at v0.3.0. The S3 work has
  NOT been tagged into a release yet.
- Known engine gap noted earlier but deferred: a genuinely-unreachable
  member should be dropped from the placement view so writes stay fully
  redundant during an outage (degraded-write path exists; view-eviction
  does not).
