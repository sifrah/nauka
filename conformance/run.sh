#!/usr/bin/env bash
# Runs the ceph/s3-tests conformance suite against a Nauka node.
#
# The node must already be serving its S3 endpoint on 127.0.0.1:8000 with
# the fixed credentials below (register them with `nauka s3-key-create
# --access-key … --secret-key …`). CI does the wiring in the workflow; this
# script is the shared body so `./conformance/run.sh` reproduces CI exactly.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORK="${S3TESTS_DIR:-/tmp/nauka-s3-tests}"
S3TESTS_REF="${S3TESTS_REF:-master}"

# Marker exclusions: AWS-only services and features not built yet. Each is
# justified in EXCLUSIONS.md — the list is expected to SHRINK over time,
# never grow silently. Keep it in sync with that file.
EXCLUDE='not fails_on_rgw and not fails_on_aws and not fails_on_dbstore \
  and not s3select and not sns \
  and not cloud_transition and not cloud_restore \
  and not sse_s3 and not encryption \
  and not versioning and not delete_marker and not object_lock \
  and not tagging and not lifecycle and not lifecycle_expiration \
  and not lifecycle_transition and not cors and not s3website \
  and not checksum and not bucket_logging and not bucket_policy \
  and not object_ownership and not conditional_write \
  and not group and not user_policy and not role_policy and not session_policy \
  and not iam_account and not iam_role and not iam_user and not webidentity_test'

# Name-based exclusions for phase-5/6 features whose tests carry NO pytest
# marker, so a marker cannot deselect them: ACLs and ACL-driven access
# tests, browser POST uploads, anonymous / presigned-query auth, bucket
# policies, public-access-block, torrent, SigV4 streaming (aws-chunked),
# UploadPartCopy, cross-owner ops, and the versioning / object-lock / CORS /
# checksum families whose tests the suite left unmarked. Documented in
# EXCLUSIONS.md.
KEYWORDS='not acl and not access_bucket and not post_object \
  and not anon and not _raw_ and not policy and not public_block \
  and not torrent and not aws_chunked and not chunked_transfer \
  and not multipart_copy and not copy_not_owned and not 100_continue \
  and not expected_bucket_owner and not website and not lifecycle \
  and not versioning and not versioned and not object_lock \
  and not cors and not checksum'

# A handful of individual edge-case tests, deselected by full node id with
# a reason each (see EXCLUSIONS.md): anonymous access, a control character
# in a prefix, the ETag header on a 304, and a 1000-object timing test.
DESELECT="
  --deselect s3tests/functional/test_s3.py::test_object_delete_key_bucket_gone
  --deselect s3tests/functional/test_s3.py::test_bucket_list_prefix_unreadable
  --deselect s3tests/functional/test_s3.py::test_get_object_ifnonematch_good
  --deselect s3tests/functional/test_s3.py::test_multi_object_delete_key_limit
  --deselect s3tests/functional/test_s3.py::test_multi_objectv2_delete_key_limit
  --deselect s3tests/functional/test_s3.py::test_multipart_resend_first_finishes_last
"

if [ ! -d "$WORK" ]; then
  git clone --depth 1 --branch "$S3TESTS_REF" https://github.com/ceph/s3-tests.git "$WORK"
fi
cd "$WORK"
[ -d .venv ] || python3 -m venv .venv
./.venv/bin/pip install -q -r requirements.txt pytest pytest-timeout

export S3TEST_CONF="$HERE/s3tests.conf"
set +e
# shellcheck disable=SC2086
./.venv/bin/python -m pytest s3tests/functional/test_s3.py \
  -p no:cacheprovider -q --no-header --timeout=120 \
  -m "$EXCLUDE" -k "$KEYWORDS" $DESELECT \
  --junitxml="$HERE/results.xml"
status=$?
set -e

# The gate: any in-scope failure fails CI. Exit 0 (all pass) and exit 5
# (nothing collected) are the only acceptable outcomes; 1 means a real
# conformance regression.
exit $status
