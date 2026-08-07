# S3 conformance

Nauka's S3 endpoint is measured, not asserted. The `ceph/s3-tests` suite —
the de-facto conformance suite of the S3 ecosystem, the same one Garage and
Ceph RGW report against — runs in CI on every change.

## Scope

The target is AWS's own S3 surface with self-hosted semantics. Some of the
suite exercises things that only exist inside AWS (IAM accounts, STS, KMS,
Glacier transitions, S3 Select) or that Nauka has deliberately not built
yet. Those are excluded by pytest marker in `run.sh`, and each exclusion is
justified in `EXCLUSIONS.md`. Everything else must pass.

## Running it locally

```sh
./conformance/run.sh            # against a node on 127.0.0.1:8000
```

The script starts nothing: point it at a running node whose S3 endpoint is
on port 8000, with the fixed credentials the suite expects (see run.sh).
