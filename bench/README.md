# Upload benchmarks

`upload-bench.sh` times 1 GiB uploads through both front doors (S3 and the
native API) from a cluster node, and anchors the numbers to what the
hardware itself measures — sequential disk write (`--dd`) and the inter-node
link (`--iperf-server`) — rather than to an instance-type label.

## Procedure

Reference hardware: 3 identical **2 vCPU / 4 GiB** instances (Scaleway
`PLAY2-NANO`, block-storage root volume), Ubuntu Noble, same zone.

1. Deploy the `nauka` binary, create S3 credentials, install AWS CLI v2 and
   `iperf3` on the entry node (`iperf3 -s` on one peer for the link floor).
2. **Single-node phase**: one node running alone (its own 1-node cluster).
   Run the script on it. This is the disk-and-encode floor.
3. **Cluster phase**: the full 3-node cluster. Run the script on one node.
   The delta against the network floor is the pipeline's overhead.

The script regenerates the random payload before every run — Nauka is
content-addressed, so re-uploading identical bytes dedups into a no-op and
would measure nothing. The payload lives in `/dev/shm` so reading it does
not compete with the data volume the upload path stages to.

```
./upload-bench.sh --size-mb 1024 --runs 3 \
    --access-key KEY --secret-key SECRET \
    --dd --iperf-server <peer-ip>
```

## Baselines — 2026-08-11, pre-streaming-ingestion

Measured on 3× PLAY2-NANO (2 vCPU / 4 GiB, 40 GB sbs root volume,
fr-par-1), medians of 3 runs, 1 GiB random payload. Hardware floors from
the same session: **disk 84.5 MB/s** sequential write (direct I/O),
**link ~720 Mbit/s** (iperf3).

| Setup | S3 PutObject | native /api/upload | profile (S3 runs) |
|---|---|---|---|
| single node | 34.28 s | 30.93 s | iowait ~45-48%, cpu bursts 140-146% |
| 3-node cluster | 45.67 s | 46.61 s | iowait ~21-46%, cpu bursts 150-160% |

The cost model these numbers confirm: the current path writes ~2.5× the
payload to disk (staging + shards), serialized with encode — at 84.5 MB/s
that is ~30 s alone — and the cluster adds the remote ⅔ of the shards over
the link (~1.0 GiB at 720 Mbit/s ≈ 11.4 s), also serialized. Measured
delta: +11.4 s. The same model reproduces the numbers that motivated the
streaming work (16.1 s / 24.3 s on a ~165 MB/s disk with a ~500 Mbit/s
link), so targets must be stated **relative to measured floors**, not in
absolute seconds: on this hardware, floor ≈ 12-13 s for `encoded` and
≈ 12.7 s (one fsynced payload write) for `local`.

Worth knowing: between the last body byte and the 200, today's server is
silent for the whole encode + fan-out window — long enough on this
hardware to trip the AWS CLI's default 60 s read timeout in cluster mode.
The bench passes `--cli-read-timeout 600`; real clients would need the
same workaround until acks overlap reception.

## Ack policies — 2026-08-11, post-streaming

Same 3-node cluster (PLAY2-MICRO, 4 vCPU / 8 GiB), same 1 GiB payloads,
alternating buckets: `encoded` (default) and `local` (bucket tagged
`nauka:ack=local`).

| run | encoded | local |
|---|---|---|
| 1 | 18.11 s | 10.73 s |
| 2 | 35.79 s | 11.42 s |
| 3 | 35.48 s | 11.28 s |

`local` is steady at ~11 s. The honest comparison is against run 1's
**18.1 s** — the only `encoded` run not competing with a background
drain. Runs 2 and 3 nearly doubled because the drains of the preceding
`local` uploads were still dispersing: **the mode's latency gain is partly
borrowed from concurrent uploads**, which the feature spec did not
anticipate. On a node serving a mixed workload, that redistribution
matters more than the headline number.

`local` does not reach the spec's ~7 s (one payload write at 170 MB/s
would be 6.3 s) because at this point the path is no longer disk-bound:
SigV4, MD5, BLAKE3 and the fsync dominate.

Verified alongside: all drains finished `degraded=0`, 3.1 GB of shards on
each of the three nodes, no staged file left, 1 GiB GET byte-identical.
