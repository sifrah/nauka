#!/usr/bin/env bash
# Upload benchmark — runs ON a cluster node, against its own front doors.
#
# Times 1 GiB uploads through both doors (S3 and the native API), several
# runs each, and reports medians plus a CPU/iowait profile sampled during
# each run. Also measures what the hardware itself can do (sequential disk
# write, and the link to a peer when asked), so a result is anchored to
# measured hardware rather than to an instance-type label.
#
# Two deliberate choices, both about honesty of measurement:
#
#   - The payload lives in /dev/shm and is REGENERATED for every run.
#     Nauka is content-addressed: uploading the same bytes twice dedups
#     into a no-op and the second run would measure nothing. Fresh random
#     bytes per run, object deleted after timing.
#
#   - The payload is read from RAM, not from the data volume. The upload
#     path stages to the data volume; a payload read from that same volume
#     would halve the apparent disk budget and corrupt the comparison
#     against the disk floor.
#
# Usage:
#   upload-bench.sh --size-mb 1024 --runs 3 \
#     --access-key KEY --secret-key SECRET \
#     [--s3 http://127.0.0.1:8333] [--api http://127.0.0.1:8080] \
#     [--bucket benchbucket] [--data-dir /var/lib/nauka] \
#     [--dd] [--iperf-server IP]
set -euo pipefail

S3_EP="http://127.0.0.1:8333"
API_EP="http://127.0.0.1:8080"
BUCKET="benchbucket"
DATA_DIR="/var/lib/nauka"
SIZE_MB=1024
RUNS=3
DO_DD=0
IPERF_SERVER=""
ACCESS_KEY=""
SECRET_KEY=""

while [ $# -gt 0 ]; do
  case "$1" in
    --s3) S3_EP="$2"; shift 2;;
    --api) API_EP="$2"; shift 2;;
    --bucket) BUCKET="$2"; shift 2;;
    --data-dir) DATA_DIR="$2"; shift 2;;
    --size-mb) SIZE_MB="$2"; shift 2;;
    --runs) RUNS="$2"; shift 2;;
    --access-key) ACCESS_KEY="$2"; shift 2;;
    --secret-key) SECRET_KEY="$2"; shift 2;;
    --dd) DO_DD=1; shift;;
    --iperf-server) IPERF_SERVER="$2"; shift 2;;
    *) echo "unknown arg: $1" >&2; exit 2;;
  esac
done
[ -n "$ACCESS_KEY" ] && [ -n "$SECRET_KEY" ] || { echo "--access-key/--secret-key required" >&2; exit 2; }

export AWS_ACCESS_KEY_ID="$ACCESS_KEY" AWS_SECRET_ACCESS_KEY="$SECRET_KEY"
export AWS_DEFAULT_REGION=us-east-1 AWS_EC2_METADATA_DISABLED=true
PAYLOAD=/dev/shm/bench-payload.bin

# ── hardware floors ──────────────────────────────────────────────────────
if [ "$DO_DD" = 1 ]; then
  echo "## disk: sequential write, direct I/O, ${SIZE_MB} MiB into $DATA_DIR"
  dd if=/dev/zero of="$DATA_DIR/bench-dd.tmp" bs=1M count="$SIZE_MB" \
     oflag=direct conv=fsync 2>&1 | tail -1
  rm -f "$DATA_DIR/bench-dd.tmp"
fi
if [ -n "$IPERF_SERVER" ]; then
  echo "## link: iperf3 to $IPERF_SERVER"
  iperf3 -c "$IPERF_SERVER" -t 5 2>/dev/null | grep -E "sender|receiver" || \
    echo "iperf3 unavailable (server not running or binary missing)"
fi

# ── /proc/stat sampler: avg iowait%, avg+max cpu% during a window ────────
# No command substitution anywhere near the background job: a `$( )` whose
# child keeps the substitution pipe open hangs the caller forever, and an
# orphaned sampler outlives a script killed by `set -e`. The pid lives in a
# global and an EXIT trap reaps it whatever happens.
SAMPLES=/dev/shm/bench-cpu.samples
SAMPLER_PID=""
trap '[ -n "$SAMPLER_PID" ] && kill "$SAMPLER_PID" 2>/dev/null; rm -f "$PAYLOAD" "$SAMPLES"' EXIT
sample_start() {
  : > "$SAMPLES"
  ( while :; do
      awk '/^cpu /{print $2+$3+$4+$7+$8, $5, $6}' /proc/stat >> "$SAMPLES"
      sleep 1
    done ) >/dev/null 2>&1 &
  SAMPLER_PID=$!
}
sample_stop() { # prints "iowait_avg% cpu_avg% cpu_max%" (cpu in per-core %)
  kill "$SAMPLER_PID" 2>/dev/null || true
  wait "$SAMPLER_PID" 2>/dev/null || true
  SAMPLER_PID=""
  # iowait is % of total machine time (vmstat convention); cpu is scaled by
  # the core count so a burst reads top-style (200% = two cores busy).
  python3 - "$SAMPLES" <<'EOF'
import os, sys
rows = [tuple(map(int, l.split())) for l in open(sys.argv[1]) if l.split()]
if len(rows) < 2: print("n/a"); raise SystemExit
ncpu = os.cpu_count() or 1
deltas = []
for (b1,i1,w1),(b2,i2,w2) in zip(rows, rows[1:]):
    busy, idle, wait = b2-b1, i2-i1, w2-w1
    total = busy + idle + wait
    if total > 0: deltas.append((100*wait/total, 100*busy/total*ncpu))
if not deltas: print("n/a"); raise SystemExit
iow = sum(d[0] for d in deltas)/len(deltas)
cav = sum(d[1] for d in deltas)/len(deltas)
cmx = max(d[1] for d in deltas)
print(f"iowait_avg={iow:.0f}% cpu_avg={cav:.0f}% cpu_max={cmx:.0f}%")
EOF
}

median() { python3 -c "
import sys, statistics
print(f'{statistics.median([float(x) for x in sys.argv[1:]]):.2f}')" "$@"; }

now() { date +%s.%N; }

# Pre-flight: every upload leaves ~1.5× its size in shards on the data
# volume, and a deleted object's shards only go away after the GC's 1-hour
# orphan grace — successive runs ACCUMULATE. An 8 GB root volume dies on
# run 3 of a 1 GiB bench (measured: ENOSPC surfacing as a bare 500).
NEED_MB=$((RUNS * 2 * SIZE_MB * 3 / 2 + SIZE_MB))
AVAIL_MB=$(df -m --output=avail "$DATA_DIR" | tail -1 | tr -d ' ')
if [ "$AVAIL_MB" -lt "$NEED_MB" ]; then
  echo "ABORT: $DATA_DIR has ${AVAIL_MB} MB free, this bench needs ~${NEED_MB} MB" >&2
  echo "(shards of deleted objects persist for the GC grace period)" >&2
  exit 3
fi

aws --endpoint-url "$S3_EP" s3api create-bucket --bucket "$BUCKET" >/dev/null 2>&1 || true

# ── the runs ─────────────────────────────────────────────────────────────
declare -a S3_TIMES API_TIMES
for run in $(seq 1 "$RUNS"); do
  head -c $((SIZE_MB * 1024 * 1024)) /dev/urandom > "$PAYLOAD"

  sample_start
  T0=$(now)
  aws --cli-read-timeout 600 --endpoint-url "$S3_EP" s3api put-object \
      --bucket "$BUCKET" --key "bench-$run" --body "$PAYLOAD" >/dev/null
  T1=$(now)
  PROF=$(sample_stop)
  S3_T=$(python3 -c "print(f'{$T1-$T0:.2f}')")
  S3_TIMES+=("$S3_T")
  echo "run $run  s3=${S3_T}s  $PROF"
  aws --endpoint-url "$S3_EP" s3api delete-object \
      --bucket "$BUCKET" --key "bench-$run" >/dev/null

  head -c $((SIZE_MB * 1024 * 1024)) /dev/urandom > "$PAYLOAD"

  sample_start
  T0=$(now)
  HASH=$(curl -sS -T "$PAYLOAD" -X POST \
      -H 'Content-Type: application/octet-stream' \
      "$API_EP/api/upload" | python3 -c "import sys,json; print(json.load(sys.stdin)['hash'])")
  T1=$(now)
  PROF=$(sample_stop)
  API_T=$(python3 -c "print(f'{$T1-$T0:.2f}')")
  API_TIMES+=("$API_T")
  echo "run $run  api=${API_T}s  $PROF"
  curl -sS -X DELETE "$API_EP/f/$HASH" >/dev/null || true
done

echo
echo "## medians over $RUNS runs, ${SIZE_MB} MiB payload"
echo "s3_put_median_s=$(median "${S3_TIMES[@]}")"
echo "api_put_median_s=$(median "${API_TIMES[@]}")"
