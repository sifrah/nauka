#!/usr/bin/env python3
"""Distributed read-consistency gate — the blind spot single-node CI can't see.

The ceph/s3-tests suite runs against ONE node, where the Raft log applies
instantly and there is no replication lag. This test drives a real
multi-node loopback cluster and asserts the consistency contract that only
exists across nodes:

  1. read-after-write   — PUT on one node, immediate GET on another, exact bytes
  2. list-after-write   — a fresh key is in the very next LIST on another node
  3. delete-after-write — a deleted key is gone from the next LIST
  4. genuine 404        — an absent key is a prompt NoSuchKey, not a hang
  5. SlowDown-not-lies  — a node made to fall behind (SIGSTOP while the log
                          advances, then SIGCONT) answers 503 SlowDown for a
                          just-written key until it catches up, and NEVER a
                          false NoSuchKey. This is the honesty guarantee for a
                          node healing after a fault.

Scope: this gates read-after-write for NEW keys (the guarantee the miss-path
catch-up provides). Read-after-write for an OVERWRITE of an existing key is a
known remaining gap — a present-but-stale local version is served without a
catch-up, since a node cannot tell locally that a newer version exists without
asking the leader on every GET (the linearizable-read tax we deliberately
avoid). Keys here are therefore made unique per run. Overwrite consistency is
future work, tracked alongside the same ReadIndex-per-read that full LIST
linearizability would need.

Exit non-zero on any violation so CI fails.

Usage:
  distributed_semantics.py --ports P1,P2,P3 --ak KEY --sk SECRET
                           --freeze-pid PID --probe-port PORT
where the S3 endpoints are on 127.0.0.1:P1.. , PID is a FOLLOWER's process
(never the leader — freezing the leader is a different scenario), and
PROBE-PORT is that follower's S3 port.
"""
import argparse, hashlib, os, signal, sys, time
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

BUCKET = "distsem"
# Unique per run: every key is new, so we gate new-object read-after-write
# rather than accidentally testing overwrite consistency (a known gap).
PREFIX = f"r{os.getpid()}-{int.from_bytes(os.urandom(3), 'big')}/"
FAILS = []


def fail(msg):
    print(f"  FAIL: {msg}")
    FAILS.append(msg)


def ok(msg):
    print(f"  ok: {msg}")


def client(port, ak, sk, read_timeout=15):
    return boto3.client(
        "s3", endpoint_url=f"http://127.0.0.1:{port}",
        aws_access_key_id=ak, aws_secret_access_key=sk, region_name="us-east-1",
        config=Config(connect_timeout=5, read_timeout=read_timeout,
                      retries={"max_attempts": 0}, s3={"addressing_style": "path"}))


def code_of(e):
    return e.response.get("Error", {}).get("Code") if isinstance(e, ClientError) else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ports", required=True)
    ap.add_argument("--ak", required=True)
    ap.add_argument("--sk", required=True)
    ap.add_argument("--freeze-pid", type=int, required=True)
    ap.add_argument("--probe-port", type=int, required=True)
    a = ap.parse_args()
    ports = [int(p) for p in a.ports.split(",")]
    cls = [client(p, a.ak, a.sk) for p in ports]

    for c in cls:
        try:
            c.create_bucket(Bucket=BUCKET)
            break
        except ClientError as e:
            if code_of(e) in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
                break
    time.sleep(2)  # let the create apply everywhere

    lst_prefix = PREFIX + "lst/"

    # 1. read-after-write, cross-node
    print("[1] read-after-write cross-node")
    n = len(FAILS)
    for i in range(40):
        body = os.urandom(8192); sha = hashlib.sha256(body).hexdigest(); k = f"{PREFIX}raw/{i}"
        cls[0].put_object(Bucket=BUCKET, Key=k, Body=body)
        reader = cls[1 + (i % (len(cls) - 1))]  # any node but the writer
        try:
            got = reader.get_object(Bucket=BUCKET, Key=k)["Body"].read()
            if hashlib.sha256(got).hexdigest() != sha:
                fail(f"raw/{i}: bytes differ after cross-node GET")
        except ClientError as e:
            fail(f"raw/{i}: {code_of(e)} on immediate cross-node GET")
    if len(FAILS) == n:
        ok("40/40 immediate cross-node GETs returned the exact object")

    # 2. list-after-write, cross-node
    print("[2] list-after-write cross-node")
    n = len(FAILS)
    for i in range(30):
        k = f"{lst_prefix}{i:03d}"
        cls[0].put_object(Bucket=BUCKET, Key=k, Body=b"x")
        reader = cls[1 + (i % (len(cls) - 1))]
        keys = {o["Key"] for o in reader.list_objects_v2(
            Bucket=BUCKET, Prefix=lst_prefix).get("Contents", [])}
        if k not in keys:
            fail(f"lst/{i:03d} missing from the next cross-node LIST")
    if len(FAILS) == n:
        ok("30/30 fresh keys present in the immediate cross-node LIST")

    # 3. delete-after-write
    print("[3] delete-after-write")
    cls[0].delete_object(Bucket=BUCKET, Key=f"{lst_prefix}000")
    time.sleep(0.5)
    keys = {o["Key"] for o in cls[-1].list_objects_v2(
        Bucket=BUCKET, Prefix=lst_prefix).get("Contents", [])}
    if f"{lst_prefix}000" in keys:
        fail("deleted key still appears in a cross-node LIST")
    else:
        ok("deleted key gone from the next cross-node LIST")

    # 4. genuine 404 is prompt
    print("[4] genuine 404")
    t0 = time.time()
    try:
        cls[1].get_object(Bucket=BUCKET, Key=f"{PREFIX}was-never-written")
        fail("absent key did not 404")
    except ClientError as e:
        dt = (time.time() - t0) * 1000
        if code_of(e) != "NoSuchKey":
            fail(f"absent key returned {code_of(e)}, expected NoSuchKey")
        elif dt > 3000:
            fail(f"genuine 404 took {dt:.0f}ms (>3s) — should be one round-trip")
        else:
            ok(f"absent key → NoSuchKey in {dt:.0f}ms")

    # 5. a lagging node answers SlowDown, never a false NoSuchKey
    print("[5] SlowDown-not-lies on a healing node")
    probe = client(a.probe_port, a.ak, a.sk, read_timeout=8)
    # Write through ANY node but the one we're about to freeze — otherwise
    # every advance-write hits the frozen process and the log never moves.
    # (The frozen node is a follower, but it may be ports[0]: the leader can
    # be any of the three.)
    writer_port = next(p for p in ports if p != a.probe_port)
    writer = client(writer_port, a.ak, a.sk, read_timeout=8)
    os.kill(a.freeze_pid, signal.SIGSTOP)         # freeze the follower
    try:
        # A frozen node also holds shards, so the first writes stall until
        # the health pinger routes placement around it (~8s). Wait it out,
        # then advance the log well past what the frozen node has applied.
        # Metadata still commits: leader + the third node are a quorum.
        time.sleep(12)
        advanced = 0
        for i in range(60):
            try:
                writer.put_object(Bucket=BUCKET, Key=f"{PREFIX}adv/{i}", Body=b"y" * 64)
                advanced += 1
            except Exception:
                pass
        for _ in range(10):                       # ensure the probe key commits
            try:
                writer.put_object(Bucket=BUCKET, Key=f"{PREFIX}probe-key", Body=b"PROBE")
                break
            except Exception:
                time.sleep(1)
        print(f"  advanced the log by {advanced} entries while the node was frozen")
    finally:
        os.kill(a.freeze_pid, signal.SIGCONT)     # thaw; it must now catch up

    false_404 = saw_slowdown = 0; served = False
    t0 = time.time()
    while time.time() - t0 < 20:
        try:
            probe.get_object(Bucket=BUCKET, Key=f"{PREFIX}probe-key")
            served = True; break
        except ClientError as e:
            c = code_of(e)
            if c in ("NoSuchKey", "404"):
                false_404 += 1
            elif c in ("SlowDown", "ServiceUnavailable", "503"):
                saw_slowdown += 1
        except Exception:
            pass
        time.sleep(0.1)
    if false_404:
        fail(f"healing node answered a FALSE NoSuchKey {false_404}x for an acked object")
    if not served:
        fail("healing node never served the acked object within 20s")
    if false_404 == 0 and served:
        ok(f"healing node: {saw_slowdown} SlowDown then served, 0 false NoSuchKey")

    print()
    if FAILS:
        print(f"DISTRIBUTED SEMANTICS: FAIL ({len(FAILS)} violation(s))")
        sys.exit(1)
    print("DISTRIBUTED SEMANTICS: PASS")


if __name__ == "__main__":
    main()
