#!/usr/bin/env python3
"""
Setup test infrastructure on a Nauka cluster via the REST API.

Usage:
    python3 tests/setup-infra-api.py <api-url> [options]

Options:
    --orgs <n>             Number of organizations (default: 1)
    --projects <n>         Projects per org (default: 1)
    --envs <n>             Environments per project (default: 1)
    --vpcs <n>             VPCs per org (default: 1)
    --subnets <n>          Subnets per VPC (default: 1)
    --vms <n>              VMs per subnet (default: 1)
    --natgw                Create a NAT gateway on each VPC
    --peering <topology>   Peering topology: none, hub, full, chain (default: none)
    --image <name>         OS image (default: ubuntu-24.04)
    --cpu <n>              vCPUs per VM (default: 1)
    --memory <n>           Memory in MB (default: 512)
    --disk <n>             Disk in GB (default: 5)
    --reconcile            SSH to host and run forge reconcile after setup
    --destroy              Destroy everything and exit
    --dry-run              Print what would be created
    --parallel <n>         Max parallel API requests (default: 10)
    --wait                 Wait for VMs to reach Running state after creation
    --retries <n>          Max retries on transient failures (default: 3)

Examples:
    # API URL from a Hetzner server
    python3 tests/setup-infra-api.py http://[fd83:bd60:ceb0:23fb:40ba:384e:4cce:ffb1]:8443

    # 120 orgs, 1 VPC each, 1 VM each
    python3 tests/setup-infra-api.py http://[...]:8443 --orgs 120 --vms 1

    # Hub-and-spoke with NAT
    python3 tests/setup-infra-api.py http://[...]:8443 --vpcs 4 --natgw --peering hub
"""

import argparse
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

# ── API client ───────────────────────────────────────────────────────────────

class NaukaAPI:
    def __init__(self, base_url, dry_run=False, retries=3):
        self.base = base_url.rstrip("/") + "/admin/v1"
        self.dry_run = dry_run
        self.retries = retries
        self.stats = {"created": 0, "failed": 0, "skipped": 0}

    def _request(self, method, path, data=None):
        url = f"{self.base}{path}"
        if self.dry_run:
            print(f"  [dry-run] {method} {path} {json.dumps(data) if data else ''}")
            return {"id": "dry-run-id", "name": "dry-run"}
        body = json.dumps(data).encode() if data else None

        last_err = None
        for attempt in range(self.retries):
            req = Request(url, data=body, method=method)
            req.add_header("Content-Type", "application/json")
            try:
                with urlopen(req, timeout=30) as resp:
                    raw = resp.read()
                    return json.loads(raw) if raw else {}
            except HTTPError as e:
                err_body = e.read().decode()
                if e.code == 409 or "already exists" in err_body:
                    self.stats["skipped"] += 1
                    return None
                # 4xx (except 409) are not retriable
                if 400 <= e.code < 500:
                    self.stats["failed"] += 1
                    print(f"  [error] {method} {path}: {e.code} {err_body[:300]}", file=sys.stderr)
                    return None
                # 5xx — retry
                last_err = f"{e.code} {err_body[:200]}"
            except (URLError, OSError) as e:
                last_err = str(e)

            if attempt < self.retries - 1:
                delay = (attempt + 1) * 2  # 2s, 4s
                time.sleep(delay)

        self.stats["failed"] += 1
        print(f"  [error] {method} {path}: {last_err} (after {self.retries} attempts)", file=sys.stderr)
        return None

    def get(self, path):
        return self._request("GET", path)

    def post(self, path, data):
        result = self._request("POST", path, data)
        if result:
            self.stats["created"] += 1
        return result

    def action(self, path, data=None):
        """POST an action (stop, start) without counting as 'created'."""
        return self._request("POST", path, data or {})

    def delete(self, path):
        return self._request("DELETE", path)

    def list_all(self, path):
        items = []
        page = 1
        while True:
            sep = "&" if "?" in path else "?"
            result = self.get(f"{path}{sep}page={page}&per_page=100")
            if not result or "data" not in result:
                break
            items.extend(result["data"])
            pagination = result.get("pagination", {})
            if page >= pagination.get("total_pages", 1):
                break
            page += 1
        return items

# ── naming ───────────────────────────────────────────────────────────────────

ORG_POOL = ["acme", "globex", "initech", "umbrella", "cyberdyne"]
PROJECT_POOL = ["backend", "frontend", "platform", "data", "infra"]
ENV_POOL = ["production", "staging", "dev", "qa", "sandbox"]
VPC_POOL = ["prod-net", "dev-net", "staging-net", "shared-net", "mgmt-net"]
SUBNET_POOL = ["web", "api", "workers", "data", "mgmt"]
VM_POOL = ["app", "web", "worker", "cache", "db"]

def pick_name(pool, idx):
    if idx < len(pool):
        return pool[idx]
    return f"{pool[0]}-{idx + 1}"

# ── CIDR generation ──────────────────────────────────────────────────────────

MAX_VPC_IDX = 255
MAX_SUBNET_IDX = 253  # .1-.254 (reserve .0 network and .255 broadcast)

def vpc_cidr(vpc_idx):
    if vpc_idx > MAX_VPC_IDX:
        raise ValueError(f"too many VPCs: max {MAX_VPC_IDX + 1} (10.0-255.0.0/16)")
    return f"10.{vpc_idx}.0.0/16"

def subnet_cidr(vpc_idx, sub_idx):
    if sub_idx > MAX_SUBNET_IDX:
        raise ValueError(f"too many subnets per VPC: max {MAX_SUBNET_IDX + 1}")
    return f"10.{vpc_idx}.{sub_idx + 1}.0/24"

# ── destroy ──────────────────────────────────────────────────────────────────

def destroy(api):
    print("\nDestroying all resources...")

    # List all orgs
    orgs = api.list_all("/orgs")
    print(f"  Found {len(orgs)} org(s)")

    for org in orgs:
        oid = org["id"]
        oname = org["name"]

        # Delete VMs
        vms = api.list_all(f"/orgs/{oid}/vms")
        for vm in vms:
            # Stop then delete
            api.action(f"/orgs/{oid}/vms/{vm['id']}/stop")
            api.delete(f"/orgs/{oid}/vms/{vm['id']}")

        # Delete peerings
        vpcs = api.list_all(f"/orgs/{oid}/vpcs")
        for vpc in vpcs:
            peerings = api.list_all(f"/orgs/{oid}/vpcs/{vpc['id']}/peerings")
            for p in peerings:
                api.delete(f"/orgs/{oid}/vpcs/{vpc['id']}/peerings/{p['id']}")

        # Delete NAT gateways
        for vpc in vpcs:
            natgws = api.list_all(f"/orgs/{oid}/vpcs/{vpc['id']}/nat-gateways")
            for n in natgws:
                api.delete(f"/orgs/{oid}/vpcs/{vpc['id']}/nat-gateways/{n['id']}")

        # Delete subnets then VPCs
        for vpc in vpcs:
            subs = api.list_all(f"/orgs/{oid}/vpcs/{vpc['id']}/subnets")
            for s in subs:
                api.delete(f"/orgs/{oid}/vpcs/{vpc['id']}/subnets/{s['id']}")
            api.delete(f"/orgs/{oid}/vpcs/{vpc['id']}")

        # Delete envs, projects
        projects = api.list_all(f"/orgs/{oid}/projects")
        for proj in projects:
            envs = api.list_all(f"/orgs/{oid}/projects/{proj['id']}/environments")
            for env in envs:
                api.delete(f"/orgs/{oid}/projects/{proj['id']}/environments/{env['id']}")
            api.delete(f"/orgs/{oid}/projects/{proj['id']}")

        # Delete org
        api.delete(f"/orgs/{oid}")
        print(f"  Deleted org '{oname}'")

    print("Done.")

# ── setup ────────────────────────────────────────────────────────────────────

def setup(api, args):
    total_vpcs = args.orgs * args.vpcs
    total_vms = args.orgs * args.vpcs * args.subnets * args.vms

    print(f"""
============================================
  Nauka Infrastructure Setup (API)
  Target: {args.api_url}
============================================
  Orgs:      {args.orgs}
  Projects:  {args.projects} per org
  Envs:      {args.envs} per project
  VPCs:      {args.vpcs} per org ({total_vpcs} total)
  Subnets:   {args.subnets} per VPC
  VMs:       {args.vms} per subnet ({total_vms} total)
  NAT GW:    {args.natgw}
  Peering:   {args.peering}
  Image:     {args.image} ({args.cpu} vCPU, {args.memory}MB, {args.disk}GB)
  Parallel:  {args.parallel}
============================================
""")

    phase_times = {}

    # ── orgs (parallel) ──
    t_phase = time.time()
    print(f"Creating {args.orgs} org(s)...")
    org_ids = {}
    with ThreadPoolExecutor(max_workers=args.parallel) as pool:
        futures = {}
        for i in range(args.orgs):
            name = pick_name(ORG_POOL, i)
            f = pool.submit(api.post, "/orgs", {"name": name})
            futures[f] = name
        for f in as_completed(futures):
            name = futures[f]
            result = f.result()
            if result:
                org_ids[name] = result["id"]

    phase_times["orgs"] = time.time() - t_phase
    print(f"  {len(org_ids)} org(s) created ({phase_times['orgs']:.1f}s)")

    # ── projects + envs (parallel per org) ──
    t_phase = time.time()
    print(f"Creating projects & environments...")
    proj_ids = {}  # (org_name, proj_name) -> id
    env_ids = {}   # (org_name, proj_name, env_name) -> id

    with ThreadPoolExecutor(max_workers=args.parallel) as pool:
        futures = {}
        for org_name, oid in org_ids.items():
            for pi in range(args.projects):
                pname = pick_name(PROJECT_POOL, pi)
                f = pool.submit(api.post, f"/orgs/{oid}/projects", {"name": pname})
                futures[f] = (org_name, pname)
        for f in as_completed(futures):
            key = futures[f]
            result = f.result()
            if result:
                proj_ids[key] = result["id"]

    with ThreadPoolExecutor(max_workers=args.parallel) as pool:
        futures = {}
        for (org_name, pname), pid in proj_ids.items():
            oid = org_ids[org_name]
            for ei in range(args.envs):
                ename = pick_name(ENV_POOL, ei)
                f = pool.submit(
                    api.post,
                    f"/orgs/{oid}/projects/{pid}/environments",
                    {"name": ename},
                )
                futures[f] = (org_name, pname, ename)
        for f in as_completed(futures):
            key = futures[f]
            result = f.result()
            if result:
                env_ids[key] = result["id"]

    phase_times["projects"] = time.time() - t_phase
    print(f"  {len(proj_ids)} project(s), {len(env_ids)} env(s) created ({phase_times['projects']:.1f}s)")

    # ── VPCs + subnets (parallel) ──
    t_phase = time.time()
    print(f"Creating {total_vpcs} VPC(s) with subnets...")
    vpc_ids = {}   # (org_name, vpc_name) -> id
    sub_ids = {}   # (org_name, vpc_name, sub_name) -> id

    with ThreadPoolExecutor(max_workers=args.parallel) as pool:
        futures = {}
        for org_name, oid in org_ids.items():
            for vi in range(args.vpcs):
                vname = pick_name(VPC_POOL, vi)
                cidr = vpc_cidr(vi)
                f = pool.submit(
                    api.post,
                    f"/orgs/{oid}/vpcs",
                    {"name": vname, "cidr": cidr},
                )
                futures[f] = (org_name, vname)
        for f in as_completed(futures):
            key = futures[f]
            result = f.result()
            if result:
                vpc_ids[key] = result["id"]

    # Subnets (must wait for VPCs)
    # Build a stable vpc_name -> vpc_idx map for CIDR generation
    vpc_name_to_idx = {}
    for org_name, oid in org_ids.items():
        for vi in range(args.vpcs):
            vname = pick_name(VPC_POOL, vi)
            vpc_name_to_idx[(org_name, vname)] = vi

    with ThreadPoolExecutor(max_workers=args.parallel) as pool:
        futures = {}
        for (org_name, vname), vid in vpc_ids.items():
            oid = org_ids[org_name]
            vi = vpc_name_to_idx.get((org_name, vname), 0)
            for si in range(args.subnets):
                sname = pick_name(SUBNET_POOL, si)
                scidr = subnet_cidr(vi, si)
                f = pool.submit(
                    api.post,
                    f"/orgs/{oid}/vpcs/{vid}/subnets",
                    {"name": sname, "cidr": scidr},
                )
                futures[f] = (org_name, vname, sname)
        for f in as_completed(futures):
            key = futures[f]
            result = f.result()
            if result:
                sub_ids[key] = result["id"]

    phase_times["vpcs"] = time.time() - t_phase
    print(f"  {len(vpc_ids)} VPC(s), {len(sub_ids)} subnet(s) created ({phase_times['vpcs']:.1f}s)")

    # ── NAT gateways ──
    if args.natgw:
        t_phase = time.time()
        hub_only = args.peering == "hub"
        label = "hub VPC only" if hub_only else "all VPCs"
        print(f"Creating NAT gateways ({label})...")
        with ThreadPoolExecutor(max_workers=args.parallel) as pool:
            futures = []
            for org_name, oid in org_ids.items():
                for vi in range(args.vpcs):
                    if hub_only and vi > 0:
                        break
                    vname = pick_name(VPC_POOL, vi)
                    key = (org_name, vname)
                    if key not in vpc_ids:
                        continue
                    vid = vpc_ids[key]
                    ngw_name = f"egress-{vname}"
                    f = pool.submit(
                        api.post,
                        f"/orgs/{oid}/vpcs/{vid}/nat-gateways",
                        {"name": ngw_name},
                    )
                    futures.append(f)
            for f in as_completed(futures):
                f.result()
        phase_times["natgw"] = time.time() - t_phase
        print(f"  NAT gateways created ({phase_times['natgw']:.1f}s)")

    # ── VMs (parallel) ──
    t_phase = time.time()
    print(f"Creating {total_vms} VM(s)...")
    vm_counter = 0
    with ThreadPoolExecutor(max_workers=args.parallel) as pool:
        futures = []
        for org_name, oid in org_ids.items():
            # Use first project/env
            first_proj = pick_name(PROJECT_POOL, 0)
            first_env = pick_name(ENV_POOL, 0)
            for vi in range(args.vpcs):
                vname = pick_name(VPC_POOL, vi)
                for si in range(args.subnets):
                    sname = pick_name(SUBNET_POOL, si)
                    for mi in range(args.vms):
                        vm_counter += 1
                        prefix = pick_name(VM_POOL, mi)
                        vm_name = f"{prefix}-{vm_counter}"
                        f = pool.submit(
                            api.post,
                            f"/orgs/{oid}/vms",
                            {
                                "name": vm_name,
                                "project": first_proj,
                                "env": first_env,
                                "vpc": vname,
                                "subnet": sname,
                                "cpu": str(args.cpu),
                                "memory": str(args.memory),
                                "disk": str(args.disk),
                                "image": args.image,
                            },
                        )
                        futures.append(f)
        for f in as_completed(futures):
            f.result()

    phase_times["vms"] = time.time() - t_phase
    print(f"  VMs submitted ({phase_times['vms']:.1f}s)")

    # ── peering (parallel) ──
    if args.peering != "none" and args.vpcs > 1:
        t_phase = time.time()
        print(f"Creating peering ({args.peering} topology)...")
        peering_calls = []  # list of (oid, vid, peer_vpc_name)
        for org_name, oid in org_ids.items():
            vpc_list = [
                (pick_name(VPC_POOL, vi), vpc_ids.get((org_name, pick_name(VPC_POOL, vi))))
                for vi in range(args.vpcs)
            ]
            vpc_list = [(n, vid) for n, vid in vpc_list if vid]

            if args.peering == "hub" and len(vpc_list) > 1:
                hub_name, hub_id = vpc_list[0]
                for spoke_name, spoke_id in vpc_list[1:]:
                    peering_calls.append((oid, hub_id, spoke_name))

            elif args.peering == "full":
                for ai in range(len(vpc_list)):
                    for bi in range(ai + 1, len(vpc_list)):
                        _, vid_a = vpc_list[ai]
                        name_b = vpc_list[bi][0]
                        peering_calls.append((oid, vid_a, name_b))

            elif args.peering == "chain":
                for i in range(len(vpc_list) - 1):
                    _, vid_a = vpc_list[i]
                    name_b = vpc_list[i + 1][0]
                    peering_calls.append((oid, vid_a, name_b))

        with ThreadPoolExecutor(max_workers=args.parallel) as pool:
            futures = []
            for oid, vid, peer_name in peering_calls:
                f = pool.submit(
                    api.post,
                    f"/orgs/{oid}/vpcs/{vid}/peerings",
                    {"peer-vpc": peer_name},
                )
                futures.append(f)
            for f in as_completed(futures):
                f.result()

        phase_times["peering"] = time.time() - t_phase
        print(f"  {len(peering_calls)} peering(s) created ({phase_times['peering']:.1f}s)")

    # ── wait for VMs ──
    if args.wait and total_vms > 0 and not args.dry_run:
        t_phase = time.time()
        print("Waiting for VMs to reach Running state...")
        max_wait = 300  # 5 minutes
        poll_interval = 5
        deadline = time.time() + max_wait
        while time.time() < deadline:
            all_running = True
            pending = 0
            for org_name, oid in org_ids.items():
                vms = api.list_all(f"/orgs/{oid}/vms")
                for vm in vms:
                    state = vm.get("state", "unknown")
                    if state not in ("running", "Running"):
                        all_running = False
                        pending += 1
            if all_running:
                break
            print(f"  {pending} VM(s) still pending...")
            time.sleep(poll_interval)
        phase_times["wait"] = time.time() - t_phase
        if all_running:
            print(f"  All VMs running ({phase_times['wait']:.1f}s)")
        else:
            print(f"  Timeout: some VMs not running after {max_wait}s")

    # ── summary ──
    timing_lines = "".join(f"  {k:12s} {v:.1f}s\n" for k, v in phase_times.items())
    print(f"""
============================================
  Results
============================================
  Created:  {api.stats['created']}
  Failed:   {api.stats['failed']}
  Skipped:  {api.stats['skipped']}

  Timing:
{timing_lines}============================================
""")

# ── main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Setup Nauka test infrastructure via API")
    parser.add_argument("api_url", help="Nauka API URL (e.g. http://[...]:8443)")
    parser.add_argument("--orgs", type=int, default=1)
    parser.add_argument("--projects", type=int, default=1)
    parser.add_argument("--envs", type=int, default=1)
    parser.add_argument("--vpcs", type=int, default=1)
    parser.add_argument("--subnets", type=int, default=1)
    parser.add_argument("--vms", type=int, default=1)
    parser.add_argument("--natgw", action="store_true")
    parser.add_argument("--peering", default="none", choices=["none", "hub", "full", "chain"])
    parser.add_argument("--image", default="ubuntu-24.04")
    parser.add_argument("--cpu", type=int, default=1)
    parser.add_argument("--memory", type=int, default=512)
    parser.add_argument("--disk", type=int, default=5)
    parser.add_argument("--destroy", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--parallel", type=int, default=10)
    parser.add_argument("--wait", action="store_true", help="Wait for VMs to reach Running state")
    parser.add_argument("--retries", type=int, default=3, help="Max retries on transient failures")
    args = parser.parse_args()

    # Validate limits
    if args.vpcs > MAX_VPC_IDX + 1:
        print(f"Error: max {MAX_VPC_IDX + 1} VPCs per org (CIDR limit)", file=sys.stderr)
        sys.exit(1)
    if args.subnets > MAX_SUBNET_IDX + 1:
        print(f"Error: max {MAX_SUBNET_IDX + 1} subnets per VPC (CIDR limit)", file=sys.stderr)
        sys.exit(1)

    api = NaukaAPI(args.api_url, dry_run=args.dry_run, retries=args.retries)

    if args.destroy:
        destroy(api)
        return

    t0 = time.time()
    setup(api, args)
    elapsed = time.time() - t0
    print(f"  Completed in {elapsed:.1f}s")

if __name__ == "__main__":
    main()
