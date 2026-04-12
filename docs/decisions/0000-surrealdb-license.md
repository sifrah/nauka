# ADR 0000 — Linking SurrealDB (BSL 1.1) into Nauka (Apache-2.0)

**Issue:** sifrah/nauka#186
**Epic:** sifrah/nauka#183
**Status:** Accepted
**Date:** 2026-04-12

> **Disclaimer.** This ADR is the engineering team's analysis of the licensing
> situation, not legal advice. Before any commercial release that bundles
> SurrealDB, Nauka should obtain a written opinion from a qualified lawyer.
> The point of this document is to (a) record the reasoning so the team is
> aligned, (b) make the failure modes explicit, and (c) describe escape paths
> if a future audit or upstream change invalidates the analysis.

## Context

The SurrealDB migration epic (sifrah/nauka#183) plans to link the SurrealDB
Rust SDK directly into the `nauka` binary. This means:

- `surrealdb` (the Rust SDK crate, v3.0.5)
- `surrealdb-core` (the engine)
- `surrealdb-types`
- `surrealdb-tikv-client` (a fork of `tikv-client` maintained by SurrealDB)

are compiled as static dependencies of the `nauka` binary that ships to users.

Nauka is licensed under **Apache License 2.0**. The SurrealDB crates above are
licensed under **Business Source License 1.1**, with Apache License 2.0 as the
Change License. The two licenses are not equivalent — Apache 2.0 is an
[OSI-approved Open Source license](https://opensource.org/license/apache-2-0),
while the BSL 1.1 is a
["source-available"](https://en.wikipedia.org/wiki/Source-available_software)
license that becomes Open Source on a future date.

We need to confirm that Nauka can ship with SurrealDB linked in **without
violating** either license, and document what would force a change of plan.

## The exact license terms

### SurrealDB v3.x — Business Source License 1.1

Verbatim from the SurrealDB v3.0.5 `LICENSE` file (the same license file applies
to `surrealdb`, `surrealdb-core`, and `surrealdb-types`):

```
Licensor:             SurrealDB Ltd.
Licensed Work:        SurrealDB 3.0
                      The Licensed Work is (c) 2025 SurrealDB Limited
Additional Use Grant: You may make use of the Licensed Work, provided that
                      you may not use the Licensed Work as a Database
                      Service.

                      "Database Service" means any product, service,
                      platform, or commercial offering in which the Licensed
                      Work is used to provide database functionality to
                      third parties, other than your direct employees or
                      contractors working on your behalf, that enables third
                      parties to create, manage, or control schemas or
                      tables.

Change Date:          2030-01-01

Change License:       Apache License, Version 2.0
```

The BSL 1.1 grants the right to "copy, modify, create derivative works,
redistribute, and make non-production use of the Licensed Work", and the
Additional Use Grant extends this to **production use** as long as the use is
**not** a "Database Service" as defined above.

The license also states:

> You must conspicuously display this License on each original or modified copy
> of the Licensed Work.

> Effective on the Change Date, or the fourth anniversary of the first publicly
> available distribution of a specific version of the Licensed Work under this
> License, whichever comes first, the Licensor hereby grants you rights under
> the terms of the Change License [Apache 2.0], and the rights granted in the
> paragraph above terminate.

### Nauka — Apache License 2.0

Standard, unmodified Apache License 2.0 (`/LICENSE` at the repository root).

## How we use SurrealDB

Within Nauka, SurrealDB is used **internally**, in-process, as the persistence
layer for Nauka's own state:

- Bootstrap state (mesh identity, hypervisor identity, peers, WireGuard keys) —
  via the `kv-surrealkv` engine, local file
- Cluster state (orgs, projects, environments, VMs, VPCs, subnets, users) —
  via the `kv-tikv` engine, against the existing TiKV cluster

End users of Nauka **never** interact with SurrealDB directly. They interact
with Nauka via:

- The `nauka` CLI (`nauka hypervisor init`, `nauka vm create`, etc.)
- Nauka's REST API (axum handlers under `/api/v1/...`)

End users **cannot**:

- Issue SurrealQL queries
- Create, modify, or drop SurrealDB tables, indexes, schemas, namespaces, or
  databases directly
- Connect to SurrealDB over the network as a database client
- Use Nauka as a generic database for arbitrary data of their own design

What end users *can* do is create Nauka resources (VMs, VPCs, etc.). These
resource types are defined by Nauka itself in `.surql` schemas committed to the
Nauka source tree (Phase 3 of sifrah/nauka#183), not authored by end users.
The shape of the data Nauka stores is fixed by Nauka's release, not by its
users.

## Compliance analysis

### Does Nauka qualify as a "Database Service" under the BSL Additional Use Grant?

A "Database Service" per the BSL definition has **two** required elements,
both of which must be present:

1. **The Licensed Work is used to provide database functionality to third
   parties** (other than direct employees / contractors).
2. **The third parties can create, manage, or control schemas or tables.**

Apply to Nauka:

| Required element | Nauka behaviour | Met? |
|---|---|---|
| Provides database functionality to third parties | Nauka end users get cloud-platform functionality (VMs, networks, etc.). The fact that Nauka happens to use SurrealDB to store state internally is invisible to them, no different from how a typical web app uses Postgres internally without exposing Postgres to its users. | **No** (arguable) |
| Third parties can create / manage / control schemas or tables | Schemas (`.surql` files) ship with the Nauka release. End users cannot author, modify, or query them. They can only create instances of pre-defined resource types (a VM, a VPC) through Nauka's own API. | **No** |

Both elements must be true for the use to be a "Database Service". The second
element is unambiguously **false** for Nauka — end users have no way to define,
modify, or even directly query SurrealDB schemas or tables.

Therefore Nauka's use of SurrealDB **does not** qualify as a "Database Service"
under the BSL Additional Use Grant, and the Additional Use Grant **does**
permit Nauka's production use.

This is the same legal posture under which thousands of applications use
Postgres, MariaDB, CockroachDB, MongoDB, and other source-available or
copyleft-licensed databases as embedded persistence layers without becoming
"a database service".

### Does Apache 2.0 (Nauka's license) prevent us from depending on a BSL crate?

No. Apache 2.0 is a permissive license; it imposes no requirement that
dependencies be permissively licensed. It only obligates Nauka itself to be
licensed under Apache 2.0 terms (notice file, no patent grant withdrawal,
etc.). Nauka is free to link a BSL-licensed dependency the same way it could
link a GPL-licensed one — the obligation that flows from the dependency's
license (BSL in this case) is what we must satisfy, not Apache 2.0.

### Does the BSL force Nauka to switch its own license to BSL?

No. The BSL applies to the Licensed Work (SurrealDB). It does not virally
relicense downstream consumers — there is no "infectious" clause analogous to
GPL section 5. Nauka remains Apache 2.0; only the BSL-licensed code inside the
binary is governed by the BSL.

### What about visible attribution?

The BSL requires:

> You must conspicuously display this License on each original or modified copy
> of the Licensed Work.

This obligation flows to Nauka's distribution because the SurrealDB binary code
ships inside the Nauka binary. To satisfy this we will:

1. **Bundle the SurrealDB LICENSE in Nauka's release artifacts.** A
   `THIRD_PARTY_LICENSES` file (or directory) at the root of every release
   tarball contains the verbatim text of the SurrealDB BSL 1.1 license, plus
   any other third-party licenses Nauka must reproduce.
2. **Surface attribution at runtime.** `nauka --version` and `nauka doctor`
   include a line pointing to `THIRD_PARTY_LICENSES` and noting that Nauka
   embeds SurrealDB under the BSL 1.1.
3. **Mention the BSL dependency in the README.** A short licensing section
   under the existing Apache-2.0 notice points readers at
   `THIRD_PARTY_LICENSES`.

Tracking these three deliverables: see "Follow-ups" below — they are not part
of this ADR's PR (which is documentation only) but must be in place **before**
any release that bundles SurrealDB.

## Change Date math

The BSL says rights revert to Apache 2.0 on:

> the Change Date, **or** the fourth anniversary of the first publicly
> available distribution of a specific version of the Licensed Work under this
> License, **whichever comes first**.

For SurrealDB v3.x (the line we are pinning):

| Marker | Date |
|---|---|
| BSL Change Date (per LICENSE file) | 2030-01-01 |
| v3.0.0 publicly released | 2026-02-17 |
| Fourth anniversary of v3.0.0 | 2030-02-17 |
| **Effective revert date for v3.x** | **2030-01-01** (Change Date is earlier) |

So v3.0.x reverts to Apache License 2.0 on **2030-01-01** — about 3 years and
9 months from today (2026-04-12). After that date, every version in the v3.0.x
line is dual-available under Apache 2.0 and the obligations from this ADR
disappear.

Newer SurrealDB versions (v3.1, v3.2, v4.0, ...) will reset their own clocks
when SurrealDB Ltd. publishes them, with their own Change Dates and Additional
Use Grants. We must re-audit on every major-version bump.

## Risks and what would invalidate this analysis

1. **SurrealDB tightens the Additional Use Grant in a future version.**
   The BSL allows the Licensor to set a different Additional Use Grant on each
   release. If SurrealDB Ltd. changes their definition of "Database Service" to
   include in-process use, our current analysis would no longer hold for the
   new version. Mitigation: pin SurrealDB at a specific version known to be
   compatible. Re-audit before bumping.

2. **A reasonable lawyer disagrees with our reading of "Database Service".**
   The BSL is relatively new and not heavily tested in court. A lawyer
   reviewing our setup might take a more conservative view than the analysis
   above. Mitigation: obtain a written legal opinion before any release that
   bundles SurrealDB. Stop bundling pending the opinion.

3. **Nauka's product evolves into something that *does* expose schema/table
   management to users.** If we ever build a feature like "users can BYO
   tables", "raw SurrealQL queries from the API", or "users define custom
   resource types in `.surql`", we cross into "Database Service" territory and
   the Additional Use Grant no longer covers us. Mitigation: keep schemas
   ship-with-the-release. Treat any "user-defined data model" feature as a
   trigger for re-audit.

4. **SurrealDB Ltd. enforces aggressively against marginal cases.**
   Even if we are technically in compliance, an aggressive enforcement posture
   could create reputational or legal risk. Mitigation: maintain a working
   relationship with SurrealDB Ltd. (they have a contact at
   <legal@surrealdb.com>). If our use case is borderline, ask them in writing
   for confirmation.

5. **The transitive `surrealdb-tikv-client` fork has different license terms.**
   `surrealdb-tikv-client` is published under SurrealDB Ltd.'s control as a
   fork of the upstream `tikv-client` (which is Apache 2.0). If the SurrealDB
   fork is also BSL-licensed, the same analysis applies. If it inherits Apache
   2.0 from the upstream, we have no extra obligation. **TODO** for the team:
   confirm the license of `surrealdb-tikv-client v0.3.0-surreal.4` before P0.3
   (sifrah/nauka#187) lands. This is a small follow-up, not a blocker for this
   ADR.

## Mitigation paths if our position becomes untenable

If at any point — through legal review, an upstream change, a product pivot,
or an enforcement action — our right to use SurrealDB under the BSL is in
doubt, we have several escape paths in increasing order of cost:

1. **Buy a commercial license from SurrealDB Ltd.**
   The BSL itself points at this option:

   > If your use of the Licensed Work does not comply with the requirements
   > currently in effect as described in this License, you must purchase a
   > commercial license from the Licensor [...] or you must refrain from
   > using the Licensed Work.

   Cost: depends on negotiation. Lowest-friction path to keeping SurrealDB.

2. **Pin to a SurrealDB version whose Change Date has already arrived.**
   Once v3.0.x reverts to Apache 2.0 on 2030-01-01, we can pin to v3.0.x and
   use it as a normal Apache-2.0 dependency forever. Cost: locked to a stale
   SurrealDB version, no upstream bug fixes for free.

3. **Fork SurrealDB v3.x and maintain it ourselves.**
   The BSL allows derivative works (subject to the same BSL terms). Once
   v3.0.x reverts to Apache 2.0, the fork can be maintained under Apache 2.0
   indefinitely. Cost: significant ongoing engineering effort to keep the
   fork alive.

4. **Replace SurrealDB with a permissively-licensed alternative.**
   Candidates: SurrealKV directly (Apache 2.0, but loses the SurrealQL/graph
   layer), Postgres + a graph extension (Apache-licensed components, very
   different architecture), CozoDB (MPL-2.0), or rolling our own thin layer
   on top of TiKV (loses SurrealQL but is the smallest semantic gap to the
   current code). Cost: highest — full rewrite of the SurrealDB integration.

The right choice depends on **why** the position became untenable:

| Trigger | Best path |
|---|---|
| Lawyer disagrees with our reading | Path 1 (commercial license) |
| SurrealDB tightens Additional Use Grant in v4.x | Path 2 (pin v3.x) until Change Date |
| SurrealDB stops maintaining v3.x before Change Date | Path 3 (fork) or Path 4 (replace) |
| Strategic pivot toward DBaaS | Path 1 (commercial license) — DBaaS is exactly what BSL forbids |

## Decision

**Nauka may link `surrealdb` (and the transitive `surrealdb-core`,
`surrealdb-types`, `surrealdb-tikv-client`) v3.0.5 into the `nauka` binary
under the Business Source License 1.1, relying on the Additional Use Grant
because Nauka does not expose schema or table management to its end users
and therefore is not a "Database Service" within the meaning of the BSL.**

This decision is contingent on:

1. The follow-up deliverables in this ADR (third-party license bundling,
   `nauka --version` attribution, README mention) being in place **before**
   any release that ships SurrealDB.
2. A written legal opinion **before** the first commercial release.
3. Re-auditing on every major SurrealDB version bump.

The decision applies to v3.0.x. v3.1, v3.2, v4.0, etc. require their own
audit at the time we adopt them.

## Follow-ups

These are not part of this ADR's PR but must be tracked separately. They are
**hard requirements** before any release that bundles SurrealDB:

- [ ] Add `THIRD_PARTY_LICENSES` (file or directory) to release artifacts,
      containing the verbatim BSL 1.1 from the upstream SurrealDB repo
- [ ] Surface attribution in `nauka --version` and `nauka doctor`
- [ ] Add a "Third-party licenses" section to the README
- [ ] Confirm the license of `surrealdb-tikv-client v0.3.0-surreal.4`
      (TODO from the risk analysis above; small task)
- [ ] Obtain a written legal opinion before the first commercial release
- [ ] Add a calendar reminder for 2030-01-01: BSL → Apache 2.0 transition,
      review and possibly drop the attribution machinery

These follow-ups should each become their own GitHub issue once the SurrealDB
migration epic gets close to a release boundary. They are mentioned here so
the team has a single source of truth for the licensing obligations.
