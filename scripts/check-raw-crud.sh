#!/usr/bin/env bash
# Fail CI if any layer or binary reaches into a resource table with
# hand-rolled `CREATE {table} SET …` / `UPDATE {table} …` /
# `DELETE {table}` SurrealQL. Enforces ADR 0006 rule #10: every
# write goes through `nauka_state::Writer` + the generated
# `ResourceOps` query builders.
#
# We discover the table list by scanning the source for
# `#[resource(table = "…")]` attributes — new resources don't need
# to be listed here manually.
#
# Tests under `tests/` are exempt: they intentionally build raw
# SurrealQL to exercise consensus / determinism properties. So are
# the `_raft_*` infrastructure tables (not resources).

set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"

# --- gather table names from every production `#[resource(table = "NAME")]`. ---
# Excludes `tests/` directories (compile-fail fixtures declare fake
# tables like `x`, `clash` on purpose).
tables=$(
  find "$root/layers" "$root/core" "$root/bin" -path '*/tests' -prune -o \
       -name '*.rs' -print 2>/dev/null \
    | xargs grep -h 'table = "' 2>/dev/null \
    | sed -n 's/.*table = "\([a-z0-9_]*\)".*/\1/p' \
    | sort -u
)

if [ -z "$tables" ]; then
  echo "no #[resource(table = …)] declarations found — nothing to check"
  exit 0
fi

echo "checking raw CRUD usage for tables:" $tables

violations=0
for table in $tables; do
  # Match literal strings starting with `CREATE <table>`,
  # `UPDATE <table>`, or `DELETE <table>` followed by whitespace
  # (the SurrealQL syntax). The record-id form
  # `CREATE <table>:⟨…⟩` is what the contract emits and is fine,
  # so we require a whitespace after the table name to catch only
  # the legacy form.
  pattern="\"(CREATE|UPDATE|DELETE) $table "
  # Search production source (not tests).
  hits=$(
    grep -rEn "$pattern" \
      --include='*.rs' \
      "$root/layers" "$root/bin" 2>/dev/null \
      | grep -v '/tests/' \
      || true
  )
  if [ -n "$hits" ]; then
    echo
    echo "::error::raw CRUD for table \`$table\` found — route it through Writer / ResourceOps instead:"
    echo "$hits"
    violations=$((violations + 1))
  fi
done

if [ "$violations" -gt 0 ]; then
  echo
  echo "$violations table(s) had raw CRUD violations — see ADR 0006."
  exit 1
fi

echo "no raw CRUD violations — all resource writes route through Writer."
