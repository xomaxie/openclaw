#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
fail() { echo "[fail] $*" >&2; exit 1; }
pass() { echo "[ok] $*"; }

required=(
  "$ROOT/README.md"
  "$ROOT/bin/gmail-admin"
  "$ROOT/docs/auth.md"
  "$ROOT/docs/safety.md"
  "$ROOT/scripts/install-gog.sh"
  "$ROOT/examples/filter-plan.notifications.json"
  "$ROOT/examples/mailbox-workflow.md"
  "$ROOT/.env.example"
)

for path in "${required[@]}"; do
  [[ -f "$path" ]] || fail "missing file: $path"
done
pass "required files exist"

bash -n "$ROOT/bin/gmail-admin"
bash -n "$ROOT/scripts/install-gog.sh"
pass "shell scripts parse cleanly"

command -v python3 >/dev/null 2>&1 || fail "python3 not found"
pass "python3 available"

if command -v gog >/dev/null 2>&1; then
  echo "[info] gog version: $(gog --version 2>/dev/null || true)"
  pass "gog available"
else
  echo "[warn] gog not installed yet"
fi

if [[ -f "$ROOT/.env" ]]; then
  pass ".env present"
else
  echo "[warn] .env not created yet"
fi

echo "Validation complete."
