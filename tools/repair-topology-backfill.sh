#!/usr/bin/env sh
#
# Safe topology repair helper.
#
# Default: dry-run only.
# Apply:   tools/repair-topology-backfill.sh --apply
set -eu

BRIDGE_URL="${BRIDGE_URL:-http://localhost:8082}"
TB_USER="${TB_USER:-sysadmin@thingsboard.org}"
TB_PASS="${TB_PASS:-sysadmin}"
APPLY=false

case "${1:-}" in
  "")
    ;;
  "--apply")
    APPLY=true
    ;;
  "-h"|"--help")
    echo "Usage: BRIDGE_URL=http://... tools/repair-topology-backfill.sh [--apply]"
    exit 0
    ;;
  *)
    echo "unknown argument: $1" >&2
    echo "Usage: BRIDGE_URL=http://... tools/repair-topology-backfill.sh [--apply]" >&2
    exit 2
    ;;
esac

need() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 127
  fi
}

login() {
  curl -fsS --max-time 20 -X POST "$BRIDGE_URL/api/auth/login" \
    -H "Content-Type: application/json" \
    -d "{\"username\":\"$TB_USER\",\"password\":\"$TB_PASS\"}" \
    | jq -r '.token'
}

post_backfill() {
  dry_run="$1"
  curl -fsS --max-time 60 -X POST "$BRIDGE_URL/api/admin/topology/backfill?dryRun=$dry_run" \
    -H "X-Authorization: Bearer $TOKEN"
}

need curl
need jq

TOKEN="$(login)"
if [ -z "$TOKEN" ] || [ "$TOKEN" = "null" ]; then
  echo "topology repair: login failed for $TB_USER" >&2
  exit 1
fi

dry_report="$(post_backfill true)"
would_insert="$(printf '%s' "$dry_report" | jq -r '.wouldInsert // -1')"
echo "topology backfill dry-run: wouldInsert=$would_insert"

if [ "$APPLY" != "true" ]; then
  printf '%s\n' "$dry_report" | jq .
  echo "dry-run only; rerun with --apply to insert missing governed edges"
  exit 0
fi

apply_report="$(post_backfill false)"
inserted="$(printf '%s' "$apply_report" | jq -r '.inserted // -1')"
echo "topology backfill apply: inserted=$inserted"
printf '%s\n' "$apply_report" | jq .

BRIDGE_URL="$BRIDGE_URL" TB_USER="$TB_USER" TB_PASS="$TB_PASS" \
  "$(dirname "$0")/check-topology-consistency.sh"
