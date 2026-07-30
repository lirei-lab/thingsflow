#!/usr/bin/env sh
#
# Fails when the modern topology model drifts from the ThingsBoard legacy
# relation mirror. Intended for verify-local.sh and target-environment smoke suites.
set -eu

BRIDGE_URL="${BRIDGE_URL:-http://localhost:8082}"
TB_USER="${TB_USER:-sysadmin@thingsboard.org}"
TB_PASS="${TB_PASS:-sysadmin}"

need() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 127
  fi
}

need curl
need jq

token="$(
  curl -fsS --max-time 20 -X POST "$BRIDGE_URL/api/auth/login" \
    -H "Content-Type: application/json" \
    -d "{\"username\":\"$TB_USER\",\"password\":\"$TB_PASS\"}" \
    | jq -r '.token'
)"
if [ -z "$token" ] || [ "$token" = "null" ]; then
  echo "topology consistency check: login failed for $TB_USER" >&2
  exit 1
fi

report="$(
  curl -fsS --max-time 20 "$BRIDGE_URL/api/admin/topology/consistency" \
    -H "X-Authorization: Bearer $token"
)"

status="$(printf '%s' "$report" | jq -r '.status // ""')"
legacy_missing="$(printf '%s' "$report" | jq -r '.legacyMissingTopology // -1')"
modern_missing_legacy="$(printf '%s' "$report" | jq -r '.topologyMissingLegacy // -1')"
cross_tenant="$(printf '%s' "$report" | jq -r '.crossTenantLegacyRelations // -1')"
invalid_type="$(printf '%s' "$report" | jq -r '.invalidTopologyRelationType // -1')"
edges="$(printf '%s' "$report" | jq -r '.topologyEdges // 0')"

if [ "$status" != "OK" ] ||
   [ "$legacy_missing" != "0" ] ||
   [ "$modern_missing_legacy" != "0" ] ||
   [ "$cross_tenant" != "0" ] ||
   [ "$invalid_type" != "0" ]; then
  echo "topology consistency check failed:" >&2
  printf '%s\n' "$report" | jq . >&2
  exit 1
fi

echo "topology consistency OK: edges=$edges drift=0"
