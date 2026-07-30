#!/usr/bin/env bash
#
# UI-independent device flow demo.
# Acts like an external portal/device integration and uses only public APIs.

set -euo pipefail

BRIDGE_URL="${BRIDGE_URL:-https://thingsflow.example.com}"
TB_USER="${TB_USER:-tenant@thingsboard.org}"
TB_PASS="${TB_PASS:-tenant}"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%d%H%M%S)-$$}"
PROFILE_NAME="${PROFILE_NAME:-ui-independent-profile-$RUN_ID}"
PROVISION_KEY="${PROVISION_KEY:-ui-independent-key-$RUN_ID}"
PROVISION_SECRET="${PROVISION_SECRET:-ui-independent-secret-$RUN_ID}"
DEVICE_NAME="${DEVICE_NAME:-ui-independent-device-$RUN_ID}"
TELEMETRY_KEY="${TELEMETRY_KEY:-uiIndependentFlow}"
TELEMETRY_VALUE="${TELEMETRY_VALUE:-$(( $(date +%s) % 100000 ))}"

need() {
  command -v "$1" >/dev/null || { echo "missing required command: $1" >&2; exit 127; }
}

need curl
need jq

TOKEN=""
DEVICE_ID=""
PROFILE_ID=""
DEVICE_TOKEN=""
HTTP_CODE=""
HTTP_BODY=""

request() {
  local method="$1" path="$2" body="${3:-}" auth="${4:-true}"
  local marker=$'\n__HTTP_STATUS__:'
  local args=(-ksS --max-time 30 -w "${marker}%{http_code}" -X "$method" "$BRIDGE_URL$path" -H "Content-Type: application/json")
  if [ "$auth" = "true" ]; then
    args+=(-H "X-Authorization: Bearer $TOKEN")
  fi
  if [ -n "$body" ]; then
    args+=(-d "$body")
  fi
  local response
  response="$(curl "${args[@]}" || true)"
  HTTP_CODE="${response##*__HTTP_STATUS__:}"
  HTTP_BODY="${response%"$marker$HTTP_CODE"}"
}

expect_code() {
  local want="$1" label="$2"
  if [ "$HTTP_CODE" != "$want" ]; then
    echo "✗ $label returned HTTP $HTTP_CODE, expected $want" >&2
    printf '%s\n' "$HTTP_BODY" | sed 's/[[:cntrl:]]//g' >&2 || true
    exit 1
  fi
  echo "  ✓ $label HTTP $HTTP_CODE"
}

json_value() {
  local expr="$1"
  printf '%s' "$HTTP_BODY" | jq -r "$expr"
}

json_has() {
  local expr="$1"
  printf '%s' "$HTTP_BODY" | jq -e "$expr" >/dev/null
}

cleanup() {
  set +e
  if [ -n "${DEVICE_ID:-}" ] && [ -n "$TOKEN" ]; then
    request DELETE "/api/device/$DEVICE_ID" "" true >/dev/null 2>&1 || true
  fi
  if [ -n "${PROFILE_ID:-}" ] && [ -n "$TOKEN" ]; then
    request DELETE "/api/deviceProfile/$PROFILE_ID" "" true >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

printf 'UI-INDEPENDENT DEVICE FLOW\n'
printf 'bridge=%s user=%s\n' "$BRIDGE_URL" "$TB_USER"

printf '→ platform admin login for provisioning policy\n'
request POST /api/auth/login "$(jq -nc --arg username "$TB_USER" --arg password "$TB_PASS" '{username:$username,password:$password}')" false
expect_code 200 "login"
TOKEN="$(json_value '.token // empty')"
[ -n "$TOKEN" ] || { echo "✗ login did not return token" >&2; exit 1; }

printf '→ create provisioning profile through public API\n'
profile_payload="$(
  jq -nc --arg name "$PROFILE_NAME" --arg key "$PROVISION_KEY" --arg secret "$PROVISION_SECRET" \
    '{name:$name,type:"DEFAULT",transportType:"DEFAULT",provisionType:"ALLOW_CREATE_NEW_DEVICES",provisionDeviceKey:$key,provisionDeviceSecret:$secret,profileData:{configuration:{type:"DEFAULT"},transportConfiguration:{type:"DEFAULT"},alarms:[]}}'
)"
request POST /api/deviceProfile "$profile_payload" true
expect_code 200 "device profile create"
PROFILE_ID="$(json_value '.id.id // .id // empty')"
[ -n "$PROFILE_ID" ] || { echo "✗ profile id missing" >&2; exit 1; }

printf '→ device self-provisions without UI\n'
provision_payload="$(jq -nc --arg name "$DEVICE_NAME" --arg key "$PROVISION_KEY" --arg secret "$PROVISION_SECRET" '{deviceName:$name,provisionDeviceKey:$key,provisionDeviceSecret:$secret}')"
request POST /api/v1/provision "$provision_payload" false
expect_code 200 "device self-provision"
if ! json_has '.status == "SUCCESS" and .credentialsType == "ACCESS_TOKEN" and (.credentialsValue | length >= 20)'; then
  echo "✗ provisioning did not return ACCESS_TOKEN success" >&2
  printf '%s\n' "$HTTP_BODY" | jq '{status, credentialsType}' >&2 || true
  exit 1
fi
DEVICE_TOKEN="$(json_value '.credentialsValue // empty')"

printf '→ external portal resolves device by name\n'
request GET "/api/tenant/devices?deviceName=$DEVICE_NAME" "" true
expect_code 200 "device lookup"
DEVICE_ID="$(json_value '.id.id // .id // empty')"
[ -n "$DEVICE_ID" ] || { echo "✗ device id missing" >&2; exit 1; }

printf '→ device publishes telemetry with provisioned token\n'
telemetry_payload="$(jq -nc --arg key "$TELEMETRY_KEY" --argjson value "$TELEMETRY_VALUE" '{($key):$value}')"
request POST "/api/v1/$DEVICE_TOKEN/telemetry" "$telemetry_payload" false
expect_code 200 "telemetry publish"

printf '→ external portal reads telemetry\n'
latest_value=""
for _ in $(seq 1 60); do
  request GET "/api/plugins/telemetry/DEVICE/$DEVICE_ID/values/timeseries?keys=$TELEMETRY_KEY&useStrictDataTypes=true" "" true
  if [ "$HTTP_CODE" = "200" ]; then
    latest_value="$(printf '%s' "$HTTP_BODY" | jq -r --arg key "$TELEMETRY_KEY" '.[$key][0].value // empty' 2>/dev/null || true)"
    [ "$latest_value" = "$TELEMETRY_VALUE" ] && break
  fi
  sleep 1
done
[ "$latest_value" = "$TELEMETRY_VALUE" ] || { echo "✗ telemetry not visible through API" >&2; exit 1; }
printf '  ✓ telemetry visible through API\n'

printf '→ rotate credentials from external portal\n'
rotate_payload="$(jq -nc --arg id "$DEVICE_ID" '{deviceId:{id:$id},credentialsType:"ACCESS_TOKEN"}')"
request POST /api/device/credentials "$rotate_payload" true
expect_code 200 "credential rotate"
NEW_DEVICE_TOKEN="$(json_value '.credentialsId // empty')"
[ -n "$NEW_DEVICE_TOKEN" ] || { echo "✗ rotated token missing" >&2; exit 1; }

printf '→ old token is rejected after rotation\n'
request POST "/api/v1/$DEVICE_TOKEN/telemetry" "$telemetry_payload" false
expect_code 401 "old token telemetry"
DEVICE_TOKEN="$NEW_DEVICE_TOKEN"

printf '→ new token works\n'
request POST "/api/v1/$DEVICE_TOKEN/telemetry" "$telemetry_payload" false
expect_code 200 "new token telemetry"

printf '→ suspend/reactivate through API\n'
request POST "/api/device/$DEVICE_ID/security" '{"securityStatus":"SUSPENDED"}' true
expect_code 200 "device suspend"
json_has '.securityStatus == "SUSPENDED"'
request POST "/api/v1/$DEVICE_TOKEN/telemetry" "$telemetry_payload" false
expect_code 401 "suspended telemetry"
request POST "/api/device/$DEVICE_ID/security" '{"securityStatus":"ACTIVE"}' true
expect_code 200 "device reactivate"
json_has '.securityStatus == "ACTIVE"'

printf '→ audit is queryable through API\n'
request GET "/api/audit/logs/entity/$DEVICE_ID?pageSize=50&page=0" "" true
expect_code 200 "audit read"
if ! printf '%s' "$HTTP_BODY" | jq -e '.data[]? | select(.actionType == "SECURITY_STATUS_UPDATED" or .actionType == "CREDENTIALS_UPDATED" or .actionType == "PROVISIONED")' >/dev/null; then
  echo "✗ expected device audit rows not found" >&2
  exit 1
fi
printf '  ✓ audit rows visible\n'

printf '→ cleanup\n'
request DELETE "/api/device/$DEVICE_ID" "" true
expect_code 200 "device delete"
DEVICE_ID=""
request DELETE "/api/deviceProfile/$PROFILE_ID" "" true
expect_code 200 "profile delete"
PROFILE_ID=""

printf '\n✅ UI-INDEPENDENT DEVICE FLOW PASSED\n'
