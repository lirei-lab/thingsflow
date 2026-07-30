#!/usr/bin/env bash
#
# API-first control-plane smoke.
# Proves that provisioning and core device operations work without the
# ThingsBoard UI. It intentionally does not print JWTs or device credentials.

set -euo pipefail

BRIDGE_URL="${BRIDGE_URL:-https://thingsflow.example.com}"
TB_USER="${TB_USER:-tenant@thingsboard.org}"
TB_PASS="${TB_PASS:-tenant}"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%d%H%M%S)-$$}"
DEVICE_NAME="${DEVICE_NAME:-api-control-plane-smoke-$RUN_ID}"
PROVISIONED_DEVICE_NAME="${PROVISIONED_DEVICE_NAME:-api-provision-smoke-$RUN_ID}"
PROVISION_PROFILE_NAME="${PROVISION_PROFILE_NAME:-api-provision-profile-$RUN_ID}"
PROVISION_KEY="${PROVISION_KEY:-api-provision-key-$RUN_ID}"
PROVISION_SECRET="${PROVISION_SECRET:-api-provision-secret-$RUN_ID}"
ASSET_NAME="${ASSET_NAME:-api-control-plane-asset-$RUN_ID}"
TELEMETRY_KEY="${TELEMETRY_KEY:-controlPlaneSmoke}"
TELEMETRY_VALUE="${TELEMETRY_VALUE:-$(( $(date +%s) % 100000 ))}"

need() {
  command -v "$1" >/dev/null || { echo "missing required command: $1" >&2; exit 127; }
}

need curl
need jq

TOKEN=""
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

cleanup_entities() {
  set +e
  if [ -n "${PROVISIONED_DEVICE_ID:-}" ] && [ -n "$TOKEN" ]; then
    request DELETE "/api/device/$PROVISIONED_DEVICE_ID" "" true >/dev/null 2>&1 || true
  fi
  if [ -n "${DEVICE_ID:-}" ] && [ -n "$TOKEN" ]; then
    request DELETE "/api/device/$DEVICE_ID" "" true >/dev/null 2>&1 || true
  fi
  if [ -n "${PROFILE_ID:-}" ] && [ -n "$TOKEN" ]; then
    request DELETE "/api/deviceProfile/$PROFILE_ID" "" true >/dev/null 2>&1 || true
  fi
  if [ -n "${ASSET_ID:-}" ] && [ -n "$TOKEN" ]; then
    request DELETE "/api/asset/$ASSET_ID" "" true >/dev/null 2>&1 || true
  fi
}
trap cleanup_entities EXIT

printf 'CONTROL PLANE API SMOKE\n'
printf 'bridge=%s user=%s\n' "$BRIDGE_URL" "$TB_USER"

printf '→ login\n'
request POST /api/auth/login "$(jq -nc --arg username "$TB_USER" --arg password "$TB_PASS" '{username:$username,password:$password}')" false
expect_code 200 "login"
TOKEN="$(json_value '.token // empty')"
if [ -z "$TOKEN" ]; then
  echo "✗ login did not return token" >&2
  exit 1
fi

printf '→ create asset\n'
request POST /api/asset "$(jq -nc --arg name "$ASSET_NAME" '{name:$name,type:"api_control_plane"}')" true
expect_code 200 "asset create"
ASSET_ID="$(json_value '.id.id // .id // empty')"
[ -n "$ASSET_ID" ] || { echo "✗ asset id missing" >&2; exit 1; }

printf '→ create device\n'
request POST /api/device "$(jq -nc --arg name "$DEVICE_NAME" '{name:$name,type:"api_control_plane"}')" true
expect_code 200 "device create"
DEVICE_ID="$(json_value '.id.id // .id // empty')"
[ -n "$DEVICE_ID" ] || { echo "✗ device id missing" >&2; exit 1; }

printf '→ read credentials\n'
request GET "/api/device/$DEVICE_ID/credentials" "" true
expect_code 200 "credentials read"
DEVICE_TOKEN="$(json_value '.credentialsId // empty')"
cred_type="$(json_value '.credentialsType // empty')"
if [ -z "$DEVICE_TOKEN" ] || [ "$cred_type" != "ACCESS_TOKEN" ]; then
  echo "✗ unexpected credentials response" >&2
  exit 1
fi
printf '  ✓ credentials present type=%s\n' "$cred_type"

printf '→ create provisioning profile\n'
profile_payload="$(jq -nc --arg name "$PROVISION_PROFILE_NAME" --arg key "$PROVISION_KEY" --arg secret "$PROVISION_SECRET" '{name:$name,type:"DEFAULT",transportType:"DEFAULT",provisionType:"ALLOW_CREATE_NEW_DEVICES",provisionDeviceKey:$key,provisionDeviceSecret:$secret,profileData:{configuration:{type:"DEFAULT"},transportConfiguration:{type:"DEFAULT"},alarms:[]}}')"
request POST /api/deviceProfile "$profile_payload" true
expect_code 200 "provisioning profile create"
PROFILE_ID="$(json_value '.id.id // .id // empty')"
[ -n "$PROFILE_ID" ] || { echo "✗ provisioning profile id missing" >&2; exit 1; }

printf '→ auto-provision device via /api/v1/provision\n'
provision_payload="$(jq -nc --arg name "$PROVISIONED_DEVICE_NAME" --arg key "$PROVISION_KEY" --arg secret "$PROVISION_SECRET" '{deviceName:$name,provisionDeviceKey:$key,provisionDeviceSecret:$secret}')"
request POST /api/v1/provision "$provision_payload" false
expect_code 200 "device auto-provision"
if ! json_has '.status == "SUCCESS" and .credentialsType == "ACCESS_TOKEN" and (.credentialsValue | length >= 20)'; then
  echo "✗ provisioning did not return ACCESS_TOKEN success" >&2
  printf '%s\n' "$HTTP_BODY" | jq '{status, credentialsType}' >&2 || true
  exit 1
fi
PROVISIONED_DEVICE_TOKEN="$(json_value '.credentialsValue // empty')"
DEVICE_JWT="$(json_value '.deviceJwt.token // empty')"
MQTT_IDENTITY="$(json_value '.deviceJwt.mqttIdentity // empty')"
if [ -z "$DEVICE_JWT" ] || [ -z "$MQTT_IDENTITY" ]; then
  echo "✗ provisioning did not return deviceJwt token and mqttIdentity" >&2
  printf '%s
' "$HTTP_BODY" | jq '{status, deviceJwt}' >&2 || true
  exit 1
fi
printf '  ✓ device JWT issued for mqttIdentity=%s
' "$MQTT_IDENTITY"

printf '→ lookup auto-provisioned device\n'
request GET "/api/tenant/devices?deviceName=$PROVISIONED_DEVICE_NAME" "" true
expect_code 200 "auto-provisioned device lookup"
PROVISIONED_DEVICE_ID="$(json_value '.id.id // .id // empty')"
[ -n "$PROVISIONED_DEVICE_ID" ] || { echo "✗ provisioned device id missing" >&2; exit 1; }

printf '→ create topology relation\n'
relation_payload="$(jq -nc --arg asset "$ASSET_ID" --arg device "$DEVICE_ID" '{from:{entityType:"ASSET",id:$asset},to:{entityType:"DEVICE",id:$device},type:"Contains",typeGroup:"COMMON"}')"
request POST /api/relation "$relation_payload" true
expect_code 200 "relation create"

printf '→ list topology relation\n'
request GET "/api/relations?fromId=$ASSET_ID&fromType=ASSET" "" true
expect_code 200 "relations list"
if ! printf '%s' "$HTTP_BODY" | jq -e --arg device "$DEVICE_ID" '.[]? | select(.to.id == $device and .type == "Contains")' >/dev/null; then
  echo "✗ created Contains relation not returned" >&2
  printf '%s\n' "$HTTP_BODY" >&2
  exit 1
fi
printf '  ✓ relation visible\n'

printf '→ suspend device\n'
request POST "/api/device/$DEVICE_ID/security" '{"securityStatus":"SUSPENDED"}' true
expect_code 200 "device suspend"
json_has '.securityStatus == "SUSPENDED"'
printf '  ✓ suspended state persisted\n'

printf '→ reactivate device\n'
request POST "/api/device/$DEVICE_ID/security" '{"securityStatus":"ACTIVE"}' true
expect_code 200 "device reactivate"
json_has '.securityStatus == "ACTIVE"'

printf '→ rotate credentials\n'
rotate_payload="$(jq -nc --arg id "$DEVICE_ID" '{deviceId:{id:$id},credentialsType:"ACCESS_TOKEN"}')"
request POST /api/device/credentials "$rotate_payload" true
expect_code 200 "credentials rotate"
rotated_type="$(json_value '.credentialsType // empty')"
[ "$rotated_type" = "ACCESS_TOKEN" ] || { echo "✗ rotated credential type mismatch" >&2; exit 1; }
printf '  ✓ credentials rotated\n'

printf '→ audit log\n'
request GET "/api/audit/logs/entity/$DEVICE_ID?pageSize=50&page=0" "" true
expect_code 200 "audit read"
if ! printf '%s' "$HTTP_BODY" | jq -e '.data[]? | select(.actionType == "SECURITY_STATUS_UPDATED" or .actionType == "CREDENTIALS_UPDATED")' >/dev/null; then
  echo "✗ expected security/credential audit rows not found" >&2
  printf '%s\n' "$HTTP_BODY" >&2
  exit 1
fi
printf '  ✓ audit rows visible\n'

printf '→ cleanup smoke entities\n'
request DELETE "/api/device/$PROVISIONED_DEVICE_ID" "" true
expect_code 200 "auto-provisioned device delete"
PROVISIONED_DEVICE_ID=""
request DELETE "/api/device/$DEVICE_ID" "" true
expect_code 200 "device delete"
DEVICE_ID=""
request DELETE "/api/deviceProfile/$PROFILE_ID" "" true
expect_code 200 "provisioning profile delete"
PROFILE_ID=""
request DELETE "/api/asset/$ASSET_ID" "" true
expect_code 200 "asset delete"
ASSET_ID=""

printf '\n✅ CONTROL PLANE API SMOKE PASSED\n'
