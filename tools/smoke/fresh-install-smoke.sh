#!/usr/bin/env bash
# Fresh-install smoke: the executable contract for "a from-scratch install works".
#
# Run against an already-started local stack:
#   docker compose -f docker/docker-compose-nats.yml up -d --build
#   bash tools/smoke/fresh-install-smoke.sh
#
# CI executes this contract via .github/workflows/fresh-install-smoke.yml on
# PRs and pushes that touch the stack. It proves the path a NEW user walks:
# demo login → create a device → device JWT → publish telemetry through the
# real edges (Envoy HTTP ingest and the RMQTT broker — never a shortcut into
# the store) → read the rows back through BOTH storage pipelines:
#   - the historical read (time-ranged query → GreptimeDB device_telemetry_kv)
#   - the latest-values read (keys-only query → NATS KV twin state)
# Verifying only one would stay green while the other pipeline is silently
# dead — the recorded GreptimeDB silent-halt incident is exactly that shape.
#
# Relationship to tools/smoke-local.sh: that script is the INNER DEV LOOP smoke
# (device provisioning path + NATS KV check, brings the stack up itself). This
# one is the FRESH-INSTALL CONTRACT: it exercises the standard control-plane
# device flow and both read paths, and is what CI executes. They are
# complementary, not competing.
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-docker/docker-compose-nats.yml}"
FLOW_CORE_URL="${FLOW_CORE_URL:-http://localhost:8082}"
INGEST_URL="${INGEST_URL:-http://localhost:8083}"
MQTT_HOST="${MQTT_HOST:-localhost}"
MQTT_PORT="${MQTT_PORT:-1883}"
TB_USER="${TB_USER:-tenant@thingsboard.org}"
TB_PASS="${TB_PASS:-tenant}"
# First boot on a cold CI runner: flow-core seeds the schema before /ready
# returns — budget generously. ROW_TIMEOUT bounds the read-back polls.
TIMEOUT_SECS="${TIMEOUT_SECS:-300}"
ROW_TIMEOUT_SECS="${ROW_TIMEOUT_SECS:-60}"
# Every curl gets a hard per-request bound so a half-up service that accepts
# connections but never answers cannot hang the run past its iteration budget.
CURL="curl --max-time 10"

log() { printf '[smoke] %s\n' "$*"; }
die() { printf '[smoke] FAIL: %s\n' "$*" >&2; exit 1; }

# Best-effort cleanup on ANY exit: failed runs must not leak smoke-* devices
# into a long-lived local stack.
DEVICE_ID=""
cleanup() {
  if [ -n "$DEVICE_ID" ] && [ -n "${TOKEN:-}" ]; then
    $CURL -fsS -X DELETE "$FLOW_CORE_URL/api/device/$DEVICE_ID" \
      -H "X-Authorization: Bearer $TOKEN" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

need() { command -v "$1" >/dev/null || die "missing required command: $1"; }
need curl
need jq
need docker

# --- a. wait for flow-core readiness (purpose-built /ready endpoint) ---------
log "waiting for $FLOW_CORE_URL/ready (up to ${TIMEOUT_SECS}s)"
ready=0
for _ in $(seq 1 $((TIMEOUT_SECS / 2))); do
  if $CURL -fsS "$FLOW_CORE_URL/ready" >/dev/null 2>&1; then ready=1; break; fi
  sleep 2
done
if [ "$ready" -ne 1 ]; then
  docker compose -f "$COMPOSE_FILE" ps -a || true
  die "flow-core never became ready"
fi
log "flow-core is ready"

# --- b. demo login (the compose stack always mounts the demo-password seed) --
# Failures are captured explicitly: under `set -e` a raw pipeline assignment
# would kill the script before any diagnostic could print.
LOGIN_BODY="$($CURL -fsS -X POST "$FLOW_CORE_URL/api/auth/login" \
  -H "Content-Type: application/json" \
  -d "$(jq -nc --arg u "$TB_USER" --arg p "$TB_PASS" '{username:$u,password:$p}')")" \
  || die "login request to $FLOW_CORE_URL failed (is the stack up and seeded?)"
TOKEN="$(printf '%s' "$LOGIN_BODY" | jq -r '.token')"
[ -n "$TOKEN" ] && [ "$TOKEN" != "null" ] || die "login as $TB_USER rejected: $LOGIN_BODY"
AUTH=(-H "X-Authorization: Bearer $TOKEN")
log "logged in as $TB_USER"

# --- c. create a device; the id is NESTED in the TB shape (.id.id) -----------
DEVICE_NAME="smoke-$(date +%s)"
DEVICE_BODY="$($CURL -fsS -X POST "$FLOW_CORE_URL/api/device" \
  -H "Content-Type: application/json" "${AUTH[@]}" \
  -d "$(jq -nc --arg n "$DEVICE_NAME" '{name:$n,type:"default"}')")" \
  || die "device creation request failed"
DEVICE_ID="$(printf '%s' "$DEVICE_BODY" | jq -r '.id.id // .id')"
[ -n "$DEVICE_ID" ] && [ "$DEVICE_ID" != "null" ] || die "device creation rejected: $DEVICE_BODY"
# ACCESS_TOKEN credentials are auto-created with the device; fetching proves it.
$CURL -fsS "$FLOW_CORE_URL/api/device/$DEVICE_ID/credentials" "${AUTH[@]}" >/dev/null \
  || die "credentials fetch for $DEVICE_ID failed"
log "created device $DEVICE_NAME ($DEVICE_ID)"

# --- d. issue the device JWT (used by BOTH edges) ----------------------------
JWT_BODY="$($CURL -fsS -X POST "$FLOW_CORE_URL/api/device/$DEVICE_ID/jwt" "${AUTH[@]}")" \
  || die "device JWT request failed"
DEVICE_JWT="$(printf '%s' "$JWT_BODY" | jq -r '.token')"
MQTT_IDENTITY="$(printf '%s' "$JWT_BODY" | jq -r '.mqttIdentity')"
{ [ -n "$DEVICE_JWT" ] && [ "$DEVICE_JWT" != "null" ] && \
  [ -n "$MQTT_IDENTITY" ] && [ "$MQTT_IDENTITY" != "null" ]; } \
  || die "device JWT issuance returned unexpected body: $JWT_BODY"
log "issued device JWT (mqttIdentity=$MQTT_IDENTITY)"

# --- e. publish via the HTTP ingest edge (Envoy JWT filter → Bento → NATS) ---
# Key names must avoid Bento's reserved set (ts/timestamp/values/fields/tags/
# name), which it strips silently. Envoy and its Bento sidecar are separate
# containers from flow-core and may still be starting — bounded retry, same
# treatment as the MQTT edge below.
EPOCH="$(date +%s)"
http_ok=0
for attempt in 1 2 3 4 5; do
  if $CURL -fsS -X POST "$INGEST_URL/api/v1/telemetry" \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer $DEVICE_JWT" \
    -d "{\"smoke_http\": $EPOCH}" >/dev/null 2>&1; then http_ok=1; break; fi
  log "HTTP publish attempt $attempt failed; retrying in 3s"
  sleep 3
done
[ "$http_ok" -eq 1 ] || die "HTTP publish through the ingest edge failed after 5 attempts"
log "published smoke_http=$EPOCH via HTTP edge"

# --- f. publish via RMQTT ----------------------------------------------------
# Contract (rmqtt-auth-jwt.toml): client-id MUST equal the JWT clientid claim
# (mqttIdentity), username is the RAW device JWT, no password. RMQTT starts
# after flow-core (it waits for the JWT public key), so retry the publish.
# Uses local mosquitto_pub when present; otherwise a throwaway dockerized
# mosquitto on the compose network (CI installs mosquitto-clients, so the
# native path is the one CI exercises).
mqtt_publish() {
  local msg="{\"smoke_mqtt\": $EPOCH}"
  local topic="thingsflow/devices/${MQTT_IDENTITY}/telemetry"
  if command -v mosquitto_pub >/dev/null; then
    mosquitto_pub -h "$MQTT_HOST" -p "$MQTT_PORT" \
      -i "$MQTT_IDENTITY" -u "$DEVICE_JWT" -t "$topic" -m "$msg"
  else
    local cid network
    cid="$(docker compose -f "$COMPOSE_FILE" ps -q rmqtt-edge)"
    network="$(docker inspect -f '{{range $n, $_ := .NetworkSettings.Networks}}{{println $n}}{{end}}' "$cid" | head -n 1)"
    docker run --rm --network "$network" eclipse-mosquitto:2 \
      mosquitto_pub -h rmqtt-edge -p 1883 \
      -i "$MQTT_IDENTITY" -u "$DEVICE_JWT" -t "$topic" -m "$msg"
  fi
}
mqtt_ok=0
for attempt in 1 2 3 4 5; do
  if mqtt_publish; then mqtt_ok=1; break; fi
  log "MQTT publish attempt $attempt failed; retrying in 3s"
  sleep 3
done
[ "$mqtt_ok" -eq 1 ] || die "MQTT publish failed after 5 attempts"
log "published smoke_mqtt=$EPOCH via RMQTT"

# --- g. read both rows back through BOTH storage pipelines -------------------
# A keys-only query short-circuits to the NATS KV latest-values store
# (twin state); a time-ranged query goes through GreptimeDB history. Each
# pipeline can die independently (the silent-halt incident killed only the
# history writer), so the smoke asserts BOTH. Polls tolerate transient
# errors: GreptimeDB auto-creates the table on first write, Bento batches.
TS_URL="$FLOW_CORE_URL/api/plugins/telemetry/DEVICE/$DEVICE_ID/values/timeseries"
START_MS=$(( (EPOCH - 60) * 1000 ))
END_MS=$(( (EPOCH + ROW_TIMEOUT_SECS + 60) * 1000 ))

poll_keys() {
  # $1: label, $2: full URL. Succeeds when BOTH keys carry at least one value.
  local body="" found=0
  for _ in $(seq 1 $((ROW_TIMEOUT_SECS / 2))); do
    body="$($CURL -sS "$2" "${AUTH[@]}" 2>/dev/null || true)"
    if printf '%s' "$body" | jq -e \
      '(.smoke_http | length > 0) and (.smoke_mqtt | length > 0)' >/dev/null 2>&1; then
      found=1; break
    fi
    sleep 2
  done
  [ "$found" -eq 1 ] || die "$1 read did not return both keys within ${ROW_TIMEOUT_SECS}s; last body: $body"
  log "$1 read OK: $(printf '%s' "$body" | jq -c .)"
}

log "polling GreptimeDB history read (time-ranged) for both keys"
poll_keys "history (GreptimeDB)" "$TS_URL?keys=smoke_http,smoke_mqtt&startTs=$START_MS&endTs=$END_MS"

log "polling latest-values read (NATS KV) for both keys"
poll_keys "latest (NATS KV)" "$TS_URL?keys=smoke_http,smoke_mqtt"

# --- h. verdict (cleanup runs via the EXIT trap) -----------------------------
log "PASS — fresh install serves login, device identity, HTTP edge, MQTT edge, GreptimeDB history, and KV latest reads"
