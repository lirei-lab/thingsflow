#!/usr/bin/env bash
# Fresh-install smoke: the executable contract for "a from-scratch install works".
#
# Run against an already-started local stack:
#   docker compose -f docker/docker-compose-nats.yml up -d --build
#   bash tools/smoke/fresh-install-smoke.sh
#
# CI runs this on every PR that touches the stack (.github/workflows/
# fresh-install-smoke.yml). It proves the path a NEW user walks: demo login →
# create a device → device JWT → publish telemetry through the real edges
# (Envoy HTTP ingest and the RMQTT broker — never a shortcut into the store) →
# read the rows back through the platform API.
#
# Relationship to tools/smoke-local.sh: that script is the INNER DEV LOOP smoke
# (device provisioning path + NATS KV check, brings the stack up itself). This
# one is the FRESH-INSTALL CONTRACT: it exercises the standard control-plane
# device flow and the history read path, and is what CI executes. They are
# complementary, not competing.
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-docker/docker-compose-nats.yml}"
FLOW_CORE_URL="${FLOW_CORE_URL:-http://localhost:8082}"
INGEST_URL="${INGEST_URL:-http://localhost:8083}"
MQTT_HOST="${MQTT_HOST:-localhost}"
MQTT_PORT="${MQTT_PORT:-1883}"
TB_USER="${TB_USER:-tenant@thingsboard.org}"
TB_PASS="${TB_PASS:-tenant}"
# First boot on a cold CI runner builds nothing here (the workflow builds), but
# flow-core still seeds the schema before /ready returns — budget generously.
TIMEOUT_SECS="${TIMEOUT_SECS:-300}"
ROW_TIMEOUT_SECS="${ROW_TIMEOUT_SECS:-60}"

log() { printf '[smoke] %s\n' "$*"; }
die() { printf '[smoke] FAIL: %s\n' "$*" >&2; exit 1; }

need() { command -v "$1" >/dev/null || die "missing required command: $1"; }
need curl
need jq
need docker

# --- a. wait for flow-core readiness (purpose-built /ready endpoint) ---------
log "waiting for $FLOW_CORE_URL/ready (up to ${TIMEOUT_SECS}s)"
ready=0
for _ in $(seq 1 $((TIMEOUT_SECS / 2))); do
  if curl -fsS "$FLOW_CORE_URL/ready" >/dev/null 2>&1; then ready=1; break; fi
  sleep 2
done
if [ "$ready" -ne 1 ]; then
  docker compose -f "$COMPOSE_FILE" ps || true
  die "flow-core never became ready"
fi
log "flow-core is ready"

# --- b. demo login (the compose stack always mounts the demo-password seed) --
TOKEN="$(curl -fsS -X POST "$FLOW_CORE_URL/api/auth/login" \
  -H "Content-Type: application/json" \
  -d "$(jq -nc --arg u "$TB_USER" --arg p "$TB_PASS" '{username:$u,password:$p}')" \
  | jq -r '.token')"
[ -n "$TOKEN" ] && [ "$TOKEN" != "null" ] || die "login as $TB_USER failed"
AUTH=(-H "X-Authorization: Bearer $TOKEN")
log "logged in as $TB_USER"

# --- c. create a device; the id is NESTED in the TB shape (.id.id) -----------
DEVICE_NAME="smoke-$(date +%s)"
DEVICE_ID="$(curl -fsS -X POST "$FLOW_CORE_URL/api/device" \
  -H "Content-Type: application/json" "${AUTH[@]}" \
  -d "$(jq -nc --arg n "$DEVICE_NAME" '{name:$n,type:"default"}')" \
  | jq -r '.id.id // .id')"
[ -n "$DEVICE_ID" ] && [ "$DEVICE_ID" != "null" ] || die "device creation failed"
# ACCESS_TOKEN credentials are auto-created with the device; fetching proves it.
curl -fsS "$FLOW_CORE_URL/api/device/$DEVICE_ID/credentials" "${AUTH[@]}" >/dev/null \
  || die "credentials fetch for $DEVICE_ID failed"
log "created device $DEVICE_NAME ($DEVICE_ID)"

# --- d. issue the device JWT (used by BOTH edges) ----------------------------
JWT_BODY="$(curl -fsS -X POST "$FLOW_CORE_URL/api/device/$DEVICE_ID/jwt" "${AUTH[@]}")"
DEVICE_JWT="$(printf '%s' "$JWT_BODY" | jq -r '.token')"
MQTT_IDENTITY="$(printf '%s' "$JWT_BODY" | jq -r '.mqttIdentity')"
{ [ -n "$DEVICE_JWT" ] && [ "$DEVICE_JWT" != "null" ] && \
  [ -n "$MQTT_IDENTITY" ] && [ "$MQTT_IDENTITY" != "null" ]; } \
  || die "device JWT issuance returned unexpected body: $JWT_BODY"
log "issued device JWT (mqttIdentity=$MQTT_IDENTITY)"

# --- e. publish via the HTTP ingest edge (Envoy JWT filter → Bento → NATS) ---
# Key names must avoid Bento's reserved set (ts/timestamp/values/fields/tags/
# name), which it strips silently.
EPOCH="$(date +%s)"
curl -fsS -X POST "$INGEST_URL/api/v1/telemetry" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $DEVICE_JWT" \
  -d "{\"smoke_http\": $EPOCH}" >/dev/null \
  || die "HTTP publish through the ingest edge failed"
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

# --- g. read both rows back through the platform API -------------------------
# Tolerate transient errors: GreptimeDB auto-creates the table on first write,
# and Bento batches — poll, never trust a single GET.
log "polling read API for both keys (up to ${ROW_TIMEOUT_SECS}s)"
BODY=""
found=0
for _ in $(seq 1 $((ROW_TIMEOUT_SECS / 2))); do
  BODY="$(curl -sS "$FLOW_CORE_URL/api/plugins/telemetry/DEVICE/$DEVICE_ID/values/timeseries?keys=smoke_http,smoke_mqtt" \
    "${AUTH[@]}" 2>/dev/null || true)"
  if printf '%s' "$BODY" | jq -e '.smoke_http and .smoke_mqtt' >/dev/null 2>&1; then
    found=1; break
  fi
  sleep 2
done
[ "$found" -eq 1 ] || die "telemetry rows not readable within ${ROW_TIMEOUT_SECS}s; last body: $BODY"
log "both keys landed and are readable: $(printf '%s' "$BODY" | jq -c .)"

# --- h. cleanup + verdict ----------------------------------------------------
curl -fsS -X DELETE "$FLOW_CORE_URL/api/device/$DEVICE_ID" "${AUTH[@]}" >/dev/null || true
log "PASS — fresh install serves login, device identity, HTTP edge, MQTT edge, and history reads"
