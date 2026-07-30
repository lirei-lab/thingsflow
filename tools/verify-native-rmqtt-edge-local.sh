#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-docker/docker-compose-nats.yml}"
FLOW_CORE_URL="${FLOW_CORE_URL:-http://localhost:8082}"
RUN_ID="${RUN_ID:-$(date +%s)}"
TB_USER="${TB_USER:-tenant@thingsboard.org}"
TB_PASS="${TB_PASS:-tenant}"
PROFILE_NAME="native-rmqtt-profile-${RUN_ID}"
PROVISION_KEY="native-rmqtt-key-${RUN_ID}"
PROVISION_SECRET="native-rmqtt-secret-${RUN_ID}"
DEVICE_NAME="native-rmqtt-device-${RUN_ID}"
MARKER_MQTT="native-rmqtt-mqtt-${RUN_ID}"

need() { command -v "$1" >/dev/null || { echo "missing required command: $1" >&2; exit 127; }; }
need curl
need docker
need jq

# GreptimeDB HTTP SQL endpoint (host port; compose maps GREPTIMEDB_HTTP_PORT -> 4000).
export GREPTIMEDB_HTTP_PORT="${GREPTIMEDB_HTTP_PORT:-4000}"
export GREPTIMEDB_PG_PORT="${GREPTIMEDB_PG_PORT:-4003}"

docker compose -f "$COMPOSE_FILE" up -d --build postgres greptimedb nats nats-bootstrap flow-core rmqtt-jwt-key rmqtt-edge nats-latest-kv nats-greptimedb nats-alarms alarm-materializer

for _ in $(seq 1 90); do
  curl -fsS "$FLOW_CORE_URL/ready" >/dev/null 2>&1 && break
  sleep 2
done

TOKEN="$(curl -fsS -X POST "$FLOW_CORE_URL/api/auth/login" \
  -H "Content-Type: application/json" \
  -d "$(jq -nc --arg username "$TB_USER" --arg password "$TB_PASS" '{username:$username,password:$password}')" \
  | jq -r '.token')"

profile_payload="$(jq -nc \
  --arg name "$PROFILE_NAME" \
  --arg key "$PROVISION_KEY" \
  --arg secret "$PROVISION_SECRET" \
  '{name:$name,type:"DEFAULT",transportType:"DEFAULT",provisionType:"ALLOW_CREATE_NEW_DEVICES",provisionDeviceKey:$key,provisionDeviceSecret:$secret,profileData:{configuration:{type:"DEFAULT"},transportConfiguration:{type:"DEFAULT"},alarms:[]}}')"
PROFILE_ID="$(curl -fsS -X POST "$FLOW_CORE_URL/api/deviceProfile" \
  -H "Content-Type: application/json" \
  -H "X-Authorization: Bearer $TOKEN" \
  -d "$profile_payload" | jq -r '.id.id // .id')"

provision_payload="$(jq -nc \
  --arg name "$DEVICE_NAME" \
  --arg key "$PROVISION_KEY" \
  --arg secret "$PROVISION_SECRET" \
  '{deviceName:$name,provisionDeviceKey:$key,provisionDeviceSecret:$secret}')"
provision_response="$(curl -fsS -X POST "$FLOW_CORE_URL/api/v1/provision" \
  -H "Content-Type: application/json" \
  -d "$provision_payload")"
DEVICE_ID="$(printf '%s' "$provision_response" | jq -r '.deviceId')"
DEVICE_JWT="$(printf '%s' "$provision_response" | jq -r '.deviceJwt.token')"
MQTT_IDENTITY="$(printf '%s' "$provision_response" | jq -r '.deviceJwt.mqttIdentity')"

if [[ -z "$DEVICE_JWT" || "$DEVICE_JWT" == "null" || -z "$MQTT_IDENTITY" || "$MQTT_IDENTITY" == "null" ]]; then
  echo "provisioning did not return deviceJwt token and mqttIdentity" >&2
  printf '%s\n' "$provision_response" >&2
  exit 1
fi

RMQTT_CID="$(docker compose -f "$COMPOSE_FILE" ps -q rmqtt-edge)"
NETWORK="$(docker inspect -f '{{range $name, $_ := .NetworkSettings.Networks}}{{println $name}}{{end}}' "$RMQTT_CID" | head -n 1)"

# -i is required: rmqtt-auth-jwt rejects connections whose MQTT Client ID
# differs from the JWT `clientid` claim (== mqttIdentity).
docker run --rm --network "$NETWORK" eclipse-mosquitto:2 \
  mosquitto_pub -h rmqtt-edge -p 1883 \
  -i "${MQTT_IDENTITY}" \
  -u "${DEVICE_JWT}" \
  -t "thingsflow/devices/${MQTT_IDENTITY}/telemetry" \
  -m "{\"source\":\"${MARKER_MQTT}\",\"temperature\":22.5}"

KV_KEY="DEVICE.aaaaaaaa-1dd2-11b2-8080-808080808080.${DEVICE_ID}.telemetry.temperature"
for _ in $(seq 1 30); do
  KV_VALUE="$(docker compose -f "$COMPOSE_FILE" run --rm nats-box -c "nats --server nats://nats:4222 kv get twin_state '$KV_KEY' --raw" 2>/dev/null || true)"
  [[ "$KV_VALUE" == *"22.5"* ]] && break
  sleep 2
done

[[ "$KV_VALUE" == *"22.5"* ]] || { echo "latest telemetry did not reach NATS KV key $KV_KEY" >&2; exit 1; }

curl -fsS -X DELETE "$FLOW_CORE_URL/api/device/$DEVICE_ID" -H "X-Authorization: Bearer $TOKEN" >/dev/null || true
curl -fsS -X DELETE "$FLOW_CORE_URL/api/deviceProfile/$PROFILE_ID" -H "X-Authorization: Bearer $TOKEN" >/dev/null || true

echo "NATIVE RMQTT EDGE LOCAL SMOKE PASSED"
