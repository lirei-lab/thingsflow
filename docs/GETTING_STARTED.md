# Getting Started

This page gives the shortest useful path from clone to a working ThingsFlow stack.

## Local Stack

Start the local compose stack:

```bash
docker compose -f docker/docker-compose-nats.yml up -d \
  postgres greptimedb nats nats-bootstrap flow-core rmqtt-edge \
  http-ingest-bento http-ingest nats-latest-kv nats-greptimedb \
  nats-alarms alarm-materializer tb-web-ui
```

Run the local smoke gate:

```bash
bash tools/smoke-local.sh
```

### Where it listens

The compose file remaps the host ports, so they are not the in-container ones:

| Service | Host | Purpose |
|---|---|---|
| ThingsBoard UI adapter | `localhost:3001` | Dashboards / device latest values |
| Flow Core | `localhost:8082` | Control-plane and ThingsBoard-compatible REST/WS |
| `http-ingest` | `localhost:8083` | Device HTTP telemetry (`POST /api/v1/telemetry`) |
| RMQTT | `localhost:1883` | Device MQTT telemetry |
| GreptimeDB | `localhost:4000` | Telemetry history, HTTP SQL |
| NATS | `localhost:4222` | Event bus |
| Postgres | `localhost:5432` | Operational state |

Check it answers before going further:

```bash
curl -s -o /dev/null -w '%{http_code}\n' -X POST localhost:8082/api/auth/login \
  -H 'Content-Type: application/json' \
  -d '{"username":"tenant@thingsboard.org","password":"tenant"}'
```

`200` means the stack is up and the demo credentials are seeded. Unlike a
default Kubernetes install, the compose stack always mounts the demo-password
seed, so this login works out of the box.

The UI is at <http://localhost:3001> (it starts after Flow Core, so give it a
few seconds). Log in as `tenant@thingsboard.org` / `tenant` to reach the
ThingsBoard tenant UI — **Devices** and **Dashboards** show what you create in
the walkthrough below.

The local stack is meant for development and compatibility checks. It includes
Flow Core for control-plane APIs, the data plane for native MQTT telemetry
ingress, storage, and the optional UI adapter surface used by the project
tests. In the NATS event plane, MQTT telemetry flows through RMQTT and HTTP
telemetry flows through Envoy+Bento `http-ingest`; both are materialized into
GreptimeDB, NATS KV, and alarm intents without the UI or Flow Core in the
ingest path.

## Connect A Device

### See a device end to end

This is the whole loop — create a device, publish telemetry through both real
edges, and read it back — as copy-paste shell. It needs only `curl`, `jq`, and
`docker` (already required for the stack) and mirrors the fresh-install smoke
gate that CI runs.

Log in as the demo tenant and keep the API token:

```bash
TOKEN=$(curl -s -X POST http://localhost:8082/api/auth/login \
  -H 'Content-Type: application/json' \
  -d '{"username":"tenant@thingsboard.org","password":"tenant"}' | jq -r '.token')
```

Create a device — note the id is nested in the ThingsBoard shape (`.id.id`):

```bash
DEVICE_ID=$(curl -s -X POST http://localhost:8082/api/device \
  -H 'Content-Type: application/json' \
  -H "X-Authorization: Bearer $TOKEN" \
  -d '{"name":"my-first-device","type":"default"}' | jq -r '.id.id')
```

`ACCESS_TOKEN` credentials are created automatically with the device:

```bash
curl -s http://localhost:8082/api/device/$DEVICE_ID/credentials \
  -H "X-Authorization: Bearer $TOKEN" | jq '.credentialsType'
```

Issue the short-lived Device JWT that both telemetry edges accept:

```bash
JWT_BODY=$(curl -s -X POST http://localhost:8082/api/device/$DEVICE_ID/jwt \
  -H "X-Authorization: Bearer $TOKEN")
DEVICE_JWT=$(echo "$JWT_BODY" | jq -r '.token')
MQTT_IDENTITY=$(echo "$JWT_BODY" | jq -r '.mqttIdentity')
```

Publish through the HTTP ingest edge (Envoy validates the JWT, Bento fans it
out to NATS — Flow Core is not in this path):

```bash
curl -s -X POST http://localhost:8083/api/v1/telemetry \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer $DEVICE_JWT" \
  -d '{"temperature": 21.5}'
```

Publish through the MQTT edge. The broker contract is: client id = the
`mqttIdentity`, username = the raw Device JWT, no password, topic
`thingsflow/devices/<mqttIdentity>/telemetry`. The one-liner below runs
`mosquitto_pub` from a throwaway container on the compose network, so nothing
needs to be installed:

```bash
NET=$(docker inspect -f '{{range $n, $_ := .NetworkSettings.Networks}}{{println $n}}{{end}}' \
  "$(docker compose -f docker/docker-compose-nats.yml ps -q rmqtt-edge)" | head -n 1)
docker run --rm --network "$NET" eclipse-mosquitto:2 \
  mosquitto_pub -h rmqtt-edge -p 1883 \
  -i "$MQTT_IDENTITY" -u "$DEVICE_JWT" \
  -t "thingsflow/devices/$MQTT_IDENTITY/telemetry" \
  -m '{"humidity": 47}'
```

If you have `mosquitto-clients` installed natively, the same publish works
against the host-mapped port: `mosquitto_pub -h localhost -p 1883` with the
identical `-i`/`-u`/`-t`/`-m` flags.

Read both keys back through the latest-values API — the same endpoint the UI
uses. The data plane is asynchronous, so wait for both keys for up to 60
seconds:

```bash
(
  DEADLINE=$(( $(date +%s) + 60 ))
  while :; do
    REMAINING=$(( DEADLINE - $(date +%s) ))
    [ "$REMAINING" -gt 0 ] || break
    if [ "$REMAINING" -lt 10 ]; then CURL_TIMEOUT="$REMAINING"; else CURL_TIMEOUT=10; fi
    LATEST=$(curl --max-time "$CURL_TIMEOUT" -s "http://localhost:8082/api/plugins/telemetry/DEVICE/$DEVICE_ID/values/timeseries?keys=temperature,humidity" \
      -H "X-Authorization: Bearer $TOKEN")
    if printf '%s' "$LATEST" | jq -e '(.temperature | length > 0) and (.humidity | length > 0)' >/dev/null; then
      printf '%s\n' "$LATEST" | jq
      exit 0
    fi
    REMAINING=$(( DEADLINE - $(date +%s) ))
    [ "$REMAINING" -gt 0 ] || break
    if [ "$REMAINING" -lt 2 ]; then sleep "$REMAINING"; else sleep 2; fi
  done
  echo "Timed out waiting for latest telemetry after 60 seconds" >&2
  exit 1
)
```

You should see one entry per key, e.g.
`{"humidity":[{"ts":...,"value":"47"}],"temperature":[{"ts":...,"value":"21.5"}]}`.
Now open the device in the UI at <http://localhost:3001> (**Entities →
Devices → my-first-device → Latest telemetry**) — the same keys are there.

One caveat when you pick your own key names: `ts`, `timestamp`, `values`,
`fields`, `tags`, and `name` are reserved envelope words that the data plane
strips silently — do not use them as telemetry keys.

### Use the SDK

For Python devices, gateways, or factory scripts, use the SDK in `sdk/python`:

```bash
python -m pip install -e sdk/python
```

The SDK provisions with `POST /api/v1/provision`, publishes HTTP telemetry to
`POST /api/v1/telemetry`, publishes MQTT telemetry to
`thingsflow/devices/{mqttIdentity}/telemetry`, and renews Device JWTs before
expiry.

See [Device SDK](DEVICE_SDK.md) for examples and the renewal model.

## Kubernetes Install

Install the public Helm chart:

```bash
helm install thingsflow ./k8s/helm/thingsflow -n thingsflow --create-namespace
```

After install, verify the deployment:

```bash
KUBECONFIG=/path/to/kubeconfig tools/verify-platform-health.sh
```

## Credentials

A default install has **no known login**. The `sysadmin@thingsboard.org` and
`tenant@thingsboard.org` rows are seeded, but their passwords are randomly
generated per install so that the well-known upstream hash is never shipped.

To get the demo credentials on an evaluation install, ask for them:

```bash
helm install thingsflow ./k8s/helm/thingsflow \
  --namespace thingsflow --create-namespace \
  --set flowCore.loadDemo=true
```

That seeds `tenant@thingsboard.org / tenant` and `sysadmin@thingsboard.org /
sysadmin`, and Helm refuses to render it when `production=true`. For any
install you intend to keep, reset the sysadmin password out of band instead.

The known `tenant` and `sysadmin` passwords have to be requested on the
**first** install. Their seed runs from Postgres's init directory, which only
executes against an empty data directory, so adding the flag to an existing
release does not create those passwords. The Flow Core demo dataset is a
separate, idempotent seed that can run on any opted-in boot; see
[Demo Profile](DEMO_PROFILE.md) for the local compose workflow.

## What To Read Next

- [Install](INSTALL.md) for the full Kubernetes path (production mode, secrets, OIDC).
- [Architecture](ARCHITECTURE.md) for the system model.
- [API Reference](API_REFERENCE.md) for the OpenAPI contract and generation
  strategy.
- [Data Plane](DATA_PLANE.md) for telemetry envelopes, NATS subjects, Bento
  processors, latest KV, GreptimeDB history, optional QuestDB, and alarms.
- [Security](SECURITY.md) before any exposed deployment.
- [Release](RELEASE.md) before controlled industrial use.
