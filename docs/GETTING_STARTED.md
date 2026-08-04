# Getting Started

This page gives the shortest useful path from clone to a working ThingsFlow stack.

## Local Stack

Start the local compose stack:

```bash
docker compose -f docker/docker-compose-nats.yml up -d \
  postgres greptimedb nats nats-bootstrap flow-core rmqtt-edge \
  http-ingest-bento http-ingest nats-latest-kv nats-greptimedb \
  nats-alarms alarm-materializer
```

Run the local smoke gate:

```bash
bash tools/smoke-local.sh
```

### Where it listens

The compose file remaps the host ports, so they are not the in-container ones:

| Service | Host | Purpose |
|---|---|---|
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

The local stack is meant for development and compatibility checks. It includes
Flow Core for control-plane APIs, the data plane for native MQTT telemetry
ingress, storage, and the optional UI adapter surface used by the project
tests. In the NATS event plane, MQTT telemetry flows through RMQTT and HTTP
telemetry flows through Envoy+Bento `http-ingest`; both are materialized into
GreptimeDB, NATS KV, and alarm intents without the UI or Flow Core in the
ingest path.

## Connect A Device

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

It has to be set on the **first** install. The seed runs from Postgres's init
directory, which only executes against an empty data directory, so adding the
flag to an existing release does nothing and the login keeps failing.

## What To Read Next

- [Install](INSTALL.md) for the full Kubernetes path (production mode, secrets, OIDC).
- [Architecture](ARCHITECTURE.md) for the system model.
- [API Reference](API_REFERENCE.md) for the OpenAPI contract and generation
  strategy.
- [Data Plane](DATA_PLANE.md) for telemetry envelopes, NATS subjects, Bento
  processors, latest KV, GreptimeDB history, optional QuestDB, and alarms.
- [Security](SECURITY.md) before any exposed deployment.
- [Release](RELEASE.md) before controlled industrial use.
