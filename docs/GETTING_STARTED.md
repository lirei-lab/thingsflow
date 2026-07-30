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

## Default Credentials

The development tenant seeded by the chart is:

```text
tenant@thingsboard.org / tenant
```

Change this before exposing the stack outside a local or controlled test
environment.

## What To Read Next

- [Install](INSTALL.md) for the full Kubernetes path (production mode, secrets, OIDC).
- [Architecture](ARCHITECTURE.md) for the system model.
- [API Reference](API_REFERENCE.md) for the OpenAPI contract and generation
  strategy.
- [Data Plane](DATA_PLANE.md) for telemetry envelopes, NATS subjects, Bento
  processors, latest KV, GreptimeDB history, optional QuestDB, and alarms.
- [Security](SECURITY.md) before any exposed deployment.
- [Release](RELEASE.md) before controlled industrial use.
