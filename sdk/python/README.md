# ThingsFlow Python Device SDK

Small Python SDK for devices, gateways, and factory provisioning scripts that
use the ThingsFlow native NATS-first data plane.

The SDK keeps Flow Core out of the telemetry hot path:

- bootstrap with `POST /api/v1/provision`;
- publish native HTTP telemetry with `POST /api/v1/telemetry`;
- publish native MQTT telemetry to `thingsflow/devices/{mqttIdentity}/telemetry`;
- renew Device JWTs before expiry by re-provisioning or through a fleet
  control-plane service.

## Install

Install this directory in editable mode during development:

```bash
python -m pip install -e sdk/python
```

MQTT support is optional:

```bash
python -m pip install -e "sdk/python[mqtt]"
```

## HTTP Device

```python
from thingsflow_device_sdk import ThingsFlowDeviceClient

device = ThingsFlowDeviceClient(
    base_url="https://thingsflow.example.com",
    device_name="sensor-001",
    provision_device_key="profile-key",
    provision_device_secret="profile-secret",
    device_type="temperature",
)

device.provision()
device.publish_http({"temperature": 22.4, "humidity": 48.2})
```

## MQTT Device

```python
from thingsflow_device_sdk import ThingsFlowDeviceClient

device = ThingsFlowDeviceClient(
    base_url="https://thingsflow.example.com",
    device_name="sensor-001",
    provision_device_key="profile-key",
    provision_device_secret="profile-secret",
)

device.provision()
device.connect_mqtt("mqtt.example.com", port=1883)
device.publish_mqtt({"temperature": 22.5})
device.close()
```

The MQTT username is the compact `deviceJwt.token`, and the default telemetry
topic is `thingsflow/devices/{mqttIdentity}/telemetry`.

## JWT Renewal

The device must renew before the Device JWT expires. The SDK parses the JWT
`exp` claim and refreshes when the token is inside `refresh_margin_seconds`.

Two renewal paths work with the current platform:

1. `reprovision`: call `POST /api/v1/provision` again. Use this only when the
   provisioning credential is per-device or otherwise tightly scoped.
2. `control_plane`: call `POST /api/device/{deviceId}/jwt` with a token
   supplied by a fleet service. Do not store human user credentials on field
   devices.

The platform also provides the device-native self-refresh API
`POST /api/v1/devices/me/jwt/refresh`, where a still-valid Device JWT can be
exchanged for a fresh one. This SDK version does not use that endpoint yet, so
its renewal strategies remain re-provisioning and fleet control-plane renewal.

## Security Notes

- Always use TLS outside a trusted private network.
- Never log Device JWTs, provisioning secrets, or control-plane tokens.
- Renew before expiry; an expired Device JWT should not mint another token.
- If renewal fails because the device is suspended or revoked, stop publishing
  and return to bootstrap or fleet-service recovery.

## Live Smoke Tests

HTTP ingest:

```bash
python tools/verify-device-sdk-live.py
```

MQTT ingest, when RMQTT is only exposed inside Kubernetes:

```bash
kubectl --kubeconfig cluster.yaml -n thingsflow port-forward svc/thingsflow-rmqtt-edge 18883:1883
uv run --with paho-mqtt python tools/verify-device-sdk-mqtt-live.py
```

## Verify against a local stack

Both live verifiers also run against the local Docker Compose stack. Bring it
up first (see `docs/GETTING_STARTED.md`):

```bash
docker compose -f docker/docker-compose-nats.yml up -d --build
curl -fsS http://localhost:8082/ready   # Flow Core readiness, retry until 200
```

### HTTP verifier (needs a small local proxy)

The HTTP verifier uses a single `BRIDGE_URL` for both the control plane and
`POST /api/v1/telemetry`, but the compose stack splits those surfaces across
ports (Flow Core on `:8082`, HTTP ingest on `:8083`) while production unifies
them behind one ingress — so locally a throwaway nginx proxy mirrors that
ingress rule. Write this `nginx.conf` somewhere temporary:

```nginx
server {
    listen 80;
    location = /api/v1/telemetry {
        proxy_pass http://http-ingest:8081;
    }
    location / {
        proxy_pass http://flow-core:8080;
    }
}
```

Then run the proxy on the compose network and the verifier through it:

```bash
docker run -d --name sdk-proxy --network thingsflow-platform_default \
  -p 8090:80 -v "$PWD/nginx.conf":/etc/nginx/conf.d/default.conf:ro nginx:alpine
BRIDGE_URL=http://localhost:8090 python3 tools/verify-device-sdk-live.py
docker rm -f sdk-proxy
```

### MQTT verifier (no proxy needed)

The MQTT verifier only talks to the control plane over HTTP, so it targets the
compose ports directly. It needs `paho-mqtt`, installed here in a clean `uv`
venv with the `[mqtt]` extra:

```bash
uv venv /tmp/sdk-mqtt-venv
uv pip install --python /tmp/sdk-mqtt-venv/bin/python -e './sdk/python[mqtt]'
BRIDGE_URL=http://localhost:8082 MQTT_PORT=1883 \
  /tmp/sdk-mqtt-venv/bin/python tools/verify-device-sdk-mqtt-live.py
```

On success each verifier prints a checklist ending in
`✓ latest telemetry visible through Flow Core` followed by
`✓ cleanup attempted`, and exits 0. When you are done, tear the stack down:

```bash
docker compose -f docker/docker-compose-nats.yml down
```
