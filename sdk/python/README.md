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

The planned production hardening endpoint is a device-native self-refresh API
such as `POST /api/v1/devices/me/jwt/refresh`, where a still-valid Device JWT
can be exchanged for a fresh one. That endpoint is not implemented yet, so the
SDK does not assume it exists.

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
