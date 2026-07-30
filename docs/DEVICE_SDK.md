# Device SDK

ThingsFlow includes a small Python device SDK in `sdk/python`. It is intended for
real devices, industrial gateways, factory provisioning scripts, and smoke
tests that need to exercise the native data plane without going through the
ThingsBoard UI.

The SDK follows the production architecture:

```text
Device/Gateway
  -> POST /api/v1/provision              # bootstrap only
  -> RMQTT or HTTP ingest with Device JWT # telemetry hot path
  -> NATS + Bento + GreptimeDB/KV         # Flow Core is not in telemetry ingest
```

## Capabilities

- device self-provisioning with `POST /api/v1/provision`;
- Device JWT parsing and expiry tracking;
- native HTTP telemetry with `POST /api/v1/telemetry`;
- native MQTT telemetry on `thingsflow/devices/{mqttIdentity}/telemetry`;
- automatic renewal before expiry;
- renewal by re-provisioning or by a fleet/control-plane token supplier.

## Install

For local development:

```bash
python -m pip install -e sdk/python
```

MQTT publishing uses optional `paho-mqtt`:

```bash
python -m pip install -e "sdk/python[mqtt]"
```

## HTTP Example

```python
from thingsflow_device_sdk import ThingsFlowDeviceClient

device = ThingsFlowDeviceClient(
    base_url="https://thingsflow.example.com",
    device_name="sensor-001",
    device_type="temperature",
    provision_device_key="profile-key",
    provision_device_secret="profile-secret",
)

device.provision()
device.publish_http({"temperature": 22.4, "humidity": 48.2})
```

## MQTT Example

```python
from thingsflow_device_sdk import ThingsFlowDeviceClient

device = ThingsFlowDeviceClient(
    base_url="https://thingsflow.example.com",
    device_name="sensor-001",
    provision_device_key="profile-key",
    provision_device_secret="profile-secret",
)

device.provision()
device.connect_mqtt("mqtt.example.com", port=8883, tls=True)  # plaintext 1883 only inside a trusted network
device.publish_mqtt({"temperature": 22.5})
device.close()
```

The SDK sends the compact `deviceJwt.token` as the MQTT username. The topic
namespace is:

```text
thingsflow/devices/{mqttIdentity}/telemetry
```

RMQTT validates the JWT issuer, audience, expiry, signature, and topic ACL.
Changing the namespace requires a coordinated broker-ACL and device SDK
compatibility window.

## JWT Renewal Workflow

A device must renew before `deviceJwt.expiresAt`. The SDK reads the JWT `exp`
claim locally and refreshes when the remaining lifetime is below
`refresh_margin_seconds`:

```python
device = ThingsFlowDeviceClient(
    base_url="https://thingsflow.example.com",
    device_name="sensor-001",
    provision_device_key="device-specific-key",
    provision_device_secret="device-specific-secret",
    refresh_margin_seconds=300,
)
```

The current platform supports two renewal strategies:

| Strategy | How It Works | Best Use |
|---|---|---|
| `ReProvisionRenewal` | Calls `POST /api/v1/provision` again before expiry. | Pilot devices and factory flows where provisioning credentials are per-device or tightly scoped. |
| `ControlPlaneRenewal` | Calls `POST /api/device/{deviceId}/jwt` with a token supplied by a fleet service. | Managed fleets where devices do not store human/operator credentials. |

Example control-plane renewal hook:

```python
from thingsflow_device_sdk import ControlPlaneRenewal, ThingsFlowDeviceClient

def fleet_service_token() -> str:
    return fetch_short_lived_token_from_local_agent()

device = ThingsFlowDeviceClient(
    base_url="https://thingsflow.example.com",
    device_name="gateway-001",
    provision_device_key="factory-key",
    provision_device_secret="factory-secret",
    renewal_strategy=ControlPlaneRenewal(fleet_service_token),
)
```

Do not store tenant admin passwords or UI user credentials on field devices.

## Device-Native Refresh

Devices and edge gateways can exchange a still-valid Device JWT for a fresh
one without storing tenant admin credentials online:

```text
POST /api/v1/devices/me/jwt/refresh
Authorization: Bearer <still-valid-deviceJwt>
```

The endpoint validates issuer, audience, signature, expiry, tenant, and device
claims, then checks that the device is still active before issuing a new
short-lived Device JWT. If the token is already expired, the device must use a
managed fleet/control-plane renewal path or re-bootstrap according to the
site's security policy.

## Security Guidance

- Use TLS outside a trusted private network.
- Treat Device JWTs and provisioning secrets as bearer credentials.
- Keep secrets out of logs and crash reports.
- Prefer per-device provisioning credentials for pilots.
- Use a fleet service or local secure element for higher-assurance refresh.
- Stop publishing if renewal fails because the device may be suspended or
  revoked.

## Live Validation

The repository includes repeatable live smokes for both native edges.

HTTP ingest:

```bash
python tools/verify-device-sdk-live.py
```

MQTT ingest, when RMQTT is not publicly exposed:

```bash
kubectl --kubeconfig <your-kubeconfig> -n thingsflow port-forward svc/thingsflow-rmqtt-edge 18883:1883
uv run --with paho-mqtt python tools/verify-device-sdk-mqtt-live.py
```

Both tests create an ephemeral provisioning profile, use the SDK as a device,
publish telemetry, verify latest state through Flow Core, and then attempt
cleanup.

Device JWT lifecycle:

```bash
python tools/verify-device-jwt-lifecycle.py
```

This verifies that active devices can receive short-lived Device JWTs and that
suspended devices cannot receive fresh Device JWTs.
