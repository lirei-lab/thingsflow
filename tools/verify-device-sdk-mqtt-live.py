#!/usr/bin/env python3
"""Live MQTT smoke for the ThingsFlow Python Device SDK.

Prerequisite when RMQTT is not publicly exposed:

    kubectl --kubeconfig cluster.yaml -n thingsflow port-forward svc/thingsflow-rmqtt-edge 18883:1883

The script creates an ephemeral provisioning profile, uses the SDK as a native
MQTT device, publishes telemetry through RMQTT, verifies latest state through
Flow Core, and cleans up the temporary entities. It does not print tokens or
secrets.
"""

from __future__ import annotations

import json
import os
import pathlib
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, Optional


ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "sdk" / "python"))

from thingsflow_device_sdk import ThingsFlowDeviceClient, ThingsFlowDeviceError  # noqa: E402


BRIDGE_URL = os.environ.get("BRIDGE_URL", "http://localhost:8080").rstrip("/")
MQTT_HOST = os.environ.get("MQTT_HOST", "127.0.0.1")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "18883"))
TB_USER = os.environ.get("TB_USER", "tenant@thingsboard.org")
TB_PASS = os.environ.get("TB_PASS", "tenant")
RUN_ID = os.environ.get("RUN_ID", f"{time.strftime('%Y%m%d%H%M%S', time.gmtime())}-{os.getpid()}")
PROFILE_NAME = os.environ.get("PROFILE_NAME", f"sdk-mqtt-profile-{RUN_ID}")
PROVISION_KEY = os.environ.get("PROVISION_KEY", f"sdk-mqtt-key-{RUN_ID}")
PROVISION_SECRET = os.environ.get("PROVISION_SECRET", f"sdk-mqtt-secret-{RUN_ID}")
DEVICE_NAME = os.environ.get("DEVICE_NAME", f"sdk-mqtt-device-{RUN_ID}")
TELEMETRY_KEY = os.environ.get("TELEMETRY_KEY", "sdkMqttSmoke")
TELEMETRY_VALUE = int(os.environ.get("TELEMETRY_VALUE", str(int(time.time()) % 100000)))
POLL_SECONDS = int(os.environ.get("POLL_SECONDS", "60"))
SKIP_TLS_VERIFY = os.environ.get("SKIP_TLS_VERIFY", "false").lower() in {"1", "true", "yes"}


ctx = ssl._create_unverified_context() if SKIP_TLS_VERIFY else None
token = ""
profile_id = ""
device_id = ""


def request(
    method: str,
    path: str,
    payload: Optional[Dict[str, Any]] = None,
    *,
    auth: bool = True,
) -> Any:
    body = None if payload is None else json.dumps(payload).encode("utf-8")
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    if auth:
        headers["X-Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(f"{BRIDGE_URL}{path}", data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=30, context=ctx) as resp:
            raw = resp.read().decode("utf-8")
            return {} if not raw.strip() else json.loads(raw)
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"{method} {path} failed HTTP {exc.code}: {raw}") from exc


def cleanup() -> None:
    if not token:
        return
    for path in [f"/api/device/{device_id}" if device_id else "", f"/api/deviceProfile/{profile_id}" if profile_id else ""]:
        if not path:
            continue
        try:
            request("DELETE", path)
        except Exception:
            pass


def main() -> int:
    global token, profile_id, device_id
    print("DEVICE SDK MQTT LIVE SMOKE")
    print(f"bridge={BRIDGE_URL} mqtt={MQTT_HOST}:{MQTT_PORT} user={TB_USER} device={DEVICE_NAME}")

    sdk: Optional[ThingsFlowDeviceClient] = None
    try:
        login = request(
            "POST",
            "/api/auth/login",
            {"username": TB_USER, "password": TB_PASS},
            auth=False,
        )
        token = str(login.get("token") or "")
        if not token:
            raise RuntimeError("login did not return token")
        print("  ✓ login")

        profile = request(
            "POST",
            "/api/deviceProfile",
            {
                "name": PROFILE_NAME,
                "type": "DEFAULT",
                "transportType": "DEFAULT",
                "provisionType": "ALLOW_CREATE_NEW_DEVICES",
                "provisionDeviceKey": PROVISION_KEY,
                "provisionDeviceSecret": PROVISION_SECRET,
                "profileData": {
                    "configuration": {"type": "DEFAULT"},
                    "transportConfiguration": {"type": "DEFAULT"},
                    "alarms": [],
                },
            },
        )
        profile_id = str(profile.get("id", {}).get("id") or profile.get("id") or "")
        if not profile_id:
            raise RuntimeError("device profile create did not return id")
        print("  ✓ provisioning profile created")

        sdk = ThingsFlowDeviceClient(
            base_url=BRIDGE_URL,
            device_name=DEVICE_NAME,
            device_type="sdk_mqtt_live",
            provision_device_key=PROVISION_KEY,
            provision_device_secret=PROVISION_SECRET,
            timeout_seconds=30,
        )
        provisioning = sdk.provision()
        device_id = provisioning.device_id or ""
        if not device_id or not provisioning.device_jwt:
            raise RuntimeError("SDK provisioning did not return device_id and device_jwt")
        print(f"  ✓ SDK provisioned device mqttIdentity={provisioning.device_jwt.mqtt_identity}")

        sdk.connect_mqtt(MQTT_HOST, port=MQTT_PORT)
        print("  ✓ SDK connected to RMQTT")
        sdk.publish_mqtt({TELEMETRY_KEY: TELEMETRY_VALUE})
        print("  ✓ SDK published native MQTT telemetry")

        encoded_key = urllib.parse.quote(TELEMETRY_KEY)
        latest_value = ""
        for _ in range(POLL_SECONDS):
            latest = request(
                "GET",
                f"/api/plugins/telemetry/DEVICE/{device_id}/values/timeseries?keys={encoded_key}&useStrictDataTypes=true",
            )
            try:
                latest_value = str(latest[TELEMETRY_KEY][0]["value"])
            except (KeyError, IndexError, TypeError):
                latest_value = ""
            if latest_value == str(TELEMETRY_VALUE):
                break
            time.sleep(1)
        if latest_value != str(TELEMETRY_VALUE):
            raise RuntimeError(f"latest telemetry mismatch: got {latest_value!r}, expected {TELEMETRY_VALUE!r}")
        print("  ✓ latest telemetry visible through Flow Core")
        return 0
    except (RuntimeError, ThingsFlowDeviceError) as exc:
        print(f"  ✗ {exc}", file=sys.stderr)
        return 1
    finally:
        if sdk is not None:
            sdk.close()
        cleanup()
        print("  ✓ cleanup attempted")


if __name__ == "__main__":
    raise SystemExit(main())
