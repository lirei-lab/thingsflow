#!/usr/bin/env python3
"""Live smoke for the ThingsFlow Python Device SDK.

The script creates an ephemeral provisioning profile through the control plane,
uses the SDK as a device, publishes native HTTP telemetry, verifies that latest
state is readable through Flow Core, and cleans up the temporary entities.

It intentionally avoids printing JWTs, provisioning secrets, or user tokens.
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
TB_USER = os.environ.get("TB_USER", "tenant@thingsboard.org")
TB_PASS = os.environ.get("TB_PASS", "tenant")
RUN_ID = os.environ.get("RUN_ID", f"{time.strftime('%Y%m%d%H%M%S', time.gmtime())}-{os.getpid()}")
PROFILE_NAME = os.environ.get("PROFILE_NAME", f"sdk-live-profile-{RUN_ID}")
PROVISION_KEY = os.environ.get("PROVISION_KEY", f"sdk-live-key-{RUN_ID}")
PROVISION_SECRET = os.environ.get("PROVISION_SECRET", f"sdk-live-secret-{RUN_ID}")
DEVICE_NAME = os.environ.get("DEVICE_NAME", f"sdk-live-device-{RUN_ID}")
TELEMETRY_KEY = os.environ.get("TELEMETRY_KEY", "sdkLiveSmoke")
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
    global token
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
    print("DEVICE SDK LIVE SMOKE")
    print(f"bridge={BRIDGE_URL} user={TB_USER} device={DEVICE_NAME}")

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
            device_type="sdk_live",
            provision_device_key=PROVISION_KEY,
            provision_device_secret=PROVISION_SECRET,
            timeout_seconds=30,
        )
        provisioning = sdk.provision()
        device_id = provisioning.device_id or ""
        if not device_id or not provisioning.device_jwt:
            raise RuntimeError("SDK provisioning did not return device_id and device_jwt")
        print(f"  ✓ SDK provisioned device mqttIdentity={provisioning.device_jwt.mqtt_identity}")

        sdk.publish_http({TELEMETRY_KEY: TELEMETRY_VALUE})
        print("  ✓ SDK published native HTTP telemetry")

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
        cleanup()
        print("  ✓ cleanup attempted")


if __name__ == "__main__":
    raise SystemExit(main())
