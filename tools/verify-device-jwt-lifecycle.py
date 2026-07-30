#!/usr/bin/env python3
"""Live lifecycle smoke for ThingsFlow Device JWT issuing.

The smoke verifies that an active device can receive a Device JWT and that a
suspended device cannot receive a fresh Device JWT. It avoids printing tokens or
provisioning secrets.
"""

from __future__ import annotations

import json
import os
import pathlib
import ssl
import sys
import time
import urllib.error
import urllib.request
from typing import Any, Dict, Optional, Tuple


ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "sdk" / "python"))

from thingsflow_device_sdk import ThingsFlowDeviceClient, ThingsFlowDeviceError  # noqa: E402


BRIDGE_URL = os.environ.get("BRIDGE_URL", "http://localhost:8080").rstrip("/")
TB_USER = os.environ.get("TB_USER", "tenant@thingsboard.org")
TB_PASS = os.environ.get("TB_PASS", "tenant")
RUN_ID = os.environ.get("RUN_ID", f"{time.strftime('%Y%m%d%H%M%S', time.gmtime())}-{os.getpid()}")
PROFILE_NAME = os.environ.get("PROFILE_NAME", f"jwt-life-profile-{RUN_ID}")
PROVISION_KEY = os.environ.get("PROVISION_KEY", f"jwt-life-key-{RUN_ID}")
PROVISION_SECRET = os.environ.get("PROVISION_SECRET", f"jwt-life-secret-{RUN_ID}")
DEVICE_NAME = os.environ.get("DEVICE_NAME", f"jwt-life-device-{RUN_ID}")
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
) -> Tuple[int, Any]:
    body = None if payload is None else json.dumps(payload).encode("utf-8")
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    if auth:
        headers["X-Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(f"{BRIDGE_URL}{path}", data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=30, context=ctx) as resp:
            raw = resp.read().decode("utf-8")
            return resp.status, {} if not raw.strip() else json.loads(raw)
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        try:
            parsed: Any = json.loads(raw) if raw.strip() else {}
        except json.JSONDecodeError:
            parsed = raw
        return exc.code, parsed


def expect(status: int, method: str, path: str, payload: Optional[Dict[str, Any]] = None, *, auth: bool = True) -> Any:
    code, body = request(method, path, payload, auth=auth)
    if code != status:
        raise RuntimeError(f"{method} {path} returned HTTP {code}, expected {status}: {redact(body)}")
    return body


def redact(value: Any) -> Any:
    if isinstance(value, dict):
        redacted: Dict[str, Any] = {}
        for key, item in value.items():
            lower = key.lower()
            if lower in {"token", "mqttusername", "credentialvalue", "credentialsvalue"}:
                redacted[key] = "<redacted>"
            else:
                redacted[key] = redact(item)
        return redacted
    if isinstance(value, list):
        return [redact(item) for item in value]
    return value


def cleanup() -> None:
    if not token:
        return
    for path in [f"/api/device/{device_id}" if device_id else "", f"/api/deviceProfile/{profile_id}" if profile_id else ""]:
        if not path:
            continue
        request("DELETE", path)


def main() -> int:
    global token, profile_id, device_id
    print("DEVICE JWT LIFECYCLE SMOKE")
    print(f"bridge={BRIDGE_URL} user={TB_USER} device={DEVICE_NAME}")

    try:
        login = expect(
            200,
            "POST",
            "/api/auth/login",
            {"username": TB_USER, "password": TB_PASS},
            auth=False,
        )
        token = str(login.get("token") or "")
        if not token:
            raise RuntimeError("login did not return token")
        print("  ✓ login")

        profile = expect(
            200,
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
            device_type="jwt_lifecycle",
            provision_device_key=PROVISION_KEY,
            provision_device_secret=PROVISION_SECRET,
            timeout_seconds=30,
        )
        provisioning = sdk.provision()
        device_id = provisioning.device_id or ""
        if not device_id or not provisioning.device_jwt:
            raise RuntimeError("SDK provisioning did not return device_id and device_jwt")
        print("  ✓ active device received initial Device JWT")

        active_jwt = expect(200, "POST", f"/api/device/{device_id}/jwt", {})
        if not active_jwt.get("token"):
            raise RuntimeError("active Device JWT renewal did not return token")
        print("  ✓ active device can renew Device JWT")

        suspended = expect(200, "POST", f"/api/device/{device_id}/security", {"securityStatus": "SUSPENDED"})
        if suspended.get("securityStatus") != "SUSPENDED":
            raise RuntimeError("device did not enter SUSPENDED state")
        print("  ✓ device suspended")

        code, body = request("POST", f"/api/device/{device_id}/jwt", {}, auth=True)
        if code not in {403, 409}:
            raise RuntimeError(
                f"suspended device received/attempted JWT renewal with HTTP {code}: {redact(body)}"
            )
        print("  ✓ suspended device cannot renew Device JWT")
        return 0
    except (RuntimeError, ThingsFlowDeviceError) as exc:
        print(f"  ✗ {exc}", file=sys.stderr)
        return 1
    finally:
        cleanup()
        print("  ✓ cleanup attempted")


if __name__ == "__main__":
    raise SystemExit(main())
