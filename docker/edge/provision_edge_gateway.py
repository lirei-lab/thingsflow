#!/usr/bin/env python3
"""Provision the local Telegraf edge gateway and render telegraf.conf.

The script is an init step for docker-compose-nats.yml. It creates or reuses a
provisioning profile, obtains a short-lived Device JWT for the edge gateway,
and writes Telegraf config without printing the token.
"""

from __future__ import annotations

import json
import os
import pathlib
import sys
import time
import urllib.error
import urllib.request
from typing import Any, Dict, Optional


ROOT = pathlib.Path("/repo")
sys.path.insert(0, str(ROOT / "sdk" / "python"))

from thingsflow_device_sdk import ThingsFlowDeviceClient  # noqa: E402


FLOW_CORE_URL = os.environ.get("FLOW_CORE_URL", "http://flow-core:8080").rstrip("/")
TB_USER = os.environ.get("TB_USER", "tenant@thingsboard.org")
TB_PASS = os.environ.get("TB_PASS", "tenant")
PROFILE_NAME = os.environ.get("EDGE_PROFILE_NAME", "Edge Gateway Profile")
PROVISION_KEY = os.environ.get("EDGE_PROVISION_KEY", "edge-gateway-key")
PROVISION_SECRET = os.environ.get("EDGE_PROVISION_SECRET", "edge-gateway-secret")
DEVICE_NAME = os.environ.get("EDGE_GATEWAY_DEVICE_NAME", "edge-gateway-telegraf")
LOCAL_TOPIC_FILTER = os.environ.get("EDGE_LOCAL_TOPIC_FILTER", "edge/devices/+/telemetry")
UPSTREAM_MQTT_SERVER = os.environ.get("EDGE_UPSTREAM_MQTT_SERVER", "tcp://rmqtt-edge:1883")
UPSTREAM_HTTP_URL = os.environ.get("EDGE_UPSTREAM_HTTP_URL", "http://http-ingest:8081/api/v1/telemetry")
TELEGRAF_INTERVAL = os.environ.get("EDGE_TELEGRAF_INTERVAL", "10s")
TELEGRAF_FLUSH_INTERVAL = os.environ.get("EDGE_TELEGRAF_FLUSH_INTERVAL", "5s")
TELEGRAF_UID = int(os.environ.get("EDGE_TELEGRAF_UID", "999"))
TELEGRAF_GID = int(os.environ.get("EDGE_TELEGRAF_GID", "999"))
CONFIG_TEMPLATE = pathlib.Path(os.environ.get("TELEGRAF_TEMPLATE", "/edge/telegraf.conf.template"))
CONFIG_OUT = pathlib.Path(os.environ.get("TELEGRAF_CONFIG_OUT", "/edge-runtime/telegraf.conf"))


def request(method: str, path: str, payload: Optional[Dict[str, Any]] = None, token: str = "") -> Any:
    body = None if payload is None else json.dumps(payload).encode("utf-8")
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    if token:
        headers["X-Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(f"{FLOW_CORE_URL}{path}", data=body, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=30) as resp:
        raw = resp.read().decode("utf-8")
        return {} if not raw.strip() else json.loads(raw)


def login() -> str:
    for _ in range(60):
        try:
            body = request("POST", "/api/auth/login", {"username": TB_USER, "password": TB_PASS})
            token = body.get("token")
            if token:
                return token
        except Exception:
            time.sleep(2)
    raise RuntimeError("could not login to Flow Core")


def ensure_profile(token: str) -> None:
    payload = {
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
    }
    try:
        request("POST", "/api/deviceProfile", payload, token)
        print("edge profile ready")
    except urllib.error.HTTPError as exc:
        # If the profile/key already exists from a previous compose run, the
        # provisioning endpoint below is the real compatibility check.
        if exc.code not in {400, 409, 500}:
            raise
        print("edge profile already present or create returned non-fatal conflict")


def render_config(device_jwt: str, mqtt_identity: str) -> None:
    template = CONFIG_TEMPLATE.read_text()
    rendered = (
        template
        .replace("${EDGE_LOCAL_TOPIC_FILTER}", LOCAL_TOPIC_FILTER)
        .replace("${EDGE_UPSTREAM_MQTT_SERVER}", UPSTREAM_MQTT_SERVER)
        .replace("${EDGE_UPSTREAM_HTTP_URL}", UPSTREAM_HTTP_URL)
        .replace("${EDGE_TELEGRAF_INTERVAL}", TELEGRAF_INTERVAL)
        .replace("${EDGE_TELEGRAF_FLUSH_INTERVAL}", TELEGRAF_FLUSH_INTERVAL)
        .replace("__DEVICE_JWT__", device_jwt)
        .replace("__MQTT_IDENTITY__", mqtt_identity)
    )
    CONFIG_OUT.parent.mkdir(parents=True, exist_ok=True)
    CONFIG_OUT.write_text(rendered)
    os.chown(CONFIG_OUT, TELEGRAF_UID, TELEGRAF_GID)
    os.chmod(CONFIG_OUT, 0o600)


def main() -> int:
    token = login()
    ensure_profile(token)
    client = ThingsFlowDeviceClient(
        base_url=FLOW_CORE_URL,
        device_name=DEVICE_NAME,
        device_type="edge_gateway",
        provision_device_key=PROVISION_KEY,
        provision_device_secret=PROVISION_SECRET,
        timeout_seconds=30,
    )
    response = client.provision()
    assert response.device_jwt is not None
    render_config(response.device_jwt.token, response.device_jwt.mqtt_identity or "")
    print(f"edge gateway provisioned mqttIdentity={response.device_jwt.mqtt_identity}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
