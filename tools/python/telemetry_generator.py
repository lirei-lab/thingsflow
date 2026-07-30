#!/usr/bin/env python3
#
# Copyright © 2016-2026 The Thingsboard Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
Continuous ThingsFlow telemetry generator.

Provisions N virtual devices and streams sensor data via MQTT. In native
native edge mode the generator uses provisioning-issued device JWTs and publishes
directly to the RMQTT -> NATS edge.
"""

import random
import time
import json
import math
import requests
import threading
import logging
import warnings

import os as _os
warnings.filterwarnings("ignore", category=DeprecationWarning)
_log_level_name = _os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, _log_level_name, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
log = logging.getLogger("telemetry-gen")

import os

# Atomic counters for the heartbeat aggregator — keep per-device sends
# at DEBUG and roll them up into a single INFO line every 30 s.
_publish_total = [0]
_publish_lock = threading.Lock()
_error_total = [0]
_error_counts = {}
_error_lock = threading.Lock()

TB_HOST   = os.environ.get("TB_HOST", "localhost")
TB_PORT   = int(os.environ.get("TB_PORT", 8080))
TB_USER   = os.environ.get("TB_USER", "tenant@thingsboard.org")
TB_PASS   = os.environ.get("TB_PASS", "tenant")
MQTT_HOST = os.environ.get("MQTT_HOST", "localhost")
MQTT_PORT = int(os.environ.get("MQTT_PORT", 1883))
ENABLE_SERVER_RPC = os.environ.get("ENABLE_SERVER_RPC", "false").lower() in ("1", "true", "yes")

# TLS for the MQTT broker — when enabled, paho.mqtt validates the
# broker cert against MQTT_CA_CERT (CA bundle). Used to point the
# generator at a remote TLS broker (e.g. broker.example.com:8883).
MQTT_USE_TLS = os.environ.get("MQTT_USE_TLS", "false").lower() in ("1", "true", "yes")
MQTT_CA_CERT = os.environ.get("MQTT_CA_CERT", "")
DEVICE_PREFIX = os.environ.get("DEVICE_PREFIX", "sim")
RUN_DURATION_SECONDS = int(os.environ.get("RUN_DURATION_SECONDS", "0") or "0")
ENABLE_AUX_TRAFFIC = os.environ.get("ENABLE_AUX_TRAFFIC", "true").lower() in ("1", "true", "yes")
THINGSFLOW_NATIVE_EDGE = os.environ.get("THINGSFLOW_NATIVE_EDGE", "false").lower() in ("1", "true", "yes")
MQTT_AUTH_MODE = os.environ.get(
    "MQTT_AUTH_MODE",
    "deviceJwtRaw" if THINGSFLOW_NATIVE_EDGE else "accessToken",
).strip().lower()
SKIP_PROVISIONING = os.environ.get("SKIP_PROVISIONING", "false").lower() in ("1", "true", "yes")
MQTT_TOPIC_TEMPLATE = os.environ.get("MQTT_TOPIC_TEMPLATE", "")
INCLUDE_EDGE_METADATA = os.environ.get("INCLUDE_EDGE_METADATA", "false").lower() in ("1", "true", "yes")
# Override the REST base URL for cases where TB lives behind HTTPS
# ingress (e.g. https://thingsflow.example.com). When set, takes
# precedence over the host/port/scheme composition below.
TB_BASE_URL = os.environ.get("TB_BASE_URL", "")

DEVICE_PROFILES = [
    {"name": "Temperature Sensor",   "count": 3, "interval": 5},
    {"name": "Power Meter",          "count": 2, "interval": 10},
    {"name": "HVAC Controller",      "count": 2, "interval": 8},
    {"name": "Motion Detector",      "count": 3, "interval": 3},
    {"name": "Smart Gateway",        "count": 1, "interval": 15},
]

BASE_URL = TB_BASE_URL or f"http://{TB_HOST}:{TB_PORT}"
BRIDGE_URL = os.environ.get("BRIDGE_URL", BASE_URL)
# ──────────────────────────────────────────────────────────────────────────────


def _env_int(env, name, default=0):
    raw = env.get(name, "")
    if raw == "":
        return default
    return int(raw)


def _env_float(env, name, default=0.0):
    raw = env.get(name, "")
    if raw == "":
        return default
    return float(raw)


def build_device_profiles(env=os.environ):
    """Return load profiles, optionally scaling all profiles to one interval."""
    device_count = _env_int(env, "LOADGEN_DEVICE_COUNT", _env_int(env, "DEVICE_COUNT", 0))
    interval = _env_float(env, "LOADGEN_INTERVAL_SECONDS", _env_float(env, "PUBLISH_INTERVAL_SECONDS", 0))
    if device_count <= 0 and interval <= 0:
        return [dict(profile) for profile in DEVICE_PROFILES]

    profiles = [dict(profile) for profile in DEVICE_PROFILES]
    if device_count > 0:
        base = device_count // len(profiles)
        remainder = device_count % len(profiles)
        for idx, profile in enumerate(profiles):
            profile["count"] = base + (1 if idx < remainder else 0)
    if interval > 0:
        for profile in profiles:
            profile["interval"] = interval
    return profiles


def build_device_name(prefix, profile_name, index):
    slug = profile_name.lower().replace(" ", "-")
    return f"{prefix}-{slug}-{index + 1:02d}"


def render_mqtt_topic(device_name, device_id, profile_name, mqtt_identity):
    if MQTT_TOPIC_TEMPLATE:
        profile_slug = profile_name.lower().replace(" ", "-")
        return MQTT_TOPIC_TEMPLATE.format(
            device_name=device_name,
            device_id=device_id,
            profile_name=profile_name,
            profile_slug=profile_slug,
            mqtt_identity=mqtt_identity,
        )
    return f"thingsflow/devices/{mqtt_identity}/telemetry" if THINGSFLOW_NATIVE_EDGE else "v1/devices/me/telemetry"


def record_error(key):
    with _error_lock:
        _error_total[0] += 1
        _error_counts[key] = _error_counts.get(key, 0) + 1


def error_snapshot():
    with _error_lock:
        return _error_total[0], dict(_error_counts)


def get_token():
    log.info("Authenticating with ThingsBoard...")
    for attempt in range(10):
        try:
            r = requests.post(
                f"{BASE_URL}/api/auth/login",
                json={"username": TB_USER, "password": TB_PASS},
                timeout=10
            )
            r.raise_for_status()
            token = r.json()["token"]
            log.info("Authenticated successfully.")
            return token
        except Exception as e:
            log.warning(f"Auth attempt {attempt+1}/10 failed: {e}. Retrying in 10s...")
            time.sleep(10)
    raise RuntimeError("Could not authenticate with ThingsBoard after 10 attempts.")


def provision_device(token, name):
    """Create device in ThingsBoard, return access token."""
    headers = {"X-Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    # Check if device already exists
    r = requests.get(f"{BASE_URL}/api/tenant/devices?deviceName={name}", headers=headers, timeout=10)
    if r.status_code == 200 and r.json():
        dev = r.json()
    else:
        r = requests.post(
            f"{BASE_URL}/api/device",
            json={"name": name, "type": "default"},
            headers=headers,
            timeout=10
        )
        r.raise_for_status()
        dev = r.json()
    device_id = dev["id"]["id"]
    if THINGSFLOW_NATIVE_EDGE:
        r = requests.post(f"{BASE_URL}/api/device/{device_id}/jwt", headers=headers, timeout=10)
        r.raise_for_status()
        issued = r.json()
        log.info(f"  Device '{name}' ready for native RMQTT edge")
        return issued["token"], device_id, issued["mqttIdentity"]
    # Get classic ThingsBoard credentials
    r = requests.get(f"{BASE_URL}/api/device/{device_id}/credentials", headers=headers, timeout=10)
    r.raise_for_status()
    access_token = r.json()["credentialsId"]
    log.info(f"  Device '{name}' ready")
    return access_token, device_id, ""


# ─── Telemetry generators per profile ────────────────────────────────────────
def make_temp_sensor_payload(t, idx):
    return {
        "temperature": round(20.0 + 5 * math.sin(t / 30 + idx) + random.gauss(0, 0.3), 2),
        "humidity":    round(50.0 + 10 * math.cos(t / 45 + idx) + random.gauss(0, 0.5), 2),
        "battery_pct": round(100 - (t % 3600) / 36, 1),
    }

def make_power_meter_payload(t, idx):
    base = 1000 + 500 * math.sin(t / 120 + idx)
    return {
        "active_power_w":   round(base + random.gauss(0, 20), 1),
        "reactive_power_w": round(base * 0.3 + random.gauss(0, 5), 1),
        "voltage_v":        round(220 + random.gauss(0, 1), 2),
        "current_a":        round(base / 220, 2),
        "energy_kwh":       round((t / 3600) * (base / 1000), 4),
    }

def make_hvac_payload(t, idx):
    return {
        "setpoint_c":       round(22.0 + random.gauss(0, 0.1), 1),
        "room_temp_c":      round(21.5 + 2 * math.sin(t / 60 + idx) + random.gauss(0, 0.2), 2),
        "compressor_on":    bool(math.sin(t / 90 + idx) > 0),
        "fan_speed_rpm":    random.choice([0, 400, 800, 1200]),
        "filter_hours":     int(t / 3600),
    }

def make_motion_payload(t, idx):
    triggered = random.random() < 0.08
    return {
        "motion_detected": triggered,
        "lux":             round(max(0, 300 * math.sin(math.pi * ((t % 86400) / 86400)) + random.gauss(0, 20)), 1),
        "pir_count":       int(t / 30),
    }

def make_gateway_payload(t, idx):
    return {
        "uptime_s":         int(t),
        "connected_devices": random.randint(5, 15),
        "msg_rate_per_s":   round(random.uniform(10, 80), 1),
        "cpu_pct":          round(random.uniform(5, 40), 1),
        "mem_used_mb":      round(random.uniform(128, 512), 1),
        "rssi_dbm":         random.randint(-80, -40),
    }

PAYLOAD_MAKERS = {
    "Temperature Sensor": make_temp_sensor_payload,
    "Power Meter":        make_power_meter_payload,
    "HVAC Controller":    make_hvac_payload,
    "Motion Detector":    make_motion_payload,
    "Smart Gateway":      make_gateway_payload,
}
# ──────────────────────────────────────────────────────────────────────────────


def run_device(device_name, device_id, profile_name, access_token, interval, idx, started_at, mqtt_identity=""):
    """Thread: stream telemetry for one device via MQTT."""
    import paho.mqtt.client as mqtt

    if started_at is None:
        started_at = time.time()

    # The broker pins the Client ID to the JWT `clientid` claim (the topic-safe device
    # identity) and its ACL scopes publishes to that id, so connecting as the
    # human-readable device name is refused.
    client = mqtt.Client(client_id=mqtt_identity or device_name)
    if THINGSFLOW_NATIVE_EDGE:
        if MQTT_AUTH_MODE == "devicejwtraw":
            client.username_pw_set(access_token)
        elif MQTT_AUTH_MODE == "devicejwt":
            client.username_pw_set(f"Bearer {access_token}")
        elif MQTT_AUTH_MODE == "none":
            pass
        else:
            raise ValueError(f"unsupported MQTT_AUTH_MODE for native edge: {MQTT_AUTH_MODE}")
    else:
        if MQTT_AUTH_MODE == "none":
            pass
        else:
            client.username_pw_set(access_token)
    if MQTT_USE_TLS:
        # Verify the broker cert against the supplied CA bundle. Without
        # MQTT_CA_CERT paho falls back to the system trust store, which
        # only works for publicly-issued certs (Let's Encrypt etc.).
        if MQTT_CA_CERT:
            client.tls_set(ca_certs=MQTT_CA_CERT)
        else:
            client.tls_set()
    maker = PAYLOAD_MAKERS[profile_name]
    stopping = {"value": False}

    def on_connect(c, ud, flags, rc):
        if rc == 0:
            log.info(f"[{device_name}] MQTT connected")
            if not THINGSFLOW_NATIVE_EDGE or ENABLE_AUX_TRAFFIC:
                c.subscribe("v1/devices/me/attributes/response/+")
                c.subscribe("v1/devices/me/rpc/request/+")
                c.subscribe("v1/devices/me/rpc/response/+")
        else:
            record_error(f"mqtt_connect_rc_{rc}")
            log.error(f"[{device_name}] MQTT connect failed rc={rc}")

    def on_disconnect(c, ud, rc):
        if stopping["value"]:
            log.debug(f"[{device_name}] MQTT disconnected during shutdown rc={rc}")
            return
        if rc == 0:
            log.debug(f"[{device_name}] MQTT disconnected cleanly")
            return
        log.warning(f"[{device_name}] MQTT disconnected (rc={rc}), reconnecting in 5s...")
        time.sleep(5)
        try:
            c.reconnect()
        except Exception as e:
            record_error(type(e).__name__)
            log.error(f"[{device_name}] Reconnect error: {e}")
            
    def on_message(c, ud, msg):
        topic = msg.topic
        payload = msg.payload.decode()
        if "attributes/response" in topic:
            log.debug(f"[{device_name}] attribute response: {payload}")
        elif "rpc/request" in topic:
            req_id = topic.split("/")[-1]
            log.debug(f"[{device_name}] SERVER RPC request {req_id}: {payload}")
            # Reply to the RPC
            resp_topic = f"v1/devices/me/rpc/response/{req_id}"
            resp_payload = '{"status": "ok", "message": "Simulated device received RPC"}'
            c.publish(resp_topic, resp_payload, qos=1)
            log.debug(f"[{device_name}] Sent RPC response to {resp_topic}")
        elif "rpc/response" in topic:
            req_id = topic.split("/")[-1]
            log.debug(f"[{device_name}] CLIENT RPC response {req_id}: {payload}")

    client.on_connect    = on_connect
    client.on_disconnect = on_disconnect
    client.on_message    = on_message

    for attempt in range(5):
        try:
            client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
            break
        except Exception as e:
            record_error(type(e).__name__)
            log.warning(f"[{device_name}] TCP connect failed (attempt {attempt+1}/5): {e}. Waiting 10s...")
            time.sleep(10)

    client.loop_start()
    t0 = time.time()
    count = 0
    req_id = 1

    while RUN_DURATION_SECONDS <= 0 or time.time() - started_at < RUN_DURATION_SECONDS:
        try:
            elapsed = time.time() - t0
            payload = maker(elapsed, idx)
            if INCLUDE_EDGE_METADATA:
                payload = {
                    **payload,
                    "edgeDeviceName": device_name,
                    "edgeProfile": profile_name,
                }
            msg = json.dumps(payload)
            topic = render_mqtt_topic(device_name, device_id, profile_name, mqtt_identity)
            client.publish(topic, msg, qos=1)
            
            # Send an attribute update sometimes
            if ENABLE_AUX_TRAFFIC and count % 5 == 0:
                attr_payload = {"firmware_version": f"1.0.{count}", "active": True}
                client.publish("v1/devices/me/attributes", json.dumps(attr_payload), qos=1)
                
            # Request attributes sometimes
            if ENABLE_AUX_TRAFFIC and count % 10 == 0:
                client.publish(f"v1/devices/me/attributes/request/{req_id}", '{"clientKeys":"firmware_version", "sharedKeys":"shared_config"}', qos=1)
                req_id += 1
                
            # Send Client-Side RPC sometimes
            if ENABLE_AUX_TRAFFIC and count % 15 == 0:
                client.publish(f"v1/devices/me/rpc/request/{req_id}", '{"method":"getTime","params":{}}', qos=1)
                req_id += 1
                
            # Simulate Server-Side RPC only when explicitly enabled. The
            # default load-test should not pollute bridge access logs with
            # unauthenticated or unsupported RPC calls.
            if ENABLE_SERVER_RPC and count % 25 == 0:
                def trigger_server_rpc():
                    try:
                        r = requests.post(f"{BRIDGE_URL}/api/plugins/rpc/twoway/{device_id}", json={"method": "ping", "params": {}}, timeout=5)
                        log.debug(f"[{device_name}] Triggered Server RPC via API. Bridge response: {r.status_code} {r.text}")
                    except Exception as e:
                        log.warning(f"[{device_name}] Failed to trigger Server RPC: {e}")
                threading.Thread(target=trigger_server_rpc).start()

            count += 1
            with _publish_lock:
                _publish_total[0] += 1
            # Per-device counter at DEBUG; the global summary heartbeat
            # below logs aggregate throughput at INFO.
            if count % 20 == 0:
                log.debug(f"[{device_name}] Sent {count} msgs | last: {msg[:80]}")
            time.sleep(interval)
        except Exception as e:
            record_error(type(e).__name__)
            log.error(f"[{device_name}] Error publishing: {e}")
            time.sleep(interval)

    stopping["value"] = True
    client.loop_stop()
    client.disconnect()
    log.info(f"[{device_name}] Finished after {count} telemetry publishes")


def main():
    log.info("=== ThingsFlow Continuous Telemetry Generator ===")
    log.info(f"Target: {BASE_URL}")
    log.info(f"MQTT:   {MQTT_HOST}:{MQTT_PORT}")
    log.info(f"Native edge: {THINGSFLOW_NATIVE_EDGE} | MQTT auth mode: {MQTT_AUTH_MODE}")
    log.info(f"Skip provisioning: {SKIP_PROVISIONING} | topic template: {MQTT_TOPIC_TEMPLATE or '<default>'}")

    token = "" if SKIP_PROVISIONING else get_token()

    threads = []
    global_idx = 0
    profiles = build_device_profiles()

    for profile in profiles:
        profile_name = profile["name"]
        count        = profile["count"]
        interval     = profile["interval"]
        for i in range(count):
            device_name  = build_device_name(DEVICE_PREFIX, profile_name, i)
            if SKIP_PROVISIONING:
                log.info(f"Preparing local edge device [{device_name}] without Flow Core provisioning...")
                access_token, device_id, mqtt_identity = "", device_name, device_name
            else:
                log.info(f"Provisioning [{device_name}]...")
                access_token, device_id, mqtt_identity = provision_device(token, device_name)
            t = threading.Thread(
                target=run_device,
                args=(device_name, device_id, profile_name, access_token, interval, global_idx, None, mqtt_identity),
                daemon=True,
                name=device_name
            )
            threads.append(t)
            global_idx += 1
            time.sleep(0.5)

    log.info("Launching %s device threads...", len(threads))
    for t in threads:
        t.start()

    total = sum(p["count"] for p in profiles)
    duration_msg = "until stopped" if RUN_DURATION_SECONDS <= 0 else f"for {RUN_DURATION_SECONDS}s"
    log.info("%s virtual devices streaming %s. Press Ctrl+C to stop.", total, duration_msg)

    try:
        last_total = 0
        while any(t.is_alive() for t in threads):
            time.sleep(30)
            alive = sum(1 for t in threads if t.is_alive())
            with _publish_lock:
                cur = _publish_total[0]
            delta = cur - last_total
            last_total = cur
            log.info(
                f"[Heartbeat] {alive}/{total} threads | published {delta} msgs in last 30s"
                f" ({delta/30:.1f}/s) | total {cur}"
            )
        with _publish_lock:
            final_total = _publish_total[0]
        errors, error_counts = error_snapshot()
        log.info(f"Load generation completed. Total telemetry publishes: {final_total}")
        print(
            json.dumps({
                "event": "done",
                "published_total": final_total,
                "errors": errors,
                "error_counts": error_counts,
            }),
            flush=True,
        )
    except KeyboardInterrupt:
        log.info("Shutting down...")


if __name__ == "__main__":
    main()
