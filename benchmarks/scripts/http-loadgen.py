#!/usr/bin/env python3
import json
import math
import os
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import requests

TB_BASE_URL = os.environ.get("TB_BASE_URL", "http://localhost:8080").rstrip("/")
TELEMETRY_BASE_URL = os.environ.get("TELEMETRY_BASE_URL", TB_BASE_URL).rstrip("/")
TB_USER = os.environ.get("TB_USER", "tenant@thingsboard.org")
TB_PASS = os.environ.get("TB_PASS", "tenant")
DEVICE_COUNT = int(os.environ.get("LOADGEN_DEVICE_COUNT", os.environ.get("DEVICE_COUNT", "100")))
INTERVAL = float(os.environ.get("LOADGEN_INTERVAL_SECONDS", os.environ.get("PUBLISH_INTERVAL_SECONDS", "5")))
RUN_DURATION = int(os.environ.get("RUN_DURATION_SECONDS", "900"))
DEVICE_PREFIX = os.environ.get("DEVICE_PREFIX", "bench-http")
THINGSFLOW_NATIVE_EDGE = os.environ.get("THINGSFLOW_NATIVE_EDGE", "false").lower() in ("1", "true", "yes")
MAX_WORKERS = int(os.environ.get("LOADGEN_MAX_WORKERS", str(DEVICE_COUNT)))
MAX_WORKERS = max(1, min(MAX_WORKERS, DEVICE_COUNT))

_counter = 0
_counter_lock = threading.Lock()
_errors = 0
_errors_lock = threading.Lock()
_error_counts = {}
_error_samples = []
MAX_ERROR_SAMPLES = int(os.environ.get("LOADGEN_MAX_ERROR_SAMPLES", "10"))


def login():
    resp = requests.post(
        f"{TB_BASE_URL}/api/auth/login",
        json={"username": TB_USER, "password": TB_PASS},
        timeout=20,
    )
    resp.raise_for_status()
    return resp.json()["token"]


def provision(session, jwt, name):
    headers = {"X-Authorization": f"Bearer {jwt}", "Content-Type": "application/json"}
    resp = session.get(f"{TB_BASE_URL}/api/tenant/devices", params={"deviceName": name}, headers=headers, timeout=20)
    if resp.status_code == 200 and resp.text and resp.json():
        device = resp.json()
    else:
        resp = session.post(f"{TB_BASE_URL}/api/device", json={"name": name, "type": "default"}, headers=headers, timeout=20)
        resp.raise_for_status()
        device = resp.json()
    device_id = device["id"]["id"]
    if THINGSFLOW_NATIVE_EDGE:
        resp = session.post(f"{TB_BASE_URL}/api/device/{device_id}/jwt", headers=headers, timeout=20)
        resp.raise_for_status()
        issued = resp.json()
        return {"token": issued["token"], "mqtt_identity": issued["mqttIdentity"]}
    resp = session.get(f"{TB_BASE_URL}/api/device/{device_id}/credentials", headers=headers, timeout=20)
    resp.raise_for_status()
    return {"classic_token": resp.json()["credentialsId"]}


def payload(ts, idx):
    return {
        "temperature": round(20 + 5 * math.sin(ts / 30 + idx) + random.gauss(0, 0.2), 2),
        "humidity": round(50 + 10 * math.cos(ts / 45 + idx) + random.gauss(0, 0.5), 2),
        "bench_seq": int(ts),
    }


def describe_error(exc):
    if isinstance(exc, requests.HTTPError):
        response = exc.response
        status_code = response.status_code if response is not None else "unknown"
        body = response.text[:200] if response is not None and response.text else ""
        return f"http_{status_code}", {"type": "http", "status_code": status_code, "body": body}
    if isinstance(exc, requests.Timeout):
        return "timeout", {"type": "timeout", "message": str(exc)[:200]}
    if isinstance(exc, requests.ConnectionError):
        return "connection_error", {"type": "connection_error", "message": str(exc)[:200]}
    return exc.__class__.__name__, {"type": exc.__class__.__name__, "message": str(exc)[:200]}


def record_error(exc):
    global _errors
    key, sample = describe_error(exc)
    with _errors_lock:
        _errors += 1
        _error_counts[key] = _error_counts.get(key, 0) + 1
        if len(_error_samples) < MAX_ERROR_SAMPLES:
            _error_samples.append(sample)


def error_snapshot():
    with _errors_lock:
        return _errors, dict(_error_counts), list(_error_samples)


def worker(idx, credential, stop_at):
    global _counter, _errors
    session = requests.Session()
    next_send = time.time()
    while time.time() < stop_at:
        now = time.time()
        if now < next_send:
            time.sleep(min(0.25, next_send - now))
            continue
        try:
            if THINGSFLOW_NATIVE_EDGE:
                mqtt_identity = credential["mqtt_identity"]
                resp = session.post(
                    f"{TELEMETRY_BASE_URL}/api/v1/devices/{mqtt_identity}/telemetry",
                    data=json.dumps(payload(now, idx)),
                    headers={"Content-Type": "application/json", "Authorization": f"Bearer {credential['token']}"},
                    timeout=10,
                )
            else:
                resp = session.post(
                    f"{TELEMETRY_BASE_URL}/api/v1/{credential['classic_token']}/telemetry",
                    data=json.dumps(payload(now, idx)),
                    headers={"Content-Type": "application/json"},
                    timeout=10,
                )
            resp.raise_for_status()
            with _counter_lock:
                _counter += 1
        except Exception as exc:
            record_error(exc)
        next_send += INTERVAL


def main():
    print(json.dumps({"event": "start", "target": TB_BASE_URL, "telemetry_target": TELEMETRY_BASE_URL, "native_edge": THINGSFLOW_NATIVE_EDGE, "devices": DEVICE_COUNT, "max_workers": MAX_WORKERS, "interval": INTERVAL, "duration": RUN_DURATION}), flush=True)
    jwt = login()
    session = requests.Session()
    credentials = []
    for idx in range(DEVICE_COUNT):
        name = f"{DEVICE_PREFIX}-{idx + 1:05d}"
        credentials.append(provision(session, jwt, name))
        if (idx + 1) % 100 == 0:
            print(json.dumps({"event": "provisioned", "count": idx + 1}), flush=True)
    stop_at = time.time() + RUN_DURATION
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        for idx, credential in enumerate(credentials):
            pool.submit(worker, idx, credential, stop_at)
        last = 0
        while time.time() < stop_at:
            time.sleep(30)
            with _counter_lock:
                total = _counter
            errors, error_counts, error_samples = error_snapshot()
            print(json.dumps({"event": "heartbeat", "published_delta": total - last, "published_total": total, "errors": errors, "error_counts": error_counts, "error_samples": error_samples}), flush=True)
            last = total
    errors, error_counts, error_samples = error_snapshot()
    print(json.dumps({"event": "done", "published_total": _counter, "errors": errors, "error_counts": error_counts, "error_samples": error_samples}), flush=True)


if __name__ == "__main__":
    main()
