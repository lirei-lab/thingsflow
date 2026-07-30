#!/usr/bin/env python3
"""
MQTT ingest benchmark for flow-core / tb-node comparison.

Spawns N concurrent device sessions, each publishing M telemetry messages
at a target rate. Measures:
  - publish rate sustained (msg/s)
  - end-to-end latency: client publish ts -> row landed in QuestDB
  - RAM/CPU of the target broker container

Usage:
    bench-mqtt.py --target flow      # native RMQTT edge (port 1883)
    bench-mqtt.py --target tb       # classic tb-node (port 11883)

Requires devices already provisioned via the bridge (flow-core uses
device_credentials.credentials_id as MQTT username; tb-node same schema).
"""

import argparse
import json
import os
import statistics
import sys
import threading
import time

import paho.mqtt.client as mqtt
import psycopg2
import requests


def login(bridge_url, user, pwd):
    r = requests.post(f"{bridge_url}/api/auth/login",
                      json={"username": user, "password": pwd}, timeout=5)
    r.raise_for_status()
    return r.json()["token"]


def provision(bridge_url, token, name):
    r = requests.post(f"{bridge_url}/api/device",
                      headers={"X-Authorization": f"Bearer {token}",
                               "Content-Type": "application/json"},
                      json={"name": name, "type": "default"}, timeout=5)
    r.raise_for_status()
    dev_id = r.json()["id"]["id"]
    r = requests.get(f"{bridge_url}/api/device/{dev_id}/credentials",
                     headers={"X-Authorization": f"Bearer {token}"}, timeout=5)
    r.raise_for_status()
    return dev_id, r.json()["credentialsId"]


def publish_loop(host, port, access_token, n_msgs, target_rate, results, idx):
    sent_at = []
    ok = 0
    err = 0
    interval = 1.0 / target_rate if target_rate > 0 else 0

    cli = mqtt.Client(client_id=f"bench-{idx}-{os.getpid()}")
    cli.username_pw_set(access_token)
    try:
        cli.connect(host, port, keepalive=30)
    except Exception as e:
        results[idx] = {"err": f"connect: {e}", "ok": 0, "sent_at": []}
        return
    cli.loop_start()

    t0 = time.time()
    for i in range(n_msgs):
        ts = time.time()
        payload = json.dumps({
            "bench_idx": idx,
            "bench_seq": i,
            "bench_publish_ms": int(ts * 1000),
            "lux": 100 + (i % 50),
        })
        info = cli.publish("v1/devices/me/telemetry", payload, qos=1)
        # Wait for PUBACK so QoS1 actually delivers (default in-flight=20)
        info.wait_for_publish(timeout=5)
        if info.rc == mqtt.MQTT_ERR_SUCCESS:
            ok += 1
            sent_at.append(ts)
        else:
            err += 1
        if interval > 0:
            sleep_for = (t0 + (i + 1) * interval) - time.time()
            if sleep_for > 0:
                time.sleep(sleep_for)

    cli.loop_stop()
    cli.disconnect()
    results[idx] = {"ok": ok, "err": err, "sent_at": sent_at}


def measure_questdb_latency(qdb_url, device_ids, since_ms):
    # Compare bench_publish_ms (in payload) vs server-side timestamp
    # Note: QuestDB stores `timestamp` from ILP write — server clock.
    rows = []
    for did in device_ids:
        q = (f"SELECT timestamp FROM device_telemetry "
             f"WHERE device_id='{did}' AND timestamp > '{since_ms}'")
        try:
            r = requests.get(f"{qdb_url}/exec",
                             params={"query": q}, timeout=10)
            data = r.json()
            for row in data.get("dataset", []):
                rows.append(row[0])
        except Exception:
            pass
    return rows


def docker_stats(container):
    import subprocess
    try:
        out = subprocess.check_output(
            ["docker", "stats", "--no-stream", "--format",
             "{{.MemUsage}} {{.CPUPerc}}", container], timeout=5).decode()
        return out.strip()
    except Exception as e:
        return f"err: {e}"


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--target", choices=["flow", "tb"], required=True)
    p.add_argument("--devices", type=int, default=5)
    p.add_argument("--msgs", type=int, default=200)
    p.add_argument("--rate", type=float, default=10.0,
                   help="msgs per device per second")
    p.add_argument("--bridge", default="http://localhost:8082")
    p.add_argument("--qdb", default="http://localhost:9000")
    p.add_argument("--user", default="tenant@thingsboard.org")
    p.add_argument("--password", default="tenant")
    args = p.parse_args()

    if args.target == "flow":
        host, port = "localhost", 21883
        container = "docker-flow-core-1"
    else:
        host, port = "localhost", 11883
        container = "docker-thingsboard-1"

    print(f"→ login + provisioning {args.devices} devices via bridge {args.bridge}")
    token = login(args.bridge, args.user, args.password)
    devices = []
    for i in range(args.devices):
        name = f"bench-{args.target}-{int(time.time())}-{i}"
        dev_id, dev_token = provision(args.bridge, token, name)
        devices.append((dev_id, dev_token))
    print(f"  ✓ {len(devices)} devices provisioned")

    print(f"→ baseline {container} stats: {docker_stats(container)}")

    n_total = args.devices * args.msgs
    print(f"→ publishing {args.devices}×{args.msgs} = {n_total} msgs "
          f"@ {args.rate}/s/device → MQTT {host}:{port}")

    t_start = time.time()
    since_ms = int(t_start * 1000) - 1000
    threads = []
    results = [None] * args.devices
    for i, (_, dev_token) in enumerate(devices):
        t = threading.Thread(target=publish_loop,
                             args=(host, port, dev_token, args.msgs, args.rate, results, i))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    t_pub_done = time.time()
    pub_secs = t_pub_done - t_start

    # Wait for landing
    time.sleep(3)
    t_land = time.time()
    print(f"→ post-load {container} stats: {docker_stats(container)}")

    ok = sum(r["ok"] for r in results if r)
    err = sum(r["err"] for r in results if r)
    print(f"→ results: ok={ok} err={err} "
          f"in {pub_secs:.2f}s → {ok/pub_secs:.1f} msg/s sustained")

    # Count what landed in QuestDB by device_id (timestamp filter is finicky)
    total_landed = 0
    per_dev = []
    for dev_id, _ in devices:
        try:
            r = requests.get(f"{args.qdb}/exec",
                             params={"query": f"SELECT count() FROM device_telemetry WHERE device_id='{dev_id}'"},
                             timeout=5)
            cnt = r.json()["dataset"][0][0]
            per_dev.append((dev_id, cnt))
            total_landed += cnt
        except Exception:
            pass
    print(f"→ QuestDB landed: {total_landed}/{ok} rows "
          f"({100*total_landed/max(ok,1):.1f}% delivery)")
    for dev_id, cnt in per_dev:
        print(f"    {dev_id[:8]}: {cnt} rows")


if __name__ == "__main__":
    main()
