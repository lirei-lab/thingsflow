#!/usr/bin/env python3
"""
ThingsFlow instrumented benchmark.

Runs a set of load scenarios against a live ThingsFlow deployment and records,
per scenario:

  * throughput   -- messages ACCEPTED (HTTP 2xx from the ingest) vs rows LANDED
                    in GreptimeDB (the silent-drop metric)
  * resources    -- per-container CPU (millicores) and working-set memory (Mi),
                    sampled from BOTH the node cgroup tree (high fidelity, also
                    yields CFS throttling counters) and `kubectl top` (portable,
                    metrics-server resolution)
  * latency      -- client-side POST latency percentiles for every message, plus
                    a small sample of true end-to-end ingest->queryable probes
  * errors       -- non-2xx / transport failures, and pod restart deltas

Design notes / honesty:
  - Every scenario writes its telemetry under a scenario-unique key name
    (bench_<scenario>) so "landed" can be counted exactly with no time-window
    ambiguity and no overlap with the demo simulator's traffic.
  - The load generator self-reports schedule lag. If the client cannot keep its
    send schedule, that is reported explicitly so a client-side limit is never
    mistaken for a platform limit.
  - Nothing is extrapolated. Only measured values are written out.

Security posture (current): platform JWT for the control API, per-device JWT for
telemetry, POST to the Envoy ingest at /api/v1/telemetry.
"""

import argparse
import csv
import json
import multiprocessing as mp
import os
import queue
import random
import statistics
import subprocess
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import requests

# --------------------------------------------------------------------------
# configuration
# --------------------------------------------------------------------------

API_BASE = os.environ.get("TF_API_BASE", "http://<node-ip>:30081").rstrip("/")
INGEST_BASE = os.environ.get("TF_INGEST_BASE", "http://<node-ip>:30808").rstrip("/")
GREPTIME_BASE = os.environ.get("TF_GREPTIME_BASE", "http://<node-ip>:30400").rstrip("/")
TF_USER = os.environ.get("TF_USER", "tenant@thingsboard.org")
TF_PASS = os.environ.get("TF_PASS", "tenant")
KCTX = os.environ.get("TF_KUBE_CONTEXT", "microk8s")
NS = os.environ.get("TF_NAMESPACE", "thingsflow-fresh")
RELEASE = os.environ.get("TF_RELEASE", "tf")
DEVICE_PREFIX = os.environ.get("TF_DEVICE_PREFIX", "bench-inst")

SAMPLE_INTERVAL = 5.0
SETTLE_BETWEEN = 60
FLUSH_WAIT = 45  # seconds to wait after load stops before counting landed rows

CGROUP_ROOT = "/sys/fs/cgroup/kubepods"


def kubectl(*args, timeout=60):
    cmd = ["kubectl", "--context", KCTX, "-n", NS] + list(args)
    env = dict(os.environ)
    env.pop("KUBECONFIG", None)
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, env=env)


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


# --------------------------------------------------------------------------
# GreptimeDB
# --------------------------------------------------------------------------

def gsql(sql, timeout=60):
    r = requests.post(
        f"{GREPTIME_BASE}/v1/sql?db=public",
        data={"sql": sql},
        timeout=timeout,
    )
    r.raise_for_status()
    out = r.json()["output"][0]
    if "records" not in out:
        return []
    return out["records"]["rows"]


def gcount(sql):
    rows = gsql(sql)
    return int(rows[0][0]) if rows else 0


# --------------------------------------------------------------------------
# platform API
# --------------------------------------------------------------------------

def login():
    r = requests.post(
        f"{API_BASE}/api/auth/login",
        json={"username": TF_USER, "password": TF_PASS},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["token"]


def provision_one(session, jwt, name):
    h = {"X-Authorization": f"Bearer {jwt}", "Content-Type": "application/json"}
    r = session.get(f"{API_BASE}/api/tenant/devices", params={"deviceName": name}, headers=h, timeout=30)
    dev = None
    if r.status_code == 200 and r.text.strip():
        try:
            dev = r.json()
        except Exception:
            dev = None
    if not dev:
        r = session.post(f"{API_BASE}/api/device", json={"name": name, "type": "benchmark"}, headers=h, timeout=30)
        r.raise_for_status()
        dev = r.json()
    did = dev["id"]["id"]
    r = session.post(f"{API_BASE}/api/device/{did}/jwt", headers=h, timeout=30)
    r.raise_for_status()
    return {"id": did, "name": name, "jwt": r.json()["token"]}


def provision(jwt, count):
    devices = [None] * count

    def work(i):
        s = requests.Session()
        for attempt in range(3):
            try:
                devices[i] = provision_one(s, jwt, f"{DEVICE_PREFIX}-{i + 1:05d}")
                return
            except Exception as exc:  # noqa: BLE001
                if attempt == 2:
                    raise
                time.sleep(1 + attempt)

    with ThreadPoolExecutor(max_workers=16) as pool:
        list(pool.map(work, range(count)))
    return devices


def delete_devices(jwt, devices):
    h = {"X-Authorization": f"Bearer {jwt}"}
    ok, fail = 0, 0
    lock = threading.Lock()

    def work(d):
        nonlocal ok, fail
        try:
            r = requests.delete(f"{API_BASE}/api/device/{d['id']}", headers=h, timeout=30)
            with lock:
                if r.status_code < 300:
                    ok += 1
                else:
                    fail += 1
        except Exception:  # noqa: BLE001
            with lock:
                fail += 1

    with ThreadPoolExecutor(max_workers=16) as pool:
        list(pool.map(work, devices))
    return ok, fail


# --------------------------------------------------------------------------
# cgroup sampling
# --------------------------------------------------------------------------

def build_container_map():
    """pod/container name -> cgroup dir, using containerd container ids."""
    r = kubectl("get", "pods", "-o", "json")
    pods = json.loads(r.stdout)["items"]
    index = {}
    for qos in ("besteffort", "burstable", "guaranteed"):
        base = os.path.join(CGROUP_ROOT, qos)
        if not os.path.isdir(base):
            continue
        for pod_dir in os.listdir(base):
            full = os.path.join(base, pod_dir)
            if not os.path.isdir(full):
                continue
            for cid in os.listdir(full):
                cdir = os.path.join(full, cid)
                if os.path.isdir(cdir) and os.path.exists(os.path.join(cdir, "cpu.stat")):
                    index[cid] = cdir

    mapping = {}
    for p in pods:
        if p["status"].get("phase") != "Running":
            continue
        pod = p["metadata"]["name"]
        for cs in p["status"].get("containerStatuses", []) or []:
            cid = (cs.get("containerID") or "").replace("containerd://", "")
            if cid and cid in index:
                mapping[f"{pod}/{cs['name']}"] = index[cid]
    return mapping


def read_cgroup(path):
    out = {}
    try:
        with open(os.path.join(path, "cpu.stat")) as fh:
            for line in fh:
                k, _, v = line.partition(" ")
                out[k] = int(v)
    except OSError:
        return None
    try:
        with open(os.path.join(path, "memory.current")) as fh:
            out["memory_current"] = int(fh.read().strip())
        inactive_file = 0
        with open(os.path.join(path, "memory.stat")) as fh:
            for line in fh:
                k, _, v = line.partition(" ")
                if k == "inactive_file":
                    inactive_file = int(v)
                    break
        out["working_set"] = max(0, out["memory_current"] - inactive_file)
    except OSError:
        out["memory_current"] = 0
        out["working_set"] = 0
    return out


class Sampler(threading.Thread):
    """Samples cgroup + kubectl top every SAMPLE_INTERVAL seconds."""

    def __init__(self, scenario, csv_path):
        super().__init__(daemon=True)
        self.scenario = scenario
        self.csv_path = csv_path
        self.stop_evt = threading.Event()
        self.rows = []
        self.top_rows = []
        self.throttle_start = {}
        self.throttle_end = {}

    def run(self):
        cmap = build_container_map()
        prev = {}
        prev_t = None
        for key, path in cmap.items():
            snap = read_cgroup(path)
            if snap:
                self.throttle_start[key] = {
                    "nr_periods": snap.get("nr_periods", 0),
                    "nr_throttled": snap.get("nr_throttled", 0),
                    "throttled_usec": snap.get("throttled_usec", 0),
                }
                prev[key] = snap
        prev_t = time.time()

        top_thread = threading.Thread(target=self._top_loop, daemon=True)
        top_thread.start()

        while not self.stop_evt.wait(SAMPLE_INTERVAL):
            now = time.time()
            dt = now - prev_t
            if dt <= 0:
                continue
            for key, path in cmap.items():
                snap = read_cgroup(path)
                if not snap or key not in prev:
                    if snap:
                        prev[key] = snap
                    continue
                d_usec = snap["usage_usec"] - prev[key]["usage_usec"]
                millicores = (d_usec / 1e6) / dt * 1000.0
                self.rows.append({
                    "scenario": self.scenario,
                    "ts": round(now, 3),
                    "container": key,
                    "cpu_millicores": round(max(0.0, millicores), 2),
                    "working_set_mi": round(snap["working_set"] / 1048576.0, 2),
                    "mem_current_mi": round(snap["memory_current"] / 1048576.0, 2),
                    "nr_throttled": snap.get("nr_throttled", 0),
                    "throttled_usec": snap.get("throttled_usec", 0),
                })
                prev[key] = snap
            prev_t = now

        for key, path in cmap.items():
            snap = read_cgroup(path)
            if snap:
                self.throttle_end[key] = {
                    "nr_periods": snap.get("nr_periods", 0),
                    "nr_throttled": snap.get("nr_throttled", 0),
                    "throttled_usec": snap.get("throttled_usec", 0),
                }
        top_thread.join(timeout=20)
        self._write_csv()

    def _top_loop(self):
        while not self.stop_evt.is_set():
            t0 = time.time()
            try:
                r = kubectl("top", "pods", "--containers", "--no-headers", timeout=30)
                if r.returncode == 0:
                    for line in r.stdout.strip().splitlines():
                        parts = line.split()
                        if len(parts) < 4:
                            continue
                        pod, cname, cpu, mem = parts[0], parts[1], parts[2], parts[3]
                        self.top_rows.append({
                            "scenario": self.scenario,
                            "ts": round(t0, 3),
                            "container": f"{pod}/{cname}",
                            "cpu_millicores": float(cpu.rstrip("m") or 0),
                            "memory_mi": float(mem.rstrip("Mi") or 0),
                        })
            except Exception:  # noqa: BLE001
                pass
            self.stop_evt.wait(max(0.0, SAMPLE_INTERVAL - (time.time() - t0)))

    def _write_csv(self):
        with open(self.csv_path, "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=[
                "scenario", "ts", "container", "cpu_millicores",
                "working_set_mi", "mem_current_mi", "nr_throttled", "throttled_usec"])
            w.writeheader()
            w.writerows(self.rows)
        top_path = self.csv_path.replace(".csv", "-kubectltop.csv")
        with open(top_path, "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=["scenario", "ts", "container", "cpu_millicores", "memory_mi"])
            w.writeheader()
            w.writerows(self.top_rows)

    def stop(self):
        self.stop_evt.set()


def pct(values, p):
    if not values:
        return None
    s = sorted(values)
    if len(s) == 1:
        return round(s[0], 3)
    k = (len(s) - 1) * p
    f = int(k)
    c = min(f + 1, len(s) - 1)
    return round(s[f] + (s[c] - s[f]) * (k - f), 3)


def summarize(series):
    if not series:
        return None
    return {
        "n": len(series),
        "min": round(min(series), 2),
        "mean": round(statistics.fmean(series), 2),
        "p95": pct(series, 0.95),
        "max": round(max(series), 2),
    }


def aggregate_samples(rows, field):
    by = {}
    for r in rows:
        by.setdefault(r["container"], []).append(r[field])
    return {k: summarize(v) for k, v in by.items()}


# --------------------------------------------------------------------------
# load generation (multiprocess)
# --------------------------------------------------------------------------

def _worker_proc(devices, rate_hz, duration, ingest, key_name, out_q):
    """One OS process: a thread per device, each sending at rate_hz."""
    interval = 1.0 / rate_hz
    stats = {
        "accepted": 0, "errors": 0, "skipped_behind": 0, "late_sends": 0,
        "status_counts": {}, "error_samples": [], "lat": [], "lag": [],
    }
    lock = threading.Lock()
    stop_at = time.time() + duration
    lat_sample_rate = 1.0  # record every send's latency; cheap enough

    def dev_worker(idx, dev):
        s = requests.Session()
        adapter = requests.adapters.HTTPAdapter(pool_connections=2, pool_maxsize=2)
        s.mount("http://", adapter)
        h = {"Authorization": f"Bearer {dev['jwt']}", "Content-Type": "application/json"}
        url = f"{ingest}/api/v1/telemetry"
        # de-phase device start times so sends are spread across the interval
        next_send = time.time() + (idx % max(1, int(rate_hz * 10))) * (interval / 10.0)
        seq = 0
        local = {"accepted": 0, "errors": 0, "skipped": 0, "late": 0,
                 "status": {}, "lat": [], "lag": [], "errs": []}
        while True:
            now = time.time()
            if now >= stop_at:
                break
            if now < next_send:
                time.sleep(min(0.2, next_send - now))
                continue
            lag = now - next_send
            local["lag"].append(lag)
            if lag > 0.25:
                local["late"] += 1
            if lag > 2 * interval and lag > 1.0:
                # too far behind: drop the backlog rather than send a burst,
                # and record it as a CLIENT-side shortfall
                missed = int(lag / interval)
                local["skipped"] += missed
                next_send = now
            seq += 1
            body = json.dumps({
                key_name: seq,
                "bench_t": round(20 + random.gauss(0, 1.5), 2),
                "bench_h": round(50 + random.gauss(0, 3.0), 2),
            })
            t0 = time.perf_counter()
            try:
                resp = s.post(url, data=body, headers=h, timeout=10)
                dt = (time.perf_counter() - t0) * 1000.0
                local["lat"].append(dt)
                code = str(resp.status_code)
                local["status"][code] = local["status"].get(code, 0) + 1
                if 200 <= resp.status_code < 300:
                    local["accepted"] += 1
                else:
                    local["errors"] += 1
                    if len(local["errs"]) < 5:
                        local["errs"].append({"status": resp.status_code, "body": resp.text[:200]})
            except Exception as exc:  # noqa: BLE001
                local["errors"] += 1
                local["status"]["exception"] = local["status"].get("exception", 0) + 1
                if len(local["errs"]) < 5:
                    local["errs"].append({"type": type(exc).__name__, "msg": str(exc)[:200]})
            next_send += interval

        with lock:
            stats["accepted"] += local["accepted"]
            stats["errors"] += local["errors"]
            stats["skipped_behind"] += local["skipped"]
            stats["late_sends"] += local["late"]
            for k, v in local["status"].items():
                stats["status_counts"][k] = stats["status_counts"].get(k, 0) + v
            # subsample latencies to keep the IPC payload bounded
            stats["lat"].extend(local["lat"][::max(1, len(local["lat"]) // 400 or 1)])
            stats["lag"].extend(local["lag"][::max(1, len(local["lag"]) // 200 or 1)])
            stats["error_samples"].extend(local["errs"][:2])

    threads = [threading.Thread(target=dev_worker, args=(i, d), daemon=True)
               for i, d in enumerate(devices)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    cpu = os.times()
    stats["proc_cpu_seconds"] = round(cpu.user + cpu.system + cpu.children_user + cpu.children_system, 2)
    stats["error_samples"] = stats["error_samples"][:10]
    out_q.put(stats)


def run_load(devices, rate_hz, duration, key_name, procs):
    if not devices:
        time.sleep(duration)
        return {"accepted": 0, "errors": 0, "skipped_behind": 0, "late_sends": 0,
                "status_counts": {}, "error_samples": [], "lat": [], "lag": [],
                "proc_cpu_seconds": 0.0}
    procs = max(1, min(procs, len(devices)))
    chunks = [devices[i::procs] for i in range(procs)]
    q = mp.Queue()
    workers = [mp.Process(target=_worker_proc,
                          args=(c, rate_hz, duration, INGEST_BASE, key_name, q))
               for c in chunks if c]
    for w in workers:
        w.start()
    results = []
    deadline = time.time() + duration + 120
    while len(results) < len(workers) and time.time() < deadline:
        try:
            results.append(q.get(timeout=5))
        except queue.Empty:
            continue
    for w in workers:
        w.join(timeout=30)
        if w.is_alive():
            w.terminate()

    agg = {"accepted": 0, "errors": 0, "skipped_behind": 0, "late_sends": 0,
           "status_counts": {}, "error_samples": [], "lat": [], "lag": [],
           "proc_cpu_seconds": 0.0}
    for r in results:
        agg["accepted"] += r["accepted"]
        agg["errors"] += r["errors"]
        agg["skipped_behind"] += r["skipped_behind"]
        agg["late_sends"] += r["late_sends"]
        agg["proc_cpu_seconds"] += r.get("proc_cpu_seconds", 0.0)
        for k, v in r["status_counts"].items():
            agg["status_counts"][k] = agg["status_counts"].get(k, 0) + v
        agg["lat"].extend(r["lat"])
        agg["lag"].extend(r["lag"])
        agg["error_samples"].extend(r["error_samples"])
    agg["error_samples"] = agg["error_samples"][:10]
    agg["worker_processes_reported"] = len(results)
    agg["worker_processes_launched"] = len(workers)
    return agg


# --------------------------------------------------------------------------
# end-to-end latency probes
# --------------------------------------------------------------------------

class ProbeThread(threading.Thread):
    """Posts a uniquely-keyed value and polls GreptimeDB until it is visible."""

    def __init__(self, device, count, duration):
        super().__init__(daemon=True)
        self.device = device
        self.count = count
        self.duration = duration
        self.results = []
        self.stop_evt = threading.Event()

    def run(self):
        spacing = self.duration / max(1, self.count)
        h = {"Authorization": f"Bearer {self.device['jwt']}", "Content-Type": "application/json"}
        s = requests.Session()
        for i in range(self.count):
            if self.stop_evt.is_set():
                break
            marker = uuid.uuid4().hex[:12]
            t0 = time.time()
            try:
                r = s.post(f"{INGEST_BASE}/api/v1/telemetry",
                           data=json.dumps({"bench_probe": marker}),
                           headers=h, timeout=10)
                if r.status_code >= 300:
                    self.results.append({"i": i, "ok": False, "reason": f"post_{r.status_code}"})
                    self.stop_evt.wait(spacing)
                    continue
            except Exception as exc:  # noqa: BLE001
                self.results.append({"i": i, "ok": False, "reason": type(exc).__name__})
                self.stop_evt.wait(spacing)
                continue

            found_ms = None
            deadline = t0 + 60
            while time.time() < deadline and not self.stop_evt.is_set():
                try:
                    n = gcount("SELECT count(*) FROM device_telemetry_kv "
                               f"WHERE telemetry_key='bench_probe' AND value_string='{marker}'")
                    if n > 0:
                        found_ms = (time.time() - t0) * 1000.0
                        break
                except Exception:  # noqa: BLE001
                    pass
                time.sleep(0.1)
            if found_ms is None:
                self.results.append({"i": i, "ok": False, "reason": "not_visible_within_60s"})
            else:
                self.results.append({"i": i, "ok": True, "e2e_ms": round(found_ms, 1)})
            self.stop_evt.wait(max(0.0, spacing - (time.time() - t0)))

    def stop(self):
        self.stop_evt.set()


# --------------------------------------------------------------------------
# pod restart / event capture
# --------------------------------------------------------------------------

def restart_counts():
    r = kubectl("get", "pods", "-o", "json")
    out = {}
    for p in json.loads(r.stdout)["items"]:
        if p["status"].get("phase") not in ("Running", "Pending"):
            continue
        for cs in p["status"].get("containerStatuses", []) or []:
            key = f"{p['metadata']['name']}/{cs['name']}"
            last = (cs.get("lastState") or {}).get("terminated") or {}
            out[key] = {"restarts": cs.get("restartCount", 0),
                        "last_terminated_reason": last.get("reason")}
    return out


def restart_delta(before, after):
    delta = {}
    for k, v in after.items():
        b = before.get(k, {}).get("restarts", 0)
        if v["restarts"] != b:
            delta[k] = {"before": b, "after": v["restarts"],
                        "last_terminated_reason": v.get("last_terminated_reason")}
    return delta


# --------------------------------------------------------------------------
# scenario runner
# --------------------------------------------------------------------------

def run_scenario(name, devices, rate_hz, duration, outdir, procs, probe_device, probe_count=20):
    key_name = f"bench_{name}"
    log(f"=== scenario '{name}': devices={len(devices)} rate={rate_hz}/s/device "
        f"target={len(devices) * rate_hz:.0f} msg/s duration={duration}s ===")

    # clear any prior rows for this key so counting is unambiguous
    try:
        gsql(f"DELETE FROM device_telemetry_kv WHERE telemetry_key='{key_name}'")
    except Exception as exc:  # noqa: BLE001
        log(f"  (could not pre-clear {key_name}: {exc})")

    baseline_rows_before = gcount("SELECT count(*) FROM device_telemetry_kv")
    restarts_before = restart_counts()

    csv_path = os.path.join(outdir, f"samples-{name}.csv")
    sampler = Sampler(name, csv_path)
    sampler.start()
    time.sleep(SAMPLE_INTERVAL)  # let the sampler establish its cgroup baseline

    probe = ProbeThread(probe_device, probe_count, duration) if probe_device else None
    if probe:
        probe.start()

    t_start = time.time()
    load = run_load(devices, rate_hz, duration, key_name, procs)
    t_end = time.time()
    actual_duration = t_end - t_start

    if probe:
        probe.stop()
        probe.join(timeout=70)

    log(f"  load stopped; waiting {FLUSH_WAIT}s for the data plane to drain")
    time.sleep(FLUSH_WAIT)
    sampler.stop()
    sampler.join(timeout=60)

    restarts_after = restart_counts()
    baseline_rows_after = gcount("SELECT count(*) FROM device_telemetry_kv")

    landed = gcount(f"SELECT count(*) FROM device_telemetry_kv WHERE telemetry_key='{key_name}'")
    landed_devices = gcount(
        f"SELECT count(*) FROM (SELECT DISTINCT device_id FROM device_telemetry_kv "
        f"WHERE telemetry_key='{key_name}')")
    # merge check: rows sharing (device_id, value_string) would indicate the
    # GreptimeDB primary key collapsed two messages into one
    distinct_msgs = gcount(
        f"SELECT count(*) FROM (SELECT DISTINCT device_id, value_string FROM device_telemetry_kv "
        f"WHERE telemetry_key='{key_name}')")

    cpu_stats = aggregate_samples(sampler.rows, "cpu_millicores")
    mem_stats = aggregate_samples(sampler.rows, "working_set_mi")
    top_cpu = aggregate_samples(sampler.top_rows, "cpu_millicores")
    top_mem = aggregate_samples(sampler.top_rows, "memory_mi")

    throttle = {}
    for k, end in sampler.throttle_end.items():
        start = sampler.throttle_start.get(k)
        if not start:
            continue
        dp = end["nr_periods"] - start["nr_periods"]
        dt_ = end["nr_throttled"] - start["nr_throttled"]
        du = end["throttled_usec"] - start["throttled_usec"]
        if dt_ > 0:
            throttle[k] = {
                "periods": dp, "throttled_periods": dt_,
                "throttled_pct": round(100.0 * dt_ / dp, 2) if dp else None,
                "throttled_seconds": round(du / 1e6, 2),
            }

    lat = load["lat"]
    lag = load["lag"]
    probe_ok = [p["e2e_ms"] for p in (probe.results if probe else []) if p.get("ok")]

    result = {
        "scenario": name,
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(t_start)),
        "duration_seconds_target": duration,
        "duration_seconds_actual": round(actual_duration, 1),
        "devices": len(devices),
        "rate_hz_per_device": rate_hz,
        "target_msg_per_sec": round(len(devices) * rate_hz, 1),
        "telemetry_key": key_name,
        "keys_per_message": 3,
        "loadgen": {
            "processes": load.get("worker_processes_launched"),
            "processes_reported": load.get("worker_processes_reported"),
            "cpu_seconds_total": load.get("proc_cpu_seconds"),
            "cpu_cores_equivalent": round(load.get("proc_cpu_seconds", 0) / actual_duration, 2)
            if actual_duration else None,
            "late_sends": load["late_sends"],
            "skipped_behind_schedule": load["skipped_behind"],
            "send_lag_seconds": {
                "mean": round(statistics.fmean(lag), 4) if lag else None,
                "p95": pct(lag, 0.95), "max": round(max(lag), 3) if lag else None,
            },
        },
        "throughput": {
            "accepted_2xx": load["accepted"],
            "accepted_msg_per_sec": round(load["accepted"] / actual_duration, 1) if actual_duration else 0,
            "landed_rows_bench_key": landed,
            "landed_msg_per_sec": round(landed / actual_duration, 1) if actual_duration else 0,
            "silent_drop_absolute": load["accepted"] - landed,
            "silent_drop_pct": round(100.0 * (load["accepted"] - landed) / load["accepted"], 4)
            if load["accepted"] else None,
            "landed_distinct_devices": landed_devices,
            "landed_distinct_messages": distinct_msgs,
            "pk_merge_suspected_rows": landed - distinct_msgs,
            "total_table_rows_before": baseline_rows_before,
            "total_table_rows_after": baseline_rows_after,
            "total_table_rows_delta": baseline_rows_after - baseline_rows_before,
            "note": "landed counts rows for this scenario's unique telemetry key; "
                    "each accepted message writes 3 rows total (1 per key), so "
                    "landed_rows_bench_key is 1:1 with accepted messages.",
        },
        "latency": {
            "client_post_ms": {
                "samples": len(lat),
                "p50": pct(lat, 0.50), "p95": pct(lat, 0.95),
                "p99": pct(lat, 0.99), "max": round(max(lat), 2) if lat else None,
            },
            "end_to_end_ingest_to_queryable_ms": {
                "probes_attempted": len(probe.results) if probe else 0,
                "probes_ok": len(probe_ok),
                "p50": pct(probe_ok, 0.50), "p95": pct(probe_ok, 0.95),
                "max": round(max(probe_ok), 1) if probe_ok else None,
                "poll_interval_ms": 100,
                "failures": [p for p in (probe.results if probe else []) if not p.get("ok")],
            },
        },
        "errors": {
            "non_2xx_and_transport": load["errors"],
            "status_counts": load["status_counts"],
            "samples": load["error_samples"],
        },
        "restarts": {
            "changed": restart_delta(restarts_before, restarts_after),
            "before": restarts_before,
            "after": restarts_after,
        },
        "resources": {
            "source_primary": "node cgroup v2 (kubepods), 5s sampling",
            "source_secondary": "kubectl top --containers (metrics-server, 15s resolution)",
            "cgroup_cpu_millicores": cpu_stats,
            "cgroup_working_set_mi": mem_stats,
            "kubectl_top_cpu_millicores": top_cpu,
            "kubectl_top_memory_mi": top_mem,
            "cfs_throttling": throttle,
        },
        "samples_csv": os.path.basename(csv_path),
    }

    with open(os.path.join(outdir, f"scenario-{name}.json"), "w") as fh:
        json.dump(result, fh, indent=2)
    log(f"  accepted={load['accepted']} landed={landed} drop={load['accepted'] - landed} "
        f"errors={load['errors']} lat_p95={result['latency']['client_post_ms']['p95']}ms")
    return result


# --------------------------------------------------------------------------
# environment capture
# --------------------------------------------------------------------------

def capture_environment():
    env = {"captured_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    try:
        r = subprocess.run(["kubectl", "--context", KCTX, "get", "node", "-o", "json"],
                           capture_output=True, text=True, timeout=30,
                           env={k: v for k, v in os.environ.items() if k != "KUBECONFIG"})
        node = json.loads(r.stdout)["items"][0]
        env["node"] = {
            "name": node["metadata"]["name"],
            "capacity": node["status"]["capacity"],
            "allocatable": node["status"]["allocatable"],
            "kubelet": node["status"]["nodeInfo"]["kubeletVersion"],
            "os_image": node["status"]["nodeInfo"]["osImage"],
            "kernel": node["status"]["nodeInfo"]["kernelVersion"],
            "container_runtime": node["status"]["nodeInfo"].get("containerRuntimeVersion"),
            "internal_ip": next((a["address"] for a in node["status"]["addresses"]
                                 if a["type"] == "InternalIP"), None),
        }
    except Exception as exc:  # noqa: BLE001
        env["node_error"] = str(exc)

    try:
        r = subprocess.run(["kubectl", "--context", KCTX, "get", "pods", "-A", "-o", "json"],
                           capture_output=True, text=True, timeout=30,
                           env={k: v for k, v in os.environ.items() if k != "KUBECONFIG"})
        others = []
        for p in json.loads(r.stdout)["items"]:
            if p["metadata"]["namespace"] == NS:
                continue
            if p["status"].get("phase") != "Running":
                continue
            others.append(f"{p['metadata']['namespace']}/{p['metadata']['name']}")
        env["other_workloads_on_node"] = sorted(others)
    except Exception as exc:  # noqa: BLE001
        env["other_workloads_error"] = str(exc)

    try:
        r = kubectl("get", "pods", "-o", "json")
        comps = {}
        for p in json.loads(r.stdout)["items"]:
            if p["status"].get("phase") != "Running":
                continue
            for c in p["spec"]["containers"]:
                comps[f"{p['metadata']['name']}/{c['name']}"] = {
                    "image": c["image"], "resources": c.get("resources", {}),
                }
        env["thingsflow_containers"] = comps
    except Exception as exc:  # noqa: BLE001
        env["containers_error"] = str(exc)

    try:
        r = subprocess.run(["helm", "--kube-context", KCTX, "-n", NS, "get", "values", RELEASE, "-o", "json"],
                           capture_output=True, text=True, timeout=30,
                           env={k: v for k, v in os.environ.items() if k != "KUBECONFIG"})
        env["helm_values"] = json.loads(r.stdout)
        r = subprocess.run(["helm", "--kube-context", KCTX, "-n", NS, "list", "-o", "json"],
                           capture_output=True, text=True, timeout=30,
                           env={k: v for k, v in os.environ.items() if k != "KUBECONFIG"})
        env["helm_release"] = json.loads(r.stdout)
    except Exception as exc:  # noqa: BLE001
        env["helm_error"] = str(exc)

    try:
        r = kubectl("get", "pvc", "-o", "json")
        env["pvcs"] = [{
            "name": p["metadata"]["name"],
            "storage_class": p["spec"].get("storageClassName"),
            "size": p["spec"]["resources"]["requests"]["storage"],
        } for p in json.loads(r.stdout)["items"]]
    except Exception as exc:  # noqa: BLE001
        env["pvc_error"] = str(exc)

    env["loadgen_host"] = {
        "hostname": os.uname().nodename,
        "cpu_count": os.cpu_count(),
        "note": "The load generator runs on the SAME physical host as the "
                "Kubernetes node under test. Client and platform contend for the "
                "same 16 CPUs; this is a measurement caveat, not an isolated rig.",
    }
    try:
        with open("/proc/loadavg") as fh:
            env["loadgen_host"]["loadavg_at_start"] = fh.read().strip()
        with open("/proc/meminfo") as fh:
            for line in fh:
                if line.startswith("MemTotal"):
                    env["loadgen_host"]["mem_total"] = line.split()[1] + " kB"
                    break
    except OSError:
        pass
    return env


# --------------------------------------------------------------------------
# main
# --------------------------------------------------------------------------

SCENARIOS = [
    # name,       devices, rate_hz, duration_s, loadgen_procs
    ("idle",        0,   0.0, 180, 1),
    ("light",      50,   1.0, 240, 4),
    ("moderate",  200,   1.0, 240, 6),
    ("heavy",     500,   2.0, 240, 10),
]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--outdir", default=None)
    ap.add_argument("--only", default=None, help="comma-separated scenario names")
    ap.add_argument("--keep-devices", action="store_true")
    ap.add_argument("--smoke", action="store_true", help="tiny fast run to validate the harness")
    args = ap.parse_args()

    root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "results-instrumented"))
    outdir = args.outdir or os.path.join(root, time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()))
    os.makedirs(outdir, exist_ok=True)
    log(f"results -> {outdir}")

    scenarios = SCENARIOS
    if args.only:
        want = {s.strip() for s in args.only.split(",")}
        scenarios = [s for s in SCENARIOS if s[0] in want]
    if args.smoke:
        global SETTLE_BETWEEN, FLUSH_WAIT
        SETTLE_BETWEEN, FLUSH_WAIT = 10, 20
        scenarios = [(n, min(d, 10), r, 30, p) for (n, d, r, _dur, p) in scenarios]

    log("capturing environment")
    env = capture_environment()
    with open(os.path.join(outdir, "environment.json"), "w") as fh:
        json.dump(env, fh, indent=2)

    jwt = login()
    max_devices = max((s[1] for s in scenarios), default=0)
    log(f"provisioning {max_devices + 1} benchmark devices (incl. 1 latency probe device)")
    devices = provision(jwt, max_devices + 1)
    probe_device = devices[-1]
    pool_devices = devices[:-1]
    log(f"provisioned {len(devices)} devices")

    results = []
    for i, (name, ndev, rate, duration, procs) in enumerate(scenarios):
        if i > 0:
            log(f"settling {SETTLE_BETWEEN}s before '{name}'")
            time.sleep(SETTLE_BETWEEN)
        res = run_scenario(name, pool_devices[:ndev], rate, duration, outdir, procs, probe_device)
        results.append(res)

    summary = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "target": {"api": API_BASE, "ingest": INGEST_BASE, "greptime": GREPTIME_BASE,
                   "namespace": NS, "release": RELEASE, "kube_context": KCTX},
        "environment": env,
        "method": {
            "resource_sampling_interval_seconds": SAMPLE_INTERVAL,
            "settle_between_scenarios_seconds": SETTLE_BETWEEN,
            "flush_wait_before_counting_seconds": FLUSH_WAIT,
            "demo_simulator": "ENABLED throughout every scenario, including idle; "
                              "it is part of the baseline, not isolated out.",
            "landed_definition": "rows in GreptimeDB device_telemetry_kv carrying the "
                                 "scenario-unique telemetry key",
            "no_extrapolation": True,
        },
        "scenarios": {},
    }
    for r in results:
        summary["scenarios"][r["scenario"]] = {
            "duration_seconds": r["duration_seconds_actual"],
            "devices": r["devices"],
            "target_msg_per_sec": r["target_msg_per_sec"],
            "throughput": r["throughput"],
            "latency": {"client_post_ms": r["latency"]["client_post_ms"],
                        "end_to_end_ms": {k: v for k, v in
                                          r["latency"]["end_to_end_ingest_to_queryable_ms"].items()
                                          if k != "failures"}},
            "errors": {"count": r["errors"]["non_2xx_and_transport"],
                       "status_counts": r["errors"]["status_counts"]},
            "restarts_changed": r["restarts"]["changed"],
            "loadgen": r["loadgen"],
            "cfs_throttling": r["resources"]["cfs_throttling"],
            "cpu_millicores": r["resources"]["cgroup_cpu_millicores"],
            "working_set_mi": r["resources"]["cgroup_working_set_mi"],
        }

    with open(os.path.join(outdir, "summary.json"), "w") as fh:
        json.dump(summary, fh, indent=2)
    log(f"wrote {os.path.join(outdir, 'summary.json')}")

    if not args.keep_devices:
        log("deleting benchmark devices")
        ok, fail = delete_devices(jwt, devices)
        log(f"deleted {ok} devices ({fail} failures)")
        with open(os.path.join(outdir, "cleanup.json"), "w") as fh:
            json.dump({"deleted": ok, "failed": fail, "prefix": DEVICE_PREFIX}, fh, indent=2)
    else:
        log(f"devices kept (prefix {DEVICE_PREFIX})")

    print(f"\nRESULTS_DIR={outdir}")


if __name__ == "__main__":
    mp.set_start_method("fork")
    main()
