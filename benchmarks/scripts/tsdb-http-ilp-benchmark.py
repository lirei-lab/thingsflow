#!/usr/bin/env python3
"""HTTP Influx Line Protocol benchmark for TSDB candidates.

The script intentionally uses only the Python standard library so it can run in
small Kubernetes benchmark Jobs without baking a custom image.
"""

from __future__ import annotations

import json
import os
import queue
import statistics
import threading
import time
import urllib.error
import urllib.request


def env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    return int(raw) if raw else default


TARGET = os.environ.get("TARGET_NAME", "tsdb")
WRITE_URL = os.environ["WRITE_URL"]
TABLE = os.environ.get("TABLE", f"tsdb_bench_{TARGET}").replace("-", "_")
TOTAL_POINTS = env_int("TOTAL_POINTS", 100_000)
BATCH_SIZE = env_int("BATCH_SIZE", 5_000)
WORKERS = env_int("WORKERS", 4)
DEVICES = env_int("DEVICES", 1_000)
KEYS = [k.strip() for k in os.environ.get("SERIES_KEYS", "temperature,co2,iaq,lux").split(",") if k.strip()]
TENANT_ID = os.environ.get("TENANT_ID", "aaaaaaaa-1dd2-11b2-8080-808080808080")
TIMEOUT_SECONDS = env_int("TIMEOUT_SECONDS", 30)


def build_line(seq: int) -> str:
    device = seq % DEVICES
    key = KEYS[seq % len(KEYS)]
    ts_ms = int(time.time() * 1000) + seq
    if key == "temperature":
        value = 18.0 + ((seq % 1200) / 100.0)
    elif key == "co2":
        value = 380.0 + float(seq % 900)
    elif key == "iaq":
        value = float(100 - (seq % 60))
    else:
        value = float(seq % 1200)
    return (
        f"{TABLE},tenant_id={TENANT_ID},device_id=device_{device:06d},telemetry_key={key} "
        f"value={value},seq={seq}i {ts_ms}"
    )


def post_batch(lines: list[str]) -> tuple[float, int | None, str | None]:
    body = ("\n".join(lines) + "\n").encode("utf-8")
    request = urllib.request.Request(
        WRITE_URL,
        data=body,
        method="POST",
        headers={"Content-Type": "text/plain"},
    )
    start = time.perf_counter()
    try:
        with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
            elapsed_ms = (time.perf_counter() - start) * 1000.0
            status = int(response.status)
            if 200 <= status < 300:
                return elapsed_ms, status, None
            return elapsed_ms, status, response.read(2048).decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        return elapsed_ms, int(exc.code), exc.read(2048).decode("utf-8", errors="replace")
    except Exception as exc:  # noqa: BLE001 - printed into benchmark evidence
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        return elapsed_ms, None, repr(exc)


def worker(work: "queue.Queue[int | None]", latencies: list[float], errors: list[dict[str, object]], lock: threading.Lock) -> None:
    while True:
        start_seq = work.get()
        if start_seq is None:
            return
        end_seq = min(start_seq + BATCH_SIZE, TOTAL_POINTS)
        lines = [build_line(seq) for seq in range(start_seq, end_seq)]
        elapsed_ms, status, error = post_batch(lines)
        with lock:
            latencies.append(elapsed_ms)
            if error:
                errors.append({"batch_start": start_seq, "status": status, "error": error})


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, round((pct / 100.0) * (len(ordered) - 1)))
    return ordered[index]


def main() -> None:
    work: "queue.Queue[int | None]" = queue.Queue()
    for start_seq in range(0, TOTAL_POINTS, BATCH_SIZE):
        work.put(start_seq)
    for _ in range(WORKERS):
        work.put(None)

    latencies: list[float] = []
    errors: list[dict[str, object]] = []
    lock = threading.Lock()
    threads = [
        threading.Thread(target=worker, args=(work, latencies, errors, lock), daemon=True)
        for _ in range(WORKERS)
    ]

    started = time.perf_counter()
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    elapsed = time.perf_counter() - started

    acked_points = 0 if errors else TOTAL_POINTS
    result = {
        "event": "done",
        "target": TARGET,
        "table": TABLE,
        "write_url": WRITE_URL,
        "total_points": TOTAL_POINTS,
        "acked_points": acked_points,
        "batch_size": BATCH_SIZE,
        "workers": WORKERS,
        "devices": DEVICES,
        "elapsed_seconds": round(elapsed, 3),
        "points_per_second": round(acked_points / elapsed, 1) if elapsed else 0,
        "batch_latency_ms_avg": round(statistics.fmean(latencies), 2) if latencies else 0,
        "batch_latency_ms_p50": round(percentile(latencies, 50), 2),
        "batch_latency_ms_p95": round(percentile(latencies, 95), 2),
        "batch_latency_ms_p99": round(percentile(latencies, 99), 2),
        "errors": errors[:20],
        "error_count": len(errors),
    }
    print(json.dumps(result, sort_keys=True), flush=True)
    raise SystemExit(1 if errors else 0)


if __name__ == "__main__":
    main()
