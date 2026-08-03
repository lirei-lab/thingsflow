#!/usr/bin/env python3
"""loadgen2 -- an open-loop, CPU-aware IoT load generator.

Why this exists
---------------
The previous generator (`benchmarks/scripts/instrumented-bench.py`) is a
closed-loop client: one request in flight per device thread, the next send waits
for the previous response. When server latency rises such a client silently
lowers the load it offers, and then reports the resulting throughput as if it
were the platform's limit. A measured example: a 1000 msg/s target that actually
offered ~385 msg/s and reported 234 msg/s as a platform number.

loadgen2 fixes that in three ways:

  1. **Open loop.** Sends are scheduled off wall time. A slow server grows the
     measured *lag*; it does not shrink the offered rate.
  2. **It confesses.** Every message the client could not place on the wire is
     counted (`schedule_deficit`, `blocked_inflight`, `behind_schedule`,
     `max_lag_ms`) and the report carries an explicit
     `client_was_bottleneck` verdict with reasons.
  3. **It knows its own ceiling.** `calibrate` ramps the rate against a trivial
     local sink (or in `--dry-run`, against nothing) and reports the maximum
     rate THIS host can generate, and the CPU it took. Any platform number above
     that ceiling is not a platform number.

Usage
-----
    ./run.sh calibrate --protocol mqtt --devices 200 --max-workers 4
    ./run.sh run --target thingsflow --protocol mqtt \
        --devices 100 --rate 500 --duration 60 --verify-landed --cleanup
    ./run.sh run --target thingsboard --protocol http \
        --api-base http://host:8080 --ingest-base http://host:8080 ...
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import multiprocessing as mp
import os
import queue as _queue
import socket
import sys
import threading
import time
import uuid

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from senders import WorkerConfig, worker_main  # noqa: E402
from stats import Histogram  # noqa: E402
from targets import build_target  # noqa: E402

try:
    import psutil
except ImportError:  # pragma: no cover
    psutil = None


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def iso(epoch):
    return dt.datetime.fromtimestamp(epoch, dt.timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# CPU accounting for the generator itself
# ---------------------------------------------------------------------------


class CpuSampler(threading.Thread):
    """Samples the generator's own process tree.

    The generator shares a 16-core box with the system under test, so 'the
    generator was not starving the SUT' is a claim that has to be measured, not
    asserted. Both a peak and a mean are reported; the mean is cross-checked
    against the exact per-worker CPU-seconds the workers self-report.
    """

    def __init__(self, interval=0.5):
        super().__init__(daemon=True)
        self.interval = interval
        self.samples = []
        self.host_samples = []
        self._stopev = threading.Event()

    def run(self):
        if psutil is None:
            return
        me = psutil.Process()
        procs = {}
        psutil.cpu_percent(interval=None)
        while not self._stopev.is_set():
            try:
                for p in [me] + me.children(recursive=True):
                    if p.pid not in procs:
                        procs[p.pid] = p
                        p.cpu_percent(interval=None)
                total = 0.0
                dead = []
                for pid, p in procs.items():
                    try:
                        total += p.cpu_percent(interval=None)
                    except Exception:  # noqa: BLE001
                        dead.append(pid)
                for pid in dead:
                    procs.pop(pid, None)
                self.samples.append(total / 100.0)
                self.host_samples.append(psutil.cpu_percent(interval=None) / 100.0)
            except Exception:  # noqa: BLE001
                pass
            self._stopev.wait(self.interval)

    def stop(self):
        self._stopev.set()
        self.join(timeout=3)

    def report(self):
        if not self.samples:
            return {"available": False, "note": "psutil not installed"}
        s = [x for x in self.samples[1:]] or self.samples
        h = [x for x in self.host_samples[1:]] or self.host_samples
        return {
            "available": True,
            "cores_mean": round(sum(s) / len(s), 3),
            "cores_peak": round(max(s), 3),
            "samples": len(s),
            "host_all_cores_mean": round(sum(h) / len(h), 3) if h else None,
            "host_all_cores_peak": round(max(h), 3) if h else None,
        }


# ---------------------------------------------------------------------------
# run one measurement
# ---------------------------------------------------------------------------


def execute_run(cfg_template: WorkerConfig, devices, workers: int, total_inflight: int):
    """Fork `workers` processes, run one open-loop measurement, merge results."""
    ctx = mp.get_context("fork")
    shards = [devices[i::workers] for i in range(workers)]
    shards = [s for s in shards if s]
    workers = len(shards)
    barrier = ctx.Barrier(workers + 1)
    out_q = ctx.Queue()
    procs = []
    n_dev = len(devices)
    for i, shard in enumerate(shards):
        wcfg = WorkerConfig(**{**cfg_template.__dict__})
        wcfg.rate = cfg_template.rate * len(shard) / n_dev
        wcfg.max_inflight = max(1, total_inflight // workers)
        p = ctx.Process(target=worker_main, args=(wcfg, shard, i, barrier, out_q), daemon=False)
        p.start()
        procs.append(p)

    sampler = CpuSampler()
    try:
        barrier.wait(timeout=300)
    except Exception as exc:  # noqa: BLE001
        log(f"WARNING: barrier failed ({exc}) -- a worker probably died during setup")
    sampler.start()

    results = []
    deadline = time.time() + cfg_template.duration + cfg_template.drain_seconds + 180
    while len(results) < workers and time.time() < deadline:
        try:
            results.append(out_q.get(timeout=2))
        except _queue.Empty:
            if all(not p.is_alive() for p in procs):
                break
    sampler.stop()
    for p in procs:
        p.join(timeout=30)
        if p.is_alive():
            p.terminate()

    return merge_results(results, cfg_template, workers, sampler.report())


def merge_results(results, cfg, workers, cpu_report):
    fatal = [r for r in results if r.get("fatal")]
    good = [r for r in results if not r.get("fatal")]
    agg = {
        "due_total": 0,
        "dispatched": 0,
        "schedule_deficit": 0,
        "blocked_inflight": 0,
        "behind_schedule": 0,
        "attempted": 0,
        "accepted": 0,
        "connects_ok": 0,
        "connects_failed": 0,
        "disconnects": 0,
        "reconnects": 0,
    }
    failed = {}
    connacks = {}
    max_lag = 0.0
    lag_weighted = 0.0
    cpu_seconds = 0.0
    wall = 0.0
    t_start = None
    t_end = None
    for r in good:
        for k in agg:
            agg[k] += r.get(k, 0)
        for k, v in (r.get("failed") or {}).items():
            failed[k] = failed.get(k, 0) + v
        for k, v in (r.get("connack_codes") or {}).items():
            connacks[k] = connacks.get(k, 0) + v
        max_lag = max(max_lag, r.get("max_lag_ms", 0.0))
        lag_weighted += r.get("mean_lag_ms", 0.0) * r.get("dispatched", 0)
        cpu_seconds += r.get("cpu_seconds", 0.0)
        wall = max(wall, r.get("wall_seconds", 0.0))
        ts, te = r.get("t_start_epoch"), r.get("t_end_epoch")
        t_start = ts if t_start is None else min(t_start, ts)
        t_end = te if t_end is None else max(t_end, te)

    hist = Histogram.merge([r.get("latency") for r in good])
    failed_total = sum(failed.values())
    disp = agg["dispatched"]
    due = agg["due_total"]
    deficit = agg["schedule_deficit"]
    keys_per_msg = good[0]["keys_per_message"] if good else 0

    reasons = []
    notes = []
    # A blocked send is always reported. Whether it makes the CLIENT the
    # bottleneck is a matter of degree: 95 blocks in 900k messages is noise, 95k
    # is a verdict. The threshold is a knob so the call is explicit, not implied.
    blocked_thresh = cfg.extra.get("bottleneck_blocked_pct", 0.1)
    deficit_pct = (deficit / due * 100.0) if due else 0.0
    behind_pct = (agg["behind_schedule"] / disp * 100.0) if disp else 0.0
    blocked_pct = (agg["blocked_inflight"] / due * 100.0) if due else 0.0
    if deficit_pct > 1.0:
        reasons.append(f"schedule_deficit {deficit_pct:.2f}% of the scheduled messages never left")
    if blocked_pct > blocked_thresh:
        reasons.append(
            f"in-flight/pool cap hit {agg['blocked_inflight']} times ({blocked_pct:.3f}% "
            f"of scheduled, threshold {blocked_thresh}%)"
        )
    elif agg["blocked_inflight"] > 0:
        notes.append(
            f"in-flight/pool cap hit {agg['blocked_inflight']} times ({blocked_pct:.4f}% "
            f"of scheduled) -- below the {blocked_thresh}% bottleneck threshold"
        )
    if behind_pct > 5.0:
        reasons.append(
            f"{behind_pct:.1f}% of sends were later than their deadline "
            f"(> {cfg.late_threshold_ms:.0f} ms)"
        )
    if failed.get("mqtt_queue_full"):
        reasons.append(f"MQTT client queue full {failed['mqtt_queue_full']} times")
    if fatal:
        reasons.append(f"{len(fatal)} worker(s) failed during setup")

    return {
        "workers": workers,
        "worker_fatals": [r["fatal"] for r in fatal],
        "t_start_epoch": t_start,
        "t_end_epoch": t_end,
        "started_at": iso(t_start) if t_start else None,
        "ended_at": iso(t_end) if t_end else None,
        "wall_seconds": round(wall, 3),
        "offered": {
            "target_rate_msg_s": cfg.rate,
            "scheduled_total": due,
            "dispatched_total": disp,
            "offered_rate_msg_s": round(disp / wall, 1) if wall else None,
            "schedule_deficit": deficit,
            "schedule_deficit_pct": round(deficit_pct, 3),
        },
        "honesty": {
            "client_was_bottleneck": bool(reasons),
            "reasons": reasons,
            "notes": notes,
            "blocked_inflight_pct": round(blocked_pct, 4),
            "bottleneck_blocked_pct_threshold": blocked_thresh,
            "behind_schedule": agg["behind_schedule"],
            "behind_schedule_pct": round(behind_pct, 3),
            "max_lag_ms": round(max_lag, 3),
            "mean_lag_ms": round(lag_weighted / disp, 3) if disp else 0.0,
            "blocked_inflight": agg["blocked_inflight"],
            "late_threshold_ms": cfg.late_threshold_ms,
        },
        "delivery": {
            "attempted": agg["attempted"],
            "accepted": agg["accepted"],
            "accepted_rate_msg_s": round(agg["accepted"] / wall, 1) if wall else None,
            "failed_total": failed_total,
            "failed": failed,
            "accept_ratio": round(agg["accepted"] / disp, 5) if disp else None,
            "keys_per_message": keys_per_msg,
            "expected_rows": agg["accepted"] * keys_per_msg,
        },
        "latency_ms": hist.summary(),
        "connections": {
            "connects_ok": agg["connects_ok"],
            "connects_failed": agg["connects_failed"],
            "disconnects_during_run": agg["disconnects"],
            "reconnects": agg["reconnects"],
            "connack_codes": connacks,
        },
        "generator_cpu": {
            **cpu_report,
            "worker_cpu_seconds_exact": round(cpu_seconds, 3),
            "worker_cores_avg_exact": round(cpu_seconds / wall, 3) if wall else None,
            "host_cores_total": os.cpu_count(),
            "max_workers_budget": workers,
        },
        "per_worker": [
            {k: v for k, v in r.items() if k not in ("latency", "per_second")} for r in good
        ],
        "per_second": [r.get("per_second") for r in good],
    }


# ---------------------------------------------------------------------------
# subcommands
# ---------------------------------------------------------------------------


def cfg_from_args(args, rate=None, duration=None) -> WorkerConfig:
    return WorkerConfig(
        protocol=args.protocol,
        duration=float(duration if duration is not None else args.duration),
        rate=float(rate if rate is not None else args.rate),
        ramp=float(args.ramp),
        late_threshold_ms=float(args.late_threshold_ms),
        payload_keys=int(args.payload_keys),
        payload_bytes=int(args.payload_bytes),
        key_prefix=args.key_prefix,
        drain_seconds=float(args.drain_seconds),
        dry_run=bool(args.dry_run),
        tick_ms=float(args.tick_ms),
        mqtt_host=args.mqtt_host,
        mqtt_port=int(args.mqtt_port),
        mqtt_keepalive=int(args.mqtt_keepalive),
        mqtt_qos=int(args.qos),
        mqtt_max_inflight=int(args.mqtt_client_inflight),
        mqtt_max_queued=int(args.mqtt_client_queue),
        mqtt_connect_rate=float(args.mqtt_connect_rate),
        http_timeout=float(args.http_timeout),
        http_warmup=not args.no_http_warmup,
        extra={
            "engine": args.engine,
            "bottleneck_blocked_pct": float(args.bottleneck_blocked_pct),
            "http_connections": max(1, args.http_connections // max(1, args.max_workers)),
        },
    )


def resolve_endpoints(args, target):
    """Point the workers at whatever the chosen target says is its MQTT endpoint."""
    if args.target == "sink":
        args.mqtt_host = args.sink_host
        args.mqtt_port = args.sink_mqtt_port
    return target


def cmd_run(args):
    run_id = args.run_id or uuid.uuid4().hex[:8]
    args.key_prefix = args.key_prefix or f"lg2_{run_id}"
    target = build_target(args)
    resolve_endpoints(args, target)

    sink = None
    if args.target == "sink":
        from sink import SinkFleet

        sink = SinkFleet(args.sink_host, args.sink_http_port, args.sink_mqtt_port, args.sink_workers)
        sink.start()
        log(f"sink up ({args.sink_workers} procs)")

    log(f"provisioning {args.devices} devices (prefix {args.device_prefix})")
    t_prov = time.time()
    devices, errors = target.provision(args.devices, args.device_prefix, args.provision_parallelism)
    log(f"provisioned {len(devices)}/{args.devices} in {time.time() - t_prov:.1f}s, {len(errors)} errors")
    if errors:
        for e in errors[:5]:
            log(f"  provision error: {e}")
    if not devices:
        log("FATAL: no devices provisioned")
        return 2

    workers = max(1, min(args.max_workers, len(devices)))
    cfg = cfg_from_args(args)
    log(
        f"run: target={args.target} protocol={args.protocol} devices={len(devices)} "
        f"rate={args.rate} msg/s duration={args.duration}s workers={workers} "
        f"key_prefix={args.key_prefix}"
    )
    report = execute_run(cfg, devices, workers, args.max_inflight)

    report["run_id"] = run_id
    report["generator"] = "loadgen2"
    report["config"] = {
        "target": args.target,
        "protocol": args.protocol,
        "devices": len(devices),
        "target_rate_msg_s": args.rate,
        "duration_s": args.duration,
        "ramp_s": args.ramp,
        "payload_keys": args.payload_keys,
        "payload_bytes": args.payload_bytes,
        "qos": args.qos,
        "max_workers": args.max_workers,
        "max_inflight_total": args.max_inflight,
        "key_prefix": args.key_prefix,
        "device_prefix": args.device_prefix,
        "engine": args.engine,
        "http_connections": args.http_connections,
        "dry_run": args.dry_run,
    }
    report["target_info"] = target.describe()

    # landed rows
    if args.verify_landed and target.supports_landed_verification:
        log(f"settling {args.settle_seconds}s before counting landed rows")
        time.sleep(args.settle_seconds)
        landed = target.landed_count(args.key_prefix)
        expected = report["delivery"]["expected_rows"]
        report["landed"] = {
            # The method is declared by the target, not invented by the report.
            # This string used to be hardwired to "greptime count(*)" and a
            # ThingsBoard run came out labelled as counted in ThingsFlow's
            # store, even though the real count was against ts_kv.
            "method": target.landed_method(args.key_prefix),
            "settle_seconds": args.settle_seconds,
            "rows": landed,
            "expected_rows": expected,
        }
        if landed is None:
            # Distinguish "there was no loss" from "it could not be checked".
            # Collapsing both cases into a missing match lets an unverified run
            # pass as valid, which is the usual silent failure.
            report["landed"]["verified"] = False
            report["landed"]["reason"] = "the count returned None (store unreachable?)"
            log("landed=NOT VERIFIED — the count failed; the run does not prove absence of loss")
        else:
            report["landed"]["verified"] = True
            report["landed"]["delta"] = landed - expected
            report["landed"]["match"] = landed == expected
            log(f"landed={landed} expected={expected} delta={landed - expected}")
    elif args.verify_landed:
        report["landed"] = {"available": False, "reason": f"{args.target} has no landed check"}

    if sink:
        report["sink"] = sink.report()
        sink.stop_all()

    if args.cleanup:
        log(f"cleaning up {len(devices)} devices")
        report["cleanup"] = target.cleanup(devices)
        log(f"cleanup: {report['cleanup']}")

    emit(report, args)
    print_summary(report)
    return 0


def cmd_calibrate(args):
    """Ramp the offered rate until the CLIENT stops keeping its schedule."""
    run_id = args.run_id or uuid.uuid4().hex[:8]
    args.key_prefix = args.key_prefix or f"lg2cal_{run_id}"
    args.target = "sink" if not args.dry_run else "sink"
    target = build_target(args)
    resolve_endpoints(args, target)

    sink = None
    if not args.dry_run:
        from sink import SinkFleet

        sink = SinkFleet(args.sink_host, args.sink_http_port, args.sink_mqtt_port, args.sink_workers)
        sink.start()
        log(f"calibration sink up: {args.sink_workers} procs, http={args.sink_http_port} mqtt={args.sink_mqtt_port}")

    devices, _ = target.provision(args.devices, args.device_prefix, 1)
    workers = max(1, min(args.max_workers, len(devices)))
    rates = args.calibrate_rates or [2000, 5000, 10000, 20000, 40000, 80000, 160000, 320000]
    steps = []
    ceiling = None
    log(
        f"calibrating protocol={args.protocol} devices={len(devices)} workers={workers} "
        f"step={args.calibrate_step_seconds}s dry_run={args.dry_run}"
    )
    for rate in rates:
        base = sink.mark() if sink else None
        cfg = cfg_from_args(args, rate=rate, duration=args.calibrate_step_seconds)
        rep = execute_run(cfg, devices, workers, args.max_inflight)
        if sink:
            rep["sink"] = sink.report(base)
        ok = (
            not rep["honesty"]["client_was_bottleneck"]
            and rep["honesty"]["max_lag_ms"] <= args.calibrate_lag_budget_ms
            and (rep["delivery"]["accept_ratio"] or 0) >= 0.99
        )
        step = {
            "target_rate": rate,
            "offered_rate": rep["offered"]["offered_rate_msg_s"],
            "accepted_rate": rep["delivery"]["accepted_rate_msg_s"],
            "accept_ratio": rep["delivery"]["accept_ratio"],
            "schedule_deficit_pct": rep["offered"]["schedule_deficit_pct"],
            "behind_schedule_pct": rep["honesty"]["behind_schedule_pct"],
            "max_lag_ms": rep["honesty"]["max_lag_ms"],
            "blocked_inflight": rep["honesty"]["blocked_inflight"],
            "failed": rep["delivery"]["failed"],
            "generator_cores_mean": rep["generator_cpu"].get("cores_mean"),
            "generator_cores_peak": rep["generator_cpu"].get("cores_peak"),
            "generator_cores_exact": rep["generator_cpu"].get("worker_cores_avg_exact"),
            "sink": rep.get("sink"),
            "latency_ms": rep["latency_ms"],
            "passed": ok,
            "reasons": rep["honesty"]["reasons"],
        }
        steps.append(step)
        log(
            f"  rate={rate:>7} offered={step['offered_rate']:>9} accepted={step['accepted_rate']:>9} "
            f"maxlag={step['max_lag_ms']:>9.1f}ms cores={step['generator_cores_exact']} "
            f"{'PASS' if ok else 'FAIL: ' + '; '.join(step['reasons'][:2])}"
        )
        if ok:
            ceiling = step
        else:
            break
        time.sleep(args.calibrate_settle_seconds)

    if sink:
        sink.stop_all()

    report = {
        "run_id": run_id,
        "generator": "loadgen2",
        "mode": "calibrate",
        "config": {
            "protocol": args.protocol,
            "devices": len(devices),
            "max_workers": args.max_workers,
            "workers_used": workers,
            "max_inflight_total": args.max_inflight,
            "payload_keys": args.payload_keys,
            "payload_bytes": args.payload_bytes,
            "qos": args.qos,
            "engine": args.engine,
            "http_connections": args.http_connections,
            "step_seconds": args.calibrate_step_seconds,
            "lag_budget_ms": args.calibrate_lag_budget_ms,
            "dry_run": args.dry_run,
            "sink_workers": 0 if args.dry_run else args.sink_workers,
        },
        "steps": steps,
        "client_ceiling_msg_s": ceiling["offered_rate"] if ceiling else None,
        "client_ceiling_target_rate": ceiling["target_rate"] if ceiling else None,
        "client_ceiling_cores": ceiling["generator_cores_exact"] if ceiling else None,
        "note": (
            "Ceiling = highest step where the generator kept its schedule "
            "(no deficit, no in-flight blocking, <5% late sends, lag within budget, "
            ">=99% accepted). Measured against a local sink sharing this host, so "
            "the true network-free ceiling is at least this high, not lower."
        ),
    }
    emit(report, args)
    if ceiling:
        log(
            f"CLIENT CEILING ({args.protocol}): {ceiling['offered_rate']} msg/s offered, "
            f"{ceiling['generator_cores_exact']} cores (workers) + "
            f"{(ceiling.get('sink') or {}).get('sink_cpu_cores_avg')} cores (sink)"
        )
    else:
        log("CLIENT CEILING: not reached -- even the first step failed. Check the reasons above.")
    return 0


def cmd_cleanup(args):
    target = build_target(args)
    devices = target.find_by_prefix(args.device_prefix)
    log(f"found {len(devices)} devices with prefix {args.device_prefix!r}")
    if not devices:
        return 0
    res = target.cleanup(devices)
    log(f"cleanup: {res}")
    return 0


def cmd_sink(args):
    from sink import SinkFleet

    fleet = SinkFleet(args.sink_host, args.sink_http_port, args.sink_mqtt_port, args.sink_workers)
    fleet.start()
    log(f"sink running http={args.sink_http_port} mqtt={args.sink_mqtt_port}; Ctrl-C to stop")
    try:
        while True:
            time.sleep(5)
            log(f"http={fleet.http_count.value} mqtt={fleet.mqtt_count.value}")
    except KeyboardInterrupt:
        fleet.stop_all()
    return 0


# ---------------------------------------------------------------------------


def emit(report, args):
    out = args.out
    if not out:
        d = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")
        os.makedirs(d, exist_ok=True)
        mode = report.get("mode", "run")
        out = os.path.join(
            d, f"{mode}-{report.get('config', {}).get('protocol', 'x')}-{report['run_id']}.json"
        )
    os.makedirs(os.path.dirname(os.path.abspath(out)), exist_ok=True)
    with open(out, "w") as f:
        json.dump(report, f, indent=2, default=str)
    log(f"report -> {out}")
    report["_out"] = out


def print_summary(r):
    o, h, d = r["offered"], r["honesty"], r["delivery"]
    print("\n" + "=" * 78)
    print(f"  target={r['config']['target']} protocol={r['config']['protocol']} "
          f"engine={r['config'].get('engine')} devices={r['config']['devices']} "
          f"rate={o['target_rate_msg_s']} msg/s")
    print(f"  offered   {o['offered_rate_msg_s']} msg/s   "
          f"(scheduled {o['scheduled_total']}, dispatched {o['dispatched_total']}, "
          f"deficit {o['schedule_deficit']} = {o['schedule_deficit_pct']}%)")
    print(f"  accepted  {d['accepted_rate_msg_s']} msg/s   "
          f"({d['accepted']}/{d['attempted']} attempted, {d['failed_total']} failed {d['failed']})")
    lat = r["latency_ms"]
    print(f"  latency   p50={lat['p50_ms']} p95={lat['p95_ms']} p99={lat['p99_ms']} max={lat['max_ms']} ms")
    print(f"  schedule  behind={h['behind_schedule']} ({h['behind_schedule_pct']}%) "
          f"max_lag={h['max_lag_ms']}ms blocked_inflight={h['blocked_inflight']}")
    cpu = r["generator_cpu"]
    print(f"  gen CPU   {cpu.get('worker_cores_avg_exact')} cores exact "
          f"(sampled mean {cpu.get('cores_mean')}, peak {cpu.get('cores_peak')}) "
          f"of {cpu.get('host_cores_total')}")
    if "landed" in r and r["landed"].get("rows") is not None:
        L = r["landed"]
        print(f"  landed    {L['rows']} rows vs expected {L['expected_rows']} "
              f"(delta {L['delta']}) -> {'MATCH' if L['match'] else 'MISMATCH'}")
    verdict = "CLIENT WAS THE BOTTLENECK" if h["client_was_bottleneck"] else "client kept its schedule"
    print(f"  VERDICT   {verdict}")
    for reason in h["reasons"]:
        print(f"            - {reason}")
    for note in h.get("notes", []):
        print(f"            ~ {note}")
    print("=" * 78 + "\n")


def build_parser():
    p = argparse.ArgumentParser(
        prog="loadgen2",
        description="Open-loop, CPU-aware IoT load generator for ThingsFlow vs ThingsBoard.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    def common(sp):
        sp.add_argument("--target", choices=["thingsflow", "thingsboard", "sink"], default="thingsflow")
        sp.add_argument("--protocol", choices=["mqtt", "http"], default="http")
        sp.add_argument("--devices", type=int, default=100)
        sp.add_argument("--rate", type=float, default=500.0, help="total offered msg/s")
        sp.add_argument("--duration", type=float, default=60.0)
        sp.add_argument("--ramp", type=float, default=0.0, help="linear ramp-up seconds")
        sp.add_argument("--payload-keys", type=int, default=3)
        sp.add_argument("--payload-bytes", type=int, default=0, help="pad payload to ~N bytes")
        sp.add_argument("--qos", type=int, choices=[0, 1], default=1)
        sp.add_argument("--max-workers", type=int, default=4,
                        help="generator processes; keep well under host cores so the SUT is not starved")
        sp.add_argument("--max-inflight", type=int, default=2048,
                        help="total outstanding requests/PUBACKs; when full, sends are counted as deficit")
        sp.add_argument("--late-threshold-ms", type=float, default=50.0)
        sp.add_argument("--tick-ms", type=float, default=1.0)
        sp.add_argument("--drain-seconds", type=float, default=15.0)
        sp.add_argument("--dry-run", action="store_true",
                        help="do all the scheduling/serialisation work but discard sends")
        sp.add_argument("--run-id", default=None)
        sp.add_argument("--key-prefix", default=None)
        sp.add_argument("--device-prefix", default="lg2")
        sp.add_argument("--out", default=None)
        # endpoints
        sp.add_argument("--api-base", default=os.environ.get("TF_API_BASE", "http://<node-ip>:30081"))
        sp.add_argument("--ingest-base", default=os.environ.get("TF_INGEST_BASE", "http://<node-ip>:30808"))
        sp.add_argument("--mqtt-host", default=os.environ.get("TF_MQTT_HOST", "<node-ip>"))
        sp.add_argument("--mqtt-port", type=int, default=int(os.environ.get("TF_MQTT_PORT", "30183")))
        sp.add_argument("--user", default=os.environ.get("TF_USER", "tenant@thingsboard.org"))
        sp.add_argument("--password", default=os.environ.get("TF_PASS", "tenant"))
        sp.add_argument("--greptime-base", default=os.environ.get("TF_GREPTIME_BASE", "http://<node-ip>:30400"))
        sp.add_argument("--greptime-table", default="device_telemetry_kv")
        sp.add_argument("--tf-mqtt-username-mode", choices=["raw", "bearer"], default="raw")
        sp.add_argument("--provision-parallelism", type=int, default=16)
        # transport knobs
        sp.add_argument("--mqtt-keepalive", type=int, default=60)
        sp.add_argument("--mqtt-client-inflight", type=int, default=200,
                        help="per-connection unacked QoS1 window")
        sp.add_argument("--mqtt-client-queue", type=int, default=200,
                        help="per-connection queue; overflow is COUNTED, not absorbed")
        sp.add_argument("--mqtt-connect-rate", type=float, default=400.0)
        sp.add_argument("--engine", choices=["raw", "lib"], default="raw",
                        help="raw = built-in asyncio HTTP/MQTT (max headroom); "
                             "lib = aiohttp/paho-mqtt (cross-check reference)")
        sp.add_argument("--bottleneck-blocked-pct", type=float, default=0.1,
                        help="%% of scheduled messages that may be blocked by the in-flight/pool "
                             "cap before the run is declared client-bottlenecked")
        sp.add_argument("--http-connections", type=int, default=512,
                        help="total keep-alive connections in the pool (raw engine)")
        sp.add_argument("--http-timeout", type=float, default=30.0)
        sp.add_argument("--no-http-warmup", action="store_true")
        # sink
        sp.add_argument("--sink-host", default="127.0.0.1")
        sp.add_argument("--sink-http-port", type=int, default=18081)
        sp.add_argument("--sink-mqtt-port", type=int, default=18883)
        sp.add_argument("--sink-workers", type=int, default=2)

    r = sub.add_parser("run", help="run one measurement against a target")
    common(r)
    r.add_argument("--verify-landed", action="store_true")
    r.add_argument("--settle-seconds", type=float, default=20.0)
    r.add_argument("--cleanup", action="store_true", help="delete the devices this run created")
    r.set_defaults(func=cmd_run)

    c = sub.add_parser("calibrate", help="find this host's own generation ceiling")
    common(c)
    c.add_argument("--calibrate-rates", type=int, nargs="*", default=None)
    c.add_argument("--calibrate-step-seconds", type=float, default=15.0)
    c.add_argument("--calibrate-settle-seconds", type=float, default=2.0)
    c.add_argument("--calibrate-lag-budget-ms", type=float, default=250.0)
    c.set_defaults(func=cmd_calibrate)

    cl = sub.add_parser("cleanup", help="delete devices created with a given prefix")
    common(cl)
    cl.set_defaults(func=cmd_cleanup)

    sk = sub.add_parser("sink", help="run the local calibration sink standalone")
    common(sk)
    sk.set_defaults(func=cmd_sink)
    return p


def main():
    args = build_parser().parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
