#!/usr/bin/env python3
"""Validity gate: checks that NO pod throttled during the measurement.

Why it exists: the first ramp declared in a comment that the limits were
"deliberately generous so that they are NOT binding", and they were not. tb-node
reached 99.3% of its ceiling and nats-alarms 90.7% in the levels that were
published as clean. A method claim that nobody verifies is an assumption with
better wording.

This script measures the real kernel CFS throttling (cgroup v2, cpu.stat) per
container, between two instants. If a container was throttled during the
measured window, its level is NOT publishable: the observed ceiling would be the
limit, not the platform.

Usage:
    throttle-gate.py snapshot <ns> <file>      # before the load
    throttle-gate.py check    <ns> <file>      # after; exits 1 if it throttled

Needs no sudo: /sys/fs/cgroup is readable on this node.
"""
import json
import os
import subprocess
import sys
import time

CGROUP_ROOT = "/sys/fs/cgroup/kubepods"

# Severity thresholds, and why this is NOT a plain "there was throttling".
#
# An absolute counter systematically punishes the JVM: its GC and thread bursts
# exceed the quota within some 100 ms period even when the average is at 43%.
# Measured on ThingsBoard: 67 periods out of 2,324 (2.9%), 4.4 s over a 232 s
# window — and the level landed 1,035,000 rows EXACTLY with no loss.
# Invalidating that run would have been penalizing an engine for its threading
# model, not for its performance, while the Go/Rust components on the other side
# pass.
#
# Criterion: throttling matters when it PREVENTS MEETING THE TARGET. Two levels
# are reported and the one that decides the verdict is the ramp, which does know
# whether the level met its target:
#   mild   (exit 1) -> there was throttling but it may not have been binding; if
#                      the level met its target with zero loss, it does not
#                      invalidate.
#   severe (exit 2) -> so much that it distorts even a level that met its
#                      target; its resource measurement is no longer reliable.
SEVERE_THROTTLED_PCT = 10.0     # % of throttled periods
SEVERE_THROTTLED_WALL_PCT = 5.0  # % of wall time lost
TOLERATED_THROTTLED_PERIODS = 5


def pod_uids(ns):
    """Maps pod uid -> name, so that whatever is found can be named."""
    out = subprocess.run(
        ["kubectl", "--context=microk8s", "-n", ns, "get", "pods",
         "-o", "jsonpath={range .items[*]}{.metadata.uid}={.metadata.name}{\"\\n\"}{end}"],
        capture_output=True, text=True, timeout=120)
    m = {}
    for line in out.stdout.splitlines():
        if "=" in line:
            uid, name = line.split("=", 1)
            m[uid.strip()] = name.strip()
    return m


def read_stat(path):
    vals = {}
    try:
        with open(path) as f:
            for line in f:
                k, _, v = line.partition(" ")
                vals[k] = int(v)
    except (OSError, ValueError):
        return None
    return vals


def read_quota(path):
    """cpu.max = "<quota|max> <period>". Returns millicores, or None if 'max'."""
    try:
        with open(path) as f:
            quota, period = f.read().split()
    except (OSError, ValueError):
        return None
    if quota == "max":
        return None
    return int(1000 * int(quota) / int(period))


def collect(ns):
    """Walks the cgroup tree and returns the stats of every container."""
    uids = pod_uids(ns)
    found = {}
    for qos in ("burstable", "besteffort", ""):
        base = os.path.join(CGROUP_ROOT, qos) if qos else CGROUP_ROOT
        if not os.path.isdir(base):
            continue
        for entry in os.listdir(base):
            if not entry.startswith("pod"):
                continue
            uid = entry[3:]
            if uid not in uids:
                continue
            poddir = os.path.join(base, entry)
            for cid in os.listdir(poddir):
                cdir = os.path.join(poddir, cid)
                if not os.path.isdir(cdir):
                    continue
                stat = read_stat(os.path.join(cdir, "cpu.stat"))
                if stat is None:
                    continue
                found[f"{uids[uid]}/{cid[:12]}"] = {
                    "nr_periods": stat.get("nr_periods", 0),
                    "nr_throttled": stat.get("nr_throttled", 0),
                    "throttled_usec": stat.get("throttled_usec", 0),
                    "usage_usec": stat.get("usage_usec", 0),
                    "quota_m": read_quota(os.path.join(cdir, "cpu.max")),
                    # memory.current is instantaneous (not cumulative): in the
                    # snapshot it is the idle value and in the check the value
                    # under load.
                    "mem_bytes": read_single(os.path.join(cdir, "memory.current")),
                }
    return found


def read_single(path):
    try:
        with open(path) as f:
            return int(f.read().strip())
    except (OSError, ValueError):
        return None


def main():
    if len(sys.argv) != 4:
        print(__doc__)
        return 2
    mode, ns, path = sys.argv[1], sys.argv[2], sys.argv[3]
    now = collect(ns)

    if mode == "snapshot":
        # The instant is stored with the snapshot: it is the ONLY valid clock
        # for averaging consumption. See the note in the cpu_m computation.
        with open(path, "w") as f:
            json.dump({"_t": time.time(), "c": now}, f)
        print(f"snapshot: {len(now)} containers in {ns}")
        return 0

    if mode != "check":
        print(f"unknown mode: {mode}", file=sys.stderr)
        return 2

    with open(path) as f:
        snap = json.load(f)
    # Compatibility with old-format snapshots (without a clock).
    if "_t" in snap:
        before, t0 = snap["c"], snap["_t"]
    else:
        before, t0 = snap, os.path.getmtime(path)
    elapsed = max(time.time() - t0, 1e-6)

    # The per-container consumption during the window IS the benchmark result,
    # not a by-product of the gate. It is persisted next to the snapshot:
    # cpu.stat.usage_usec is cumulative, so the delta between snapshot and check
    # divided by the elapsed time gives the real average millicores, measured by
    # the kernel and not sampled by metrics-server every 15 s.
    usage_out = {}

    offenders, clean = [], 0
    for key, cur in sorted(now.items()):
        prev = before.get(key)
        if prev is None:
            continue  # pod born during the window; not comparable
        d_thr = cur["nr_throttled"] - prev["nr_throttled"]
        d_per = cur["nr_periods"] - prev["nr_periods"]
        d_us = cur["throttled_usec"] - prev["throttled_usec"]
        d_use = cur["usage_usec"] - prev["usage_usec"]

        # The divisor is the wall clock, IDENTICAL for every container.
        #
        # Do NOT use nr_periods * 0.1: nr_periods only advances when the cgroup
        # has runnable tasks, so a bursty container accumulates few periods and
        # dividing by them INFLATES its millicores. Measured: within one and the
        # same window the "durations" derived from nr_periods ranged from 0.2 s
        # to 177.9 s, and latest-kv appeared to be spending 945m with HTTP at
        # 1,000 msg/s against 672m with MQTT at 8,000 — more CPU with eight
        # times less load, which is impossible and gave away the error.
        usage_out[key] = {
            "cpu_m": round(d_use / 1000 / elapsed),
            "mem_bytes": cur.get("mem_bytes"),
            "quota_m": cur["quota_m"],
            "throttled_periods": d_thr,
            # Raw, so it can be recomputed without repeating the measurement.
            "cpu_usec_delta": d_use,
            "window_s": round(elapsed, 1),
        }

        if d_thr > TOLERATED_THROTTLED_PERIODS:
            pct = 100.0 * d_thr / d_per if d_per else 0.0
            # How much CPU it consumed of its quota: it gives away the container
            # that is up against the ceiling even when the throttling is still
            # moderate. Same wall clock as above, for the same reason.
            used_m = round(d_use / 1000 / elapsed)
            offenders.append((key, d_thr, d_per, pct, d_us / 1e6,
                              used_m, cur["quota_m"]))
        else:
            clean += 1

    with open(path + ".usage.json", "w") as f:
        json.dump(usage_out, f, indent=1)

    tot_cpu = sum(v["cpu_m"] or 0 for v in usage_out.values())
    tot_mem = sum(v["mem_bytes"] or 0 for v in usage_out.values())
    print(f"platform consumption over the window: CPU={tot_cpu}m  "
          f"MEM={tot_mem / 1048576:.0f}MiB")
    top = sorted(usage_out.items(), key=lambda x: -(x[1]["cpu_m"] or 0))[:5]
    for k, v in top:
        if v["cpu_m"]:
            print(f"    {k.split('/')[0]:52s} {v['cpu_m']:5d}m "
                  f"{(v['mem_bytes'] or 0) / 1048576:7.0f}MiB")

    print(f"comparable containers: {clean + len(offenders)}  without throttling: {clean}")
    if not offenders:
        print("GATE OK — no limit was binding during the measured window.")
        print("The observed ceiling is the platform's, not the cage's.")
        return 0

    severe = [o for o in offenders
              if o[3] >= SEVERE_THROTTLED_PCT
              or (100.0 * o[4] / elapsed) >= SEVERE_THROTTLED_WALL_PCT]
    severity = "SEVERE" if severe else "MILD"
    print(f"\nGATE: {severity} throttling — {len(offenders)} container(s):")
    print(f"{'container':52s} {'periods':>16s} {'%':>6s} {'sec':>8s} {'used/quota':>14s}")
    for key, d_thr, d_per, pct, secs, used_m, quota in sorted(
            offenders, key=lambda x: -x[1]):
        q = f"{used_m}m/{quota}m" if quota else f"{used_m}m/no-limit"
        print(f"{key:52s} {d_thr:7d}/{d_per:<8d} {pct:5.1f}% {secs:7.1f}s {q:>14s}")
    if severe:
        print("\nSEVERE: the throttling distorts the measurement even when the level")
        print("meets its target. Raise the limit of the listed containers and repeat.")
        return 2
    print("\nMILD: it may not have been binding. If the level met its target with zero")
    print("loss and zero errors, this does not invalidate it — penalizing GC bursts")
    print("would be punishing the threading model, not the performance. If the level")
    print("did NOT meet its target, the platform's ceiling cannot be distinguished")
    print("from the cage's, and then it does invalidate.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
