#!/usr/bin/env python3
"""Verifies the FAIRNESS RULE against the deployed cluster, not against the YAML.

The rule: no container may have a CPU limit below 3x its observed consumption.
If it does, the platform is measuring its cage.

Why against the cluster and not against the values file: an override may not
land. It already happened — the previous profile wrote `resources.nats` when the
chart reads `nats.resources`, Helm ignored it silently, NATS ran with 500m and
that ceiling was published as ThingsFlow's MQTT limit. The only thing that does
not lie is what the kubelet actually applied.

Complements throttle-gate.py: the gate detects throttling *after* it happens;
this warns *before* spending an entire ramp.

Usage:
    verify-effective-limits.py <namespace> [minimum-headroom]   # default 3.0

Run it WITH LOAD applied: without load the consumption is the idle one and the
headroom comes out artificially enormous, so the check says nothing.
"""
import json
import subprocess
import sys

K = ["kubectl", "--context=microk8s"]


def millicores(v):
    if not v:
        return None
    v = str(v)
    if v.endswith("m"):
        return int(v[:-1])
    return int(float(v) * 1000)


def limits(ns):
    out = subprocess.run(K + ["-n", ns, "get", "pods", "-o", "json"],
                         capture_output=True, text=True, timeout=180)
    res = {}
    for p in json.loads(out.stdout)["items"]:
        if p["status"].get("phase") != "Running":
            continue
        for c in p["spec"]["containers"]:
            lim = c.get("resources", {}).get("limits", {}).get("cpu")
            res[(p["metadata"]["name"], c["name"])] = millicores(lim)
    return res


def usage(ns):
    out = subprocess.run(K + ["-n", ns, "top", "pods", "--containers",
                              "--no-headers"],
                         capture_output=True, text=True, timeout=180)
    if out.returncode != 0:
        print(f"metrics-server is not responding: {out.stderr.strip()}", file=sys.stderr)
        return None
    res = {}
    for line in out.stdout.splitlines():
        parts = line.split()
        if len(parts) >= 3:
            res[(parts[0], parts[1])] = millicores(parts[2])
    return res


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        return 2
    ns = sys.argv[1]
    need = float(sys.argv[2]) if len(sys.argv) > 2 else 3.0

    lim, use = limits(ns), usage(ns)
    if use is None:
        return 2

    tight, unlimited, ok = [], [], 0
    for key, l in sorted(lim.items()):
        u = use.get(key)
        if u is None:
            continue
        if l is None:
            unlimited.append((key, u))
            continue
        # A container that barely consumes anything cannot violate the rule in
        # any meaningful way; demanding 3x over 2m would produce noise without
        # information.
        if u < 50:
            ok += 1
            continue
        if l < need * u:
            tight.append((key, u, l, l / u))
        else:
            ok += 1

    print(f"namespace {ns}: {ok} containers with enough headroom "
          f"(>= {need:g}x), {len(tight)} tight, {len(unlimited)} without a limit")

    for (pod, c), u in unlimited:
        print(f"  no limit: {pod}/{c} using {u}m  (not binding, but it declares the gap)")

    if not tight:
        print(f"\nRULE MET — no limit is below {need:g}x the consumption.")
        print("What is measured here is the platform, not the cage.")
        return 0

    print(f"\nRULE VIOLATED — {len(tight)} container(s) without headroom:")
    print(f"  {'container':56s} {'used':>8s} {'limit':>8s} {'headroom':>9s}")
    for (pod, c), u, l, ratio in sorted(tight, key=lambda x: x[3]):
        print(f"  {pod + '/' + c:56s} {u:7d}m {l:7d}m {ratio:8.2f}x")
    print("\nRaise these limits before measuring: their ceiling would be the result.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
