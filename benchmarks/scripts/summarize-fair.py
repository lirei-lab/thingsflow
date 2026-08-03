#!/usr/bin/env python3
"""Summarizes the fair ramp: one level per row, with its verdict and real cost.

Publication rule: **only what passed the gate is summarized**. An INVALID level
is listed with its reason but does NOT enter the "clean maximum", because a
ceiling measured under throttling is the cage's ceiling.

Usage:
    summarize-fair.py <results-fair-dir> [--md]
"""
import json
import os
import sys


def load(path):
    try:
        with open(path) as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


def level_rows(d):
    rows = []
    for fn in sorted(os.listdir(d)):
        if not fn.endswith(".json") or fn.startswith("."):
            continue
        rep = load(os.path.join(d, fn))
        if not rep or "delivery" not in rep:
            continue
        tag = fn[:-5]
        parts = tag.split("-")
        target, proto, rate = parts[0], parts[1], int(parts[2])

        de = rep["delivery"]
        lat = rep.get("latency_ms", {})
        landed = rep.get("landed", {})
        gen = rep.get("generator_cpu", {})

        usage = load(os.path.join(d, f".thr-{tag}.json.usage.json")) or {}
        cpu_m = sum(v.get("cpu_m") or 0 for v in usage.values())
        mem_mib = sum(v.get("mem_bytes") or 0 for v in usage.values()) / 1048576
        throttled = [k for k, v in usage.items()
                     if (v.get("throttled_periods") or 0) > 5]

        rows.append({
            "target": target, "proto": proto, "rate": rate,
            "accepted": de.get("accepted"), "failed": de.get("failed_total", 0),
            "rows": landed.get("rows"), "expected": landed.get("expected_rows"),
            "verified": landed.get("verified"),
            "p50": lat.get("p50_ms"), "p95": lat.get("p95_ms"), "p99": lat.get("p99_ms"),
            "cpu_m": cpu_m or None, "mem_mib": round(mem_mib) if mem_mib else None,
            "gen_cores": gen.get("worker_cores_avg_exact"),
            "node_pct": round(100 * (cpu_m / 1000 + (gen.get("worker_cores_avg_exact") or 0))
                              / (gen.get("host_cores_total") or 16)) if cpu_m else None,
            "throttled": throttled,
        })
    return rows


def clean(r):
    """All three criteria at once. Any failure disqualifies the level."""
    if r["failed"]:
        return False
    if r["verified"] is False or r["rows"] is None:
        return False
    if r["expected"] and r["rows"] != r["expected"]:
        return False
    if r["throttled"]:
        return False
    return True


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        return 2
    d = sys.argv[1]
    rows = level_rows(d)
    if not rows:
        print(f"no results in {d}")
        return 1

    md = "--md" in sys.argv
    sep = " | " if md else "  "
    hdr = ["platform", "proto", "msg/s", "verdict", "CPU", "MEM",
           "p50", "p95", "p99", "node"]
    if md:
        print("| " + " | ".join(hdr) + " |")
        print("|" + "---|" * len(hdr))

    for r in sorted(rows, key=lambda x: (x["target"], x["proto"], x["rate"])):
        v = "clean" if clean(r) else "INVALID"
        if not clean(r):
            why = []
            if r["failed"]:
                why.append(f"{r['failed']} errors")
            if r["throttled"]:
                why.append(f"throttling in {len(r['throttled'])}")
            if r["expected"] and r["rows"] is not None and r["rows"] != r["expected"]:
                why.append(f"loss {r['expected'] - r['rows']}")
            if r["rows"] is None:
                why.append("not verified")
            v += " (" + "; ".join(why) + ")"
        cells = [
            r["target"], r["proto"], f"{r['rate']}", v,
            f"{r['cpu_m']}m" if r["cpu_m"] else "-",
            f"{r['mem_mib']}MiB" if r["mem_mib"] else "-",
            f"{r['p50']}" if r["p50"] else "-",
            f"{r['p95']}" if r["p95"] else "-",
            f"{r['p99']}" if r["p99"] else "-",
            f"{r['node_pct']}%" if r["node_pct"] else "-",
        ]
        print(("| " + sep.join(cells) + " |") if md else sep.join(f"{c:<12}" for c in cells))

    print("\n=== clean maximum per combination ===")
    best = {}
    for r in rows:
        if clean(r):
            k = (r["target"], r["proto"])
            if r["rate"] > best.get(k, {}).get("rate", -1):
                best[k] = r
    for (t, p), r in sorted(best.items()):
        # Declare when the ceiling was NOT found: if the node was close to
        # saturation, the measured limit is the hardware, not the platform.
        note = ""
        if r["node_pct"] and r["node_pct"] >= 75:
            note = f"  <- node at {r['node_pct']}%: ceiling NOT found, the hardware ran out"
        print(f"  {t:12s} {p:5s} {r['rate']:6d} msg/s   "
              f"CPU={r['cpu_m']}m MEM={r['mem_mib']}MiB p95={r['p95']}ms{note}")

    missing = [f"{r['target']}-{r['proto']}-{r['rate']}" for r in rows if not r["cpu_m"]]
    if missing:
        print(f"\nNO resource data ({len(missing)}): {', '.join(missing)}")
        print("They were measured before the gate was instrumented; they must be repeated.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
