#!/usr/bin/env python3
"""Render a compact human-readable table from an instrumented-bench summary.json.

Usage: summarize-instrumented.py <results-dir-or-summary.json> [--top N]
"""
import json
import os
import sys

SHORT = {
    "flow-core": "flow-core",
    "http-ingest": "http-ingest",
    "nats-greptimedb": "bento/greptimedb",
    "nats-latest-kv": "bento/latest-kv",
    "nats-entity-greptimedb": "bento/entity-gdb",
    "nats-alarms": "bento/alarms",
    "alarm-materializer": "alarm-materializer",
    "greptimedb": "greptimedb",
    "postgres": "postgres",
    "rmqtt-edge": "rmqtt-edge",
    "demo-simulator": "demo-simulator",
    "thingsboard-ui": "tb-web-ui",
    "nats": "nats",
}


def short_name(key):
    pod, _, cname = key.partition("/")
    body = pod.replace("tf-thingsflow-", "")
    for k in sorted(SHORT, key=len, reverse=True):
        if body.startswith(k):
            base = SHORT[k]
            break
    else:
        base = body
    if cname in ("envoy", "bento") and "http-ingest" in body:
        return f"http-ingest/{cname}"
    return base


def main():
    path = sys.argv[1] if len(sys.argv) > 1 else "."
    if os.path.isdir(path):
        path = os.path.join(path, "summary.json")
    top_n = 3
    if "--top" in sys.argv:
        top_n = int(sys.argv[sys.argv.index("--top") + 1])
    d = json.load(open(path))

    print(f"# ThingsFlow instrumented benchmark  ({d['generated_at']})")
    node = d["environment"].get("node", {})
    print(f"node={node.get('name')} cpu={node.get('capacity',{}).get('cpu')} "
          f"mem={node.get('capacity',{}).get('memory')} k8s={node.get('kubelet')}")
    print()

    hdr = (f"{'scenario':<10} {'dur_s':>6} {'dev':>5} {'target/s':>9} "
           f"{'accepted':>9} {'acc/s':>7} {'landed':>8} {'drop':>6} {'drop%':>7} "
           f"{'err':>5} {'p50ms':>7} {'p95ms':>7} {'p99ms':>7} {'e2e_p50':>8}")
    print(hdr)
    print("-" * len(hdr))
    for name, s in d["scenarios"].items():
        t = s["throughput"]
        lat = s["latency"]["client_post_ms"]
        e2e = s["latency"]["end_to_end_ms"]
        print(f"{name:<10} {s['duration_seconds']:>6.0f} {s['devices']:>5} "
              f"{s['target_msg_per_sec']:>9.0f} {t['accepted_2xx']:>9} "
              f"{t['accepted_msg_per_sec']:>7.0f} {t['landed_rows_bench_key']:>8} "
              f"{t['silent_drop_absolute']:>6} "
              f"{(t['silent_drop_pct'] if t['silent_drop_pct'] is not None else 0):>7.3f} "
              f"{s['errors']['count']:>5} "
              f"{_f(lat['p50']):>7} {_f(lat['p95']):>7} {_f(lat['p99']):>7} {_f(e2e['p50']):>8}")

    for name, s in d["scenarios"].items():
        cpu = s["cpu_millicores"]
        mem = s["working_set_mi"]
        ranked = sorted(cpu.items(), key=lambda kv: kv[1]["mean"], reverse=True)[:top_n]
        print(f"\n## {name} — top {top_n} components by mean CPU "
              f"(target {s['target_msg_per_sec']:.0f} msg/s)")
        print(f"{'component':<22} {'cpu_min':>8} {'cpu_mean':>9} {'cpu_p95':>8} {'cpu_max':>8} "
              f"{'mem_min':>8} {'mem_mean':>9} {'mem_p95':>8} {'mem_max':>8}")
        for k, c in ranked:
            m = mem.get(k, {})
            print(f"{short_name(k):<22} {c['min']:>8.1f} {c['mean']:>9.1f} {c['p95']:>8.1f} "
                  f"{c['max']:>8.1f} {m.get('min',0):>8.1f} {m.get('mean',0):>9.1f} "
                  f"{m.get('p95',0):>8.1f} {m.get('max',0):>8.1f}")
        thr = s.get("cfs_throttling") or {}
        if thr:
            print("  CFS throttling: " + ", ".join(
                f"{short_name(k)} {v['throttled_pct']}% of periods ({v['throttled_seconds']}s)"
                for k, v in sorted(thr.items(), key=lambda kv: -(kv[1]['throttled_pct'] or 0))))
        lg = s.get("loadgen") or {}
        if lg.get("late_sends") or lg.get("skipped_behind_schedule"):
            print(f"  CLIENT SHORTFALL: late_sends={lg['late_sends']} "
                  f"skipped={lg['skipped_behind_schedule']} "
                  f"lag_p95={lg['send_lag_seconds']['p95']}s "
                  f"loadgen_cores={lg.get('cpu_cores_equivalent')}")
        if s.get("restarts_changed"):
            print(f"  RESTARTS: {json.dumps(s['restarts_changed'])}")


def _f(v):
    return "-" if v is None else f"{v:.1f}"


if __name__ == "__main__":
    main()
