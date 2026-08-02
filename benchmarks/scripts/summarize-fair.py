#!/usr/bin/env python3
"""Resume la rampa justa: un nivel por fila, con su veredicto y su coste real.

Regla de publicación: **solo se resume lo que pasó el portón**. Un nivel
INVALIDO se lista con su motivo pero NO entra en el "máximo limpio", porque un
techo medido bajo estrangulamiento es el techo de la jaula.

Uso:
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
    """Los tres criterios a la vez. Cualquier fallo descalifica el nivel."""
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
        print(f"sin resultados en {d}")
        return 1

    md = "--md" in sys.argv
    sep = " | " if md else "  "
    hdr = ["plataforma", "proto", "msg/s", "veredicto", "CPU", "MEM",
           "p50", "p95", "p99", "nodo"]
    if md:
        print("| " + " | ".join(hdr) + " |")
        print("|" + "---|" * len(hdr))

    for r in sorted(rows, key=lambda x: (x["target"], x["proto"], x["rate"])):
        v = "limpio" if clean(r) else "INVALIDO"
        if not clean(r):
            why = []
            if r["failed"]:
                why.append(f"{r['failed']} errores")
            if r["throttled"]:
                why.append(f"throttling en {len(r['throttled'])}")
            if r["expected"] and r["rows"] is not None and r["rows"] != r["expected"]:
                why.append(f"perdida {r['expected'] - r['rows']}")
            if r["rows"] is None:
                why.append("sin verificar")
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

    print("\n=== máximo limpio por combinación ===")
    best = {}
    for r in rows:
        if clean(r):
            k = (r["target"], r["proto"])
            if r["rate"] > best.get(k, {}).get("rate", -1):
                best[k] = r
    for (t, p), r in sorted(best.items()):
        # Declarar cuándo el techo NO se encontró: si el nodo estaba cerca de
        # saturarse, el límite medido es el hardware, no la plataforma.
        note = ""
        if r["node_pct"] and r["node_pct"] >= 75:
            note = f"  <- nodo al {r['node_pct']}%: techo NO encontrado, se acabó el hardware"
        print(f"  {t:12s} {p:5s} {r['rate']:6d} msg/s   "
              f"CPU={r['cpu_m']}m MEM={r['mem_mib']}MiB p95={r['p95']}ms{note}")

    missing = [f"{r['target']}-{r['proto']}-{r['rate']}" for r in rows if not r["cpu_m"]]
    if missing:
        print(f"\nSIN datos de recursos ({len(missing)}): {', '.join(missing)}")
        print("Se midieron antes de instrumentar el portón; hay que repetirlos.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
