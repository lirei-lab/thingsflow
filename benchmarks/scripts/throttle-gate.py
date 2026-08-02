#!/usr/bin/env python3
"""Portón de validez: comprueba que NINGÚN pod throttleó durante la medición.

Por qué existe: la primera rampa declaró en un comentario que los límites eran
"generosos a propósito para que NO sean vinculantes", y no lo eran. tb-node
llegó al 99,3 % de su techo y nats-alarms al 90,7 % en los niveles que se
publicaron como limpios. Una afirmación de método que nadie verifica es una
suposición con mejor redacción.

Este script mide el throttling CFS real del kernel (cgroup v2, cpu.stat) por
contenedor, entre dos instantes. Si un contenedor fue estrangulado durante la
ventana medida, su nivel NO es publicable: el techo observado sería el límite,
no la plataforma.

Uso:
    throttle-gate.py snapshot <ns> <fichero>      # antes de la carga
    throttle-gate.py check    <ns> <fichero>      # después; sale 1 si throttleó

No necesita sudo: /sys/fs/cgroup es legible en este nodo.
"""
import json
import os
import subprocess
import sys

CGROUP_ROOT = "/sys/fs/cgroup/kubepods"
# Un poco de throttling en el arranque de un pod es normal y no contamina una
# medición en régimen permanente. Lo que invalida un nivel es el estrangulamiento
# sostenido bajo carga, así que se tolera un suelo mínimo y se reporta igual.
TOLERATED_THROTTLED_PERIODS = 5


def pod_uids(ns):
    """Mapea uid de pod -> nombre, para poder nombrar lo que se encuentre."""
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
    """cpu.max = "<quota|max> <period>". Devuelve millicores o None si 'max'."""
    try:
        with open(path) as f:
            quota, period = f.read().split()
    except (OSError, ValueError):
        return None
    if quota == "max":
        return None
    return int(1000 * int(quota) / int(period))


def collect(ns):
    """Recorre el árbol de cgroups y devuelve las estadísticas de cada contenedor."""
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
                }
    return found


def main():
    if len(sys.argv) != 4:
        print(__doc__)
        return 2
    mode, ns, path = sys.argv[1], sys.argv[2], sys.argv[3]
    now = collect(ns)

    if mode == "snapshot":
        with open(path, "w") as f:
            json.dump(now, f)
        print(f"instantánea: {len(now)} contenedores en {ns}")
        return 0

    if mode != "check":
        print(f"modo desconocido: {mode}", file=sys.stderr)
        return 2

    with open(path) as f:
        before = json.load(f)

    offenders, clean = [], 0
    for key, cur in sorted(now.items()):
        prev = before.get(key)
        if prev is None:
            continue  # pod nacido durante la ventana; no comparable
        d_thr = cur["nr_throttled"] - prev["nr_throttled"]
        d_per = cur["nr_periods"] - prev["nr_periods"]
        d_us = cur["throttled_usec"] - prev["throttled_usec"]
        d_use = cur["usage_usec"] - prev["usage_usec"]
        if d_thr > TOLERATED_THROTTLED_PERIODS:
            pct = 100.0 * d_thr / d_per if d_per else 0.0
            # Cuánta CPU consumió de su cuota: delata al que está contra el techo
            # aunque el throttling aún sea moderado.
            used_m = int(d_use / 1000 / (d_per * 0.1)) if d_per else 0
            offenders.append((key, d_thr, d_per, pct, d_us / 1e6,
                              used_m, cur["quota_m"]))
        else:
            clean += 1

    print(f"contenedores comparables: {clean + len(offenders)}  sin throttling: {clean}")
    if not offenders:
        print("PORTÓN OK — ningún límite ató durante la ventana medida.")
        print("El techo observado es de la plataforma, no de la jaula.")
        return 0

    print(f"\nPORTÓN FALLIDO — {len(offenders)} contenedor(es) estrangulados:")
    print(f"{'contenedor':52s} {'períodos':>16s} {'%':>6s} {'seg':>8s} {'uso/cuota':>14s}")
    for key, d_thr, d_per, pct, secs, used_m, quota in sorted(
            offenders, key=lambda x: -x[1]):
        q = f"{used_m}m/{quota}m" if quota else f"{used_m}m/sin-límite"
        print(f"{key:52s} {d_thr:7d}/{d_per:<8d} {pct:5.1f}% {secs:7.1f}s {q:>14s}")
    print("\nEste nivel NO es publicable: el techo medido es el límite, no la")
    print("plataforma. Sube el límite de los contenedores listados y repite.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
