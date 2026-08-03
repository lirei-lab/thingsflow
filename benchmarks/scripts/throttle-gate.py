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
import time

CGROUP_ROOT = "/sys/fs/cgroup/kubepods"

# Umbrales de severidad, y por qué NO es un simple "hubo throttling".
#
# Un contador absoluto castiga sistemáticamente a la JVM: sus ráfagas de GC e
# hilos superan la cuota dentro de algún período de 100 ms aunque el promedio
# esté al 43 %. Medido en ThingsBoard: 67 períodos de 2 324 (2,9 %), 4,4 s sobre
# una ventana de 232 s — y el nivel aterrizó 1 035 000 filas EXACTAS sin pérdida.
# Invalidar esa corrida habría sido penalizar a un motor por su modelo de hilos,
# no por su rendimiento, mientras los componentes Go/Rust del otro lado pasan.
#
# Criterio: el throttling importa cuando IMPIDE CUMPLIR. Se reportan dos niveles
# y quien decide el veredicto es la rampa, que sí sabe si el nivel cumplió:
#   leve  (exit 1) -> hubo estrangulamiento pero pudo no atar; si el nivel
#                     cumplió con cero pérdida, no invalida.
#   grave (exit 2) -> tanto que distorsiona incluso un nivel que cumplió; su
#                     medición de recursos ya no es fiable.
SEVERE_THROTTLED_PCT = 10.0     # % de períodos estrangulados
SEVERE_THROTTLED_WALL_PCT = 5.0  # % del tiempo de pared perdido
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
                    # memory.current es instantáneo (no acumulado): en el
                    # snapshot es el reposo y en el check el valor bajo carga.
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
        # El instante se guarda con la instantánea: es el ÚNICO reloj válido
        # para promediar consumo. Ver la nota en el cálculo de cpu_m.
        with open(path, "w") as f:
            json.dump({"_t": time.time(), "c": now}, f)
        print(f"instantánea: {len(now)} contenedores en {ns}")
        return 0

    if mode != "check":
        print(f"modo desconocido: {mode}", file=sys.stderr)
        return 2

    with open(path) as f:
        snap = json.load(f)
    # Compatibilidad con instantáneas del formato viejo (sin reloj).
    if "_t" in snap:
        before, t0 = snap["c"], snap["_t"]
    else:
        before, t0 = snap, os.path.getmtime(path)
    elapsed = max(time.time() - t0, 1e-6)

    # El consumo por contenedor durante la ventana ES el resultado del
    # benchmark, no un subproducto del portón. Se persiste junto al snapshot:
    # cpu.stat.usage_usec es acumulado, así que el delta entre snapshot y check
    # dividido por el tiempo transcurrido da los millicores medios reales,
    # medidos por el kernel y no muestreados por metrics-server cada 15 s.
    usage_out = {}

    offenders, clean = [], 0
    for key, cur in sorted(now.items()):
        prev = before.get(key)
        if prev is None:
            continue  # pod nacido durante la ventana; no comparable
        d_thr = cur["nr_throttled"] - prev["nr_throttled"]
        d_per = cur["nr_periods"] - prev["nr_periods"]
        d_us = cur["throttled_usec"] - prev["throttled_usec"]
        d_use = cur["usage_usec"] - prev["usage_usec"]

        # El divisor es el reloj de pared, IDÉNTICO para todos los contenedores.
        #
        # NO usar nr_periods * 0.1: nr_periods solo avanza cuando el cgroup
        # tiene tareas ejecutables, así que un contenedor a ráfagas acumula
        # pocos períodos y dividir por ellos INFLA sus millicores. Medido: en
        # una misma ventana las "duraciones" derivadas de nr_periods iban de
        # 0,2 s a 177,9 s, y latest-kv aparecía gastando 945m con HTTP a 1 000
        # msg/s frente a 672m con MQTT a 8 000 — más CPU con ocho veces menos
        # carga, que es imposible y delató el error.
        usage_out[key] = {
            "cpu_m": round(d_use / 1000 / elapsed),
            "mem_bytes": cur.get("mem_bytes"),
            "quota_m": cur["quota_m"],
            "throttled_periods": d_thr,
            # Crudo, para poder recalcular sin repetir la medición.
            "cpu_usec_delta": d_use,
            "window_s": round(elapsed, 1),
        }

        if d_thr > TOLERATED_THROTTLED_PERIODS:
            pct = 100.0 * d_thr / d_per if d_per else 0.0
            # Cuánta CPU consumió de su cuota: delata al que está contra el techo
            # aunque el throttling aún sea moderado. Mismo reloj de pared que
            # arriba, por la misma razón.
            used_m = round(d_use / 1000 / elapsed)
            offenders.append((key, d_thr, d_per, pct, d_us / 1e6,
                              used_m, cur["quota_m"]))
        else:
            clean += 1

    with open(path + ".usage.json", "w") as f:
        json.dump(usage_out, f, indent=1)

    tot_cpu = sum(v["cpu_m"] or 0 for v in usage_out.values())
    tot_mem = sum(v["mem_bytes"] or 0 for v in usage_out.values())
    print(f"consumo de la plataforma en la ventana: CPU={tot_cpu}m  "
          f"MEM={tot_mem / 1048576:.0f}MiB")
    top = sorted(usage_out.items(), key=lambda x: -(x[1]["cpu_m"] or 0))[:5]
    for k, v in top:
        if v["cpu_m"]:
            print(f"    {k.split('/')[0]:52s} {v['cpu_m']:5d}m "
                  f"{(v['mem_bytes'] or 0) / 1048576:7.0f}MiB")

    print(f"contenedores comparables: {clean + len(offenders)}  sin throttling: {clean}")
    if not offenders:
        print("PORTÓN OK — ningún límite ató durante la ventana medida.")
        print("El techo observado es de la plataforma, no de la jaula.")
        return 0

    severe = [o for o in offenders
              if o[3] >= SEVERE_THROTTLED_PCT
              or (100.0 * o[4] / elapsed) >= SEVERE_THROTTLED_WALL_PCT]
    nivel = "GRAVE" if severe else "LEVE"
    print(f"\nPORTÓN: estrangulamiento {nivel} — {len(offenders)} contenedor(es):")
    print(f"{'contenedor':52s} {'períodos':>16s} {'%':>6s} {'seg':>8s} {'uso/cuota':>14s}")
    for key, d_thr, d_per, pct, secs, used_m, quota in sorted(
            offenders, key=lambda x: -x[1]):
        q = f"{used_m}m/{quota}m" if quota else f"{used_m}m/sin-límite"
        print(f"{key:52s} {d_thr:7d}/{d_per:<8d} {pct:5.1f}% {secs:7.1f}s {q:>14s}")
    if severe:
        print("\nGRAVE: el estrangulamiento distorsiona la medición aunque el nivel")
        print("cumpla. Sube el límite de los contenedores listados y repite.")
        return 2
    print("\nLEVE: puede no haber atado. Si el nivel cumplió con cero pérdida y cero")
    print("errores, no lo invalida — penalizar rafagas de GC seria castigar el modelo")
    print("de hilos, no el rendimiento. Si el nivel NO cumplio, no se puede distinguir")
    print("el techo de la plataforma del de la jaula, y ahi si invalida.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
