#!/usr/bin/env python3
"""Verifica la REGLA DE JUSTICIA contra el cluster desplegado, no contra el YAML.

La regla: ningún contenedor puede tener un límite de CPU inferior a 3x su
consumo observado. Si lo tiene, la plataforma está midiendo su jaula.

Por qué contra el cluster y no contra el fichero de values: un override puede no
llegar. Ya pasó — el perfil anterior escribía `resources.nats` cuando el chart
lee `nats.resources`, Helm lo ignoró en silencio, NATS corrió con 500m y ese
techo se publicó como el límite MQTT de ThingsFlow. Lo único que no miente es
lo que el kubelet aplicó de verdad.

Complementa a throttle-gate.py: el portón detecta el estrangulamiento *después*
de que ocurra; esto avisa *antes* de gastar una rampa entera.

Uso:
    verify-effective-limits.py <namespace> [headroom-mínimo]   # por defecto 3.0

Ejecutar CON CARGA aplicada: sin carga el consumo es de reposo y la holgura
sale artificialmente enorme, con lo que la comprobación no dice nada.
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
        print(f"metrics-server no responde: {out.stderr.strip()}", file=sys.stderr)
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
        # Un contenedor que apenas consume no puede violar la regla de forma
        # significativa; exigir 3x sobre 2m produciría ruido sin información.
        if u < 50:
            ok += 1
            continue
        if l < need * u:
            tight.append((key, u, l, l / u))
        else:
            ok += 1

    print(f"namespace {ns}: {ok} contenedores con holgura suficiente "
          f"(>= {need:g}x), {len(tight)} ajustados, {len(unlimited)} sin límite")

    for (pod, c), u in unlimited:
        print(f"  sin límite: {pod}/{c} usando {u}m  (no ata, pero declara el hueco)")

    if not tight:
        print(f"\nREGLA CUMPLIDA — ningún límite está a menos de {need:g}x del consumo.")
        print("Lo que se mida aquí es la plataforma, no la jaula.")
        return 0

    print(f"\nREGLA INCUMPLIDA — {len(tight)} contenedor(es) sin holgura:")
    print(f"  {'contenedor':56s} {'uso':>8s} {'límite':>8s} {'holgura':>9s}")
    for (pod, c), u, l, ratio in sorted(tight, key=lambda x: x[3]):
        print(f"  {pod + '/' + c:56s} {u:7d}m {l:7d}m {ratio:8.2f}x")
    print("\nSube estos límites antes de medir: su techo sería el resultado.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
