#!/usr/bin/env python3
"""Detecta claves de un fichero de values que el chart NO lee.

Por qué existe: el perfil de benchmark anterior contenía

    resources:
      nats:
        limits: { cpu: "3000m" }

pero el chart lee `nats.resources`, no `resources.nats`. Helm no avisa de
claves desconocidas: las ignora en silencio. NATS corrió toda la rampa con su
valor por defecto de 500m, se estranguló en 1 819 periodos, y ese techo se
publicó como el límite MQTT de ThingsFlow a 8 000 msg/s.

Un override que no se aplica es peor que no ponerlo: produce una medición que
parece configurada y no lo está.

LIMITACIÓN, y es importante: esto es una HEURÍSTICA, no una prueba. Compara las
claves del override contra las del values.yaml del chart, así que da falsos
positivos cuando la plantilla vuelca un subárbol entero con `toYaml` — ahí una
clave nueva sí se aplica aunque no figure en values.yaml. Ejemplo real:
`resources.postgres.limits.cpu` se marca como desconocida y sin embargo llega,
porque postgres.yaml hace `toYaml .Values.resources.postgres`.

Trátalo como «revisa estas claves», no como «estas claves están rotas». La
prueba definitiva es leer los límites efectivos del cluster ya desplegado:
    verify-effective-limits.py

Uso:
    check-values-keys.py <chart-dir> <values-override.yaml> [más overrides...]

Sale 1 si alguna clave del override no existe en el values.yaml del chart.
"""
import sys

try:
    import yaml
except ImportError:
    print("falta PyYAML: pip install pyyaml", file=sys.stderr)
    sys.exit(2)


def flatten(node, prefix=""):
    """Rutas de todas las claves. Las hojas de lista no se recorren: los charts
    las consumen enteras y sus elementos no son claves de configuración."""
    out = set()
    if isinstance(node, dict):
        for k, v in node.items():
            path = f"{prefix}.{k}" if prefix else str(k)
            out.add(path)
            out |= flatten(v, path)
    return out


def main():
    if len(sys.argv) < 3:
        print(__doc__)
        return 2
    chart_values = f"{sys.argv[1].rstrip('/')}/values.yaml"
    try:
        with open(chart_values) as f:
            known = flatten(yaml.safe_load(f) or {})
    except OSError as e:
        print(f"no se pudo leer {chart_values}: {e}", file=sys.stderr)
        return 2

    bad = False
    for override in sys.argv[2:]:
        with open(override) as f:
            keys = flatten(yaml.safe_load(f) or {})
        unknown = sorted(k for k in keys if k not in known)
        # Una clave hija de una desconocida no aporta información nueva: basta
        # con reportar la raíz del error.
        roots = [k for k in unknown
                 if not any(k.startswith(o + ".") for o in unknown)]
        print(f"\n{override}: {len(keys)} claves, {len(roots)} a revisar")
        if not roots:
            print("  OK — todas las claves aparecen en el values.yaml del chart.")
            continue
        bad = True
        for k in roots:
            # Sugerir la permutación invertida, que es el error típico
            parts = k.split(".")
            hint = ""
            if len(parts) >= 2:
                swapped = ".".join(parts[:-2] + [parts[-1], parts[-2]])
                if swapped in known:
                    hint = f"   ¿querías decir '{swapped}'?"
            print(f"  A REVISAR: {k}{hint}")

    if bad:
        print("\nEstas claves no figuran en el values.yaml del chart. Puede ser un")
        print("override que no llega (medición mal configurada) o un falso positivo")
        print("por `toYaml` sobre el subárbol padre. Confírmalo con")
        print("verify-effective-limits.py contra el cluster ya desplegado.")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
