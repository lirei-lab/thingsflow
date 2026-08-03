#!/usr/bin/env python3
"""Detects keys in a values file that the chart does NOT read.

Why it exists: the previous benchmark profile contained

    resources:
      nats:
        limits: { cpu: "3000m" }

but the chart reads `nats.resources`, not `resources.nats`. Helm does not warn
about unknown keys: it ignores them silently. NATS ran the whole ramp with its
default value of 500m, was throttled in 1,819 periods, and that ceiling was
published as ThingsFlow's MQTT limit at 8,000 msg/s.

An override that is not applied is worse than not setting it at all: it produces
a measurement that looks configured and is not.

LIMITATION, and it is important: this is a HEURISTIC, not a proof. It compares
the override's keys against those of the chart's values.yaml, so it gives false
positives when the template dumps a whole subtree with `toYaml` — there a new
key does apply even though it does not appear in values.yaml. Real example:
`resources.postgres.limits.cpu` is flagged as unknown and yet it does land,
because postgres.yaml does `toYaml .Values.resources.postgres`.

Treat it as "review these keys", not as "these keys are broken". The definitive
proof is reading the effective limits of the already deployed cluster:
    verify-effective-limits.py

Usage:
    check-values-keys.py <chart-dir> <values-override.yaml> [more overrides...]

Exits 1 if any key of the override does not exist in the chart's values.yaml.
"""
import sys

try:
    import yaml
except ImportError:
    print("PyYAML is missing: pip install pyyaml", file=sys.stderr)
    sys.exit(2)


def flatten(node, prefix=""):
    """Paths of every key. List leaves are not walked: charts consume them whole
    and their elements are not configuration keys."""
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
        print(f"could not read {chart_values}: {e}", file=sys.stderr)
        return 2

    bad = False
    for override in sys.argv[2:]:
        with open(override) as f:
            keys = flatten(yaml.safe_load(f) or {})
        unknown = sorted(k for k in keys if k not in known)
        # A key that is a child of an unknown one adds no new information: it is
        # enough to report the root of the error.
        roots = [k for k in unknown
                 if not any(k.startswith(o + ".") for o in unknown)]
        print(f"\n{override}: {len(keys)} keys, {len(roots)} to review")
        if not roots:
            print("  OK — every key appears in the chart's values.yaml.")
            continue
        bad = True
        for k in roots:
            # Suggest the swapped permutation, which is the typical mistake
            parts = k.split(".")
            hint = ""
            if len(parts) >= 2:
                swapped = ".".join(parts[:-2] + [parts[-1], parts[-2]])
                if swapped in known:
                    hint = f"   did you mean '{swapped}'?"
            print(f"  TO REVIEW: {k}{hint}")

    if bad:
        print("\nThese keys do not appear in the chart's values.yaml. It may be an")
        print("override that does not land (a misconfigured measurement) or a false")
        print("positive from `toYaml` over the parent subtree. Confirm it with")
        print("verify-effective-limits.py against the already deployed cluster.")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
