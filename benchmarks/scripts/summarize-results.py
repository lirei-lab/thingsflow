#!/usr/bin/env python3
import json
import re
import sys
from pathlib import Path


def summarize_file(path: Path):
    text = path.read_text(errors="replace")
    summary = {"file": str(path), "published_total": None, "errors": None, "target": None, "namespace": None}
    for line in text.splitlines():
        if line.startswith("target="):
            summary["target"] = line.split("=", 1)[1]
        if line.startswith("namespace="):
            summary["namespace"] = line.split("=", 1)[1]
        try:
            event = json.loads(line)
        except Exception:
            continue
        if event.get("event") == "done":
            summary["published_total"] = event.get("published_total")
            summary["errors"] = event.get("errors")
    if summary["published_total"] is None:
        m = re.search(r"Total telemetry publishes: (\d+)", text)
        if m:
            summary["published_total"] = int(m.group(1))
    return summary


def main():
    root = Path(sys.argv[1] if len(sys.argv) > 1 else "benchmarks/results")
    files = sorted([
        p
        for p in root.glob("**/*")
        if p.is_file() and p.suffix in {".log", ".txt"}
    ])
    for item in [summarize_file(p) for p in files]:
        print(json.dumps(item, sort_keys=True))


if __name__ == "__main__":
    main()
