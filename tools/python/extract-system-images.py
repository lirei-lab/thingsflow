#!/usr/bin/env python3
# Extract widget preview images that stock TB used to ship inline as base64 in
# widget_type / widget_bundle JSONs. The upstream repo was cleaned commit by
# commit — each new widget arrived with inline base64 and a follow-up replaced
# the inline payload with a `tb-image;/api/images/system/<key>` URL ref. So no
# single commit has every image inline.
#
# Strategy: walk git history for every JSON under the widget paths, scan each
# blob for the inline form, and dedup by key (first one wins). One commit gives
# ~218 images; the full walk recovers every image that was ever inline.
#
# Output:
#   flow-core/tb-resources/system_images/<key>            decoded bytes
#   flow-core/tb-resources/system_images/_manifest.json   key -> {title, mediaType}
#
# tb-image format inside a JSON image field:
#   tb-image:<b64-key>[:<b64-name>[:<b64-subtype>[:<etag>]]];data:<mediaType>;base64,<payload>

import base64
import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
SCAN_PATHS = [
    "application/src/main/data/json/system/widget_types",
    "application/src/main/data/json/system/widget_bundles",
    "application/src/main/data/json/demo/dashboards",
]
OUT_DIR = REPO / "flow-core" / "tb-resources" / "system_images"

PATTERN = re.compile(
    r"tb-image:([^;\"\\]+);data:([^;]+);base64,([A-Za-z0-9+/=]+)"
)


def b64dec(s: str) -> bytes:
    pad = "=" * (-len(s) % 4)
    return base64.b64decode(s + pad)


def git_run(*args: str) -> str:
    out = subprocess.run(
        ["git", *args], check=True, capture_output=True, cwd=REPO,
    )
    return out.stdout.decode("utf-8", errors="replace")


def list_unique_blobs(scan_paths: list[str]) -> list[tuple[str, str]]:
    """Returns [(blob_sha, file_path)] across all of git history under scan_paths.

    We dedup by blob_sha so we only read each unique file content once even if
    it appeared in many commits. That keeps the walk fast on a large repo.
    """
    seen: set[str] = set()
    blobs: list[tuple[str, str]] = []
    out = git_run(
        "log", "--all", "--no-merges", "--pretty=format:",
        "--raw", "--diff-filter=AM",
        "--", *scan_paths,
    )
    for line in out.splitlines():
        # "raw" line: ":<srcmode> <dstmode> <srcsha> <dstsha> <status>\t<path>"
        if not line.startswith(":"):
            continue
        try:
            meta, path = line.split("\t", 1)
        except ValueError:
            continue
        parts = meta.split()
        if len(parts) < 5:
            continue
        dst_sha = parts[3]
        if not path.endswith(".json") or dst_sha == "0" * len(dst_sha):
            continue
        if dst_sha in seen:
            continue
        seen.add(dst_sha)
        blobs.append((dst_sha, path))
    return blobs


def read_blob(sha: str) -> str:
    return git_run("cat-file", "-p", sha)


def extract(text: str, source_label: str, manifest: dict, written: dict):
    for m in PATTERN.finditer(text):
        meta_raw, media_type, payload = m.group(1), m.group(2), m.group(3)
        parts = meta_raw.split(":")
        try:
            key = b64dec(parts[0]).decode("utf-8")
        except Exception:
            print(f"  WARN: bad key in {source_label}, skipping", file=sys.stderr)
            continue
        title = ""
        if len(parts) >= 2 and parts[1]:
            try:
                title = b64dec(parts[1]).decode("utf-8")
            except Exception:
                pass
        try:
            data = b64dec(payload)
        except Exception:
            print(f"  WARN: bad base64 for {key}, skipping", file=sys.stderr)
            continue
        existing = written.get(key)
        if existing is not None:
            if existing != data:
                print(f"  WARN: {key} appears twice with differing bytes — keeping first", file=sys.stderr)
            continue
        written[key] = data
        (OUT_DIR / key).write_bytes(data)
        manifest[key] = {
            "title": title,
            "mediaType": media_type,
            "etag": hashlib.sha256(data).hexdigest(),
        }


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for old in OUT_DIR.iterdir():
        if old.name == "_manifest.json" or old.is_file():
            old.unlink()

    manifest: dict[str, dict] = {}
    written: dict[str, bytes] = {}

    print(f"→ enumerating unique blobs across git history under:")
    for p in SCAN_PATHS:
        print(f"    {p}")
    blobs = list_unique_blobs(SCAN_PATHS)
    print(f"  {len(blobs)} unique blobs to scan")

    for i, (sha, path) in enumerate(blobs):
        if i % 500 == 0 and i:
            print(f"  ... {i}/{len(blobs)} blobs scanned, {len(manifest)} images so far")
        try:
            text = read_blob(sha)
        except subprocess.CalledProcessError:
            continue
        extract(text, f"{sha[:8]} {path}", manifest, written)

    (OUT_DIR / "_manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n"
    )
    print(f"✓ wrote {len(manifest)} images to {OUT_DIR}")


if __name__ == "__main__":
    main()
