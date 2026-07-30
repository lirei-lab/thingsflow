#!/usr/bin/env bash
set -euo pipefail

TARGET="${1:-}"
NAMESPACE="${2:-}"
RESULTS_DIR="${RESULTS_DIR:-$(pwd)/benchmarks/results}"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)-$TARGET-metrics}"

[ -n "$TARGET" ] || { echo "Usage: benchmarks/scripts/collect-metrics.sh <target> <namespace>" >&2; exit 2; }
[ -n "$NAMESPACE" ] || { echo "Usage: benchmarks/scripts/collect-metrics.sh <target> <namespace>" >&2; exit 2; }
command -v kubectl >/dev/null || { echo "missing required command: kubectl" >&2; exit 127; }
mkdir -p "$RESULTS_DIR"

OUT="$RESULTS_DIR/$RUN_ID.txt"
{
  echo "target=$TARGET"
  echo "namespace=$NAMESPACE"
  echo "captured_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo
  echo "# pods"
  kubectl -n "$NAMESPACE" get pods -o wide || true
  echo
  echo "# services"
  kubectl -n "$NAMESPACE" get svc -o wide || true
  echo
  echo "# top pods"
  kubectl -n "$NAMESPACE" top pods || true
  echo
  echo "# events"
  kubectl -n "$NAMESPACE" get events --sort-by=.lastTimestamp | tail -80 || true
  echo
  echo "# helm"
  helm -n "$NAMESPACE" list || true
} | tee "$OUT"

echo "wrote $OUT"
