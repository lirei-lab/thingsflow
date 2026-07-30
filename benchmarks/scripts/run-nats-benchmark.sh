#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

NAMESPACE="${NAMESPACE:-thingsflow}"
RELEASE="${RELEASE:-thingsflow}"
RESULTS_DIR="${RESULTS_DIR:-$ROOT/benchmarks/results}"
RUN_ID="${RUN_ID:-nats-$(date -u +%Y%m%dt%H%M%sz)}"
RUN_ID="$(printf %s "$RUN_ID" | tr "[:upper:]" "[:lower:]" | sed "s/[^a-z0-9.-]/-/g; s/^[^a-z0-9]*//; s/[^a-z0-9]*$//")"

NATS_IMAGE="${NATS_IMAGE:-natsio/nats-box:0.16.0}"
NATS_URL="${NATS_URL:-nats://$RELEASE-nats:4222}"
NATS_JS_STREAM="${NATS_JS_STREAM:-TF_BENCH_RAW}"
NATS_LATEST_STREAM="${NATS_LATEST_STREAM:-TF_LATEST}"
NATS_KV_BUCKET="${NATS_KV_BUCKET:-bench_twin_state}"
NATS_JS_SUBJECT="${NATS_JS_SUBJECT:-tf.bench.raw}"
NATS_KV_SUBJECT="${NATS_KV_SUBJECT:-tenant.bench.device.latest}"

NATS_JS_MESSAGES="${NATS_JS_MESSAGES:-200000}"
NATS_JS_PUBLISHERS="${NATS_JS_PUBLISHERS:-16}"
NATS_JS_SIZE="${NATS_JS_SIZE:-256}"
NATS_KV_MESSAGES="${NATS_KV_MESSAGES:-50000}"
NATS_KV_PUBLISHERS="${NATS_KV_PUBLISHERS:-8}"
NATS_KV_SIZE="${NATS_KV_SIZE:-256}"
NATS_KV_HISTORY="${NATS_KV_HISTORY:-8}"
JOB_TIMEOUT_SECONDS="${JOB_TIMEOUT_SECONDS:-600}"
CLEANUP_NATS="${CLEANUP_NATS:-true}"

KUBECTL=(kubectl)
if [ -n "${KUBECONFIG:-}" ]; then
  KUBECTL+=(--kubeconfig "$KUBECONFIG")
fi

need() {
  command -v "$1" >/dev/null || { echo "missing required command: $1" >&2; exit 127; }
}

kc() {
  "${KUBECTL[@]}" "$@"
}

cleanup_benchmark_jobs() {
  kc -n "$NAMESPACE" delete job -l app.kubernetes.io/part-of=nats-benchmark --ignore-not-found >/dev/null || true
}

cleanup_nats() {
  [ "$CLEANUP_NATS" = "true" ] || return 0
  if [ -n "${MANIFEST:-}" ] && [ -f "$MANIFEST" ]; then
    kc -n "$NAMESPACE" delete -f "$MANIFEST" --ignore-not-found >/dev/null || true
  fi
}

ensure_benchmark_stream() {
  local job="$RUN_ID-bootstrap"
  create_benchmark_job "$job" \
    "nats --server \"\$NATS_URL\" stream add \"$NATS_JS_STREAM\" --subjects \"$NATS_JS_SUBJECT\" --storage memory --retention limits --discard old --max-msgs \"$NATS_JS_MESSAGES\" --defaults || nats --server \"\$NATS_URL\" stream edit \"$NATS_JS_STREAM\" --subjects \"$NATS_JS_SUBJECT\" --storage memory --retention limits --discard old --max-msgs \"$NATS_JS_MESSAGES\" --defaults"
  wait_for_job "$job"
}

ensure_benchmark_kv_bucket() {
  local job="$RUN_ID-kv-bootstrap"
  create_benchmark_job "$job" \
    "nats --server \"\$NATS_URL\" kv add \"$NATS_KV_BUCKET\" --storage memory --history \"$NATS_KV_HISTORY\" --max-value-size 1048576 || true"
  wait_for_job "$job"
}

wait_for_job() {
  local job="$1"
  if kc -n "$NAMESPACE" wait --for=condition=complete "job/$job" --timeout="${JOB_TIMEOUT_SECONDS}s"; then
    kc -n "$NAMESPACE" logs "job/$job" >"$RESULTS_DIR/$RUN_ID-$job.log"
  else
    kc -n "$NAMESPACE" logs "job/$job" >"$RESULTS_DIR/$RUN_ID-$job.log" 2>/dev/null || true
    kc -n "$NAMESPACE" describe "job/$job" >"$RESULTS_DIR/$RUN_ID-$job.describe.txt" 2>/dev/null || true
    echo "job $job did not complete; logs saved under $RESULTS_DIR/$RUN_ID-$job.*" >&2
    return 1
  fi
}

create_benchmark_job() {
  local job="$1"
  local command="$2"
  kc -n "$NAMESPACE" delete job "$job" --ignore-not-found >/dev/null
  kc -n "$NAMESPACE" apply -f - <<YAML
apiVersion: batch/v1
kind: Job
metadata:
  name: $job
  labels:
    app.kubernetes.io/part-of: nats-benchmark
spec:
  backoffLimit: 0
  template:
    metadata:
      labels:
        app.kubernetes.io/part-of: nats-benchmark
    spec:
      restartPolicy: Never
      containers:
      - name: nats-bench
        image: $NATS_IMAGE
        imagePullPolicy: IfNotPresent
        env:
        - name: NATS_URL
          value: "$NATS_URL"
        command: ["sh", "-c"]
        args:
        - |
          set -e
          until nats --server "\$NATS_URL" server check connection >/dev/null 2>&1; do
            sleep 2
          done
          $command
YAML
}

main() {
  need kubectl
  need helm
  need date
  mkdir -p "$RESULTS_DIR"
  trap 'cleanup_benchmark_jobs; cleanup_nats' EXIT

  MANIFEST="$(mktemp /tmp/thingsflow-nats.XXXXXX.yaml)"
  helm template "$RELEASE" "$ROOT/k8s/helm/thingsflow" \
    --show-only templates/nats.yaml \
    --set nats.enabled=true >"$MANIFEST"

  kc create namespace "$NAMESPACE" --dry-run=client -o yaml | kc apply -f - >/dev/null
  kc -n "$NAMESPACE" delete job "$RELEASE-nats-bootstrap" --ignore-not-found >/dev/null || true
  kc -n "$NAMESPACE" apply -f "$MANIFEST" >/dev/null
  kc -n "$NAMESPACE" rollout status "deploy/$RELEASE-nats" --timeout=120s >/dev/null
  kc -n "$NAMESPACE" wait --for=condition=complete "job/$RELEASE-nats-bootstrap" --timeout=120s >/dev/null

  cleanup_benchmark_jobs
  ensure_benchmark_stream
  ensure_benchmark_kv_bucket
  create_benchmark_job "$RUN_ID-js" \
    "nats --server \"\$NATS_URL\" bench \"$NATS_JS_SUBJECT\" --js --stream \"$NATS_JS_STREAM\" --pub \"$NATS_JS_PUBLISHERS\" --msgs \"$NATS_JS_MESSAGES\" --size \"$NATS_JS_SIZE\" --no-progress"
  create_benchmark_job "$RUN_ID-kv" \
    "nats --server \"\$NATS_URL\" bench \"$NATS_KV_SUBJECT\" --kv --bucket \"$NATS_KV_BUCKET\" --storage memory --pub \"$NATS_KV_PUBLISHERS\" --msgs \"$NATS_KV_MESSAGES\" --size \"$NATS_KV_SIZE\" --history \"$NATS_KV_HISTORY\" --no-progress"

  status=0
  wait_for_job "$RUN_ID-js" || status=1
  wait_for_job "$RUN_ID-kv" || status=1

  {
    echo "run_id=$RUN_ID"
    echo "namespace=$NAMESPACE"
    echo "nats_url=$NATS_URL"
    echo "js_stream=$NATS_JS_STREAM"
    echo "js_subject=$NATS_JS_SUBJECT"
    echo "kv_bucket=$NATS_KV_BUCKET"
    echo "js_messages=$NATS_JS_MESSAGES"
    echo "js_publishers=$NATS_JS_PUBLISHERS"
    echo "kv_messages=$NATS_KV_MESSAGES"
    echo "kv_publishers=$NATS_KV_PUBLISHERS"
    echo
    echo "# JetStream benchmark"
    cat "$RESULTS_DIR/$RUN_ID-$RUN_ID-js.log"
    echo
    echo "# KV benchmark"
    cat "$RESULTS_DIR/$RUN_ID-$RUN_ID-kv.log"
  } >"$RESULTS_DIR/$RUN_ID-summary.txt"

  echo "wrote $RESULTS_DIR/$RUN_ID-summary.txt"
  [ "$status" -eq 0 ] || exit "$status"
}

main "$@"
