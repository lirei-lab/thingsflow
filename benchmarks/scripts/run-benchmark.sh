#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TARGET="${1:-}"
SCENARIO_FILE="${2:-}"

usage() {
  echo "Usage: benchmarks/scripts/run-benchmark.sh <thingsflow|thingsboard-classic> <scenario.env>" >&2
  exit 2
}

[ -n "$TARGET" ] || usage
[ -n "$SCENARIO_FILE" ] || usage
[ -f "$SCENARIO_FILE" ] || { echo "missing scenario: $SCENARIO_FILE" >&2; exit 2; }

ENV_PROTOCOL="${PROTOCOL:-}"
ENV_DEVICE_COUNT="${DEVICE_COUNT:-}"
ENV_PUBLISH_INTERVAL_SECONDS="${PUBLISH_INTERVAL_SECONDS:-}"
ENV_RUN_DURATION_SECONDS="${RUN_DURATION_SECONDS:-}"
ENV_DEVICE_PREFIX="${DEVICE_PREFIX:-}"
ENV_ENABLE_AUX_TRAFFIC="${ENABLE_AUX_TRAFFIC:-}"
ENV_LOADGEN_SHARDS="${LOADGEN_SHARDS:-}"
ENV_LOADGEN_MAX_WORKERS="${LOADGEN_MAX_WORKERS:-}"
ENV_MQTT_AUTH_MODE="${MQTT_AUTH_MODE:-}"

# shellcheck disable=SC1090
. "$SCENARIO_FILE"

PROTOCOL="${ENV_PROTOCOL:-${PROTOCOL:-mqtt}}"
DEVICE_COUNT="${ENV_DEVICE_COUNT:-${DEVICE_COUNT:-100}}"
PUBLISH_INTERVAL_SECONDS="${ENV_PUBLISH_INTERVAL_SECONDS:-${PUBLISH_INTERVAL_SECONDS:-5}}"
RUN_DURATION_SECONDS="${ENV_RUN_DURATION_SECONDS:-${RUN_DURATION_SECONDS:-900}}"
DEVICE_PREFIX="${ENV_DEVICE_PREFIX:-${DEVICE_PREFIX:-bench}}"
ENABLE_AUX_TRAFFIC="${ENV_ENABLE_AUX_TRAFFIC:-${ENABLE_AUX_TRAFFIC:-false}}"
LOADGEN_SHARDS="${ENV_LOADGEN_SHARDS:-${LOADGEN_SHARDS:-1}}"
LOADGEN_MAX_WORKERS="${ENV_LOADGEN_MAX_WORKERS:-${LOADGEN_MAX_WORKERS:-}}"
MQTT_AUTH_MODE="${ENV_MQTT_AUTH_MODE:-${MQTT_AUTH_MODE:-}}"
PYTHON_IMAGE="${PYTHON_IMAGE:-python:3.13-slim}"
RESULTS_DIR="${RESULTS_DIR:-$ROOT/benchmarks/results}"
WAIT_FOR_COMPLETION="${WAIT_FOR_COMPLETION:-false}"
JOB_TIMEOUT_SECONDS="${JOB_TIMEOUT_SECONDS:-$((RUN_DURATION_SECONDS + 600))}"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dt%H%M%sz)-$TARGET-${PROTOCOL}-${DEVICE_COUNT}}"
RUN_ID="$(printf %s "$RUN_ID" | tr "[:upper:]" "[:lower:]" | sed "s/[^a-z0-9.-]/-/g; s/^[^a-z0-9]*//; s/[^a-z0-9]*$//")"
NAMESPACE="${NAMESPACE:-}"
RELEASE="${RELEASE:-}"

need() {
  command -v "$1" >/dev/null || { echo "missing required command: $1" >&2; exit 127; }
}
need kubectl
need helm
need date
mkdir -p "$RESULTS_DIR"

case "$TARGET" in
  thingsflow)
    NAMESPACE="${NAMESPACE:-thingsflow-bench}"
    RELEASE="${RELEASE:-thingsflow}"
    API_SERVICE="${API_SERVICE:-$RELEASE-flow-core}"
    MQTT_SERVICE="${MQTT_SERVICE:-$RELEASE-rmqtt-edge}"
    TB_BASE_URL="${TB_BASE_URL:-http://$API_SERVICE.$NAMESPACE.svc.cluster.local:8080}"
    MQTT_HOST="${MQTT_HOST:-$MQTT_SERVICE.$NAMESPACE.svc.cluster.local}"
    MQTT_PORT="${MQTT_PORT:-1883}"
    THINGSFLOW_NATIVE_EDGE="${THINGSFLOW_NATIVE_EDGE:-true}"
    MQTT_AUTH_MODE="${MQTT_AUTH_MODE:-deviceJwtRaw}"
    if [ "$PROTOCOL" = "http" ]; then
      HTTP_TELEMETRY_SERVICE="${HTTP_TELEMETRY_SERVICE:-$RELEASE-flow-core}"
      TELEMETRY_BASE_URL="${TELEMETRY_BASE_URL:-http://$HTTP_TELEMETRY_SERVICE.$NAMESPACE.svc.cluster.local:8080}"
    else
      TELEMETRY_BASE_URL="${TELEMETRY_BASE_URL:-$TB_BASE_URL}"
    fi
    ;;
  thingsboard-classic)
    NAMESPACE="${NAMESPACE:-tb-classic-bench}"
    RELEASE="${RELEASE:-tb-classic}"
    helm upgrade --install "$RELEASE" "$ROOT/benchmarks/helm/thingsboard-classic" \
      -n "$NAMESPACE" --create-namespace
    API_SERVICE="${API_SERVICE:-$RELEASE-thingsboard-classic}"
    MQTT_SERVICE="${MQTT_SERVICE:-$API_SERVICE}"
    TB_BASE_URL="${TB_BASE_URL:-http://$API_SERVICE.$NAMESPACE.svc.cluster.local:8080}"
    MQTT_HOST="${MQTT_HOST:-$MQTT_SERVICE.$NAMESPACE.svc.cluster.local}"
    MQTT_PORT="${MQTT_PORT:-1883}"
    TELEMETRY_BASE_URL="${TELEMETRY_BASE_URL:-$TB_BASE_URL}"
    THINGSFLOW_NATIVE_EDGE="${THINGSFLOW_NATIVE_EDGE:-false}"
    MQTT_AUTH_MODE="${MQTT_AUTH_MODE:-accessToken}"
    ;;
  *)
    usage
    ;;
esac

case "$LOADGEN_SHARDS" in
  ''|*[!0-9]*) echo "LOADGEN_SHARDS must be a positive integer" >&2; exit 2 ;;
esac
[ "$LOADGEN_SHARDS" -gt 0 ] || { echo "LOADGEN_SHARDS must be > 0" >&2; exit 2; }
[ "$DEVICE_COUNT" -ge "$LOADGEN_SHARDS" ] || { echo "DEVICE_COUNT must be >= LOADGEN_SHARDS" >&2; exit 2; }

kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl -n "$NAMESPACE" delete configmap thingsflow-benchmark-loadgen --ignore-not-found >/dev/null
kubectl -n "$NAMESPACE" create configmap thingsflow-benchmark-loadgen \
  --from-file=telemetry_generator.py="$ROOT/tools/python/telemetry_generator.py" \
  --from-file=http_loadgen.py="$ROOT/benchmarks/scripts/http-loadgen.py" >/dev/null

GENERATOR="telemetry_generator.py"
if [ "$PROTOCOL" = "http" ]; then
  GENERATOR="http_loadgen.py"
fi

BASE_DEVICE_PREFIX="$DEVICE_PREFIX"
BASE_SHARD_DEVICE_COUNT=$((DEVICE_COUNT / LOADGEN_SHARDS))
EXTRA_SHARD_DEVICES=$((DEVICE_COUNT % LOADGEN_SHARDS))
JOBS=()

write_job_yaml() {
  local SHARD_INDEX="$1"
  local SHARD_DEVICE_COUNT="$2"
  local SHARD_DEVICE_PREFIX="$3"
  local JOB="bench-$RUN_ID-s$SHARD_INDEX"
  local JOB_FILE="$RESULTS_DIR/$RUN_ID-s$SHARD_INDEX.job.yaml"

  cat >"$JOB_FILE" <<YAML
apiVersion: batch/v1
kind: Job
metadata:
  name: $JOB
  labels:
    app.kubernetes.io/part-of: iot-benchmark
    benchmark.thingsflow.io/target: $TARGET
    benchmark.thingsflow.io/protocol: $PROTOCOL
    benchmark.thingsflow.io/shard-index: "$SHARD_INDEX"
    benchmark.thingsflow.io/shard-count: "$LOADGEN_SHARDS"
spec:
  backoffLimit: 0
  template:
    metadata:
      labels:
        app.kubernetes.io/part-of: iot-benchmark
        benchmark.thingsflow.io/target: $TARGET
        benchmark.thingsflow.io/shard-index: "$SHARD_INDEX"
    spec:
      restartPolicy: Never
      containers:
        - name: loadgen
          image: $PYTHON_IMAGE
          imagePullPolicy: IfNotPresent
          command: ["/bin/bash", "-c"]
          args:
            - pip install --quiet --disable-pip-version-check --root-user-action=ignore requests paho-mqtt && python /scripts/$GENERATOR
          env:
            - name: TB_BASE_URL
              value: "$TB_BASE_URL"
            - name: BRIDGE_URL
              value: "$TB_BASE_URL"
            - name: TELEMETRY_BASE_URL
              value: "$TELEMETRY_BASE_URL"
            - name: THINGSFLOW_NATIVE_EDGE
              value: "$THINGSFLOW_NATIVE_EDGE"
            - name: MQTT_AUTH_MODE
              value: "$MQTT_AUTH_MODE"
            - name: TB_USER
              value: "${TB_USER:-tenant@thingsboard.org}"
            - name: TB_PASS
              value: "${TB_PASS:-tenant}"
            - name: MQTT_HOST
              value: "$MQTT_HOST"
            - name: MQTT_PORT
              value: "$MQTT_PORT"
            - name: LOADGEN_DEVICE_COUNT
              value: "$SHARD_DEVICE_COUNT"
            - name: LOADGEN_INTERVAL_SECONDS
              value: "$PUBLISH_INTERVAL_SECONDS"
            - name: LOADGEN_MAX_WORKERS
              value: "${LOADGEN_MAX_WORKERS:-$SHARD_DEVICE_COUNT}"
            - name: RUN_DURATION_SECONDS
              value: "$RUN_DURATION_SECONDS"
            - name: DEVICE_PREFIX
              value: "$SHARD_DEVICE_PREFIX"
            - name: ENABLE_AUX_TRAFFIC
              value: "$ENABLE_AUX_TRAFFIC"
            - name: SHARD_INDEX
              value: "$SHARD_INDEX"
            - name: SHARD_COUNT
              value: "$LOADGEN_SHARDS"
          volumeMounts:
            - name: scripts
              mountPath: /scripts
      volumes:
        - name: scripts
          configMap:
            name: thingsflow-benchmark-loadgen
YAML
}

for SHARD_INDEX in $(seq 0 $((LOADGEN_SHARDS - 1))); do
  SHARD_DEVICE_COUNT="$BASE_SHARD_DEVICE_COUNT"
  if [ "$SHARD_INDEX" -lt "$EXTRA_SHARD_DEVICES" ]; then
    SHARD_DEVICE_COUNT=$((SHARD_DEVICE_COUNT + 1))
  fi
  SHARD_DEVICE_PREFIX="$BASE_DEVICE_PREFIX-s$SHARD_INDEX"
  JOB="bench-$RUN_ID-s$SHARD_INDEX"
  JOBS+=("$JOB")
  kubectl -n "$NAMESPACE" delete job "$JOB" --ignore-not-found >/dev/null
  write_job_yaml "$SHARD_INDEX" "$SHARD_DEVICE_COUNT" "$SHARD_DEVICE_PREFIX"
  kubectl -n "$NAMESPACE" apply -f "$RESULTS_DIR/$RUN_ID-s$SHARD_INDEX.job.yaml"
  echo "benchmark job: $JOB devices=$SHARD_DEVICE_COUNT shard=$SHARD_INDEX/$LOADGEN_SHARDS"
done

echo "namespace: $NAMESPACE"
echo "results prefix: $RESULTS_DIR/$RUN_ID"
echo "shards: $LOADGEN_SHARDS"
echo "watch: kubectl -n $NAMESPACE logs -f job/${JOBS[0]}"

if [ "$WAIT_FOR_COMPLETION" = "true" ]; then
  echo "waiting for benchmark job completion (${JOB_TIMEOUT_SECONDS}s per shard)"
  status=0
  for SHARD_INDEX in $(seq 0 $((LOADGEN_SHARDS - 1))); do
    JOB="bench-$RUN_ID-s$SHARD_INDEX"
    if kubectl -n "$NAMESPACE" wait --for=condition=complete "job/$JOB" --timeout="${JOB_TIMEOUT_SECONDS}s"; then
      kubectl -n "$NAMESPACE" logs "job/$JOB" >"$RESULTS_DIR/$RUN_ID-s$SHARD_INDEX.log"
      echo "wrote $RESULTS_DIR/$RUN_ID-s$SHARD_INDEX.log"
    else
      status=1
      kubectl -n "$NAMESPACE" logs "job/$JOB" >"$RESULTS_DIR/$RUN_ID-s$SHARD_INDEX.log" 2>/dev/null || true
      kubectl -n "$NAMESPACE" describe "job/$JOB" >"$RESULTS_DIR/$RUN_ID-s$SHARD_INDEX.describe.txt" 2>/dev/null || true
      echo "benchmark shard $SHARD_INDEX did not complete; logs saved under $RESULTS_DIR/$RUN_ID-s$SHARD_INDEX.*" >&2
    fi
  done
  [ "$status" -eq 0 ] || exit "$status"

  if [ "$TARGET" = "thingsboard-classic" ]; then
    if kubectl -n "$NAMESPACE" logs "deploy/$API_SERVICE" --tail=500 | grep -q "Partitions info for queue QK(Main,TB_RULE_ENGINE,system) is missing"; then
      echo "thingsboard-classic health gate failed: TB_RULE_ENGINE partition info is missing" >&2
      exit 1
    fi
  fi
fi
