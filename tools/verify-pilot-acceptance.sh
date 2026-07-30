#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

NAMESPACE="${NAMESPACE:-thingsflow}"
RELEASE="${RELEASE:-thingsflow}"
CHART="${CHART:-$ROOT/k8s/helm/thingsflow}"
VALUES_FILE="${VALUES_FILE:-$ROOT/k8s/helm/thingsflow/values-pilot.example.yaml}"
SCENARIO_FILE="${SCENARIO_FILE:-$ROOT/benchmarks/scenarios/mqtt-1000.env}"
RESULTS_DIR="${RESULTS_DIR:-$ROOT/benchmarks/results}"

# Canonical deployment names for the default public release:
# thingsflow-rmqtt-edge
# thingsflow-nats
# thingsflow-greptimedb
# thingsflow-postgres
# thingsflow-nats-latest-kv
# thingsflow-nats-greptimedb
# thingsflow-nats-alarms
# thingsflow-alarm-materializer
# thingsflow-thingsboard-ui

RUN_BENCHMARK="${RUN_BENCHMARK:-true}"
RUN_ID="${RUN_ID:-pilot-acceptance-$(date -u +%Y%m%dt%H%M%sz)}"
EXPECTED_DEVICE_COUNT="${EXPECTED_DEVICE_COUNT:-2000}"
EXPECTED_LATEST_ROWS="${EXPECTED_LATEST_ROWS:-6000}"
EXPECTED_HISTORY_ROWS="${EXPECTED_HISTORY_ROWS:-6000}"
EXPECTED_PUBLISHED_MIN="${EXPECTED_PUBLISHED_MIN:-120000}"
EXPECTED_ERRORS_MAX="${EXPECTED_ERRORS_MAX:-0}"
POST_BENCHMARK_DRAIN_SECONDS="${POST_BENCHMARK_DRAIN_SECONDS:-90}"
EDGE_LOG_SINCE="${EDGE_LOG_SINCE:-120s}"
EDGE_LOG_LINES_MAX="${EDGE_LOG_LINES_MAX:-80}"
LOADGEN_SHARDS="${LOADGEN_SHARDS:-16}"
PUBLISH_INTERVAL_SECONDS="${PUBLISH_INTERVAL_SECONDS:-1}"
RUN_DURATION_SECONDS="${RUN_DURATION_SECONDS:-60}"
DEVICE_PREFIX="${DEVICE_PREFIX:-pilot-acceptance-mqtt}"
JOB_TIMEOUT_SECONDS="${JOB_TIMEOUT_SECONDS:-600}"
OIDC_REQUIRED="${OIDC_REQUIRED:-true}"
EXPECTED_OIDC_PROVIDER="${EXPECTED_OIDC_PROVIDER:-}"
NATS_AUTH_REQUIRED="${NATS_AUTH_REQUIRED:-true}"
NATS_AUTH_SECRET="${NATS_AUTH_SECRET:-thingsflow-nats-auth}"
NATS_AUTH_USER_KEY="${NATS_AUTH_USER_KEY:-username}"
NATS_AUTH_PASSWORD_KEY="${NATS_AUTH_PASSWORD_KEY:-password}"
CHECK_HISTORY_STORE="${CHECK_HISTORY_STORE:-true}"
GREPTIMEDB_TABLE="${GREPTIMEDB_TABLE:-device_telemetry_kv}"

KUBECTL=(kubectl)
if [ -n "${KUBECONFIG:-}" ]; then
  KUBECTL+=(--kubeconfig "$KUBECONFIG")
fi

pass() { printf 'PASS %s\n' "$*"; }
fail() { printf 'FAIL %s\n' "$*" >&2; exit 1; }

need() {
  command -v "$1" >/dev/null || fail "missing required command: $1"
}

kc() {
  "${KUBECTL[@]}" "$@"
}

require_secret_key() {
  local secret="$1"
  local key="$2"
  local secret_json
  secret_json="$(mktemp)"
  kc -n "$NAMESPACE" get secret "$secret" -o json >"$secret_json"
  python3 - "$secret" "$key" "$secret_json" <<'PY'
import json
import pathlib
import sys

secret = sys.argv[1]
key = sys.argv[2]
data = json.loads(pathlib.Path(sys.argv[3]).read_text()).get("data") or {}
if key not in data or not data[key]:
    raise SystemExit(f"secret {secret} is missing required key {key}")
PY
  rm -f "$secret_json"
}

require_number_le() {
  local label="$1"
  local actual="$2"
  local max="$3"
  case "$actual" in
    ''|*[!0-9]*) fail "$label is not numeric: $actual" ;;
  esac
  [ "$actual" -le "$max" ] || fail "$label=$actual exceeds max=$max"
  pass "$label=$actual <= $max"
}

require_number_ge() {
  local label="$1"
  local actual="$2"
  local min="$3"
  case "$actual" in
    ''|*[!0-9]*) fail "$label is not numeric: $actual" ;;
  esac
  [ "$actual" -ge "$min" ] || fail "$label=$actual below min=$min"
  pass "$label=$actual >= $min"
}

cluster_wget() {
  cluster_get "$1" >/dev/null
}

cluster_get() {
  local url="$1"
  local name="thingsflow-acceptance-wget"
  kc -n "$NAMESPACE" delete pod "$name" --ignore-not-found >/dev/null 2>&1 || true
  kc -n "$NAMESPACE" run "$name" \
    --image=busybox:1.36 \
    --restart=Never \
    --rm \
    -i \
    --quiet \
    -- wget -q -O - "$url"
}

render_chart() {
  helm template "$RELEASE" "$CHART" -f "$VALUES_FILE" >/tmp/thingsflow-pilot-acceptance-render.yaml
  pass "helm template rendered"
}

rollout() {
  local name="$1"
  if kc -n "$NAMESPACE" get "deploy/$name" >/dev/null 2>&1; then
    kc -n "$NAMESPACE" rollout status "deploy/$name" --timeout=180s >/dev/null
    pass "rollout status deploy/$name"
    return
  fi
  if kc -n "$NAMESPACE" get "statefulset/$name" >/dev/null 2>&1; then
    kc -n "$NAMESPACE" rollout status "statefulset/$name" --timeout=180s >/dev/null
    pass "rollout status statefulset/$name"
    return
  fi
  fail "missing workload deploy/$name or statefulset/$name"
}

check_core_rollouts() {
  rollout "$RELEASE-rmqtt-edge"
  rollout "$RELEASE-nats"
  rollout "$RELEASE-greptimedb"
  rollout "$RELEASE-postgres"
  rollout "$RELEASE-nats-latest-kv"
  rollout "$RELEASE-nats-greptimedb"
  rollout "$RELEASE-nats-alarms"
  rollout "$RELEASE-alarm-materializer"
  rollout "$RELEASE-flow-core"
  rollout "$RELEASE-thingsboard-ui"
}

check_no_runtime_restarts() {
  local pod_json
  pod_json="$(mktemp)"
  kc -n "$NAMESPACE" get pods -o json >"$pod_json"
  python3 - "$NAMESPACE" "$pod_json" <<'PY'
import json
import pathlib
import sys

namespace = sys.argv[1]
pod_json = pathlib.Path(sys.argv[2])
data = json.loads(pod_json.read_text())
bad = []
for item in data["items"]:
    labels = item.get("metadata", {}).get("labels", {})
    if labels.get("app.kubernetes.io/part-of") == "iot-benchmark":
        continue
    owners = item.get("metadata", {}).get("ownerReferences", []) or []
    if any(owner.get("kind") == "Job" for owner in owners):
        continue
    name = item["metadata"]["name"]
    phase = item.get("status", {}).get("phase")
    if phase != "Running":
        bad.append(f"{name}: phase={phase}")
    for status in item.get("status", {}).get("containerStatuses", []):
        restarts = int(status.get("restartCount", 0))
        if restarts:
            bad.append(f"{name}/{status.get('name')}: restarts={restarts}")
if bad:
    print(f"runtime pod gate failed in namespace {namespace}", file=sys.stderr)
    for row in bad:
        print(row, file=sys.stderr)
    sys.exit(1)
PY
  rm -f "$pod_json"
  pass "runtime pods running with zero restarts"
}

check_no_obsolete_workloads() {
  local obsolete
  obsolete="$(kc -n "$NAMESPACE" get deploy,statefulset,svc,pod -o name | awk 'tolower($0) ~ /(redpanda|zilla|flow-rules|rules)/ { print }')"
  if [ -n "$obsolete" ]; then
    printf '%s\n' "$obsolete" >&2
    fail "obsolete Redpanda/Zilla/Flow Rules workload still present"
  fi
  pass "no obsolete Redpanda/Zilla/Flow Rules workloads"
}

check_required_secrets() {
  require_secret_key thingsflow-platform-keys jwt-token-signing-key
  require_secret_key thingsflow-device-jwt device-jwt-es256-private-key-pem-b64
  require_secret_key thingsflow-device-jwt previous-public-jwks-b64
  require_secret_key thingsflow-postgres password
  if [ "$NATS_AUTH_REQUIRED" = "true" ]; then
    require_secret_key "$NATS_AUTH_SECRET" "$NATS_AUTH_USER_KEY"
    require_secret_key "$NATS_AUTH_SECRET" "$NATS_AUTH_PASSWORD_KEY"
  fi
  if [ "$OIDC_REQUIRED" = "true" ]; then
    require_secret_key thingsflow-oidc client-secret
    require_secret_key thingsflow-oidc state-signing-key
  fi
  pass "required production secrets exist"
}

check_config_posture() {
  local cm_json deploy_json
  cm_json="$(mktemp)"
  deploy_json="$(mktemp)"
  kc -n "$NAMESPACE" get configmap "$RELEASE-config" -o json >"$cm_json"
  kc -n "$NAMESPACE" get deployment "$RELEASE-flow-core" -o json >"$deploy_json"
  python3 - "$cm_json" "$deploy_json" "$OIDC_REQUIRED" <<'PY'
import json
import pathlib
import sys

cm = json.loads(pathlib.Path(sys.argv[1]).read_text()).get("data") or {}
deploy = json.loads(pathlib.Path(sys.argv[2]).read_text())
oidc_required = sys.argv[3] == "true"

expected = {
    "EVENT_BROKER": "nats",
    "FLOW_DATA_PLANE_CONSUMER_ENABLED": "false",
    "FLOW_CORE_DEVICE_INGEST_ENABLED": "false",
    "TELEMETRY_HISTORY_STORE": "greptimedb",
}
for key, value in expected.items():
    if cm.get(key) != value:
        raise SystemExit(f"{key}={cm.get(key)!r}, expected {value!r}")
if cm.get("ALLOWED_ORIGIN") in ("", "*"):
    raise SystemExit("ALLOWED_ORIGIN must be an explicit origin")
if oidc_required:
    for key in ("OIDC_ENABLED", "OIDC_PROVIDER_ID", "OIDC_CLIENT_ID", "OIDC_USERINFO_URL"):
        if not cm.get(key):
            raise SystemExit(f"{key} must be configured when OIDC_REQUIRED=true")
    if cm.get("OIDC_ENABLED") != "true":
        raise SystemExit("OIDC_ENABLED must be true when OIDC_REQUIRED=true")

containers = deploy["spec"]["template"]["spec"].get("containers", [])
env = {}
for container in containers:
    if container.get("name") == "flow-core":
        for item in container.get("env", []):
            if "value" in item:
                env[item["name"]] = item["value"]
if env.get("FLOW_ENV") != "production":
    raise SystemExit("flow-core must run with FLOW_ENV=production")
PY
  rm -f "$cm_json" "$deploy_json"
  pass "production config posture is NATS-first and OIDC-ready"
}

check_http_surfaces() {
  cluster_wget "http://$RELEASE-flow-core.$NAMESPACE.svc.cluster.local:8080/api/noauth/device-jwks"
  cluster_wget "http://$RELEASE-flow-core.$NAMESPACE.svc.cluster.local:8080/api/noauth/device-jwt-public.pem"
  pass "api/noauth/device-jwks and device public PEM reachable"
  cluster_wget "http://$RELEASE-thingsboard-ui.$NAMESPACE.svc.cluster.local:8080/"
  pass "thingsboard-ui service reachable"
  if [ "$OIDC_REQUIRED" = "true" ]; then
    local clients clients_json
    clients="$(cluster_get "http://$RELEASE-flow-core.$NAMESPACE.svc.cluster.local:8080/api/noauth/oauth2Clients")"
    clients_json="$(mktemp)"
    printf '%s' "$clients" >"$clients_json"
    python3 - "$EXPECTED_OIDC_PROVIDER" "$clients_json" <<'PY'
import json
import pathlib
import sys

expected_provider = sys.argv[1]
clients = json.loads(pathlib.Path(sys.argv[2]).read_text())
if not clients:
    raise SystemExit("OIDC client list is empty")
if expected_provider and not any(client.get("id") == expected_provider for client in clients):
    raise SystemExit(f"OIDC provider {expected_provider!r} was not advertised")
for client in clients:
    if not client.get("url", "").startswith("/api/noauth/oidc/authorize/"):
        raise SystemExit(f"unexpected OIDC authorize URL: {client!r}")
PY
    rm -f "$clients_json"
    pass "OIDC client discovery reachable"
  fi
}

run_benchmark() {
  if [ "$RUN_BENCHMARK" != "true" ]; then
    pass "benchmark skipped by RUN_BENCHMARK=$RUN_BENCHMARK"
    return
  fi
  kc -n "$NAMESPACE" delete job -l app.kubernetes.io/part-of=iot-benchmark --ignore-not-found >/dev/null
  KUBECONFIG="${KUBECONFIG:-}" \
  NAMESPACE="$NAMESPACE" \
  RUN_ID="$RUN_ID" \
  DEVICE_COUNT="$EXPECTED_DEVICE_COUNT" \
  LOADGEN_SHARDS="$LOADGEN_SHARDS" \
  PUBLISH_INTERVAL_SECONDS="$PUBLISH_INTERVAL_SECONDS" \
  RUN_DURATION_SECONDS="$RUN_DURATION_SECONDS" \
  DEVICE_PREFIX="$DEVICE_PREFIX" \
  WAIT_FOR_COMPLETION=true \
  JOB_TIMEOUT_SECONDS="$JOB_TIMEOUT_SECONDS" \
    "$ROOT/benchmarks/scripts/run-benchmark.sh" thingsflow "$SCENARIO_FILE"
  pass "benchmark completed run_id=$RUN_ID"
}

check_benchmark_results() {
  if [ "$RUN_BENCHMARK" != "true" ]; then
    pass "benchmark result check skipped by RUN_BENCHMARK=$RUN_BENCHMARK"
    return
  fi
  python3 - "$RESULTS_DIR" "$RUN_ID" "$EXPECTED_PUBLISHED_MIN" "$EXPECTED_ERRORS_MAX" <<'PY'
import json
import pathlib
import sys
from collections import Counter

results_dir = pathlib.Path(sys.argv[1])
run_id = sys.argv[2]
published_min = int(sys.argv[3])
errors_max = int(sys.argv[4])
published = 0
errors = 0
error_counts = Counter()
files = sorted(results_dir.glob(f"{run_id}-s*.log"))
if not files:
    raise SystemExit(f"no benchmark logs found for run_id={run_id}")
for path in files:
    for line in path.read_text().splitlines():
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if event.get("event") == "done":
            published += int(event.get("published_total", event.get("published", 0)))
            errors += int(event.get("errors", 0))
            error_counts.update(event.get("error_counts") or {})
if published < published_min or errors > errors_max:
    print(
        {
            "run_id": run_id,
            "published_total": published,
            "errors": errors,
            "error_counts": dict(error_counts),
        },
        file=sys.stderr,
    )
    raise SystemExit(1)
print(
    {
        "run_id": run_id,
        "published_total": published,
        "errors": errors,
        "error_counts": dict(error_counts),
    }
)
PY
  pass "benchmark thresholds met"
}

latest_rows() {
  local nats_server="${NATS_SERVER:-nats://$RELEASE-nats:4222}"
  local pod_name="$RUN_ID-nats-kv-check"
  local nats_command='nats --server "$NATS_SERVER" kv ls twin_state 2>/dev/null | grep -c "^DEVICE\\." || true'
  local overrides
  if [ "$NATS_AUTH_REQUIRED" = "true" ]; then
    overrides='{"spec":{"containers":[{"name":"'"$pod_name"'","image":"natsio/nats-box:0.16.0","env":[{"name":"NATS_SERVER","value":"'"$nats_server"'"},{"name":"NATS_USER","valueFrom":{"secretKeyRef":{"name":"'"$NATS_AUTH_SECRET"'","key":"'"$NATS_AUTH_USER_KEY"'"}}},{"name":"NATS_PASSWORD","valueFrom":{"secretKeyRef":{"name":"'"$NATS_AUTH_SECRET"'","key":"'"$NATS_AUTH_PASSWORD_KEY"'"}}}],"command":["sh","-c","nats --server \"$NATS_SERVER\" --user \"$NATS_USER\" --password \"$NATS_PASSWORD\" kv ls twin_state 2>/dev/null | grep -c \"^DEVICE\\\\.\" || true"]}],"restartPolicy":"Never"}}'
  else
    overrides='{"spec":{"containers":[{"name":"'"$pod_name"'","image":"natsio/nats-box:0.16.0","env":[{"name":"NATS_SERVER","value":"'"$nats_server"'"}],"command":["sh","-c","'"$nats_command"'"]}],"restartPolicy":"Never"}}'
  fi
  kc -n "$NAMESPACE" delete pod "$pod_name" --ignore-not-found >/dev/null 2>&1 || true
  kc -n "$NAMESPACE" run "$pod_name" \
    --image=natsio/nats-box:0.16.0 \
    --restart=Never \
    --overrides="$overrides" >/dev/null
  local phase=""
  for _ in $(seq 1 60); do
    phase="$(kc -n "$NAMESPACE" get pod "$pod_name" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
    if [ "$phase" = "Succeeded" ] || [ "$phase" = "Failed" ]; then
      break
    fi
    sleep 1
  done
  kc -n "$NAMESPACE" logs "pod/$pod_name" 2>/dev/null || true
  kc -n "$NAMESPACE" delete pod "$pod_name" --ignore-not-found >/dev/null 2>&1 || true
  [ "$phase" = "Succeeded" ] || fail "NATS KV check pod ended with phase=${phase:-unknown}"
}

check_latest_rows() {
  local result
  result="$(latest_rows)"
  require_number_ge "nats_kv_latest_keys" "$result" "$EXPECTED_LATEST_ROWS"
}

history_rows() {
  kc -n "$NAMESPACE" exec "$RELEASE-postgres-0" -- \
    psql "postgres://$RELEASE-greptimedb:4003/public?sslmode=disable" \
      -tAc "select count(*) from $GREPTIMEDB_TABLE;" 2>/dev/null
}

check_history_rows() {
  if [ "$CHECK_HISTORY_STORE" != "true" ]; then
    pass "history store check skipped by CHECK_HISTORY_STORE=$CHECK_HISTORY_STORE"
    return
  fi
  local result
  result="$(history_rows | awk '/^[[:space:]]*[0-9]+[[:space:]]*$/ { print $1; exit }')"
  require_number_ge "greptimedb_history_rows" "$result" "$EXPECTED_HISTORY_ROWS"
}

wait_for_post_benchmark_drain() {
  case "$POST_BENCHMARK_DRAIN_SECONDS" in
    ''|*[!0-9]*) fail "POST_BENCHMARK_DRAIN_SECONDS is not numeric: $POST_BENCHMARK_DRAIN_SECONDS" ;;
  esac
  if [ "$POST_BENCHMARK_DRAIN_SECONDS" -gt 0 ]; then
    sleep "$POST_BENCHMARK_DRAIN_SECONDS"
    pass "post-benchmark drain wait ${POST_BENCHMARK_DRAIN_SECONDS}s"
  fi
}


check_edge_logs() {
  local lines
  lines="$(kc -n "$NAMESPACE" logs deploy/$RELEASE-rmqtt-edge --since="$EDGE_LOG_SINCE" 2>/dev/null | wc -l | tr -d ' ')"
  require_number_le "rmqtt recent log lines" "$lines" "$EDGE_LOG_LINES_MAX"
}

cleanup_benchmark_jobs() {
  kc -n "$NAMESPACE" delete job -l app.kubernetes.io/part-of=iot-benchmark --ignore-not-found >/dev/null || true
}

main() {
  need kubectl
  need helm
  need python3
  need awk
  mkdir -p "$RESULTS_DIR"
  trap cleanup_benchmark_jobs EXIT

  render_chart
  check_no_obsolete_workloads
  check_required_secrets
  check_core_rollouts
  check_no_runtime_restarts
  check_config_posture
  check_http_surfaces
  run_benchmark
  check_benchmark_results
  check_no_runtime_restarts
  check_latest_rows
  wait_for_post_benchmark_drain
  check_history_rows
  check_edge_logs

  pass "Pilot acceptance gate passed"
}

main "$@"
