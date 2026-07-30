#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="${NAMESPACE:-thingsflow}"
RELEASE="${RELEASE:-thingsflow}"
LOG_SINCE="${LOG_SINCE:-5m}"
EXPECTED_DEVICE_COUNT="${EXPECTED_DEVICE_COUNT:-1}"
EXPECTED_GREPTIME_ROWS="${EXPECTED_GREPTIME_ROWS:-1}"
EXPECTED_NATS_KV_KEYS="${EXPECTED_NATS_KV_KEYS:-1}"
GREPTIMEDB_TABLE="${GREPTIMEDB_TABLE:-device_telemetry_kv}"
NATS_KV_BUCKET="${NATS_KV_BUCKET:-twin_state}"
NATS_AUTH_REQUIRED="${NATS_AUTH_REQUIRED:-true}"
NATS_AUTH_SECRET="${NATS_AUTH_SECRET:-thingsflow-nats-auth}"
NATS_AUTH_USER_KEY="${NATS_AUTH_USER_KEY:-username}"
NATS_AUTH_PASSWORD_KEY="${NATS_AUTH_PASSWORD_KEY:-password}"
NATS_CHECK_IMAGE="${NATS_CHECK_IMAGE:-natsio/nats-box:0.16.0}"
HTTP_CHECK_IMAGE="${HTTP_CHECK_IMAGE:-busybox:1.36}"
KUBE_CONFIG="${KUBE_CONFIG:-${KUBECONFIG:-}}"

while [ "$#" -gt 0 ]; do
  case "$1" in
    --kubeconfig)
      KUBE_CONFIG="${2:-}"
      shift 2
      ;;
    --namespace)
      NAMESPACE="${2:-}"
      shift 2
      ;;
    --release)
      RELEASE="${2:-}"
      shift 2
      ;;
    --log-since)
      LOG_SINCE="${2:-}"
      shift 2
      ;;
    --expected-device-count)
      EXPECTED_DEVICE_COUNT="${2:-}"
      shift 2
      ;;
    --expected-greptime-rows)
      EXPECTED_GREPTIME_ROWS="${2:-}"
      shift 2
      ;;
    --expected-nats-kv-keys)
      EXPECTED_NATS_KV_KEYS="${2:-}"
      shift 2
      ;;
    *)
      printf 'FAIL unknown argument: %s\n' "$1" >&2
      exit 1
      ;;
  esac
done

KUBECTL=(kubectl)
if [ -n "$KUBE_CONFIG" ]; then
  KUBECTL+=(--kubeconfig "$KUBE_CONFIG")
fi

pass() { printf 'PASS %s\n' "$*"; }
warn() { printf 'WARN %s\n' "$*" >&2; }
fail() { printf 'FAIL %s\n' "$*" >&2; exit 1; }

need() {
  command -v "$1" >/dev/null || fail "missing required command: $1"
}

kc() {
  "${KUBECTL[@]}" "$@"
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

rollout() {
  local name="$1"
  if kc -n "$NAMESPACE" get deployment "$name" >/dev/null 2>&1; then
    kc -n "$NAMESPACE" rollout status deployment "$name" --timeout=180s >/dev/null
    pass "rollout deploy/$name"
    return
  fi
  if kc -n "$NAMESPACE" get statefulset "$name" >/dev/null 2>&1; then
    kc -n "$NAMESPACE" rollout status statefulset "$name" --timeout=180s >/dev/null
    pass "rollout statefulset/$name"
    return
  fi
  fail "missing workload $name"
}

check_rollouts() {
  rollout "$RELEASE-postgres"
  rollout "$RELEASE-greptimedb"
  rollout "$RELEASE-nats"
  rollout "$RELEASE-nats-latest-kv"
  rollout "$RELEASE-nats-greptimedb"
  rollout "$RELEASE-rmqtt-edge"
  rollout "$RELEASE-flow-core"
  rollout "$RELEASE-thingsboard-ui"
}

check_pods_clean() {
  local pod_json
  pod_json="$(mktemp)"
  kc -n "$NAMESPACE" get pods -o json >"$pod_json"
  python3 - "$pod_json" <<'PY'
import json
import pathlib
import sys

data = json.loads(pathlib.Path(sys.argv[1]).read_text())
bad = []
for item in data.get("items", []):
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
    for row in bad:
        print(row, file=sys.stderr)
    sys.exit(1)
PY
  rm -f "$pod_json"
  pass "runtime pods are running with zero restarts"
}

check_no_recent_events() {
  local events_json pods_json pvcs_json
  events_json="$(mktemp)"
  pods_json="$(mktemp)"
  pvcs_json="$(mktemp)"
  kc -n "$NAMESPACE" get events -o json >"$events_json" 2>/dev/null || true
  kc -n "$NAMESPACE" get pods -o json >"$pods_json" 2>/dev/null || true
  kc -n "$NAMESPACE" get pvc -o json >"$pvcs_json" 2>/dev/null || true
  set +e
  python3 - "$events_json" "$pods_json" "$pvcs_json" "$LOG_SINCE" <<'PY'
import datetime as dt
import json
import pathlib
import re
import sys

events_path = pathlib.Path(sys.argv[1])
pods_path = pathlib.Path(sys.argv[2])
pvcs_path = pathlib.Path(sys.argv[3])
since = sys.argv[4]

match = re.fullmatch(r"(\d+)([smhd])", since)
if not match:
    print(f"unsupported LOG_SINCE value for event filtering: {since}", file=sys.stderr)
    sys.exit(2)

amount = int(match.group(1))
unit = match.group(2)
delta = {
    "s": dt.timedelta(seconds=amount),
    "m": dt.timedelta(minutes=amount),
    "h": dt.timedelta(hours=amount),
    "d": dt.timedelta(days=amount),
}[unit]
cutoff = dt.datetime.now(dt.timezone.utc) - delta

def parse_ts(value):
    if not value:
        return None
    value = str(value).replace("Z", "+00:00")
    try:
        parsed = dt.datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed

try:
    data = json.loads(events_path.read_text() or "{}")
    pods_data = json.loads(pods_path.read_text() or "{}")
    pvcs_data = json.loads(pvcs_path.read_text() or "{}")
except json.JSONDecodeError as exc:
    print(f"could not parse kubernetes json: {exc}", file=sys.stderr)
    sys.exit(2)

healthy_pods = set()
for pod in pods_data.get("items", []):
    name = pod.get("metadata", {}).get("name", "")
    status = pod.get("status", {}) or {}
    if status.get("phase") != "Running":
        continue
    ready = False
    for condition in status.get("conditions", []) or []:
        if condition.get("type") == "Ready" and condition.get("status") == "True":
            ready = True
            break
    if not ready:
        continue
    if any(int(cs.get("restartCount", 0)) for cs in status.get("containerStatuses", []) or []):
        continue
    healthy_pods.add(name)

live_pvcs = {
    pvc.get("metadata", {}).get("name", "")
    for pvc in pvcs_data.get("items", [])
}

bad = []
reason_pattern = re.compile(r"Failed|BackOff|Unhealthy|Killing", re.I)
for item in data.get("items", []):
    event_ts = (
        parse_ts(item.get("eventTime"))
        or parse_ts((item.get("series") or {}).get("lastObservedTime"))
        or parse_ts(item.get("lastTimestamp"))
        or parse_ts(item.get("deprecatedLastTimestamp"))
        or parse_ts(item.get("metadata", {}).get("creationTimestamp"))
    )
    if event_ts is None or event_ts < cutoff:
        continue
    event_type = item.get("type", "")
    reason = item.get("reason", "")
    if event_type != "Warning" and not reason_pattern.search(reason):
        continue
    involved = item.get("involvedObject", {}) or {}
    kind = involved.get("kind", "?")
    name = involved.get("name", "?")
    if kind == "Pod" and reason == "FailedKillPod" and name in healthy_pods:
        continue
    if kind == "PersistentVolumeClaim" and reason == "ProvisioningFailed" and name not in live_pvcs:
        continue
    obj = f"{kind}/{name}"
    message = item.get("message", "")
    when = event_ts.isoformat()
    bad.append(f"{when} {event_type} {reason} {obj}: {message}")

if bad:
    print("\n".join(bad), file=sys.stderr)
    sys.exit(1)
PY
  local rc=$?
  set -e
  rm -f "$events_json" "$pods_json" "$pvcs_json"
  [ "$rc" -eq 0 ] || fail "recent warning events found within LOG_SINCE=$LOG_SINCE"
  pass "no warning events within LOG_SINCE=$LOG_SINCE"
}

check_postgres() {
  kc -n "$NAMESPACE" exec "$RELEASE-postgres-0" -- \
    pg_isready -U postgres -d thingsboard >/dev/null 2>/dev/null
  pass "postgres accepts connections"

  local result
  result="$(kc -n "$NAMESPACE" exec "$RELEASE-postgres-0" -- \
    psql -U postgres -d thingsboard -Atc \
      "select 'devices=' || count(*) from device; select 'dashboards=' || count(*) from dashboard; select 'alarms=' || count(*) from alarm;" \
    2>/dev/null)"
  printf '%s\n' "$result"
  local devices
  devices="$(printf '%s\n' "$result" | awk -F= '$1 == "devices" { print $2; exit }')"
  require_number_ge "postgres_devices" "$devices" "$EXPECTED_DEVICE_COUNT"
}

check_greptimedb() {
  local health
  health="$(kc -n "$NAMESPACE" exec "$RELEASE-greptimedb-0" -- \
    curl -fsS http://localhost:4000/health 2>/dev/null || true)"
  [ "$health" = "{}" ] || fail "greptimedb health endpoint did not return {}"
  pass "greptimedb health endpoint"

  local result rows latest_ts
  result="$(kc -n "$NAMESPACE" exec "$RELEASE-postgres-0" -- \
    psql "postgres://$RELEASE-greptimedb:4003/public?sslmode=disable" -Atc \
      "select 'telemetry_rows=' || count(*) from $GREPTIMEDB_TABLE; select 'latest_ts=' || coalesce(max(greptime_timestamp)::text, '') from $GREPTIMEDB_TABLE;" \
    2>/dev/null)"
  printf '%s\n' "$result"
  rows="$(printf '%s\n' "$result" | awk -F= '$1 == "telemetry_rows" { print $2; exit }')"
  latest_ts="$(printf '%s\n' "$result" | awk -F= '$1 == "latest_ts" { print $2; exit }')"
  require_number_ge "greptimedb_history_rows" "$rows" "$EXPECTED_GREPTIME_ROWS"
  [ -n "$latest_ts" ] || fail "greptimedb latest_ts is empty"
  pass "greptimedb latest_ts=$latest_ts"
}

nats_kv_count() {
  local pod_name="${RELEASE}-health-nats-kv"
  local nats_server="${NATS_SERVER:-nats://$RELEASE-nats:4222}"
  local manifest
  manifest="$(mktemp)"
  kc -n "$NAMESPACE" delete pod "$pod_name" --ignore-not-found >/dev/null 2>&1 || true
  if [ "$NATS_AUTH_REQUIRED" = "true" ]; then
    cat >"$manifest" <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: $pod_name
spec:
  restartPolicy: Never
  containers:
    - name: $pod_name
      image: $NATS_CHECK_IMAGE
      env:
        - name: NATS_SERVER
          value: "$nats_server"
        - name: NATS_KV_BUCKET
          value: "$NATS_KV_BUCKET"
        - name: NATS_USER
          valueFrom:
            secretKeyRef:
              name: $NATS_AUTH_SECRET
              key: $NATS_AUTH_USER_KEY
        - name: NATS_PASSWORD
          valueFrom:
            secretKeyRef:
              name: $NATS_AUTH_SECRET
              key: $NATS_AUTH_PASSWORD_KEY
      command: ["sh", "-c"]
      args:
        - |
          set -e
          nats --server "\$NATS_SERVER" --user "\$NATS_USER" --password "\$NATS_PASSWORD" kv info "\$NATS_KV_BUCKET" >/tmp/kv-info.txt
          nats --server "\$NATS_SERVER" --user "\$NATS_USER" --password "\$NATS_PASSWORD" kv ls "\$NATS_KV_BUCKET" 2>/dev/null | grep -c "^DEVICE\\."
EOF
  else
    cat >"$manifest" <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: $pod_name
spec:
  restartPolicy: Never
  containers:
    - name: $pod_name
      image: $NATS_CHECK_IMAGE
      env:
        - name: NATS_SERVER
          value: "$nats_server"
        - name: NATS_KV_BUCKET
          value: "$NATS_KV_BUCKET"
      command: ["sh", "-c"]
      args:
        - |
          set -e
          nats --server "\$NATS_SERVER" kv info "\$NATS_KV_BUCKET" >/tmp/kv-info.txt
          nats --server "\$NATS_SERVER" kv ls "\$NATS_KV_BUCKET" 2>/dev/null | grep -c "^DEVICE\\."
EOF
  fi
  kc -n "$NAMESPACE" apply -f "$manifest" >/dev/null
  rm -f "$manifest"
  local phase=""
  for _ in $(seq 1 60); do
    phase="$(kc -n "$NAMESPACE" get pod "$pod_name" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
    if [ "$phase" = "Succeeded" ] || [ "$phase" = "Failed" ]; then
      break
    fi
    sleep 1
  done
  kc -n "$NAMESPACE" logs "$pod_name" 2>/dev/null || true
  kc -n "$NAMESPACE" delete pod "$pod_name" --ignore-not-found >/dev/null 2>&1 || true
  [ "$phase" = "Succeeded" ] || fail "NATS KV check pod ended with phase=${phase:-unknown}"
}

check_nats_kv() {
  local result
  result="$(nats_kv_count | awk '/^[[:space:]]*[0-9]+[[:space:]]*$/ { print $1; exit }')"
  require_number_ge "nats_kv_latest_keys" "$result" "$EXPECTED_NATS_KV_KEYS"
}

check_http_surfaces() {
  local pod_name="${RELEASE}-health-http"
  kc -n "$NAMESPACE" delete pod "$pod_name" --ignore-not-found >/dev/null 2>&1 || true
  kc -n "$NAMESPACE" run "$pod_name" \
    --image="$HTTP_CHECK_IMAGE" \
    --restart=Never \
    --rm \
    -i \
    --quiet \
    -- sh -c "wget -q -O - http://$RELEASE-flow-core:8080/ready >/dev/null && wget -q -O - http://$RELEASE-flow-core:8080/api/noauth/device-jwks >/dev/null && wget -q -O - http://$RELEASE-flow-core:8080/api/noauth/oauth2Clients >/dev/null"
  pass "flow-core readiness and noauth health surfaces respond"
}

check_recent_logs() {
  local selectors=(
    "deploy/$RELEASE-nats-latest-kv"
    "deploy/$RELEASE-nats-greptimedb"
    "deploy/$RELEASE-nats-alarms"
    "deploy/$RELEASE-rmqtt-edge"
    "statefulset/$RELEASE-postgres"
    "statefulset/$RELEASE-greptimedb"
    "statefulset/$RELEASE-nats"
  )
  local bad_file
  bad_file="$(mktemp)"
  for target in "${selectors[@]}"; do
    kc -n "$NAMESPACE" logs "$target" --since="$LOG_SINCE" --all-containers=true 2>/dev/null \
      | grep -Eai 'error|warn|panic|fail|denied|invalid|timeout|backoff|unhealthy' \
      | sed "s|^|$target: |" >>"$bad_file" || true
  done
  if [ -s "$bad_file" ]; then
    cat "$bad_file" >&2
    rm -f "$bad_file"
    fail "recent warning/error log lines found within LOG_SINCE=$LOG_SINCE"
  fi
  rm -f "$bad_file"
  pass "no warning/error log lines in core data-plane logs since $LOG_SINCE"
}

check_resources() {
  if kc -n "$NAMESPACE" top pods >/dev/null 2>&1; then
    kc -n "$NAMESPACE" top pods
    pass "resource metrics available"
  else
    warn "kubectl top pods is unavailable; skipping resource snapshot"
  fi
}

main() {
  need kubectl
  need python3
  need awk
  need grep

  check_rollouts
  check_pods_clean
  check_no_recent_events
  check_postgres
  check_greptimedb
  check_nats_kv
  check_http_surfaces
  check_recent_logs
  check_resources

  pass "ThingsFlow platform health check passed"
}

main "$@"
