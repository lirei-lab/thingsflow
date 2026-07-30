#!/usr/bin/env bash
# LEGACY (QuestDB era): this evidence collector was written when the history
# store was QuestDB (see QUESTDB_SINCE_ISO and QuestDB queries below). It is
# kept as-is as a historical tool and is pending an update for GreptimeDB
# (HTTP SQL on :4000, /v1/sql?db=public). Do not treat its QuestDB references
# as evidence that QuestDB is deployed on the current platform.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

NAMESPACE="${NAMESPACE:-thingsflow}"
RELEASE="${RELEASE:-thingsflow}"
RESULTS_DIR="${RESULTS_DIR:-$ROOT/benchmarks/results}"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)-thingsflow-evidence}"
DEVICE_PREFIX="${DEVICE_PREFIX:-}"
QUESTDB_SINCE_ISO="${QUESTDB_SINCE_ISO:-}"
LOG_SINCE="${LOG_SINCE:-10m}"

need() {
  command -v "$1" >/dev/null || { echo "missing required command: $1" >&2; exit 127; }
}

need kubectl
need date
need python3
mkdir -p "$RESULTS_DIR"

RUN_ID="$(printf %s "$RUN_ID" | tr "[:upper:]" "[:lower:]" | sed "s/[^a-z0-9.-]/-/g; s/^[^a-z0-9]*//; s/[^a-z0-9]*$//")"
OUT="$RESULTS_DIR/$RUN_ID-evidence.txt"
SUMMARY="$RESULTS_DIR/$RUN_ID-summary.json"

section() {
  printf "\n# %s\n" "$1"
}

redact_sensitive() {
  sed -E 's#eyJ[^[:space:]/]+\.[^[:space:]/]+\.[^[:space:]/]+#[REDACTED_JWT]#g'
}

rmqtt_api_get() {
  local path="$1"
  local pod="curl-${RUN_ID:0:38}"
  local url="http://$RELEASE-rmqtt-edge.$NAMESPACE.svc.cluster.local:6060$path"
  kubectl -n "$NAMESPACE" delete pod "$pod" --ignore-not-found >/dev/null 2>&1 || true
  kubectl -n "$NAMESPACE" run "$pod" \
    --image=curlimages/curl:8.10.1 \
    --restart=Never \
    --rm -i --quiet \
    --command -- curl -fsS "$url" 2>/dev/null || true
}

nats_cli() {
  local command="$1"
  local pod="nats-${RUN_ID:0:38}"
  kubectl -n "$NAMESPACE" delete pod "$pod" --ignore-not-found >/dev/null 2>&1 || true
  kubectl -n "$NAMESPACE" run "$pod" \
    --image=natsio/nats-box:0.16.0 \
    --restart=Never \
    --rm -i --quiet \
    --command -- sh -c "nats --server nats://$RELEASE-nats:4222 $command" 2>/dev/null || true
}

parse_loadgen_logs() {
  python3 - "$RESULTS_DIR" "$RUN_ID" "$SUMMARY" <<'PY'
import glob
import json
import re
import sys
from pathlib import Path

results_dir, run_id, summary_path = sys.argv[1:]
total_publishes = 0
shards = []
errors = {}

for path in sorted(glob.glob(str(Path(results_dir) / f"{run_id}-s*.log"))):
    text = Path(path).read_text(errors="replace")
    publishes = 0
    for match in re.finditer(r"Total telemetry publishes:\s*(\d+)", text):
        publishes = int(match.group(1))
    if not publishes:
        for line in text.splitlines():
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            if event.get("event") in {"done", "loadgen_done"}:
                publishes = int(event.get("published_total") or event.get("total_publishes") or event.get("publish_count") or 0)
    total_publishes += publishes
    shard_errors = len(re.findall(r"(?i)\b(error|exception|failed|timeout|refused)\b", text))
    errors[Path(path).name] = shard_errors
    shards.append({"file": Path(path).name, "publishes": publishes, "error_lines": shard_errors})

summary = {
    "run_id": run_id,
    "shards": shards,
    "total_publishes": total_publishes,
    "total_error_lines": sum(errors.values()),
}
Path(summary_path).write_text(json.dumps(summary, indent=2) + "\n")
print(json.dumps(summary, indent=2))
PY
}

{
  echo "run_id=$RUN_ID"
  echo "namespace=$NAMESPACE"
  echo "release=$RELEASE"
  echo "captured_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "device_prefix=$DEVICE_PREFIX"
  echo "questdb_since_iso=$QUESTDB_SINCE_ISO"
  echo "log_since=$LOG_SINCE"

  section "loadgen summary"
  parse_loadgen_logs || true

  section "kubernetes version"
  kubectl version --short 2>/dev/null || kubectl version 2>/dev/null || true

  section "nodes"
  kubectl get nodes -o wide || true

  section "pods"
  kubectl -n "$NAMESPACE" get pods -o wide || true

  section "top pods"
  kubectl -n "$NAMESPACE" top pods || true

  section "services"
  kubectl -n "$NAMESPACE" get svc -o wide || true

  section "recent events"
  kubectl -n "$NAMESPACE" get events --sort-by=.lastTimestamp | tail -120 || true

  section "helm release"
  helm -n "$NAMESPACE" list 2>/dev/null || true

  section "rmqtt brokers"
  rmqtt_api_get "/api/v1/brokers"

  section "rmqtt nodes"
  rmqtt_api_get "/api/v1/nodes"

  section "rmqtt clients"
  rmqtt_api_get "/api/v1/clients?_limit=20"

  section "rmqtt plugins"
  rmqtt_api_get "/api/v1/plugins"

  section "nats streams"
  nats_cli "stream ls"

  section "nats twin kv"
  nats_cli "kv info twin_state"

  if [ -n "$DEVICE_PREFIX" ]; then
    section "postgres benchmark devices"
    kubectl -n "$NAMESPACE" exec "$RELEASE-postgres-0" -- psql -U postgres -d thingsboard -Atc \
      "select count(*) from device where name like '${DEVICE_PREFIX}%';" || true

    section "nats twin kv latest keys"
    nats_cli "kv ls twin_state"
  fi

  if [ -n "$QUESTDB_SINCE_ISO" ]; then
    section "questdb history since window"
    kubectl -n "$NAMESPACE" exec "$RELEASE-postgres-0" -- env PGPASSWORD=quest psql -h "$RELEASE-questdb" -p 8812 -U admin -d qdb -Atc \
      "select count_distinct(device_id), count(), max(timestamp) from device_telemetry where timestamp > '$QUESTDB_SINCE_ISO';" || true
  fi

  section "rmqtt warning/error samples"
  kubectl -n "$NAMESPACE" logs "deploy/$RELEASE-rmqtt-edge" --since="$LOG_SINCE" --tail=2000 2>/dev/null | grep -Ei "error|warn|panic|fail|denied|invalid|timeout" | redact_sensitive | tail -80 || true

  section "nats/bento warning/error samples"
  kubectl -n "$NAMESPACE" logs -l app=nats-latest-kv --since="$LOG_SINCE" --all-containers=true --tail=4000 2>/dev/null | grep -Ei "error|warn|panic|fail|denied|invalid|timeout|lag" | redact_sensitive | tail -80 || true
  kubectl -n "$NAMESPACE" logs -l app=nats-questdb --since="$LOG_SINCE" --all-containers=true --tail=4000 2>/dev/null | grep -Ei "error|warn|panic|fail|denied|invalid|timeout|lag" | redact_sensitive | tail -80 || true
  kubectl -n "$NAMESPACE" logs -l app=nats-alarms --since="$LOG_SINCE" --all-containers=true --tail=4000 2>/dev/null | grep -Ei "error|warn|panic|fail|denied|invalid|timeout|lag" | redact_sensitive | tail -80 || true
  kubectl -n "$NAMESPACE" logs -l app=alarm-materializer --since="$LOG_SINCE" --all-containers=true --tail=4000 2>/dev/null | grep -Ei "error|warn|panic|fail|denied|invalid|timeout|lag" | redact_sensitive | tail -80 || true

  section "flow-core warning/error samples"
  kubectl -n "$NAMESPACE" logs "deploy/$RELEASE-flow-core" --since="$LOG_SINCE" --tail=2000 2>/dev/null | grep -Ei "error|warn|panic|fail|denied|invalid|timeout" | redact_sensitive | tail -80 || true
} | tee "$OUT"

echo "wrote $OUT"
echo "wrote $SUMMARY"
