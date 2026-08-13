#!/usr/bin/env bash
# Phase 2 follow-up: runs one generate-input diagnostic variant (generate-nats-kv or
# generate-nats-jetstream — see configs/generate-nats-kv.yaml's header for the full
# rationale) live against the test cluster, and prints the measured throughput.
#
# Unlike run-localization.sh's variants, these have NO external publisher and NO
# JetStream input consumer at all — input.generate synthesizes messages in-process
# at full (unthrottled, interval:"") speed, isolating the pipeline+output
# combination as the only throughput constraint. Because there is no consumer to
# read a pending/delivered count from, throughput is measured authoritatively via
# the twin_state bucket's backing stream (KV_${NATS_KV_BUCKET})'s `last_seq`
# delta — every successful Put (nats_kv or nats_jetstream, both are JetStream
# publishes under the hood) increments this monotonic counter, confirmed live
# (2026-08-13): `nats stream info KV_twin_state --json` exposes `state.last_seq`.
#
# NEVER touches the production Helm release. Every resource this script creates
# lives in namespace thingsflow-fresh, is labelled app=twin-state-diag-generate,
# and is deleted in a `trap ... EXIT` so a failed run never leaves orphaned state.
#
# Usage:
#   run-generate-benchmark.sh <generate-nats-kv|generate-nats-jetstream> [count]
#
# Env overrides (all optional):
#   NAMESPACE, KUBECTL_CONTEXT, NATS_URL, BENTO_IMAGE — same defaults as
#   run-localization.sh.
#
# Prints, to stdout, a single machine-parseable summary line:
#   GENERATE_RESULT variant=<name> writes=<n> elapsed_s=<n> rate_msg_s=<n>
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$HERE/../.." && pwd)"

NAMESPACE="${NAMESPACE:-thingsflow-fresh}"
KUBECTL_CONTEXT="${KUBECTL_CONTEXT:-microk8s}"
NATS_URL="${NATS_URL:-nats://thingsflow-nats:4222}"
BENTO_IMAGE="${BENTO_IMAGE:-ghcr.io/warpstreamlabs/bento:1.8.1}"
NATS_IMAGE="${NATS_IMAGE:-natsio/nats-box:0.16.0}"

VARIANT="${1:-}"
GENERATE_COUNT="${2:-500000}"

case "$VARIANT" in
  generate-nats-kv|generate-nats-jetstream) ;;
  *) echo "usage: $0 <generate-nats-kv|generate-nats-jetstream> [count]" >&2; exit 2 ;;
esac

kc() { kubectl --context="$KUBECTL_CONTEXT" -n "$NAMESPACE" "$@"; }
log() { echo "[$(date -u +%H:%M:%S)] $*" >&2; }

NAME="twin-state-diag-${VARIANT}"
CONFIG_FILE="$HERE/configs/${VARIANT}.yaml"
[[ -f "$CONFIG_FILE" ]] || { echo "ERROR: missing config file $CONFIG_FILE" >&2; exit 1; }

cleanup() {
  log "-- tearing down $NAME"
  kc delete pod "$NAME" --ignore-not-found >/dev/null 2>&1 || true
  kc delete configmap "${NAME}-config" --ignore-not-found >/dev/null 2>&1 || true
  rm -f "/tmp/${NAME}-configmap.yaml" "/tmp/${NAME}-pod.yaml"
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# Resolve NATS_KV_BUCKET and production resources from values.yaml — same
# resolve_values() logic as run-localization.sh, duplicated here to keep this
# script independently runnable (run-localization.sh is not designed to be
# sourced as a library — it executes main() on load).
# ---------------------------------------------------------------------------
resolved="$(python3 - "$REPO_ROOT/k8s/helm/thingsflow/values.yaml" <<'PY'
import sys, json
import yaml
with open(sys.argv[1]) as f:
    v = yaml.safe_load(f)
bucket = (v.get("nats", {}) or {}).get("twinKv", {}).get("bucket", "twin_state")
res = (((v.get("natsDataPlane", {}) or {}).get("latestKv", {}) or {}).get("resources")) or {
    "limits": {"cpu": "750m", "memory": "512Mi"},
    "requests": {"cpu": "100m", "memory": "128Mi"},
}
print(json.dumps({"bucket": bucket, "resources": res}))
PY
)"
[[ -n "$resolved" ]] || { echo "BLOCKED: could not resolve nats.twinKv.bucket from values.yaml" >&2; exit 1; }
BUCKET="$(echo "$resolved" | python3 -c 'import json,sys; print(json.load(sys.stdin)["bucket"])')"
LIMITS_CPU="$(echo "$resolved" | python3 -c 'import json,sys; print(json.load(sys.stdin)["resources"]["limits"]["cpu"])')"
LIMITS_MEM="$(echo "$resolved" | python3 -c 'import json,sys; print(json.load(sys.stdin)["resources"]["limits"]["memory"])')"
REQUESTS_CPU="$(echo "$resolved" | python3 -c 'import json,sys; print(json.load(sys.stdin)["resources"]["requests"]["cpu"])')"
REQUESTS_MEM="$(echo "$resolved" | python3 -c 'import json,sys; print(json.load(sys.stdin)["resources"]["requests"]["memory"])')"
log "-- bucket=$BUCKET resources.limits=${LIMITS_CPU}/${LIMITS_MEM} resources.requests=${REQUESTS_CPU}/${REQUESTS_MEM}"

KV_STREAM="KV_${BUCKET}"

# ---------------------------------------------------------------------------
# Reads the twin_state bucket's backing stream last_seq — the authoritative,
# monotonic count of every successful Put (nats_kv or nats_jetstream) ever
# made to this bucket. Confirmed live (2026-08-13) that this field exists and
# increments per-Put regardless of KV compaction (max_msgs_per_subject).
# ---------------------------------------------------------------------------
read_last_seq() {
  local podname="diag-seq-$RANDOM"
  kc run "$podname" --rm -i --restart=Never --image="$NATS_IMAGE" --command -- \
    sh -c "nats --server '$NATS_URL' stream info '$KV_STREAM' --json 2>/dev/null | jq -r '.state.last_seq // -1' 2>/dev/null || echo -1" \
    2>/dev/null | grep -E '^-?[0-9]+$' | tail -1
}

data_plane_ok() {
  local podname="diag-genhealth-$RANDOM"
  local info
  info="$(kc run "$podname" --rm -i --restart=Never --image="$NATS_IMAGE" --command -- \
    sh -c "nats --server '$NATS_URL' consumer info TF_RAW thingsflow-latest-kv-durable 2>&1" 2>/dev/null)"
  printf '%s\n' "$info" | grep -qi 'No interest' && return 1
  printf '%s\n' "$info" | grep -qiE 'not found|nats: error|deadline exceeded|connection refused|i/o timeout|no responders' && return 1
  return 0
}

log "-- pre-flight: production data-plane health"
if ! data_plane_ok; then
  echo "BLOCKED: production data-plane unhealthy before this run — aborting" >&2
  exit 1
fi

PRE_SEQ="$(read_last_seq)"
[[ "$PRE_SEQ" =~ ^-?[0-9]+$ ]] || { echo "BLOCKED: could not read $KV_STREAM last_seq before run" >&2; exit 1; }
log "-- pre-run last_seq=$PRE_SEQ"

# ---------------------------------------------------------------------------
# Render and run the throwaway pod. Resources match production's latest-kv
# consumer (values.yaml natsDataPlane.latestKv.resources) so this diagnostic
# is not "unequal budgets" versus the real consumer. Runs to completion
# (input.generate self-terminates after GENERATE_COUNT messages) via
# `kubectl run --rm -i`, foreground, so wall-clock timing is directly
# observable by this script.
# ---------------------------------------------------------------------------
{
  echo "apiVersion: v1"
  echo "kind: ConfigMap"
  echo "metadata:"
  echo "  name: ${NAME}-config"
  echo "  labels:"
  echo "    app: twin-state-diag-generate"
  echo "    variant: ${VARIANT}"
  echo "data:"
  echo "  config.yaml: |"
  sed 's/^/    /' "$CONFIG_FILE"
} > "/tmp/${NAME}-configmap.yaml"
kc apply -f "/tmp/${NAME}-configmap.yaml" >/dev/null

cat > "/tmp/${NAME}-pod.yaml" <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${NAME}
  labels:
    app: twin-state-diag-generate
    variant: ${VARIANT}
spec:
  restartPolicy: Never
  containers:
  - name: bento
    image: "${BENTO_IMAGE}"
    imagePullPolicy: IfNotPresent
    command: ["/bento", "-c", "/etc/bento/config.yaml"]
    env:
    - name: LATEST_KV_MAX_IN_FLIGHT
      value: "1024"
    - name: NATS_URL
      value: "${NATS_URL}"
    - name: NATS_KV_BUCKET
      value: "${BUCKET}"
    - name: GENERATE_COUNT
      value: "${GENERATE_COUNT}"
    - name: METRICS_PORT
      value: "4195"
    resources:
      limits:
        cpu: "${LIMITS_CPU}"
        memory: "${LIMITS_MEM}"
      requests:
        cpu: "${REQUESTS_CPU}"
        memory: "${REQUESTS_MEM}"
    volumeMounts:
    - name: config
      mountPath: /etc/bento
      readOnly: true
  volumes:
  - name: config
    configMap:
      name: ${NAME}-config
EOF

log "-- running $NAME (count=$GENERATE_COUNT) — timing wall-clock"
START="$(date +%s)"
kc apply -f "/tmp/${NAME}-pod.yaml" >/dev/null
if ! kc wait --for=jsonpath='{.status.phase}'=Succeeded "pod/${NAME}" --timeout=180s >&2; then
  log "!! pod did not reach Succeeded within timeout — logs:"
  kc logs "$NAME" >&2 2>&1 || true
  echo "BLOCKED: generate-benchmark pod did not complete successfully" >&2
  exit 1
fi
END="$(date +%s)"
ELAPSED=$(( END - START ))
[[ "$ELAPSED" -lt 1 ]] && ELAPSED=1
kc logs "$NAME" >&2 2>&1 || true

log "-- post-run: production data-plane health"
if ! data_plane_ok; then
  echo "BLOCKED: production data-plane unhealthy after this run — do not trust this measurement, investigate before re-running" >&2
  exit 1
fi

POST_SEQ="$(read_last_seq)"
[[ "$POST_SEQ" =~ ^-?[0-9]+$ ]] || { echo "BLOCKED: could not read $KV_STREAM last_seq after run" >&2; exit 1; }
log "-- post-run last_seq=$POST_SEQ"

WRITES=$(( POST_SEQ - PRE_SEQ ))
if [[ "$WRITES" -lt 0 ]]; then
  echo "ERROR: last_seq went backwards ($PRE_SEQ -> $POST_SEQ) — stream was likely purged/rolled over concurrently; measurement invalid" >&2
  exit 1
fi

RATE="$(python3 -c "print(f'{$WRITES / $ELAPSED:.1f}')")"
echo "GENERATE_RESULT variant=$VARIANT writes=$WRITES elapsed_s=$ELAPSED rate_msg_s=$RATE"
