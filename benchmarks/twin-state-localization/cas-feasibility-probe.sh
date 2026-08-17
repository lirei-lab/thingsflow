#!/usr/bin/env bash
# CAS feasibility probe — Milestone 4 Phase 1 (one-document-per-device twin-state).
#
# Answers the design doc's highest-priority gate BEFORE any measurement commits:
#   Half A: can a STOCK Bento 1.8.1 nats_kv cache get expose the KV entry's
#           revision/subject-sequence (the CAS header source)?
#   Half B: does a raw JetStream publish to $KV.<bucket>.<key> with a STALE
#           Nats-Expected-Last-Subject-Sequence header get REJECTED by the
#           bucket's backing stream (leaving the value unchanged)?
#
# Prints machine-parseable verdict lines:
#   HALF_A_REVISION=PASS|FAIL|UNKNOWN   (PASS = revision observable from cache get)
#   HALF_B_CASREJECT=PASS|FAIL|UNKNOWN  (PASS = stale expected-seq publish rejected)
#   FEASIBLE=true|false
# Exits 0 iff FEASIBLE=true, non-zero otherwise.
#
# Uses the SAME single-pod diagnostic pattern as run-generate-benchmark.sh
# (namespace thingsflow-fresh, context microk8s, trap-cleanup, no orphans).
# NEVER touches the production Helm release or the production latest-kv durable.
#
# Usage:
#   cas-feasibility-probe.sh
#
# Env overrides (all optional):
#   NAMESPACE, KUBECTL_CONTEXT, NATS_URL, BENTO_IMAGE, NATS_IMAGE, NATS_KV_BUCKET

set -uo pipefail  # deliberately NOT -e: every check must run and report

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$HERE/../.." && pwd)"

NAMESPACE="${NAMESPACE:-thingsflow-fresh}"
KUBECTL_CONTEXT="${KUBECTL_CONTEXT:-microk8s}"
NATS_URL="${NATS_URL:-nats://thingsflow-nats:4222}"
BENTO_IMAGE="${BENTO_IMAGE:-ghcr.io/warpstreamlabs/bento:1.8.1}"
NATS_IMAGE="${NATS_IMAGE:-natsio/nats-box:0.16.0}"
NATS_KV_BUCKET="${NATS_KV_BUCKET:-twin_state}"

kc() { kubectl --context="$KUBECTL_CONTEXT" -n "$NAMESPACE" "$@"; }
log() { echo "[$(date -u +%H:%M:%S)] $*" >&2; }

STAMP="$(date -u +%m%d%H%M%S)-$RANDOM"
DIAG_KEY="DEVICE.diag-tenant.diag-device"
PROBE_A_NAME="twin-state-diag-casprobe-a-$STAMP"
PROBE_B_NAME="twin-state-diag-casprobe-b-$STAMP"

HALF_A="UNKNOWN"
HALF_B="UNKNOWN"

cleanup() {
  log "-- cleanup"
  kc delete pod "$PROBE_A_NAME" --ignore-not-found >/dev/null 2>&1 || true
  kc delete pod "$PROBE_B_NAME" --ignore-not-found >/dev/null 2>&1 || true
  kc delete configmap "${PROBE_A_NAME}-config" --ignore-not-found >/dev/null 2>&1 || true
  kc run "diag-cas-clean-$RANDOM" --rm -i --restart=Never --image="$NATS_IMAGE" --command -- \
    sh -c "nats --server '$NATS_URL' kv del '$NATS_KV_BUCKET' '$DIAG_KEY' >/dev/null 2>&1 || true" >/dev/null 2>&1 || true
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# Pre-flight: data-plane health + diagnostic key setup
# ---------------------------------------------------------------------------
data_plane_ok() {
  local podname="diag-cashealth-$RANDOM"
  local info
  info="$(kc run "$podname" --rm -i --restart=Never --image="$NATS_IMAGE" --command -- \
    sh -c "nats --server '$NATS_URL' consumer info TF_RAW thingsflow-latest-kv-durable 2>&1" 2>/dev/null)"
  printf '%s\n' "$info" | grep -qi 'No interest' && return 1
  printf '%s\n' "$info" | grep -qiE 'not found|nats: error|deadline exceeded|connection refused|i/o timeout|no responders' && return 1
  return 0
}

log "-- pre-flight: production data-plane health"
if ! data_plane_ok; then
  echo "HALF_A_REVISION=UNKNOWN"
  echo "HALF_B_CASREJECT=UNKNOWN"
  echo "FEASIBLE=false"
  echo "BLOCKED: production data-plane unhealthy before probe — aborting" >&2
  exit 1
fi

# Seed a diagnostic KV entry with a known value + record its current revision.
log "-- seeding diagnostic KV entry $DIAG_KEY"
SEED_OUT="$(kc run "diag-cas-seed-$RANDOM" --rm -i --restart=Never --image="$NATS_IMAGE" --command -- \
  sh -c "nats --server '$NATS_URL' kv put '$NATS_KV_BUCKET' '$DIAG_KEY' '{\"schema\":\"probe\",\"ts\":1,\"value\":\"seed\"}' 2>&1" 2>/dev/null)"
printf '%s\n' "$SEED_OUT" | grep -qiE 'not found|nats: error|deadline|refused|timeout|no responders' && {
  echo "BLOCKED: could not seed diagnostic KV entry: $SEED_OUT" >&2
  echo "HALF_A_REVISION=UNKNOWN"
  echo "HALF_B_CASREJECT=UNKNOWN"
  echo "FEASIBLE=false"
  exit 1
}
log "-- seed ok: $SEED_OUT"

# ---------------------------------------------------------------------------
# HALF A — can a stock Bento cache get expose the revision?
# ---------------------------------------------------------------------------
log "-- HALF A: Bento nats_kv cache get — revision observable?"
{
  echo "apiVersion: v1"
  echo "kind: ConfigMap"
  echo "metadata:"
  echo "  name: ${PROBE_A_NAME}-config"
  echo "  labels:"
  echo "    app: twin-state-diag-casprobe"
  echo "    half: a"
  echo "data:"
  echo "  config.yaml: |"
  sed 's/^/    /' "$HERE/configs/cas-probe-get.yaml"
} > "/tmp/${PROBE_A_NAME}-configmap.yaml"
kc apply -f "/tmp/${PROBE_A_NAME}-configmap.yaml" >/dev/null

cat > "/tmp/${PROBE_A_NAME}-pod.yaml" <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${PROBE_A_NAME}
  labels:
    app: twin-state-diag-casprobe
    half: a
spec:
  restartPolicy: Never
  containers:
  - name: bento
    image: "${BENTO_IMAGE}"
    imagePullPolicy: IfNotPresent
    command: ["/bento", "-c", "/etc/bento/config.yaml"]
    env:
    - name: NATS_URL
      value: "${NATS_URL}"
    - name: NATS_KV_BUCKET
      value: "${NATS_KV_BUCKET}"
    - name: METRICS_PORT
      value: "4196"
    volumeMounts:
    - name: config
      mountPath: /etc/bento
      readOnly: true
  volumes:
  - name: config
    configMap:
      name: ${PROBE_A_NAME}-config
EOF

kc apply -f "/tmp/${PROBE_A_NAME}-pod.yaml" >/dev/null
if ! kc wait --for=jsonpath='{.status.phase}'=Succeeded "pod/${PROBE_A_NAME}" --timeout=90s >&2; then
  log "!! probe A pod did not Succeed — logs:"
  kc logs "$PROBE_A_NAME" >&2 2>&1 || true
  HALF_A="UNKNOWN"
else
  A_LOG="$(kc logs "$PROBE_A_NAME" 2>/dev/null || true)"
  log "-- probe A output:"
  printf '%s\n' "$A_LOG" | tail -20 >&2
  # Stock Bento nats_kv cache get returns only value bytes; there is no
  # revision/sequence field in the cache interface. Detect whether the cache
  # get ACTUALLY exposed a revision/sequence/expected-last indicator as a JSON
  # key in the emitted result. We match JSON keys ONLY for real revision/
  # sequence/expected-last indicator names — this is an ALLOW-LIST of genuine
  # NATS/KV metadata keys. Do NOT add placeholder field names here (e.g. a
  # probe config's own "revision_exposed" field): matching a placeholder would
  # silently flip Half A to a false PASS (review finding 2026-08-17).
  # Seed-assertion guard: verify the cache get actually returned the seeded
  # value before judging revision-absence — a failed/empty get would otherwise
  # be misattributed as "no revision exposed" (review finding 2026-08-17).
  if printf '%s\n' "$A_LOG" | grep -q '"value":"seed"'; then
    if printf '%s\n' "$A_LOG" | grep -qE '"revision"|"seq[_-]?num"|"sequence"|"expected_last|"last_seq"'; then
      HALF_A="PASS"
    else
      HALF_A="FAIL"
    fi
  else
    log "!! Half A: cache get did not return the seeded value — cannot judge revision-absence"
    HALF_A="UNKNOWN"
  fi
fi

# ---------------------------------------------------------------------------
# HALF B — is a stale expected-last-subject-seq publish rejected?
# ---------------------------------------------------------------------------
log "-- HALF B: stale Nats-Expected-Last-Subject-Sequence rejection"
# Read the CURRENT revision of the diagnostic key, then publish with an
# impossible stale expected seq and check the value is unchanged.
B_SEQ="$(kc run "diag-cas-seq-$RANDOM" --rm -i --restart=Never --image="$NATS_IMAGE" --command -- \
  sh -c "nats --server '$NATS_URL' kv history '$NATS_KV_BUCKET' '$DIAG_KEY' --json 2>/dev/null | jq -r '.[-1].revision // empty' 2>/dev/null || echo ''" 2>/dev/null | grep -E '^[0-9]+$' | tail -1)"

STALE_SEQ=$(( ${B_SEQ:-1} + 100000 ))

# Publish with a deliberately stale expected-last-subject-sequence via raw
# JetStream publish with the header. nats-box supports `--header`. Capture
# BOTH the publish output AND its exit status: a rejected CAS publish is the
# direct signal (value unchanged is corroborating, but the rejection itself is
# the proof).
B_PUB_OUT="$(kc run "diag-cas-pub-$RANDOM" --rm -i --restart=Never --image="$NATS_IMAGE" --command -- \
  sh -c "nats --server '$NATS_URL' pub '\\\$KV.${NATS_KV_BUCKET}.${DIAG_KEY}' '{\"schema\":\"probe\",\"ts\":2,\"value\":\"stale-write\"}' --header 'Nats-Expected-Last-Subject-Sequence: ${STALE_SEQ}'; echo PUBRC=\$?" 2>&1 || true)"

# Read back — if the value is still the seed, the stale write did not land.
# Settle delay so the readback reflects the publish outcome, not a race with
# pod scheduling latency (review finding 2026-08-17).
sleep 1
B_READ="$(kc run "diag-cas-read-$RANDOM" --rm -i --restart=Never --image="$NATS_IMAGE" --command -- \
  sh -c "nats --server '$NATS_URL' kv get '$NATS_KV_BUCKET' '$DIAG_KEY' 2>&1" 2>/dev/null)"

log "-- half B pub output: $(printf '%s' "$B_PUB_OUT" | grep -vE 'recorded in|command prompt' | head -4)"
log "-- half B readback: $(printf '%s' "$B_READ" | grep -vE 'recorded in|command prompt' | head -3)"

# PASS requires BOTH: the stale publish was rejected (PUBRC != 0 or a
# rejection/expect error) AND the readback value is unchanged at "seed".
PUB_REJECTED=0
if printf '%s' "$B_PUB_OUT" | grep -qE 'PUBRC=[1-9]'; then
  PUB_REJECTED=1
elif printf '%s' "$B_PUB_OUT" | grep -qiE 'wrong last sequence|expected last|no message response|nats: error|rejected|subject sequence|stale'; then
  PUB_REJECTED=1
fi

if [[ "$PUB_REJECTED" == "1" ]] && printf '%s' "$B_READ" | grep -q '"value":"seed"'; then
  # We observed an explicit rejection (non-zero exit or a rejection/expect
  # error) AND the value is unchanged: genuine CAS enforcement.
  HALF_B="PASS"
elif printf '%s' "$B_READ" | grep -q '"value":"stale-write"'; then
  # The stale write landed: CAS was NOT enforced.
  HALF_B="FAIL"
else
  # The value stayed at seed but we did NOT capture an explicit rejection
  # (fire-and-forget `nats pub`, no ack): the stale write may have been
  # silently dropped (still a CAS-enforcement signal) OR the header may not
  # have been enforced (the publish was a no-op). Without an observed
  # rejection we cannot distinguish these. Record UNKNOWN honestly — do NOT
  # claim PASS on inferential evidence alone (review finding 2026-08-17).
  HALF_B="UNKNOWN"
fi

# ---------------------------------------------------------------------------
# Verdict
# ---------------------------------------------------------------------------
echo "HALF_A_REVISION=${HALF_A}"
echo "HALF_B_CASREJECT=${HALF_B}"
if [[ "$HALF_A" == "PASS" && "$HALF_B" == "PASS" ]]; then
  echo "FEASIBLE=true"
  exit 0
elif [[ "$HALF_A" == "FAIL" ]]; then
  echo "FEASIBLE=false"
  echo "MISSING_PRIMITIVE=revision-not-observable-from-bento-nats_kv-cache-get (no revision/sequence field in stock cache interface)"
  exit 1
elif [[ "$HALF_B" == "FAIL" ]]; then
  echo "FEASIBLE=false"
  echo "MISSING_PRIMITIVE=stale-Nats-Expected-Last-Subject-Sequence-publish-not-rejected (CAS not enforced by KV backing stream)"
  exit 1
else
  echo "FEASIBLE=false"
  echo "MISSING_PRIMITIVE=probe-inconclusive (see logs)"
  exit 1
fi
