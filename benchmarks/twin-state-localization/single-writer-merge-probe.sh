#!/usr/bin/env bash
# Option A — single-writer merge probe (one serialized consumer, NO CAS).
#
# Milestone 4 (one-document-per-device twin-state). Phase 1 answered the CAS
# gate: the config-only CAS merge writer is INFEASIBLE in Bento 1.8.1 (no KV
# revision source config-only). This probe tests the OTHER route to race-safety:
# remove the concurrent writer instead of coordinating it.
#
# Design under test (configs/merge-doc-writer-single.yaml):
#   A SINGLE JetStream consumer (single replica pod + input max_in_flight:1 +
#   consumer MaxAckPending:1) serializes every message: cache get -> merge ->
#   cache set completes and acks BEFORE the next message is delivered, so the
#   read-modify-write is atomic w.r.t. the writer with NO revision/CAS needed.
#
# The probe proves, with evidence:
#   FEASIBILITY — a stock Bento 1.8.1 config can do get -> merge -> set in one
#                 serialized pass (config-only, no custom Go).
#   RACE_SAFE   — 200 concurrent messages for ONE device (20 keys x 10 updates,
#                 increasing ts per update) all land: ZERO key loss, each key
#                 converges to its highest-ts value (LWW correct regardless of
#                 delivery order).
#   STALE_DROP  — a deterministic stale write (ts=1 for an existing key) is
#                 dropped by the per-key LWW merge.
#   ROUNDTRIP   — the full State doc is carried forward: attributes/schema/
#                 tenantId/entityType/entityId survive the merge.
#   UPDATEDTS   — updatedTs == max ts across all writes.
#
# Prints machine-parseable verdict lines:
#   MERGE_WRITER_SINGLE=FEASIBLE|INFEASIBLE|BLOCKED
#   RACE_SAFE=PASS|FAIL
#   KEYS_EXPECTED=20 KEYS_PRESENT=.. KEYS_LOST=..
#   LWW_FINAL=PASS|FAIL   (every key == its highest-ts value)
#   STALE_DROP=PASS|FAIL
#   ROUNDTRIP=PASS|FAIL
#   UPDATEDTS=PASS|FAIL
# Exits 0 iff MERGE_WRITER_SINGLE=FEASIBLE and every check PASSes.
#
# Uses a dedicated DIAGNOSTIC JetStream stream TF_MERGE_DIAG (subject
# tf.merge.diag.>) so NOTHING lands on TF_RAW and NO production consumer ever
# sees probe traffic. Writes ONLY the diagnostic doc key
# DEVICE.diag-tenant.diag-device (flow-core never writes diag-tenant).
# Single-pod diagnostic pattern, trap-cleanup on exit, no orphans left.
# NEVER touches the production Helm release or the production latest-kv durable.
#
# Usage:
#   single-writer-merge-probe.sh
#
# Env overrides (all optional):
#   NAMESPACE, KUBECTL_CONTEXT, NATS_URL, BENTO_IMAGE, NATS_IMAGE,
#   NATS_KV_BUCKET, MERGE_STREAM

set -uo pipefail  # deliberately NOT -e: every check must run and report

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$HERE/../.." && pwd)"

NAMESPACE="${NAMESPACE:-thingsflow-fresh}"
KUBECTL_CONTEXT="${KUBECTL_CONTEXT:-microk8s}"
NATS_URL="${NATS_URL:-nats://thingsflow-nats:4222}"
BENTO_IMAGE="${BENTO_IMAGE:-ghcr.io/warpstreamlabs/bento:1.8.1}"
NATS_IMAGE="${NATS_IMAGE:-natsio/nats-box:0.16.0}"
NATS_KV_BUCKET="${NATS_KV_BUCKET:-twin_state}"
MERGE_STREAM="${MERGE_STREAM:-TF_MERGE_DIAG}"

MERGE_SUBJECT="tf.merge.diag.>"
PUB_SUBJECT="tf.merge.diag.contention"
DIAG_KEY="DEVICE.diag-tenant.diag-device"

EXPECTED_KEYS=20
UPDATES_PER_KEY=10
MSG_COUNT=$((EXPECTED_KEYS * UPDATES_PER_KEY))
TS_BASE=1000000          # final ts for key i, seq 9: TS_BASE + 9000 + i

STAMP="$(date -u +%m%d%H%M%S)-$RANDOM"
MERGE_DURABLE="thingsflow-merge-diag-${STAMP}"
MERGE_QUEUE="thingsflow-merge-diag-${STAMP}"
MERGE_TARGET="tf.deliver.merge-diag-${STAMP}"
MERGE_POD="twin-state-diag-single-merge-${STAMP}"
MERGE_CM="${MERGE_POD}-config"
LINT_POD="twin-state-diag-merge-lint-${STAMP}"

kc() { kubectl --context="$KUBECTL_CONTEXT" -n "$NAMESPACE" "$@"; }
log() { echo "[$(date -u +%H:%M:%S)] $*" >&2; }

MERGE_WRITER="UNKNOWN"
RACE_SAFE="FAIL"
KEYS_PRESENT=0
KEYS_LOST="?"
LWW_FINAL="FAIL"
STALE_DROP="FAIL"
ROUNDTRIP="FAIL"
UPDATEDTS="FAIL"

PUB_CM="diag-merge-pub-${STAMP}"
PUB_POD="diag-merge-pub-${STAMP}"

cleanup() {
  log "-- cleanup"
  kc delete pod "$MERGE_POD" --ignore-not-found >/dev/null 2>&1 || true
  kc delete pod "$LINT_POD" --ignore-not-found >/dev/null 2>&1 || true
  kc delete pod "$PUB_POD" --ignore-not-found >/dev/null 2>&1 || true
  kc delete configmap "$MERGE_CM" --ignore-not-found >/dev/null 2>&1 || true
  kc delete configmap "${MERGE_CM}-lint" --ignore-not-found >/dev/null 2>&1 || true
  kc delete configmap "$PUB_CM" --ignore-not-found >/dev/null 2>&1 || true
  # Delete the diagnostic stream (removes its consumer too) + the diagnostic doc key.
  kc run "diag-merge-clean-$RANDOM" --rm -i --restart=Never --image="$NATS_IMAGE" --command -- \
    sh -c "nats --server '$NATS_URL' stream rm '$MERGE_STREAM' -f >/dev/null 2>&1 || true; nats --server '$NATS_URL' kv del '$NATS_KV_BUCKET' '$DIAG_KEY' -f >/dev/null 2>&1 || true" >/dev/null 2>&1 || true
}
trap cleanup EXIT

# publish_payloads <payloads-file>  — publishes one JSON message per line from a
# host file by mounting it into a nats-box pod (avoids nested shell/JSON quote
# hell: payloads are generated on the host with python3 and read line-by-line
# inside the pod). Publishes in batches of 20 concurrent `nats pub` processes.
# Returns the pod exit code (0 = PUBLISHED_DONE).
publish_payloads() {
  local payloads_file="$1"
  kc delete pod "$PUB_POD" --ignore-not-found >/dev/null 2>&1 || true
  kc delete configmap "$PUB_CM" --ignore-not-found >/dev/null 2>&1 || true
  # publish.sh — POSIX sh, reads payloads one per line, 20 concurrent at a time.
  cat > "/tmp/${PUB_CM}-publish.sh" <<'SH'
#!/bin/sh
set -u
n=0
while IFS= read -r line; do
  [ -z "$line" ] && continue
  nats --server "$NATS_URL" pub "$PUB_SUBJECT" "$line" >/dev/null 2>&1 &
  n=$((n+1))
  if [ $((n % 20)) -eq 0 ]; then wait; fi
done < /payloads/payloads.txt
wait
echo PUBLISHED_DONE
SH
  kc create configmap "$PUB_CM" \
    --from-file=publish="/tmp/${PUB_CM}-publish.sh" \
    --from-file=payloads="$payloads_file" --dry-run=client -o yaml | kc apply -f - >/dev/null
  cat > "/tmp/${PUB_POD}.yaml" <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${PUB_POD}
  labels:
    app: twin-state-diag-merge
    stage: publish
spec:
  restartPolicy: Never
  containers:
  - name: nats
    image: "${NATS_IMAGE}"
    imagePullPolicy: IfNotPresent
    command: ["/bin/sh", "-c", "sh /scripts/publish.sh"]
    env:
    - name: NATS_URL
      value: "${NATS_URL}"
    - name: PUB_SUBJECT
      value: "${PUB_SUBJECT}"
    volumeMounts:
    - name: payloads
      mountPath: /payloads
    - name: scripts
      mountPath: /scripts
  volumes:
  - name: payloads
    configMap:
      name: ${PUB_CM}
      items:
      - key: payloads
        path: payloads.txt
  - name: scripts
    configMap:
      name: ${PUB_CM}
      items:
      - key: publish
        path: publish.sh
EOF
  kc apply -f "/tmp/${PUB_POD}.yaml" >/dev/null
  if ! kc wait --for=jsonpath='{.status.phase}'=Succeeded "pod/${PUB_POD}" --timeout=120s >/dev/null 2>&1; then
    log "!! publish pod ${PUB_POD} did not Succeed — logs:"
    kc logs "$PUB_POD" >&2 2>&1 || true
    return 1
  fi
  if ! kc logs "$PUB_POD" 2>/dev/null | grep -q PUBLISHED_DONE; then
    log "!! publish pod ${PUB_POD} finished without PUBLISHED_DONE marker"
    return 1
  fi
  return 0
}

# ---------------------------------------------------------------------------
# Pre-flight: cluster guard + data-plane health
# ---------------------------------------------------------------------------
SERVER="$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}' 2>/dev/null)"
if [[ "$SERVER" != "https://172.16.128.7:16443" ]]; then
  echo "MERGE_WRITER_SINGLE=BLOCKED"
  echo "BLOCKED: active cluster is $SERVER, expected test cluster 172.16.128.7:16443 (microk8s) — refusing to run against any other cluster" >&2
  exit 1
fi

data_plane_ok() {
  local podname="diag-mergehealth-$RANDOM"
  local info
  info="$(kc run "$podname" --rm -i --restart=Never --image="$NATS_IMAGE" --command -- \
    sh -c "nats --server '$NATS_URL' consumer info TF_RAW thingsflow-latest-kv-durable 2>&1" 2>/dev/null)"
  printf '%s\n' "$info" | grep -qiE 'not found|nats: error|deadline exceeded|connection refused|i/o timeout|no responders|No interest' && return 1
  return 0
}

log "-- pre-flight: cluster=$SERVER ns=$NAMESPACE"
if ! data_plane_ok; then
  echo "MERGE_WRITER_SINGLE=BLOCKED"
  echo "BLOCKED: production data-plane unhealthy before probe — aborting" >&2
  exit 1
fi

# ---------------------------------------------------------------------------
# Seed the diagnostic whole-state doc (attributes carried-forward baseline).
# ---------------------------------------------------------------------------
SEED_DOC='{"schema":"thingsflow.twin-state.v1","tenantId":"diag-tenant","entityType":"DEVICE","entityId":"diag-device","updatedTs":0,"telemetry":{},"attributes":{"SERVER_SCOPE":{"pre":{"ts":0,"value":"seed"}}},"activity":{}}'
log "-- seeding diagnostic doc $DIAG_KEY"
SEED_OUT="$(kc run "diag-merge-seed-$RANDOM" --rm -i --restart=Never --image="$NATS_IMAGE" --command -- \
  sh -c "nats --server '$NATS_URL' kv put '$NATS_KV_BUCKET' '$DIAG_KEY' '$SEED_DOC' 2>&1" 2>/dev/null)"
printf '%s\n' "$SEED_OUT" | grep -qiE 'not found|nats: error|deadline|refused|timeout|no responders' && {
  echo "BLOCKED: could not seed diagnostic KV entry: $SEED_OUT" >&2
  echo "MERGE_WRITER_SINGLE=BLOCKED"
  exit 1
}
log "-- seed ok"

# ---------------------------------------------------------------------------
# Lint the merge config (catch schema/Bloblang errors before deploying).
# ---------------------------------------------------------------------------
log "-- linting merge-doc-writer-single.yaml"
{
  echo "apiVersion: v1"
  echo "kind: ConfigMap"
  echo "metadata:"
  echo "  name: ${MERGE_CM}-lint"
  echo "  labels:"
  echo "    app: twin-state-diag-merge"
  echo "    stage: lint"
  echo "data:"
  echo "  config.yaml: |"
  sed 's/^/    /' "$HERE/configs/merge-doc-writer-single.yaml"
} > "/tmp/${MERGE_CM}-lint.yaml"
kc apply -f "/tmp/${MERGE_CM}-lint.yaml" >/dev/null
# `kubectl run` cannot mount a ConfigMap with the config in a single invocation,
# so lint runs via a short dedicated pod with the ConfigMap mounted.
# NOTE: this heredoc is deliberately UNQUOTED so the ${VAR}s below expand, but
# it must contain NO backticks and NO ${VAR} style text in comments (an
# unquoted heredoc expands both — a backtick comment executed `bento lint` and
# a ${VAR} comment tripped `set -u` on 2026-08-17). Env vars are supplied via
# the pod's env list instead of an inline export so `bento lint` sees them.
cat > "/tmp/${LINT_POD}.yaml" <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${LINT_POD}
  labels:
    app: twin-state-diag-merge
    stage: lint
spec:
  restartPolicy: Never
  containers:
  - name: bento
    image: "${BENTO_IMAGE}"
    imagePullPolicy: IfNotPresent
    command: ["/bento", "lint", "/etc/bento/config.yaml"]
    env:
    - name: METRICS_PORT
      value: "4196"
    - name: NATS_URL
      value: "${NATS_URL}"
    - name: NATS_KV_BUCKET
      value: "${NATS_KV_BUCKET}"
    - name: MERGE_STREAM
      value: "${MERGE_STREAM}"
    - name: MERGE_DURABLE
      value: "${MERGE_DURABLE}"
    - name: MERGE_SUBJECT
      value: "${MERGE_SUBJECT}"
    - name: MERGE_QUEUE
      value: "${MERGE_QUEUE}"
    volumeMounts:
    - name: config
      mountPath: /etc/bento
      readOnly: true
  volumes:
  - name: config
    configMap:
      name: ${MERGE_CM}-lint
EOF
kc apply -f "/tmp/${LINT_POD}.yaml" >/dev/null
if kc wait --for=jsonpath='{.status.phase}'=Succeeded "pod/${LINT_POD}" --timeout=60s >&2; then
  log "-- lint clean"
else
  log "!! lint reported issues (pod ${LINT_POD} did not exit 0):"
  kc logs "$LINT_POD" >&2 2>&1 || true
fi

# ---------------------------------------------------------------------------
# Create the diagnostic stream + serialized consumer (deliver new, max-pending 1)
# ---------------------------------------------------------------------------
log "-- creating diagnostic stream $MERGE_STREAM (subject $MERGE_SUBJECT)"
kc run "diag-merge-stream-$RANDOM" --rm -i --restart=Never --image="$NATS_IMAGE" --command -- \
  sh -c "nats --server '$NATS_URL' stream rm '$MERGE_STREAM' -f >/dev/null 2>&1 || true; nats --server '$NATS_URL' stream add '$MERGE_STREAM' --subjects '$MERGE_SUBJECT' --storage file --retention limits --max-msgs 5000 --max-age 1h --defaults" 2>&1 | tail -3

log "-- creating serialized consumer $MERGE_DURABLE (max-pending 1, deliver new)"
# nats-box 0.16.0 non-interactive quirk (WR-05): `consumer add` REQUIRES
# --target (and --deliver-group) or it errors "could not request delivery
# target: cannot prompt for user input without a terminal". Both flags are
# passed together, mirroring bootstrap-diag-consumer.sh / the production
# latest-KV push consumer shape.
kc run "diag-merge-consumer-$RANDOM" --rm -i --restart=Never --image="$NATS_IMAGE" --command -- \
  sh -c "nats --server '$NATS_URL' consumer add '$MERGE_STREAM' '$MERGE_DURABLE' --filter '$MERGE_SUBJECT' --ack explicit --deliver new --replay instant --max-deliver 500 --wait 30s --max-pending 1 --target '$MERGE_TARGET' --deliver-group '$MERGE_QUEUE' --defaults" 2>&1 | grep -viE 'deleted|recorded in|command prompt|press enter|All commands' | tail -4

# ---------------------------------------------------------------------------
# Deploy the single-replica merge-writer pod
# ---------------------------------------------------------------------------
{
  echo "apiVersion: v1"
  echo "kind: ConfigMap"
  echo "metadata:"
  echo "  name: ${MERGE_CM}"
  echo "  labels:"
  echo "    app: twin-state-diag-merge"
  echo "data:"
  echo "  config.yaml: |"
  sed 's/^/    /' "$HERE/configs/merge-doc-writer-single.yaml"
} > "/tmp/${MERGE_CM}.yaml"
kc apply -f "/tmp/${MERGE_CM}.yaml" >/dev/null

cat > "/tmp/${MERGE_POD}.yaml" <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${MERGE_POD}
  labels:
    app: twin-state-diag-merge
    role: single-writer
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
    - name: MERGE_STREAM
      value: "${MERGE_STREAM}"
    - name: MERGE_DURABLE
      value: "${MERGE_DURABLE}"
    - name: MERGE_QUEUE
      value: "${MERGE_QUEUE}"
    - name: MERGE_SUBJECT
      value: "${MERGE_SUBJECT}"
    - name: METRICS_PORT
      value: "4196"
    volumeMounts:
    - name: config
      mountPath: /etc/bento
      readOnly: true
  volumes:
  - name: config
    configMap:
      name: ${MERGE_CM}
EOF
log "-- deploying single-replica merge writer pod $MERGE_POD"
kc apply -f "/tmp/${MERGE_POD}.yaml" >/dev/null
if ! kc wait --for=jsonpath='{.status.phase}'=Running "pod/${MERGE_POD}" --timeout=90s >&2; then
  log "!! merge pod did not start Running — logs:"
  kc logs "$MERGE_POD" >&2 2>&1 || true
  echo "MERGE_WRITER_SINGLE=INFEASIBLE"
  echo "MISSING_PRIMITIVE=merge-writer-pod-failed-to-start (see logs)"
  exit 1
fi
log "-- merge pod Running; giving it 8s to bind the durable"
sleep 8

# ---------------------------------------------------------------------------
# Contention batch: 20 keys x 10 updates, increasing ts, published concurrently
# ---------------------------------------------------------------------------
log "-- generating $MSG_COUNT payloads (keys k0..k$(($EXPECTED_KEYS-1)), ts increasing per seq)"
python3 - "$EXPECTED_KEYS" "$UPDATES_PER_KEY" "$TS_BASE" > "/tmp/${MERGE_CM}-contention.txt" <<'PYEOF'
import json, sys
exp, upd, base = int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3])
for i in range(exp):
    for s in range(upd):
        ts = base + s * 1000 + i
        print(json.dumps({"tenantId": "diag-tenant", "deviceId": "diag-device", "ts": ts,
                          "values": {"k%d" % i: {"ts": ts, "value": "v%d-%d" % (i, s)}}},
                         separators=(",", ":")))
PYEOF
log "-- publishing $MSG_COUNT concurrent messages for one device"
if ! publish_payloads "/tmp/${MERGE_CM}-contention.txt"; then
  echo "MERGE_WRITER_SINGLE=INFEASIBLE"
  echo "MISSING_PRIMITIVE=publish-pod-failed (see logs)"
  exit 1
fi
log "-- contention batch published"

# ---------------------------------------------------------------------------
# Wait for the consumer to drain (delivered == ack floor == MSG_COUNT)
# ---------------------------------------------------------------------------
log "-- waiting for merge consumer to drain ($MSG_COUNT messages)"
drain_ok=0
for attempt in $(seq 1 30); do
  INFO="$(kc run "diag-merge-info-$RANDOM" --rm -i --restart=Never --image="$NATS_IMAGE" --command -- \
    sh -c "nats --server '$NATS_URL' consumer info '$MERGE_STREAM' '$MERGE_DURABLE' 2>&1" 2>/dev/null)"
  DELIVERED="$(printf '%s\n' "$INFO" | sed -n 's/.*Last Delivered Message: Consumer sequence: \([0-9]*\).*/\1/p' | head -1)"
  ACK_FLOOR="$(printf '%s\n' "$INFO" | sed -n 's/.*Acknowledgment Floor: Consumer sequence: \([0-9]*\).*/\1/p' | head -1)"
  UNPROC="$(printf '%s\n' "$INFO" | sed -n 's/.*Unprocessed Messages: \([0-9]*\).*/\1/p' | head -1)"
  if [[ "${DELIVERED:-0}" -eq "$MSG_COUNT" && "${ACK_FLOOR:-0}" -eq "$MSG_COUNT" && "${UNPROC:-999}" -eq "0" ]]; then
    drain_ok=1
    break
  fi
  sleep 3
done
if [[ "$drain_ok" != "1" ]]; then
  log "!! consumer did not drain: delivered=${DELIVERED:-?} ack=${ACK_FLOOR:-?} unprocessed=${UNPROC:-?} (want $MSG_COUNT/$MSG_COUNT/0)"
fi

# ---------------------------------------------------------------------------
# Read the final doc and verify
# ---------------------------------------------------------------------------
DOC_RAW="$(kc run "diag-merge-read-$RANDOM" --rm -i --restart=Never --image="$NATS_IMAGE" --command -- \
  sh -c "nats --server '$NATS_URL' kv get '$NATS_KV_BUCKET' '$DIAG_KEY' --raw 2>&1" 2>/dev/null)"
# kc run --rm -i appends `pod "..." deleted from namespace` to STDOUT on the
# SAME line as the kv get --raw value (kv get --raw emits no trailing newline),
# so keep only the JSON object: greedy grep from the opening brace to the LAST
# closing brace (the pod-deletion noise contains no braces).
DOC_RAW="$(printf '%s\n' "$DOC_RAW" | grep -o '^{.*}' | head -1)"
printf '%s\n' "$DOC_RAW" | head -c 1200 >&2
echo "" >&2

# Pass the doc as an ARGV so python's stdin stays free for the heredoc program:
# `printf ... | python3 - <<'PYEOF'` does NOT work — the heredoc redirect
# REPLACES the pipe as python's stdin, so sys.stdin.read() returns empty
# (verified live 2026-08-17: RAW_LEN=0). With the doc in argv and `python3 -`
# reading the program from the heredoc, sys.argv[1] carries the doc.
VERIFY_OUT="$(python3 - "$DOC_RAW" "$EXPECTED_KEYS" "$TS_BASE" <<'PYEOF'
import json, sys
raw = sys.argv[1]
exp_keys = int(sys.argv[2])
ts_base = int(sys.argv[3])
try:
    doc = json.loads(raw)
except Exception as e:
    print("JSON_PARSE=FAIL detail=%s" % e)
    print("VERDICT=FAIL")
    sys.exit(1)
problems = []
tele = doc.get("telemetry", {}) if isinstance(doc, dict) else {}
present = 0
for i in range(exp_keys):
    k = "k%d" % i
    if k not in tele:
        problems.append("missing %s" % k)
        continue
    present += 1
    v = tele[k]
    want_ts = ts_base + 9000 + i
    want_val = "v%d-9" % i
    if v.get("ts") != want_ts or v.get("value") != want_val:
        problems.append("%s got %s want ts=%d val=%s" % (k, v, want_ts, want_val))
# roundtrip
attrs = (doc.get("attributes") or {}).get("SERVER_SCOPE") or {}
if attrs.get("pre", {}).get("value") != "seed":
    problems.append("attributes roundtrip lost: %r" % attrs)
if doc.get("schema") != "thingsflow.twin-state.v1":
    problems.append("schema not carried: %r" % doc.get("schema"))
if doc.get("tenantId") != "diag-tenant" or doc.get("entityId") != "diag-device":
    problems.append("identity not carried: %r/%r" % (doc.get("tenantId"), doc.get("entityId")))
# updatedTs
want_upd = ts_base + 9000 + (exp_keys - 1)
if doc.get("updatedTs") != want_upd:
    problems.append("updatedTs %r != %d" % (doc.get("updatedTs"), want_upd))
print("KEYS_EXPECTED=%d" % exp_keys)
print("KEYS_PRESENT=%d" % present)
print("KEYS_LOST=%d" % (exp_keys - present))
print("LWW_FINAL=%s" % ("PASS" if not any(p.startswith("k") or p.startswith("missing") for p in problems) else "FAIL"))
print("ROUNDTRIP=%s" % ("PASS" if not any("roundtrip" in p or "schema" in p or "identity" in p for p in problems) else "FAIL"))
print("UPDATEDTS=%s" % ("PASS" if "updatedTs" not in str(problems) else "FAIL"))
print("PROBLEMS=%s" % ("; ".join(problems) if problems else "none"))
print("VERDICT=%s" % ("PASS" if not problems else "FAIL"))
sys.exit(0)
PYEOF
)"

printf '%s\n' "$VERIFY_OUT" | grep -E '^(KEYS_|LWW_FINAL|ROUNDTRIP|UPDATEDTS|VERDICT|PROBLEMS)'
eval "$(printf '%s\n' "$VERIFY_OUT" | grep -E '^(KEYS_PRESENT|KEYS_LOST|LWW_FINAL|ROUNDTRIP|UPDATEDTS)=' )"

# ---------------------------------------------------------------------------
# Deterministic stale-drop check: write ts=1 for an existing key AFTER drain
# ---------------------------------------------------------------------------
log "-- stale-drop check: publishing ts=1 for k0 (should be dropped, k0 stays v0-9)"
python3 - > "/tmp/${MERGE_CM}-stale.txt" <<'PYEOF'
import json
print(json.dumps({"tenantId": "diag-tenant", "deviceId": "diag-device", "ts": 1,
                  "values": {"k0": {"ts": 1, "value": "stale"}}}, separators=(",", ":")))
PYEOF
if ! publish_payloads "/tmp/${MERGE_CM}-stale.txt"; then
  log "!! stale publish pod failed"
fi

for attempt in $(seq 1 20); do
  INFO="$(kc run "diag-merge-info2-$RANDOM" --rm -i --restart=Never --image="$NATS_IMAGE" --command -- \
    sh -c "nats --server '$NATS_URL' consumer info '$MERGE_STREAM' '$MERGE_DURABLE' 2>&1" 2>/dev/null)"
  ACK_FLOOR="$(printf '%s\n' "$INFO" | sed -n 's/.*Acknowledgment Floor: Consumer sequence: \([0-9]*\).*/\1/p' | head -1)"
  if [[ "${ACK_FLOOR:-0}" -eq $((MSG_COUNT + 1)) ]]; then break; fi
  sleep 2
done

DOC_AFTER="$(kc run "diag-merge-read2-$RANDOM" --rm -i --restart=Never --image="$NATS_IMAGE" --command -- \
  sh -c "nats --server '$NATS_URL' kv get '$NATS_KV_BUCKET' '$DIAG_KEY' --raw 2>&1" 2>/dev/null)"
DOC_AFTER="$(printf '%s\n' "$DOC_AFTER" | grep -o '^{.*}' | head -1)"
if printf '%s' "$DOC_AFTER" | python3 -c 'import json,sys; d=json.load(sys.stdin); v=d.get("telemetry",{}).get("k0",{}); sys.exit(0 if v.get("value")=="v0-9" else 1)' 2>/dev/null; then
  STALE_DROP="PASS"
else
  STALE_DROP="FAIL"
  log "!! stale write landed or k0 wrong after stale check:"
  printf '%s' "$DOC_AFTER" | head -c 600 >&2
  echo "" >&2
fi

# ---------------------------------------------------------------------------
# Verdict
# ---------------------------------------------------------------------------
if [[ "$drain_ok" == "1" ]]; then
  MERGE_WRITER="FEASIBLE"
else
  MERGE_WRITER="INFEASIBLE"
  echo "MERGE_WRITER_SINGLE=INFEASIBLE"
  echo "MISSING_PRIMITIVE=consumer-did-not-drain (see logs)"
  exit 1
fi
echo "MERGE_WRITER_SINGLE=${MERGE_WRITER}"

RACE_SAFE="PASS"
[[ "$KEYS_LOST" == "0" && "$LWW_FINAL" == "PASS" ]] && RACE_SAFE="PASS" || RACE_SAFE="FAIL"

echo "RACE_SAFE=${RACE_SAFE}"
echo "STALE_DROP=${STALE_DROP}"

if [[ "$RACE_SAFE" == "PASS" && "$STALE_DROP" == "PASS" && "$ROUNDTRIP" == "PASS" && "$UPDATEDTS" == "PASS" ]]; then
  echo "-- ALL CHECKS PASS: single-writer (serialized consumer) merge is config-only buildable and race-safe without CAS" >&2
  exit 0
else
  echo "-- CHECKS FAILED: see RACE_SAFE/LWW_FINAL/STALE_DROP/ROUNDTRIP/UPDATEDTS lines above" >&2
  exit 1
fi
