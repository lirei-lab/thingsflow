#!/usr/bin/env bash
# Orchestrates all three Phase 1 discriminating experiments (drop-output,
# jetstream-output, pre-split) plus the $KV. publish-semantics check, end-to-end
# against the live test cluster (kubectl --context=microk8s -n thingsflow-fresh),
# and prints the measured results run-localization.sh's caller uses to write
# benchmarks/FINDING-twin-state-localization.md.
#
# NEVER touches the production Helm release (release name "thingsflow", namespace
# "thingsflow"). Every resource this script creates lives in namespace
# thingsflow-fresh, is labelled app=twin-state-diag, and is torn down in a
# `trap ... EXIT` so a failed run never leaves orphaned cluster state.
#
# Usage:
#   run-localization.sh --dry-run          (default) print the plan, touch nothing
#   run-localization.sh --live             execute against the live test cluster
#                                           (interactive confirmation required)
#
# Env overrides (all optional):
#   NAMESPACE          (default thingsflow-fresh)
#   KUBECTL_CONTEXT    (default microk8s)
#   NATS_URL           (default nats://thingsflow-nats:4222)
#   BENTO_IMAGE        (default ghcr.io/warpstreamlabs/bento:1.8.1, matches
#                       k8s/helm/thingsflow/values.yaml images.bento)
#   LOAD_RATE          offered msg/s for the drop-output / jetstream-output HTTP
#                       load (default 4000 — near the ~3,900 msg/s ceiling from
#                       benchmarks/FINDING-twin-state.md)
#   LOAD_DURATION      seconds for each loadgen2 run (default 60)
#   LOAD_DEVICES       device count for loadgen2 (default 500)
#   LOAD_HTTP_CONNECTIONS  loadgen2 --http-connections: total keep-alive HTTP
#                       connections in the client's raw-engine pool (default
#                       1536). HISTORY (2026-08-12): the loadgen2 default is
#                       512 (128/worker * 4 workers), which the
#                       2026-08-11T205044Z run showed was too small — 87% of
#                       scheduled sends were rejected client-side as
#                       "pool_full" (VERDICT: CLIENT WAS THE BOTTLENECK).
#                       A first fix attempt tried 8000/8192 by Little's-Law
#                       estimate alone and made things WORSE: at 8000
#                       connections the 2026-08-12T130911Z-corrected run saw
#                       accepted throughput COLLAPSE to ~15 msg/s with new
#                       failure modes (conn_lost, drain_incomplete) never seen
#                       at smaller pools — this cluster's single node cannot
#                       sustain that many concurrent connections against this
#                       target. A live sweep (512/768/1536/3072 connections,
#                       standalone probes, not part of the committed variant
#                       results) found accepted throughput plateaus at
#                       ~450-500 msg/s across THAT WHOLE RANGE regardless of
#                       pool size — latency scales with pool size (900ms at
#                       512 conns -> 4.6s at 3072) while accepted stays flat,
#                       and real HTTP 503/504 first appear at 3072. 1536 is
#                       the sweep's best point before diminishing returns and
#                       before the 8000-conn collapse. NOTE (review cycle 1):
#                       this sweep was never saved to a retained evidence file
#                       and directly contradicts the committed run's own
#                       loadgen2 VERDICT of CLIENT_WAS_THE_BOTTLENECK — treat
#                       "~500 msg/s is a platform ceiling, not a generator
#                       artifact" as an UNVERIFIED HYPOTHESIS pending a re-run
#                       that saves its raw output, not a settled fact. See
#                       benchmarks/FINDING-twin-state-localization.md's
#                       "HTTP ingest path congestion" section.
#   LOAD_MAX_INFLIGHT  loadgen2 --max-inflight: total outstanding requests
#                       across all workers (default 2048, comfortably above
#                       LOAD_HTTP_CONNECTIONS so the HTTP connection pool,
#                       not this cap, is the deliberate ceiling being raised)
#   PRESPLIT_COUNT     total messages for the pre-split synthetic publisher
#                       (default 240000 => ~4,000 msg/s * 60s)
#   PRESPLIT_PUBLISHERS parallel nats-box connections for publish-presplit.sh
#                       (default 2)
set -uo pipefail  # deliberately NOT -e: teardown must run on any failure path.

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$HERE/../.." && pwd)"

NAMESPACE="${NAMESPACE:-thingsflow-fresh}"
KUBECTL_CONTEXT="${KUBECTL_CONTEXT:-microk8s}"
NATS_URL="${NATS_URL:-nats://thingsflow-nats:4222}"
BENTO_IMAGE="${BENTO_IMAGE:-ghcr.io/warpstreamlabs/bento:1.8.1}"
LOAD_RATE="${LOAD_RATE:-4000}"
LOAD_DURATION="${LOAD_DURATION:-60}"
LOAD_DEVICES="${LOAD_DEVICES:-500}"
LOAD_HTTP_CONNECTIONS="${LOAD_HTTP_CONNECTIONS:-1536}"
LOAD_MAX_INFLIGHT="${LOAD_MAX_INFLIGHT:-2048}"
PRESPLIT_COUNT="${PRESPLIT_COUNT:-240000}"
PRESPLIT_PUBLISHERS="${PRESPLIT_PUBLISHERS:-2}"

RESULTS_DIR="${RESULTS_DIR:-$HERE/.results}"
RUN_STAMP="$(date -u +%Y%m%dT%H%M%SZ)"

kc() {
  kubectl --context="$KUBECTL_CONTEXT" -n "$NAMESPACE" "$@"
}

log() {
  echo "[$(date -u +%H:%M:%S)] $*" >&2
}

# ---------------------------------------------------------------------------
# Variant registry: name -> filter subject. drop-output and jetstream-output
# share the PRODUCTION filter (tf.ingest.>) so real ingest traffic (driven via
# loadgen2, same as fair-ramp.sh) reaches them exactly as it reaches the
# production consumer. pre-split uses its own diagnostic-only subtree, fed by
# publish-presplit.sh.
# ---------------------------------------------------------------------------
variant_filter_subject() {
  case "$1" in
    drop-output|jetstream-output) echo "tf.ingest.>" ;;
    pre-split) echo "tf.ingest.http.raw.diag-presplit.>" ;;
    *) echo "ERROR: unknown variant $1" >&2; return 1 ;;
  esac
}

# ---------------------------------------------------------------------------
# Resolve NATS_KV_BUCKET from values.yaml (nats.twinKv.bucket), and the
# production latestKv resources block (natsDataPlane.latestKv.resources) —
# per this plan's Task 3 instruction: the diagnostic Deployment must mirror
# templates/nats-data-plane-bento.yaml's resources verbatim from values.yaml,
# not a guessed/lower default ("unequal budgets" would invalidate the
# comparison).
# ---------------------------------------------------------------------------
resolve_values() {
  python3 - "$REPO_ROOT/k8s/helm/thingsflow/values.yaml" <<'PY'
import sys, json
try:
    import yaml
except ImportError:
    print("ERROR: PyYAML not available on this host", file=sys.stderr)
    sys.exit(1)
with open(sys.argv[1]) as f:
    v = yaml.safe_load(f)
bucket = (v.get("nats", {}) or {}).get("twinKv", {}).get("bucket", "twin_state")
res = (((v.get("natsDataPlane", {}) or {}).get("latestKv", {}) or {}).get("resources")) or {
    "limits": {"cpu": "750m", "memory": "512Mi"},
    "requests": {"cpu": "100m", "memory": "128Mi"},
}
print(json.dumps({"bucket": bucket, "resources": res}))
PY
}

# ---------------------------------------------------------------------------
# Finding 6 (Phase 1 review cycle 1): resolve whether the actual deployed
# release has NATS auth enabled, and if so, its secret name/keys — mirrors
# _helpers.tpl's thingsflow.natsAuthEnabled / thingsflow.natsAuthSecretName /
# thingsflow.natsAuthUserKey / thingsflow.natsAuthPasswordKey. This test
# cluster today has auth disabled (confirmed live), but values-cluster.yaml
# enables it for real cluster deploys — deploy_variant() must not silently
# ship an unauthenticated NATS_URL if this harness is ever pointed there.
# ---------------------------------------------------------------------------
resolve_nats_auth() {
  local release
  release="$(helm list -n "$NAMESPACE" -o json 2>/dev/null | python3 -c 'import json,sys
items=json.load(sys.stdin)
print(items[0]["name"] if items else "")' 2>/dev/null)"
  [[ -z "$release" ]] && release="thingsflow"
  local merged
  merged="$(helm get values "$release" -n "$NAMESPACE" -a -o json 2>/dev/null)" || merged="{}"
  python3 - "$merged" "$release" <<'PY'
import json, sys
try:
    v = json.loads(sys.argv[1]) if sys.argv[1].strip() else {}
except Exception:
    v = {}
nats = v.get("nats", {}) or {}
auth = nats.get("auth", {}) or {}
existing = auth.get("existingSecret", {}) or {}
enabled = bool(nats.get("enabled", True)) and bool(auth.get("enabled", False))
print(json.dumps({
    "enabled": enabled,
    "secret": existing.get("name") or f"{sys.argv[2]}-nats-auth",
    "userKey": existing.get("userKey") or "username",
    "passwordKey": existing.get("passwordKey") or "password",
}))
PY
}

# ---------------------------------------------------------------------------
# Renders and applies the ConfigMap + throwaway Deployment for one variant.
# Mirrors k8s/helm/thingsflow/templates/nats-data-plane-bento.yaml's shape
# (initContainer wait-for-nats, env block, readiness/liveness probes,
# resources) but under distinct twin-state-diag-<variant> names — NEVER an
# `helm upgrade` of the production release.
# ---------------------------------------------------------------------------
deploy_variant() {
  local variant="$1" bucket="$2" limits_cpu="$3" limits_mem="$4" requests_cpu="$5" requests_mem="$6"
  local auth_enabled="${7:-false}" auth_secret="${8:-}" auth_user_key="${9:-username}" auth_password_key="${10:-password}"
  local filter; filter="$(variant_filter_subject "$variant")"
  local name="twin-state-diag-${variant}"
  local durable="thingsflow-latest-kv-diag-${variant}"
  local queue="thingsflow-nats-latest-kv-diag-${variant}"
  local config_file="$HERE/configs/${variant}.yaml"
  local metrics_port=$(( 4297 + $(echo "$variant" | cksum | cut -d' ' -f1) % 100 ))
  # Finding 6 (review cycle 1): this Deployment's URL and auth env block must
  # match the production pattern (thingsflow.natsAuthEnv / thingsflow.natsURL
  # in _helpers.tpl) — omitting it works today only because nats.auth.enabled
  # is false on this test cluster; values-cluster.yaml sets it true for real
  # cluster deploys, where the old unauthenticated URL would silently
  # ROLLOUT_FAILED with no obvious cause.
  local effective_nats_url="$NATS_URL"
  if [[ "$auth_enabled" == "true" ]]; then
    effective_nats_url="nats://\$(NATS_USER):\$(NATS_PASSWORD)@thingsflow-nats:4222"
  fi

  [[ -f "$config_file" ]] || { log "ERROR: missing config file $config_file"; return 1; }

  log "-- deploying diagnostic variant: $name (filter=$filter durable=$durable)"

  # ConfigMap holding the exact variant YAML (indented under config.yaml, same
  # shape as the production ConfigMap in nats-data-plane-bento.yaml).
  {
    echo "apiVersion: v1"
    echo "kind: ConfigMap"
    echo "metadata:"
    echo "  name: ${name}-config"
    echo "  labels:"
    echo "    app: twin-state-diag"
    echo "    variant: ${variant}"
    echo "data:"
    echo "  config.yaml: |"
    sed 's/^/    /' "$config_file"
  } > "/tmp/${name}-configmap.yaml"

  local auth_env_block=""
  if [[ "$auth_enabled" == "true" ]]; then
    auth_env_block="        - name: NATS_USER
          valueFrom:
            secretKeyRef:
              name: ${auth_secret}
              key: ${auth_user_key}
        - name: NATS_PASSWORD
          valueFrom:
            secretKeyRef:
              name: ${auth_secret}
              key: ${auth_password_key}
"
  fi

  cat > "/tmp/${name}-deployment.yaml" <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ${name}
  labels:
    app: twin-state-diag
    variant: ${variant}
spec:
  replicas: 1
  selector:
    matchLabels:
      app: twin-state-diag
      variant: ${variant}
  template:
    metadata:
      labels:
        app: twin-state-diag
        variant: ${variant}
    spec:
      initContainers:
      - name: wait-for-nats
        image: busybox:1.36
        command: ["sh", "-c", "until nc -z -w 2 thingsflow-nats 4222; do sleep 2; done"]
      containers:
      - name: bento
        image: "${BENTO_IMAGE}"
        imagePullPolicy: IfNotPresent
        command: ["/bento", "-c", "/etc/bento/config.yaml"]
        env:
${auth_env_block}        - name: LATEST_KV_MAX_IN_FLIGHT
          value: "1024"
        - name: NATS_URL
          value: "${effective_nats_url}"
        - name: NATS_QUEUE_GROUP
          value: "${queue}"
        - name: NATS_STREAM
          value: "TF_RAW"
        - name: NATS_DURABLE
          value: "${durable}"
        - name: NATS_FILTER_SUBJECT
          value: "${filter}"
        - name: NATS_KV_BUCKET
          value: "${bucket}"
        - name: DEFAULT_TENANT_ID
          value: "aaaaaaaa-1dd2-11b2-8080-808080808080"
        - name: METRICS_PORT
          value: "${metrics_port}"
        ports:
        - name: http
          containerPort: ${metrics_port}
        readinessProbe:
          httpGet:
            path: /ready
            port: http
          initialDelaySeconds: 10
          periodSeconds: 5
        livenessProbe:
          httpGet:
            path: /ping
            port: http
          initialDelaySeconds: 10
          periodSeconds: 15
        resources:
          limits:
            cpu: "${limits_cpu}"
            memory: "${limits_mem}"
          requests:
            cpu: "${requests_cpu}"
            memory: "${requests_mem}"
        volumeMounts:
        - name: config
          mountPath: /etc/bento
          readOnly: true
      volumes:
      - name: config
        configMap:
          name: ${name}-config
EOF

  kc apply -f "/tmp/${name}-configmap.yaml" || return 1
  kc apply -f "/tmp/${name}-deployment.yaml" || return 1
}

wait_variant_ready() {
  local variant="$1" name="twin-state-diag-${variant}"
  log "-- waiting for $name rollout"
  kc rollout status "deploy/${name}" --timeout=180s
}

teardown_variant() {
  local variant="$1" name="twin-state-diag-${variant}"
  log "-- tearing down diagnostic variant: $name"
  kc delete deploy "$name" --ignore-not-found >/dev/null 2>&1 || true
  kc delete configmap "${name}-config" --ignore-not-found >/dev/null 2>&1 || true
  # Finding 2 (review cycle 1): teardown-diag-consumer.sh now distinguishes a
  # genuine "not found" from an AMBIGUOUS_STATE failure and exits non-zero on
  # the latter — surface that loudly instead of swallowing it, so an operator
  # notices a possible orphaned consumer instead of trusting a silent no-op.
  if ! "$HERE/teardown-diag-consumer.sh" "$variant"; then
    log "!! WARNING: teardown-diag-consumer.sh reported a FAILED/AMBIGUOUS state for thingsflow-latest-kv-diag-${variant}. Verify manually: nats consumer info TF_RAW thingsflow-latest-kv-diag-${variant}"
  fi
  rm -f "/tmp/${name}-configmap.yaml" "/tmp/${name}-deployment.yaml"
}

# ---------------------------------------------------------------------------
# Production consumer lag caveat: reads thingsflow-latest-kv-durable's
# num_pending during/after the window, so the finding can note whether the
# PRODUCTION consumer was also under contention while a diagnostic variant ran
# (both share the same NATS server and, for drop-output/jetstream-output, the
# SAME underlying ingest traffic — JetStream gives each consumer its own full
# copy, but the two Bento pods still share node/NATS resources).
#
# FIX (2026-08-12, resuming interrupted run): two bugs made every prior
# capture of this function garbage. (1) `python3` is NOT installed in
# natsio/nats-box:0.16.0 (confirmed live: `which python3` -> not found), so the
# json.load pipeline always failed and silently fell back to `echo -1` — `jq`
# IS present in that image and is used instead. (2) `kubectl run --rm -i`
# prints its own "pod ... deleted from <namespace> namespace" confirmation to
# STDOUT (not stderr) on this kubectl version, so `2>/dev/null | tail -1` was
# capturing kubectl's own status line, not the pod's last output line
# (confirmed live: CAPTURED was literally the `pod "..." deleted...` string).
# Filtering to lines that are ONLY a bare (possibly negative) integer before
# taking the last one discards that noise regardless of its stream.
# ---------------------------------------------------------------------------
consumer_pending() {
  local consumer="$1"
  local podname="diag-info-$RANDOM"
  local result
  result="$(kc run "$podname" --rm -i --restart=Never --image=natsio/nats-box:0.16.0 --command -- \
    sh -c "nats --server '$NATS_URL' consumer info TF_RAW '$consumer' --json 2>/dev/null | jq -r '.num_pending // -1' 2>/dev/null || echo -1" \
    2>/dev/null | grep -E '^-?[0-9]+$' | tail -1)"
  [[ -z "$result" ]] && result="-1"
  echo "$result"
}

# ---------------------------------------------------------------------------
# Finding 3 (Phase 1 review cycle 1): cluster-wide data-plane health check.
# The original run only ever checked the diagnostic consumer's own pending
# count and the production thingsflow-latest-kv-durable consumer — never the
# other three production data-plane consumers. This is exactly why a real
# incident during this phase's own execution (NATS OOM-restart -> all 4
# downstream Bento consumers silently disconnected, "Failed to read message:
# nats: connection closed" looping with zero reconnection attempts, ~10-15
# min data-plane ingest halt) went undetected by the executing agent and had
# to be caught by the orchestrator's independent post-hoc verification. Any
# measurement taken while ingest was actually halted platform-wide is not
# trustworthy, regardless of what the diagnostic's own consumer reports.
# ---------------------------------------------------------------------------
PROD_DURABLES=(thingsflow-latest-kv-durable thingsflow-greptimedb-durable thingsflow-entity-greptimedb-durable thingsflow-alarms-durable)

check_dataplane_health() {
  local bad=() d info
  for d in "${PROD_DURABLES[@]}"; do
    info="$(kc run "diag-health-$RANDOM" --rm -i --restart=Never --image=natsio/nats-box:0.16.0 --command -- \
      sh -c "nats --server '$NATS_URL' consumer info TF_RAW '$d' 2>&1" 2>/dev/null)"
    if [[ -z "$info" ]]; then
      bad+=("$d: EMPTY_RESPONSE (pod failed to run or NATS unreachable)")
      continue
    fi
    if printf '%s\n' "$info" | grep -qiE 'not found|nats: error|deadline exceeded|connection refused|i/o timeout|no responders'; then
      bad+=("$d: ERROR -- $(printf '%s\n' "$info" | grep -iE 'not found|nats: error|deadline exceeded|connection refused|i/o timeout|no responders' | head -1)")
      continue
    fi
    # Review cycle 2 fix: the old check greped the "Active Interest" LABEL line
    # for the substring "Active" — but the unhealthy value's own label text is
    # "Active Interest: No interest", which also contains "Active" (from the
    # label), so the old check matched regardless of the actual value and could
    # never detect the unhealthy state. Match the VALUE explicitly instead, and
    # fail safe (treat as bad) if the line is missing entirely (unexpected
    # output shape) rather than silently passing.
    local interest_line
    interest_line="$(printf '%s\n' "$info" | grep -i 'Active Interest' | head -1)"
    if [[ -z "$interest_line" ]]; then
      bad+=("$d: no 'Active Interest' line found in consumer info output — unexpected format, treating as unhealthy")
    elif printf '%s\n' "$interest_line" | grep -qi 'No interest'; then
      bad+=("$d: $interest_line")
    fi
  done
  if [[ "${#bad[@]}" -gt 0 ]]; then
    log "!! DATA-PLANE HEALTH CHECK FAILED:"
    local b
    for b in "${bad[@]}"; do log "     $b"; done
    return 1
  fi
  log "-- data-plane health check OK: all ${#PROD_DURABLES[@]} production durables Active"
  return 0
}

pod_cpu_snapshot() {
  local variant="$1"
  kc top pod -l "variant=${variant}" --no-headers 2>/dev/null || echo "N/A (metrics-server unavailable or pod not yet reporting)"
}

# ---------------------------------------------------------------------------
# FIX (2026-08-12): compute and print the PROCESSED_RATE line the interrupted
# prior run never surfaced. Same formula as benchmarks/FINDING-twin-state.md:
# `(accepted - pending at close) / duration`. "accepted" and "duration" come
# from the load driver's own report (loadgen2's delivery.accepted/wall_seconds
# for drop-output/jetstream-output; publish-presplit.sh's PUBLISH_RESULT
# count/elapsed_s for pre-split, since that variant bypasses HTTP ingest and
# publishes straight to NATS). "pending at close" is the diagnostic consumer's
# own num_pending, read via the now-fixed consumer_pending(). With
# bootstrap-diag-consumer.sh now using --deliver new (see that script's header
# comment), pending_before should read 0 for a freshly created consumer; a
# non-zero value is surfaced as an explicit WARNING rather than silently
# folded into the math, since it would mean the fix did not fully eliminate
# backlog contamination for this run.
# ---------------------------------------------------------------------------
compute_processed_rate() {
  local variant="$1" load_out="$2" pending_before="$3" pending_after="$4"
  local accepted="" duration=""

  if [[ "$variant" == "pre-split" ]]; then
    local line
    line="$(grep '^PUBLISH_RESULT' "$load_out" 2>/dev/null | tail -1)"
    accepted="$(echo "$line" | sed -n 's/.*count=\([0-9][0-9]*\).*/\1/p')"
    duration="$(echo "$line" | sed -n 's/.*elapsed_s=\([0-9][0-9]*\).*/\1/p')"
  elif [[ -f "$load_out" ]]; then
    accepted="$(python3 -c "import json,sys
try:
    print(json.load(open('$load_out'))['delivery']['accepted'])
except Exception:
    pass" 2>/dev/null)"
    duration="$(python3 -c "import json,sys
try:
    print(json.load(open('$load_out'))['wall_seconds'])
except Exception:
    pass" 2>/dev/null)"
  fi

  if [[ -z "$accepted" || -z "$duration" ]]; then
    echo "PROCESSED_RATE=UNAVAILABLE (could not parse accepted/duration from $load_out)"
    return 1
  fi
  if [[ ! "$pending_after" =~ ^-?[0-9]+$ ]] || (( pending_after < 0 )); then
    echo "PROCESSED_RATE=UNAVAILABLE (pending_after=[$pending_after] not a valid non-negative integer — consumer info read failed, see consumer_pending() output above)"
    return 1
  fi
  if [[ "$pending_before" =~ ^[0-9]+$ ]] && (( pending_before != 0 )); then
    echo "WARNING: diag_consumer_pending_before=$pending_before is non-zero even with --deliver new — possible residual backlog or concurrent traffic on this filter subject; PROCESSED_RATE below may be understated"
  fi

  python3 -c "
accepted = $accepted
pending_after = $pending_after
duration = $duration
rate = (accepted - pending_after) / duration if duration else 0
print(f'PROCESSED_RATE={rate:.1f} msg/s (accepted={accepted} pending_after={pending_after} duration_s={duration})')
"
}

# ---------------------------------------------------------------------------
# Load drivers.
# ---------------------------------------------------------------------------
drive_load_http() {
  local variant="$1" out_file="$2"
  log "-- driving HTTP load via loadgen2: rate=${LOAD_RATE} duration=${LOAD_DURATION}s devices=${LOAD_DEVICES} http-connections=${LOAD_HTTP_CONNECTIONS} max-inflight=${LOAD_MAX_INFLIGHT}"
  local api_ip ingest_ip mqtt_ip
  api_ip="$(kc get svc thingsflow-flow-core -o jsonpath='{.spec.clusterIP}')"
  ingest_ip="$(kc get svc thingsflow-http-ingest -o jsonpath='{.spec.clusterIP}')"
  mqtt_ip="$(kc get svc thingsflow-rmqtt-edge -o jsonpath='{.spec.clusterIP}')"
  # FIX (2026-08-12): explicit --http-connections/--max-inflight, raised above
  # loadgen2's default (512) to the empirically swept value — see
  # LOAD_HTTP_CONNECTIONS doc comment at the top of this file for the full
  # sweep (512/768/1536/3072/8000) and why accepted throughput plateaus
  # around ~450-500 msg/s across that whole range on this cluster today.
  "$REPO_ROOT/benchmarks/scripts/loadgen2/run.sh" run \
    --target thingsflow --protocol http --devices "$LOAD_DEVICES" \
    --rate "$LOAD_RATE" --duration "$LOAD_DURATION" --qos 1 --ramp 10 \
    --max-workers 4 --http-connections "$LOAD_HTTP_CONNECTIONS" --max-inflight "$LOAD_MAX_INFLIGHT" \
    --api-base "http://${api_ip}:8080" --ingest-base "http://${ingest_ip}:8081" \
    --mqtt-host "${mqtt_ip}" --mqtt-port 1883 \
    --run-id "diag-${variant}-${RUN_STAMP}" --out "$out_file" --cleanup \
    --settle-seconds 10 2>&1 | tail -20
}

drive_load_presplit() {
  local out_file="$1"
  log "-- driving pre-split synthetic load via publish-presplit.sh: count=${PRESPLIT_COUNT} publishers=${PRESPLIT_PUBLISHERS}"
  NAMESPACE="$NAMESPACE" KUBECTL_CONTEXT="$KUBECTL_CONTEXT" NATS_URL="$NATS_URL" \
    COUNT="$PRESPLIT_COUNT" PUBLISHERS="$PRESPLIT_PUBLISHERS" \
    "$HERE/publish-presplit.sh" > "$out_file" 2>&1
  cat "$out_file" >&2
}

# ---------------------------------------------------------------------------
# Runs one full experiment: deploy -> bootstrap consumer -> wait ready ->
# drive load -> capture results -> teardown (always, via trap).
# ---------------------------------------------------------------------------
# Top-level safety net: if this script is killed mid-variant (SIGINT/SIGTERM,
# not just a normal function return), CURRENT_VARIANT still gets torn down.
# The per-variant RETURN trap in run_variant is the primary mechanism; this is
# defense in depth for the case a plain `return` never executes.
CURRENT_VARIANT=""
top_level_cleanup() {
  if [[ -n "$CURRENT_VARIANT" ]]; then
    log "-- top-level safety-net teardown firing for CURRENT_VARIANT=$CURRENT_VARIANT"
    teardown_variant "$CURRENT_VARIANT"
    CURRENT_VARIANT=""
  fi
}
trap top_level_cleanup EXIT INT TERM

run_variant() {
  local variant="$1" bucket="$2" limits_cpu="$3" limits_mem="$4" requests_cpu="$5" requests_mem="$6"
  local auth_enabled="${7:-false}" auth_secret="${8:-}" auth_user_key="${9:-username}" auth_password_key="${10:-password}"
  local filter; filter="$(variant_filter_subject "$variant")"
  local result_file="$RESULTS_DIR/${variant}-${RUN_STAMP}.txt"
  mkdir -p "$RESULTS_DIR"
  CURRENT_VARIANT="$variant"

  local torn_down=0
  cleanup_this_variant() {
    if [[ "$torn_down" -eq 0 ]]; then
      teardown_variant "$variant"
      torn_down=1
      CURRENT_VARIANT=""
    fi
  }
  trap cleanup_this_variant RETURN

  echo "===== VARIANT: $variant =====" | tee "$result_file" >&2

  if ! "$HERE/bootstrap-diag-consumer.sh" "$variant" "$filter" >>"$result_file" 2>&1; then
    echo "BOOTSTRAP_FAILED" | tee -a "$result_file" >&2
    return 1
  fi

  if ! deploy_variant "$variant" "$bucket" "$limits_cpu" "$limits_mem" "$requests_cpu" "$requests_mem" \
      "$auth_enabled" "$auth_secret" "$auth_user_key" "$auth_password_key" >>"$result_file" 2>&1; then
    echo "DEPLOY_FAILED" | tee -a "$result_file" >&2
    return 1
  fi

  if ! wait_variant_ready "$variant" >>"$result_file" 2>&1; then
    echo "ROLLOUT_FAILED" | tee -a "$result_file" >&2
    cat "$result_file" >&2
    return 1
  fi

  local pending_before; pending_before="$(consumer_pending "thingsflow-latest-kv-diag-${variant}")"
  local prod_pending_before; prod_pending_before="$(consumer_pending "thingsflow-latest-kv-durable")"
  {
    echo "diag_consumer_pending_before=${pending_before}"
    echo "production_consumer_pending_before=${prod_pending_before}"
  } >> "$result_file"

  local load_out="$RESULTS_DIR/${variant}-${RUN_STAMP}-load.json"
  if [[ "$variant" == "pre-split" ]]; then
    drive_load_presplit "$load_out"
  else
    drive_load_http "$variant" "$load_out" >> "$result_file" 2>&1
  fi

  # Finding 3 (review cycle 1): check cluster-wide data-plane health
  # immediately after driving load, before trusting this variant's numbers.
  # A load push that (like the incident during this phase's own execution)
  # knocks the shared NATS pod over invalidates whatever this variant's own
  # consumer reports — abort the variant rather than reporting numbers
  # measured while ingest was actually halted platform-wide.
  if ! check_dataplane_health; then
    echo "DATAPLANE_UNHEALTHY_AFTER_LOAD -- aborting variant $variant, results NOT valid" | tee -a "$result_file" >&2
    return 1
  fi

  log "-- settling ${LOAD_DURATION}s worth of in-flight processing before reading pending (settle-seconds pattern from fair-ramp.sh)"
  sleep 15

  local pending_after; pending_after="$(consumer_pending "thingsflow-latest-kv-diag-${variant}")"
  local prod_pending_after; prod_pending_after="$(consumer_pending "thingsflow-latest-kv-durable")"
  local cpu_snapshot; cpu_snapshot="$(pod_cpu_snapshot "$variant")"
  local processed_rate_line; processed_rate_line="$(compute_processed_rate "$variant" "$load_out" "$pending_before" "$pending_after")"

  {
    echo "diag_consumer_pending_after=${pending_after}"
    echo "production_consumer_pending_after=${prod_pending_after}"
    echo "pod_cpu_snapshot: ${cpu_snapshot}"
    echo "load_output_file=${load_out}"
    echo "${processed_rate_line}"
  } >> "$result_file"

  echo "===== END VARIANT: $variant =====" >> "$result_file"
  cat "$result_file" >&2
  echo "$result_file"
}

# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
main() {
  local mode="dry-run"
  for arg in "$@"; do
    case "$arg" in
      --live) mode="live" ;;
      --dry-run) mode="dry-run" ;;
      *) echo "unknown argument: $arg" >&2; exit 2 ;;
    esac
  done

  if [[ "$mode" == "dry-run" ]]; then
    cat >&2 <<EOF
DRY RUN — no cluster state will be touched. Planned steps for --live:
  1. kubectl --context=$KUBECTL_CONTEXT cluster-info reachability check
  2. Resolve NATS_KV_BUCKET from k8s/helm/thingsflow/values.yaml (nats.twinKv.bucket)
  3. For each variant [drop-output, jetstream-output, pre-split], SEQUENTIALLY:
       a. bootstrap-diag-consumer.sh <variant> <filter-subject>
       b. deploy a throwaway ConfigMap+Deployment (twin-state-diag-<variant>) in
          namespace $NAMESPACE, resources mirrored from values.yaml
          natsDataPlane.latestKv.resources
       c. drive load: loadgen2 HTTP at ${LOAD_RATE} msg/s for ${LOAD_DURATION}s,
          http-connections=${LOAD_HTTP_CONNECTIONS} max-inflight=${LOAD_MAX_INFLIGHT}
          (drop-output/jetstream-output) or publish-presplit.sh at
          ${PRESPLIT_COUNT} messages (pre-split)
       d. capture diag + production consumer pending (via jq, not python3 —
          not present in nats-box), compute PROCESSED_RATE, pod CPU
       e. teardown-diag-consumer.sh + delete the throwaway Deployment/ConfigMap
  4. Run verify-kv-publish-semantics.sh
  5. Print a combined results summary (the caller writes
     benchmarks/FINDING-twin-state-localization.md from these captured numbers)

Re-run with --live to execute against the real cluster ($KUBECTL_CONTEXT / $NAMESPACE).
This deploys throwaway diagnostic Deployments, creates/deletes ephemeral JetStream
consumers, and generates synthetic + real ingest load. It NEVER touches the
production Helm release or the "thingsflow" namespace.
EOF
    exit 0
  fi

  # --live mode
  cat >&2 <<EOF
ABOUT TO RUN LIVE against kubectl --context=$KUBECTL_CONTEXT -n $NAMESPACE:
  - deploy 3 throwaway diagnostic Bento Deployments (twin-state-diag-*), one at a time
  - create/delete ephemeral JetStream consumers on stream TF_RAW
    (thingsflow-latest-kv-diag-drop-output / -jetstream-output / -pre-split)
  - drive real HTTP ingest load (loadgen2, ${LOAD_RATE} msg/s, ${LOAD_DURATION}s) for
    2 of the 3 variants, and synthetic diagnostic-subject load for the 3rd
  - run the \$KV. publish-semantics check (writes + deletes a diagnostic-only key)
  - NEVER touches the production Helm release "thingsflow" or namespace "thingsflow"
EOF
  read -r -p "Type 'yes' to proceed: " confirm
  if [[ "$confirm" != "yes" ]]; then
    echo "Aborted by operator (confirmation was not 'yes')." >&2
    exit 1
  fi

  log "-- step 1: cluster reachability"
  if ! kubectl --context="$KUBECTL_CONTEXT" cluster-info >/tmp/cluster-info.out 2>&1; then
    echo "BLOCKED: cluster-info failed — $(cat /tmp/cluster-info.out)" >&2
    exit 1
  fi

  log "-- step 1b: data-plane health precondition (Finding 3, review cycle 1)"
  if ! check_dataplane_health; then
    echo "BLOCKED: production data-plane is unhealthy before this run even started — see log above. Fix cluster health before running diagnostic load against it (do not proceed, per this phase's own incident history)." >&2
    exit 1
  fi

  log "-- step 2: resolving NATS_KV_BUCKET and production resources from values.yaml"
  local resolved bucket limits_cpu limits_mem requests_cpu requests_mem
  resolved="$(resolve_values)"
  if [[ -z "$resolved" ]]; then
    echo "BLOCKED: could not resolve nats.twinKv.bucket from values.yaml" >&2
    exit 1
  fi
  bucket="$(echo "$resolved" | python3 -c 'import json,sys; print(json.load(sys.stdin)["bucket"])')"
  limits_cpu="$(echo "$resolved" | python3 -c 'import json,sys; print(json.load(sys.stdin)["resources"]["limits"]["cpu"])')"
  limits_mem="$(echo "$resolved" | python3 -c 'import json,sys; print(json.load(sys.stdin)["resources"]["limits"]["memory"])')"
  requests_cpu="$(echo "$resolved" | python3 -c 'import json,sys; print(json.load(sys.stdin)["resources"]["requests"]["cpu"])')"
  requests_mem="$(echo "$resolved" | python3 -c 'import json,sys; print(json.load(sys.stdin)["resources"]["requests"]["memory"])')"
  log "   bucket=$bucket resources.limits=${limits_cpu}/${limits_mem} resources.requests=${requests_cpu}/${requests_mem}"
  if [[ -z "$bucket" ]]; then
    echo "BLOCKED: NATS_KV_BUCKET could not be determined from values.yaml — refusing to guess a bucket name" >&2
    exit 1
  fi

  log "-- step 2b: resolving NATS auth (Finding 6, review cycle 1)"
  local auth_resolved auth_enabled auth_secret auth_user_key auth_password_key
  auth_resolved="$(resolve_nats_auth)"
  auth_enabled="$(echo "$auth_resolved" | python3 -c 'import json,sys; print(str(json.load(sys.stdin)["enabled"]).lower())')"
  auth_secret="$(echo "$auth_resolved" | python3 -c 'import json,sys; print(json.load(sys.stdin)["secret"])')"
  auth_user_key="$(echo "$auth_resolved" | python3 -c 'import json,sys; print(json.load(sys.stdin)["userKey"])')"
  auth_password_key="$(echo "$auth_resolved" | python3 -c 'import json,sys; print(json.load(sys.stdin)["passwordKey"])')"
  log "   nats.auth.enabled=$auth_enabled secret=$auth_secret"
  if [[ "$auth_enabled" == "true" ]]; then
    if ! kc get secret "$auth_secret" >/dev/null 2>&1; then
      echo "BLOCKED: nats.auth.enabled=true on this release but secret '$auth_secret' was not found in namespace $NAMESPACE — cannot deploy diagnostic pods that would fail to authenticate" >&2
      exit 1
    fi
  fi

  mkdir -p "$RESULTS_DIR"
  local all_results=()
  for variant in drop-output jetstream-output pre-split; do
    log "===================================================================="
    log "  RUNNING VARIANT: $variant"
    log "===================================================================="
    local rf
    if rf="$(run_variant "$variant" "$bucket" "$limits_cpu" "$limits_mem" "$requests_cpu" "$requests_mem" \
        "$auth_enabled" "$auth_secret" "$auth_user_key" "$auth_password_key")"; then
      all_results+=("$rf")
    else
      log "!! variant $variant FAILED — see log above; continuing to next variant (teardown already ran)"
    fi
  done

  log "-- step 4: \$KV. publish-semantics check"
  local kv_sem_out="$RESULTS_DIR/kv-semantics-${RUN_STAMP}.txt"
  NAMESPACE="$NAMESPACE" KUBECTL_CONTEXT="$KUBECTL_CONTEXT" NATS_URL="$NATS_URL" NATS_KV_BUCKET="$bucket" \
    "$HERE/verify-kv-publish-semantics.sh" > "$kv_sem_out" 2>&1
  local kv_sem_rc=$?
  cat "$kv_sem_out" >&2

  log "===================================================================="
  log "  COMBINED RESULTS SUMMARY"
  log "===================================================================="
  for rf in "${all_results[@]:-}"; do
    [[ -f "$rf" ]] && cat "$rf" >&2
  done
  log "kv_semantics_result_file=$kv_sem_out kv_semantics_exit_code=$kv_sem_rc"
  log "All result files under: $RESULTS_DIR (NOT tracked in git — raw per-run results, per benchmarks/README.md's artifact policy)"
}

main "$@"
