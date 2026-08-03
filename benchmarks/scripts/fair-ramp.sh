#!/usr/bin/env bash
# FAIR comparative ramp: no level is published without passing the gate.
#
# What it fixes relative to the previous ramp:
#
#   1. Equal budgets. Before, ThingsFlow had 29,500m of CPU limits and
#      ThingsBoard 10,000m. Now both run a profile with limits >= 3x their
#      observed peak (benchmarks/profiles/fair-*.yaml).
#   2. Throttling gate. Before, the method claimed "the limits are not
#      binding" and that was not true: tb-node reached 99.3% of its ceiling
#      and nats-alarms 90.7%. Now every level is measured with
#      throttle-gate.py and any level that throttles is marked INVALID
#      instead of being published as clean.
#   3. Comparable MQTT front end: 3 rmqtt nodes in a raft cluster against the
#      2 tb-mqtt-transport on the other side (it used to be 1 against 2).
#
# A level only counts as CLEAN if all three hold at once:
#   - zero errors and zero loss (landed == accepted),
#   - the generator was not the bottleneck (blocked_inflight low),
#   - the throttling gate passed.
# Any other combination is recorded with its reason. A level that fails
# because of the cage is NOT a data point about the platform.

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOADGEN="$HERE/loadgen2/run.sh"
GATE="$HERE/throttle-gate.py"
ISOLATE="$HERE/sut-isolate.sh"
OUTDIR="${OUTDIR:-$HERE/../results-fair}"
NODE="$(kubectl --context=microk8s get node -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}')"

DEVICES="${DEVICES:-2000}"
DURATION="${DURATION:-120}"
WARMUP="${WARMUP:-45}"
SETTLE="${SETTLE:-30}"

# Unique stamp per run. Without it the key prefix repeats across runs of the
# same level and the landed-row count ADDS UP the previous runs: a repeat of
# 33,744 rows gave landed=67,488 against expected=33,744 and would have been
# read as "data duplication" instead of what it was, cross-run contamination.
STAMP="${STAMP:-$(date -u +%m%d%H%M%S)}"

mkdir -p "$OUTDIR"

ns_for() { [[ "$1" == "thingsflow" ]] && echo "thingsflow-fresh" || echo "tb-classic"; }

# Both platforms are reached over ClusterIP, not over NodePort.
#
# Why it matters: the previous ramp reached ThingsFlow over ClusterIP
# (<cluster-ip>:1883) and ThingsBoard over NodePort (<node-ip>:30188). A
# NodePort adds an extra kube-proxy DNAT and, with externalTrafficPolicy
# Cluster, can add SNAT. It is a small difference but a systematic one, and
# always in the same direction. The generator runs on the node itself and has
# a route to the service CIDR, so ClusterIP is reachable for both.
clusterip() {  # ns svc-substring
  kubectl --context=microk8s -n "$1" get svc -o json \
    | python3 -c "
import json,sys
for s in json.load(sys.stdin)['items']:
    n=s['metadata']['name']
    if '$2' in n and not n.startswith('np-') and '-np-' not in n:
        ip=s['spec'].get('clusterIP')
        if ip and ip != 'None':
            print(ip); break"
}

target_args() {  # target protocol
  local t="$1"
  if [[ "$t" == "thingsflow" ]]; then
    echo "--api-base http://$(clusterip thingsflow-fresh flow-core):8080 \
          --ingest-base http://$(clusterip thingsflow-fresh http-ingest):8081 \
          --mqtt-host $(clusterip thingsflow-fresh rmqtt-edge) --mqtt-port 1883 \
          --greptime-base http://$(clusterip thingsflow-fresh greptimedb):4000"
  else
    echo "--api-base http://$(clusterip tb-classic -tb-node):8080 \
          --ingest-base http://$(clusterip tb-classic http-transport):8081 \
          --mqtt-host $(clusterip tb-classic mqtt-transport) --mqtt-port 1883 \
          --user tenant@thingsboard.org --password tenant"
  fi
}

# Wait until the platform ACTUALLY answers, not until its pods are Running.
#
# Why: a `sleep 45` after scaling was enough for ThingsFlow (Go binary) and not
# for ThingsBoard (JVM + Spring, minutes). All ten ThingsBoard levels ran
# against a login that returned "Connection refused" and were recorded as
# clean. Pods Running != service ready.
wait_ready() {  # target
  local t="$1" url deadline=$((SECONDS + 900))
  if [[ "$t" == "thingsflow" ]]; then
    url="http://$(clusterip thingsflow-fresh flow-core):8080/api/auth/login"
  else
    url="http://$(clusterip tb-classic -tb-node):8080/api/auth/login"
  fi
  echo -n "-- waiting for $t to accept requests"
  while (( SECONDS < deadline )); do
    # A 400/401 already proves there is a service listening and routing; only a
    # connection failure (000) means it is not up yet.
    # BEWARE of `|| echo`: curl prints "000" when the connection fails AND ALSO
    # exits with code 7, so `$(curl ... || echo 000)` concatenates and produces
    # "000000", which is != "000" and made the guard pass. ThingsBoard was
    # declared ready after 3279s without being ready, and all 10 levels ran
    # against a dead login. Assigning inside the `||` does not concatenate.
    local code
    code="$(curl -s -o /dev/null -m 10 -w '%{http_code}' -X POST "$url" \
              -H 'Content-Type: application/json' -d '{}' 2>/dev/null)" || code="000"
    if [[ -n "$code" && "$code" != "000" ]]; then
      echo " OK (HTTP $code after ${SECONDS}s)"
      return 0
    fi
    echo -n "."
    sleep 10
  done
  echo " TIMED OUT: $t did not respond within 900s" >&2
  return 1
}

# Wait until NO backlog from the previous level is left before measuring the next.
#
# Why: without this a level measures the previous level's work. Measured: after
# ramp v1 the `latest-kv` consumer was dragging 1,823,148 pending messages and
# burned 2,282m draining them. The 1,000 msg/s level measured on top of that
# reported 6,659m of CPU — almost all of it was somebody else's backlog.
#
# The REAL backlog of each platform is queried, not a CPU proxy: on NATS the
# pending messages per consumer, on ThingsBoard the growth of ts_kv. A "low CPU"
# proxy would confuse a drained system with a stuck one.
# Returns ThingsFlow to the same state before every level: empty streams AND an
# empty KV bucket.
#
# The bucket matters as much as the streams, and for a non-obvious reason: the
# telemetry key prefix carries the run stamp, so EVERY level writes 6,000 new KV
# keys instead of overwriting the previous level's. After six runs the bucket
# held 35,999 entries with `history=1` and 6,000 expected unique keys. KV writes
# get more expensive as the bucket grows, so without this reset the later levels
# come out artificially expensive — and, worse, `latest-kv` could be flagged as
# a "lagging consumer" because of a problem the test bench itself manufactured.
#
# In production the keys are stable (temperature, co2, ...) and the bucket stays
# bounded at devices x keys. Emptying it between levels is NOT window dressing:
# it restores the condition the system really has.
tf_reset_state() {
  kubectl --context=microk8s -n thingsflow-fresh run "nats-reset-$RANDOM" \
    --rm -i --restart=Never --image=natsio/nats-box:0.16.0 --command -- \
    sh -c 'for s in TF_RAW TF_ENTITY TF_ALARMS; do
             nats --server nats://tf-thingsflow-nats:4222 stream purge $s -f >/dev/null 2>&1
           done
           nats --server nats://tf-thingsflow-nats:4222 stream purge KV_twin_state -f >/dev/null 2>&1' \
    >/dev/null 2>&1 || true
}

wait_drained() {  # target
  local t="$1" deadline=$((SECONDS + 3600))
  echo -n "-- waiting for $t to drain"
  while (( SECONDS < deadline )); do
    local pending
    if [[ "$t" == "thingsflow" ]]; then
      kubectl --context=microk8s -n thingsflow-fresh port-forward \
        tf-thingsflow-nats-0 18222:8222 >/dev/null 2>&1 &
      local pf=$!
      sleep 4
      pending="$(python3 - <<'PY' 2>/dev/null || echo 999999
import json,urllib.request
try:
    d=json.load(urllib.request.urlopen("http://127.0.0.1:18222/jsz?streams=1&consumers=1",timeout=15))
except Exception:
    raise SystemExit(1)
tot=0
for acc in d.get('account_details',[]):
    for s in acc.get('stream_detail',[]):
        for c in s.get('consumer_detail',[]):
            tot += c.get('num_pending',0)
print(tot)
PY
)"
      kill $pf 2>/dev/null
    else
      # ThingsBoard: the backlog lives in the tb-node heap and in Kafka, and
      # there is no direct figure. What is observable is ts_kv stopping growing.
      local pod a b
      pod="$(kubectl --context=microk8s -n tb-classic get pods --no-headers \
             | awk '/postgres/{print $1; exit}')"
      a="$(kubectl --context=microk8s -n tb-classic exec "$pod" -- psql -U thingsboard \
            -d thingsboard -tAc 'SELECT count(*) FROM ts_kv' 2>/dev/null | tr -dc '0-9')"
      sleep 20
      b="$(kubectl --context=microk8s -n tb-classic exec "$pod" -- psql -U thingsboard \
            -d thingsboard -tAc 'SELECT count(*) FROM ts_kv' 2>/dev/null | tr -dc '0-9')"
      pending=$(( ${b:-0} - ${a:-0} ))
    fi
    if [[ "${pending:-999999}" -le 1000 ]]; then
      echo " OK (backlog=${pending} after ${SECONDS}s)"
      return 0
    fi

    # If the backlog is large, PURGE instead of waiting.
    #
    # The previous level's lag is already recorded in .lag-*.txt — that is the
    # finding and it is not lost. What is left in the stream is contamination
    # for the next level, not information. Waiting for it to drain at 644 msg/s
    # would cost 74 min after a 16,000 msg/s level: the ramp would take a day
    # and the data would be the same.
    if [[ "$t" == "thingsflow" && "${pending:-0}" -gt 50000 ]]; then
      echo -n " [purging ${pending}]"
      tf_reset_state
      sleep 10
      continue
    fi
    echo -n " [${pending}]"
    sleep 30
  done
  echo " WARNING: $t did not drain within 3600s; the next level will be contaminated" >&2
  return 1
}

run_level() {  # target protocol rate
  local t="$1" p="$2" rate="$3"
  local ns; ns="$(ns_for "$t")"
  local tag="${t}-${p}-${rate}"
  local snap="$OUTDIR/.thr-$tag.json"
  local res="$OUTDIR/$tag.json"

  echo "=============================================================="
  echo "  $t / $p / $rate msg/s   ($DEVICES devices, ${DURATION}s)"
  echo "=============================================================="

  # Warm-up: JVM JIT, caches, connections. Without this you measure the cold
  # start of the system, which systematically penalizes the JVM.
  echo "-- warming up ${WARMUP}s"
  # shellcheck disable=SC2046
  "$LOADGEN" run --target "$t" --protocol "$p" --devices "$DEVICES" \
      --rate "$((rate / 4))" --duration "$WARMUP" --qos 1 \
      $(target_args "$t" "$p") --run-id "warm-$STAMP-$tag" >/dev/null 2>&1 || true

  # Drain BEFORE opening the gate: if backlog from the previous level is left,
  # its cost would be charged to this level. That is the difference between
  # measuring the platform and measuring what the platform still owes the
  # previous experiment.
  wait_drained "$t" || true
  # Unconditional reset, not only when there is backlog: even if the streams are
  # empty, the KV bucket still holds the previous level's keys.
  [[ "$t" == "thingsflow" ]] && tf_reset_state

  # The gate opens AFTER the warm-up and the drain: start-up throttling is real
  # but says nothing about the steady state being measured.
  python3 "$GATE" snapshot "$ns" "$snap"

  # shellcheck disable=SC2046
  "$LOADGEN" run --target "$t" --protocol "$p" --devices "$DEVICES" \
      --rate "$rate" --duration "$DURATION" --qos 1 --ramp 15 \
      --verify-landed --settle-seconds "$SETTLE" \
      $(target_args "$t" "$p") --run-id "$STAMP-$tag" --out "$res" 2>&1 | tail -20

  # Consumer lag AT THE END of the level.
  #
  # Why it matters: the landing verification counts rows in GreptimeDB, so a
  # level comes out "clean" even if ANOTHER consumer of the same flow fell
  # behind. It happened: at high rates the latest-value writer (twin state)
  # accumulated 1.8M pending messages while the history was up to date. The
  # history was complete and the UI would have shown stale values. A level
  # where a consumer does not keep up is NOT a sustained level.
  local lag=0
  if [[ "$t" == "thingsflow" ]]; then
    kubectl --context=microk8s -n thingsflow-fresh port-forward \
      tf-thingsflow-nats-0 18222:8222 >/dev/null 2>&1 &
    local pf=$!
    sleep 4
    lag="$(python3 - <<'PY' 2>/dev/null || echo -1
import json,urllib.request
d=json.load(urllib.request.urlopen("http://127.0.0.1:18222/jsz?streams=1&consumers=1",timeout=15))
worst=0; name=""
for acc in d.get('account_details',[]):
    for s in acc.get('stream_detail',[]):
        for c in s.get('consumer_detail',[]):
            p=c.get('num_pending',0)
            if p>worst: worst, name = p, c['name']
print(f"{worst} {name}")
PY
)"
    kill $pf 2>/dev/null
    echo "-- worst consumer lag at level close: $lag"
  fi
  echo "$lag" > "$OUTDIR/.lag-$tag.txt"

  # Effective headroom READ FROM THE CLUSTER, while the load is still warm.
  # It does not trust the YAML: an override may not land (it already happened
  # with nats.resources).
  set +e
  python3 "$HERE/verify-effective-limits.py" "$ns" 3.0 2>&1 | tail -8
  set -e

  local gate_out gate_rc
  set +e
  gate_out="$(python3 "$GATE" check "$ns" "$snap" 2>&1)"; gate_rc=$?
  set -e
  echo "$gate_out"

  python3 - "$res" "$gate_rc" "$OUTDIR/verdicts.tsv" "$tag" "$OUTDIR/.lag-$tag.txt" <<'PY'
import json, os, sys
res, rc, tsv, tag = sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4]
lag_path = sys.argv[5] if len(sys.argv) > 5 else None
try:
    d = json.load(open(res))
except Exception:
    d = {}

# EXACT paths into the report. Searching for the first matching key at any
# depth was a bug: it found `per_second[0][0].accepted` (a one-second bucket)
# instead of `delivery.accepted`, and the verdict came out computed over
# 125 messages instead of 11,248.
delivery = d.get("delivery", {})
landed = d.get("landed", {})
honesty = d.get("honesty", {})

acc = delivery.get("accepted", 0)
fail = delivery.get("failed_total", 0)
blocked = honesty.get("blocked_inflight", 0)
expected = delivery.get("expected_rows")
rows = landed.get("rows")
verified = landed.get("verified")

reasons = []

# FIRST: did anything run at all?
#
# Without this check a run that sent NOTHING comes out CLEAN: zero errors
# because there were no attempts, zero loss because there was nothing to lose,
# and the remaining guards are skipped on null values. It happened: all ten
# ThingsBoard levels were recorded as clean after every one of them failed at
# login against a platform that was still starting up. A total failure must not
# look like a success.
if not d:
    reasons.append("NO REPORT (the run produced no output)")
elif not acc:
    reasons.append("DID NOT RUN (0 messages accepted)")

# rc: 0 no throttling, 1 mild, 2 severe.
#
# Mild only disqualifies if the level ALSO failed. Reason: an absolute counter
# punishes the JVM, whose GC bursts exceed the quota in some 100 ms period even
# when the average is at 43%. Measured on ThingsBoard: 67 periods out of 2,324
# (2.9%), 4.4 s over 232 s, and the level landed 1,035,000 rows EXACTLY.
# Invalidating that would be penalizing a threading model, not a performance.
#
# If the level failed AND there was throttling, the platform's ceiling cannot be
# separated from the cage's: in that case it does invalidate.
if rc >= 2:
    reasons.append("SEVERELY THROTTLED (the resource measurement is not reliable)")
throttled_mild = (rc == 1)
if fail:
    reasons.append(f"errors={fail}")
if verified is False or (rows is None and expected):
    reasons.append("landing NOT VERIFIED")
elif rows is not None and expected and rows != expected:
    reasons.append(f"loss={expected - rows} of {expected} rows")
if acc and blocked > 0.02 * acc:
    reasons.append(f"generator-limited(blocked={blocked})")

# A consumer that does not keep up disqualifies the level even if the history
# landed in full: the system did not sustain the rate, only part of it did.
lag_n, lag_name = 0, ""
if lag_path and os.path.exists(lag_path):
    raw = open(lag_path).read().split()
    if raw and raw[0].lstrip("-").isdigit():
        lag_n = int(raw[0])
        lag_name = raw[1] if len(raw) > 1 else ""
if lag_n > 5000:
    reasons.append(f"lagging consumer: {lag_name or '?'} with {lag_n} pending")

# Mild throttling only counts if something else failed (see note above).
if throttled_mild and reasons:
    reasons.append("with mild throttling: ceiling indistinguishable from the cage")
verdict = "CLEAN" if not reasons else "INVALID"
if not reasons and throttled_mild:
    verdict = "CLEAN*"   # met the target despite brief bursts; * = see note
line = f"{tag}\t{verdict}\t{acc}\t{rows}\t{expected}\t{lag_n}\t{';'.join(reasons) or '-'}\n"
new = not os.path.exists(tsv)
with open(tsv, "a") as f:
    if new:
        f.write("level\tverdict\taccepted\tlanded\texpected\tlag\treason\n")
    f.write(line)
print(f"\n>>> {tag}: {verdict}  {';'.join(reasons) or ''}")
PY
}

preflight() {
  # Check the profiles BEFORE spending hours of ramp. The previous measurement
  # was lost in its entirety over a misspelled key that nobody validated.
  echo "=== preflight: profile keys ==="
  python3 "$HERE/check-values-keys.py" "$HERE/../../k8s/helm/thingsflow" \
      "$HERE/../profiles/fair-thingsflow.yaml" || true
  python3 "$HERE/check-values-keys.py" "$HERE/../helm/thingsboard-cluster" \
      "$HERE/../profiles/fair-thingsboard.yaml" || true
  echo
}

main() {
  preflight
  local targets="${TARGETS:-thingsflow thingsboard}"
  local protos="${PROTOS:-mqtt http}"
  local rates="${RATES:-2000 4000 8000 16000}"
  for t in $targets; do
    "$ISOLATE" "$([[ "$t" == thingsflow ]] && echo thingsflow || echo tb)"
    wait_ready "$t" || { echo "skipping $t: it did not start"; continue; }
    sleep 45   # ready: margin for the warm start to settle
    for p in $protos; do
      for r in $rates; do
        run_level "$t" "$p" "$r" || echo "level $t/$p/$r aborted, continuing"
      done
    done
  done
  echo
  echo "=== verdicts ==="
  column -t -s$'\t' "$OUTDIR/verdicts.tsv" 2>/dev/null || cat "$OUTDIR/verdicts.tsv"
}

main "$@"
