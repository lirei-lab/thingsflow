#!/usr/bin/env bash
# Verify $KV. WHOLE-DOC publish semantics for the one-document-per-device model
# (Milestone 4 Phase 1). Extends verify-kv-publish-semantics.sh (per-key) to
# the whole-doc key shape DEVICE.<tenant>.<entity> AND adds a CAS-enforcement
# check (CASREJECT) that the per-key verifier never had.
#
# Checks:
#   1. READBACK   — a raw JetStream publish to $KV.<bucket>.DEVICE.<tenant>.<entity>
#                   is retrievable via the KV API (`nats kv get`).
#   2. WATCHER    — a `nats kv watch` subscriber receives an update event for a
#                   raw whole-doc publish (the TWIN_STATE_WATCH_ENABLED path).
#   3. WRITETWICE — publishing the same whole-doc key twice with different
#                   values leaves the SECOND as final (last-write-wins for plain
#                   publishes).
#   4. CASREJECT  — publishing with a deliberately STALE
#                   Nats-Expected-Last-Subject-Sequence header is REJECTED by the
#                   KV backing stream and the doc value is unchanged. This is the
#                   CAS-enforcement proof the one-document-per-device merge write
#                   path depends on (verified live 2026-08-17: the backing stream
#                   rejects a stale expected-seq, value stays at the prior write).
#
# Prints one machine-parseable PASS/FAIL line per check:
#   READBACK=PASS|FAIL
#   WATCHER=PASS|FAIL
#   WRITETWICE=PASS|FAIL
#   CASREJECT=PASS|FAIL
# Exits 0 iff all four PASS, non-zero otherwise.
#
# Cleans up its diagnostic key (nats kv del) on exit, even on failure.

set -uo pipefail  # deliberately NOT -e: every check must run and report

NAMESPACE="${NAMESPACE:-thingsflow-fresh}"
KUBECTL_CONTEXT="${KUBECTL_CONTEXT:-microk8s}"
NATS_URL="${NATS_URL:-nats://thingsflow-nats:4222}"
NATS_IMAGE="${NATS_IMAGE:-natsio/nats-box:0.16.0}"
NATS_KV_BUCKET="${NATS_KV_BUCKET:-twin_state}"

STAMP="${STAMP:-$(date -u +%m%d%H%M%S)-$RANDOM}"
DIAG_KEY="DEVICE.diag-tenant.diag-device"

kc() { kubectl --context="$KUBECTL_CONTEXT" -n "$NAMESPACE" "$@"; }

run_nbox() {
  local podname="kv-doc-sem-$RANDOM"
  kc run "$podname" --rm -i --restart=Never --image="$NATS_IMAGE" --command -- \
    sh -c "$1" 2>&1
}

# Extracts ONLY the text a pod's script printed between __NBOX_BEGIN__ and
# __NBOX_END__ markers, discarding kubectl's interactive-session noise (the
# banner and trailing `pod "..." deleted` line land on the same stdout stream).
# Newline-agnostic split via awk record separators; strips one leading/trailing
# newline so single-line values compare cleanly with `==`.
run_nbox_value() {
  local podname="kv-doc-sem-$RANDOM"
  local script="$1"
  local raw
  raw="$(kc run "$podname" --rm -i --restart=Never --image="$NATS_IMAGE" --command -- \
    sh -c "$(printf 'echo __NBOX_BEGIN__\n%s\necho __NBOX_END__\n' "$script")" 2>/dev/null)"
  local extracted
  extracted="$(printf '%s' "$raw" | awk 'BEGIN{RS="__NBOX_BEGIN__"} NR==2' | awk 'BEGIN{RS="__NBOX_END__"} NR==1')"
  extracted="${extracted#$'\n'}"
  extracted="${extracted%$'\n'}"
  printf '%s' "$extracted"
}

READBACK_RESULT="FAIL"
WATCHER_RESULT="FAIL"
WRITETWICE_RESULT="FAIL"
CASREJECT_RESULT="FAIL"

cleanup() {
  echo "-- cleaning up diagnostic key $DIAG_KEY" >&2
  run_nbox "nats --server '$NATS_URL' kv del '$NATS_KV_BUCKET' '$DIAG_KEY' -f >/dev/null 2>&1 || true" >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "-- KV WHOLE-DOC publish-semantics check against bucket=$NATS_KV_BUCKET key=$DIAG_KEY" >&2

# ---------------------------------------------------------------------------
# Check 1: READBACK — raw publish to the whole-doc $KV.<bucket>.DEVICE.<t>.<e>
# subject, read back through the KV API.
# ---------------------------------------------------------------------------
echo "-- Check 1: READBACK (whole-doc)" >&2
READBACK_VALUE="{\"schema\":\"probe\",\"ts\":1,\"value\":\"readback-${STAMP}\"}"
READBACK_OUT="$(run_nbox_value "
  nats --server '$NATS_URL' pub '\$KV.$NATS_KV_BUCKET.$DIAG_KEY' '$READBACK_VALUE' >/dev/null 2>&1
  sleep 1
  nats --server '$NATS_URL' kv get '$NATS_KV_BUCKET' '$DIAG_KEY' --raw
")"
if [[ "$READBACK_OUT" == "$READBACK_VALUE" ]]; then
  READBACK_RESULT="PASS"
else
  echo "   readback mismatch: got [$READBACK_OUT] want [$READBACK_VALUE]" >&2
fi
echo "READBACK=$READBACK_RESULT"

# ---------------------------------------------------------------------------
# Check 2: WATCHER — a kv watch subscriber must see an update event for a raw
# whole-doc publish (the TWIN_STATE_WATCH_ENABLED mechanism).
# ---------------------------------------------------------------------------
echo "-- Check 2: WATCHER (whole-doc)" >&2
WATCHER_VALUE="{\"schema\":\"probe\",\"ts\":2,\"value\":\"watcher-${STAMP}\"}"
WATCHER_OUT="$(run_nbox_value "
  nats --server '$NATS_URL' kv watch '$NATS_KV_BUCKET' '$DIAG_KEY' > /tmp/watch.out 2>&1 &
  WPID=\$!
  sleep 2
  nats --server '$NATS_URL' pub '\$KV.$NATS_KV_BUCKET.$DIAG_KEY' '$WATCHER_VALUE' >/dev/null 2>&1
  sleep 5
  kill \$WPID 2>/dev/null || true
  cat /tmp/watch.out
")"
if printf '%s' "$WATCHER_OUT" | grep -qF "$WATCHER_VALUE"; then
  WATCHER_RESULT="PASS"
else
  echo "   watcher did not observe an event containing [$WATCHER_VALUE]; captured output:" >&2
  echo "$WATCHER_OUT" >&2
fi
echo "WATCHER=$WATCHER_RESULT"

# ---------------------------------------------------------------------------
# Check 3: WRITETWICE — same whole-doc key twice, deterministic order; final
# read must be the SECOND value (last-write-wins for plain publishes).
# ---------------------------------------------------------------------------
echo "-- Check 3: WRITETWICE (whole-doc)" >&2
FIRST_VALUE="{\"schema\":\"probe\",\"ts\":3,\"value\":\"first-${STAMP}\"}"
SECOND_VALUE="{\"schema\":\"probe\",\"ts\":4,\"value\":\"second-${STAMP}\"}"
WRITETWICE_OUT="$(run_nbox_value "
  nats --server '$NATS_URL' pub '\$KV.$NATS_KV_BUCKET.$DIAG_KEY' '$FIRST_VALUE' >/dev/null 2>&1
  sleep 1
  nats --server '$NATS_URL' pub '\$KV.$NATS_KV_BUCKET.$DIAG_KEY' '$SECOND_VALUE' >/dev/null 2>&1
  sleep 1
  nats --server '$NATS_URL' kv get '$NATS_KV_BUCKET' '$DIAG_KEY' --raw
")"
if [[ "$WRITETWICE_OUT" == "$SECOND_VALUE" ]]; then
  WRITETWICE_RESULT="PASS"
else
  echo "   write-twice result mismatch: got [$WRITETWICE_OUT] want [$SECOND_VALUE]" >&2
fi
echo "WRITETWICE=$WRITETWICE_RESULT"

# ---------------------------------------------------------------------------
# Check 4: CASREJECT — publish with a deliberately STALE
# Nats-Expected-Last-Subject-Sequence; the backing stream must REJECT it and
# the doc must remain at the prior value (proves CAS enforcement, the load-
# bearing mechanism of the one-document-per-device merge write path).
# ---------------------------------------------------------------------------
echo "-- Check 4: CASREJECT (whole-doc)" >&2
CAS_BASE_VALUE="{\"schema\":\"probe\",\"ts\":5,\"value\":\"cas-base-${STAMP}\"}"
CAS_STALE_VALUE="{\"schema\":\"probe\",\"ts\":6,\"value\":\"cas-stale-${STAMP}\"}"
CASREJECT_OUT="$(run_nbox_value "
  nats --server '$NATS_URL' pub '\$KV.$NATS_KV_BUCKET.$DIAG_KEY' '$CAS_BASE_VALUE' >/dev/null 2>&1
  sleep 1
  REV=\$(nats --server '$NATS_URL' kv history '$NATS_KV_BUCKET' '$DIAG_KEY' --json 2>/dev/null | jq -r '.[-1].revision // empty' 2>/dev/null || echo '')
  STALE=\$(( \${REV:-1} + 100000 ))
  nats --server '$NATS_URL' pub '\$KV.$NATS_KV_BUCKET.$DIAG_KEY' '$CAS_STALE_VALUE' --header \"Nats-Expected-Last-Subject-Sequence: \$STALE\" >/dev/null 2>&1
  sleep 1
  nats --server '$NATS_URL' kv get '$NATS_KV_BUCKET' '$DIAG_KEY' --raw
")"
if [[ "$CASREJECT_OUT" == "$CAS_BASE_VALUE" ]]; then
  CASREJECT_RESULT="PASS"
else
  echo "   CASREJECT: doc changed to [$CASREJECT_OUT]; expected unchanged [$CAS_BASE_VALUE]" >&2
fi
echo "CASREJECT=$CASREJECT_RESULT"

if [[ "$READBACK_RESULT" == "PASS" && "$WATCHER_RESULT" == "PASS" && "$WRITETWICE_RESULT" == "PASS" && "$CASREJECT_RESULT" == "PASS" ]]; then
  echo "-- ALL CHECKS PASS: whole-doc \$KV. publish semantics (incl. CAS enforcement) verified" >&2
  exit 0
else
  echo "-- AT LEAST ONE CHECK FAILED: see READBACK/WATCHER/WRITETWICE/CASREJECT lines above" >&2
  exit 1
fi
