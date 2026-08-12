#!/usr/bin/env bash
# Proves (or disproves) that publishing directly to $KV.<bucket>.<key> (a raw
# JetStream publish, NOT the nats_kv/`nats kv put` API) is observationally
# identical to a nats_kv Put for every reader of the twin_state bucket:
#
#   1. READBACK  — a raw publish to $KV.<bucket>.<key> is retrievable via the KV
#                  API (`nats kv get`), proving the bucket's underlying stream
#                  config (max_msgs_per_subject=1, last-write-wins) treats a raw
#                  publish as a valid KV entry.
#   2. WATCHER   — a `nats kv watch` subscriber (the same code path
#                  flow-core/internal/twinstore's TWIN_STATE_WATCH_ENABLED watcher
#                  depends on) receives an update event for a raw publish.
#   3. WRITETWICE — publishing the same key twice with different values, in
#                  order, leaves the SECOND value as the final KV read (last-write-
#                  wins, matching twinstore's per-key LWW merge semantics).
#
# This directly informs the Phase 2 rung-1 fix candidate (swap output.nats_kv ->
# output.nats_jetstream publishing to $KV.<bucket>.<key>): a FAIL on any reader
# vetoes that candidate — see benchmarks/FINDING-twin-state-localization.md's
# "$KV. Semantics" / Verdict sections.
#
# Adapted from .claude/skills/verify-nats-streams/SKILL.md's Test F (DURA-03)
# write-twice + read-back METHODOLOGY (that skill targets TF_RAW/TF_ENTITY; this
# script targets the twin_state KV bucket, a different target — not a literal
# reuse of that skill's scripts).
#
# Prints one machine-parseable PASS/FAIL line per check to stdout:
#   READBACK=PASS|FAIL
#   WATCHER=PASS|FAIL
#   WRITETWICE=PASS|FAIL
# Exits 0 iff all three PASS, non-zero otherwise.
#
# Cleans up its diagnostic key (nats kv del) on exit, even on failure.
set -uo pipefail  # deliberately NOT -e: every check must run and report even if
                   # an earlier one fails, so the summary is always complete.

NAMESPACE="${NAMESPACE:-thingsflow-fresh}"
KUBECTL_CONTEXT="${KUBECTL_CONTEXT:-microk8s}"
NATS_URL="${NATS_URL:-nats://thingsflow-nats:4222}"
NATS_IMAGE="${NATS_IMAGE:-natsio/nats-box:0.16.0}"
NATS_KV_BUCKET="${NATS_KV_BUCKET:-twin_state}"

# Diagnostic-only key namespace so this never collides with real device data.
STAMP="${STAMP:-$(date -u +%m%d%H%M%S)-$RANDOM}"
DIAG_KEY="DEVICE.diag-tenant.diag-device.telemetry.kvsemantics-check-${STAMP}"

kc() {
  kubectl --context="$KUBECTL_CONTEXT" -n "$NAMESPACE" "$@"
}

# Runs a single sh -c command string inside one ephemeral nats-box pod and
# returns its combined stdout+stderr; the caller inspects the text. Only used
# for fire-and-forget commands (bootstrap/cleanup) where the exact output text
# does not matter.
run_nbox() {
  local podname="kv-sem-$RANDOM"
  kc run "$podname" --rm -i --restart=Never --image="$NATS_IMAGE" --command -- \
    sh -c "$1" 2>&1
}

# Runs a single sh -c command string inside one ephemeral nats-box pod and
# extracts ONLY the text the pod's own script printed between two unique
# markers, discarding kubectl's own out-of-band noise (the interactive-session
# banner and the trailing `pod "..." deleted` line both land on the SAME stdout
# stream kubectl attaches, so a plain capture is NOT safe to compare verbatim —
# this was confirmed live: a first draft of this script matched against the raw
# capture and every check false-failed on that noise). The pod script itself
# MUST silence every intermediate command's own output (nats pub logs
# "Published N bytes" to stdout by default) so only the one line between the
# markers is the value under test.
run_nbox_value() {
  local podname="kv-sem-$RANDOM"
  local script="$1"
  local raw
  # NOTE: the marker echoes and $script are joined with NEWLINES, not `;` —
  # $script's own last line ends in a newline from the caller's heredoc-style
  # multi-line string, and appending `; echo __NBOX_END__` directly after it
  # produced a bare leading `;` with nothing before it on that line, which is a
  # syntax error in POSIX sh ("unexpected ';'") that silently aborted the pod's
  # script before __NBOX_END__ ever printed — confirmed live, every check
  # false-failed on an empty capture until this was fixed to use newlines.
  raw="$(kc run "$podname" --rm -i --restart=Never --image="$NATS_IMAGE" --command -- \
    sh -c "$(printf 'echo __NBOX_BEGIN__\n%s\necho __NBOX_END__\n' "$script")" 2>/dev/null)"
  # `nats kv get --raw` prints NO trailing newline, so the value and the
  # __NBOX_END__ marker can land on the SAME line — a line-range sed delete
  # (first/last line) was tried first and silently ate the value along with
  # the markers whenever that happened (confirmed live). Splitting on the
  # marker STRINGS themselves via awk's record separator is newline-agnostic
  # and survives that case.
  local extracted
  extracted="$(printf '%s' "$raw" | awk 'BEGIN{RS="__NBOX_BEGIN__"} NR==2' | awk 'BEGIN{RS="__NBOX_END__"} NR==1')"
  # `echo __NBOX_BEGIN__` always contributes exactly one newline before the
  # script's own output begins; strip that one leading newline (and a
  # trailing one, if the last command's output ended with its own newline)
  # so single-line values compare cleanly with `==` in the caller.
  extracted="${extracted#$'\n'}"
  extracted="${extracted%$'\n'}"
  printf '%s' "$extracted"
}

READBACK_RESULT="FAIL"
WATCHER_RESULT="FAIL"
WRITETWICE_RESULT="FAIL"

cleanup() {
  echo "-- cleaning up diagnostic key $DIAG_KEY" >&2
  run_nbox "nats --server '$NATS_URL' kv del '$NATS_KV_BUCKET' '$DIAG_KEY' -f >/dev/null 2>&1 || true" >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "-- KV publish-semantics check against bucket=$NATS_KV_BUCKET key=$DIAG_KEY" >&2

# ---------------------------------------------------------------------------
# Check 1: READBACK — raw JetStream publish to $KV.<bucket>.<key>, then read
# back through the KV API.
# ---------------------------------------------------------------------------
echo "-- Check 1: READBACK" >&2
READBACK_VALUE="readback-v1-${STAMP}"
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
# publish (the same mechanism TWIN_STATE_WATCH_ENABLED relies on).
# ---------------------------------------------------------------------------
echo "-- Check 2: WATCHER" >&2
WATCHER_VALUE="watcher-v1-${STAMP}"
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
# Check 3: WRITETWICE — publish the SAME key twice with different values, in
# deterministic order; the final read must be the SECOND value (last-write-wins).
# ---------------------------------------------------------------------------
echo "-- Check 3: WRITETWICE" >&2
FIRST_VALUE="writetwice-first-${STAMP}"
SECOND_VALUE="writetwice-second-${STAMP}"
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

if [[ "$READBACK_RESULT" == "PASS" && "$WATCHER_RESULT" == "PASS" && "$WRITETWICE_RESULT" == "PASS" ]]; then
  echo "-- ALL CHECKS PASS: \$KV.<bucket>.<key> publish is observationally identical to a nats_kv Put" >&2
  exit 0
else
  echo "-- AT LEAST ONE CHECK FAILED: see READBACK/WATCHER/WRITETWICE lines above" >&2
  exit 1
fi
