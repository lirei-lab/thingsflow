#!/usr/bin/env bash
# Isolates a system under test by scaling the other one to zero.
#
# Why: both platforms live on the same 16-core node. At idle, TB Classic burns
# ~1 core continuously (2 tb-node JVMs + Kafka) and ThingsFlow ~0.1. Measuring
# one while the other is breathing puts that noise into the result and, worse,
# takes CPU away from the system that is actually being measured.
#
# Usage:
#   sut-isolate.sh thingsflow   -> TB to 0, ThingsFlow up
#   sut-isolate.sh tb           -> ThingsFlow to 0, TB up
#   sut-isolate.sh both         -> both up (only for the idle baseline)
#
# StatefulSets are ALSO scaled to zero. Measured: idle Kafka burns ~450m
# (2.8% of the node) even with no tb-node alive, and that noise goes straight
# into the measurement of the other system. They come back up with their PVC
# intact, so no state is lost.

set -euo pipefail
K="kubectl --context=microk8s"
TF_NS=thingsflow-fresh
TB_NS=tb-classic

scale_deploys() {  # ns, up|down  — covers Deployments and StatefulSets
  local ns="$1" mode="$2"
  if [[ "$mode" == "down" ]]; then
    $K -n "$ns" get deploy,statefulset -o name | while read -r d; do
      cur=$($K -n "$ns" get "$d" -o jsonpath='{.spec.replicas}')
      # Do not overwrite the annotation when it is already at 0: a second "down"
      # would record restore-replicas=0 and the "up" would leave the system off.
      if [[ "$cur" != "0" ]]; then
        $K -n "$ns" annotate "$d" bench.restore-replicas="$cur" --overwrite >/dev/null
      fi
      $K -n "$ns" scale "$d" --replicas=0 >/dev/null
    done
  else
    # Helm owns the intent, NOT the annotation.
    #
    # Why: the annotation is written on the way down and goes stale. If between
    # the "down" and the "up" there is a helm upgrade that changes replicas, the
    # restore writes the old value and silently OVERRIDES Helm. It happened:
    # nats-alarms stayed at 2 replicas for an entire ramp while the release
    # asked for 3, and the key detector cannot see it because the key does
    # exist and the template does read it — the override landed and was then
    # undone.
    #
    # `helm get manifest` is what the release wants right now. The annotation is
    # left only as a fallback for resources that do not belong to a release.
    local rel manifest
    rel="$(helm --kube-context=microk8s -n "$ns" list -q 2>/dev/null | head -1)"
    manifest=""
    [[ -n "$rel" ]] && manifest="$(helm --kube-context=microk8s -n "$ns" get manifest "$rel" 2>/dev/null || true)"

    $K -n "$ns" get deploy,statefulset -o name | while read -r d; do
      local name kind want
      kind="${d%%/*}"; name="${d##*/}"
      want=""
      if [[ -n "$manifest" ]]; then
        want="$(printf '%s' "$manifest" | python3 -c "
import sys,re
kind_want={'deployment':'Deployment','deployment.apps':'Deployment',
           'statefulset':'StatefulSet','statefulset.apps':'StatefulSet'}.get('$kind','')
for doc in sys.stdin.read().split('\n---\n'):
    k=re.search(r'^kind:\s*(\S+)', doc, re.M)
    n=re.search(r'^\s{2}name:\s*(\S+)', doc, re.M)
    if k and n and k.group(1)==kind_want and n.group(1).strip('\"')=='$name':
        r=re.search(r'^\s{2}replicas:\s*(\d+)', doc, re.M)
        if r: print(r.group(1))
        break" 2>/dev/null || true)"
      fi
      if [[ -z "$want" ]]; then
        want="$($K -n "$ns" get "$d" -o jsonpath='{.metadata.annotations.bench\.restore-replicas}' 2>/dev/null || true)"
      fi
      [[ -n "$want" && "$want" != "0" ]] && $K -n "$ns" scale "$d" --replicas="$want" >/dev/null || true
    done
  fi
}

wait_gone() {  # ns
  local ns="$1" n
  for _ in $(seq 1 60); do
    n=$($K -n "$ns" get pods --no-headers 2>/dev/null | grep -cE 'Running|Pending' || true)
    [[ "$n" -eq 0 ]] && return 0
    sleep 5
  done
  echo "WARNING: $n deployment pods remain in $ns after waiting" >&2
}

case "${1:-}" in
  thingsflow) scale_deploys "$TB_NS" down; wait_gone "$TB_NS"; scale_deploys "$TF_NS" up ;;
  tb)         scale_deploys "$TF_NS" down; wait_gone "$TF_NS"; scale_deploys "$TB_NS" up ;;
  both)       scale_deploys "$TF_NS" up;  scale_deploys "$TB_NS" up ;;
  *) echo "usage: $0 {thingsflow|tb|both}" >&2; exit 2 ;;
esac

echo "isolation applied: $1"
$K top nodes --no-headers 2>/dev/null | awk '{printf "node: CPU=%s (%s) MEM=%s (%s)\n", $2,$3,$4,$5}'
