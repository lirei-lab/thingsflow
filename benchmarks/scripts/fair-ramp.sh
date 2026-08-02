#!/usr/bin/env bash
# Rampa comparativa JUSTA: ningún nivel se publica sin pasar el portón.
#
# Qué corrige respecto a la rampa anterior:
#
#   1. Presupuestos iguales. Antes ThingsFlow tenía 29 500 m de límites de CPU
#      y ThingsBoard 10 000 m. Ahora ambos llevan un perfil con límites >= 3x
#      su pico observado (benchmarks/profiles/fair-*.yaml).
#   2. Portón de throttling. Antes el método decía "los límites no son
#      vinculantes" y no era cierto: tb-node llegó al 99,3 % de su techo y
#      nats-alarms al 90,7 %. Ahora cada nivel se mide con throttle-gate.py y
#      el que estrangule se marca INVALIDO en vez de publicarse como limpio.
#   3. Frente MQTT comparable: 3 nodos rmqtt en cluster raft frente a los
#      2 tb-mqtt-transport del otro lado (antes era 1 contra 2).
#
# Un nivel solo cuenta como LIMPIO si se cumplen las tres cosas a la vez:
#   - cero errores y cero pérdida (landed == accepted),
#   - el generador no fue el cuello de botella (blocked_inflight bajo),
#   - el portón de throttling pasó.
# Cualquier otra combinación se registra con su motivo. Un nivel que falla por
# la jaula NO es un dato sobre la plataforma.

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

mkdir -p "$OUTDIR"

ns_for() { [[ "$1" == "thingsflow" ]] && echo "thingsflow-fresh" || echo "tb-classic"; }

# Ambas plataformas se alcanzan por ClusterIP, no por NodePort.
#
# Por qué importa: la rampa anterior llegaba a ThingsFlow por ClusterIP
# (10.152.183.46:1883) y a ThingsBoard por NodePort (172.16.128.7:30188). Un
# NodePort mete un DNAT extra de kube-proxy y, con externalTrafficPolicy
# Cluster, puede añadir SNAT. Es una diferencia pequeña pero sistemática y
# siempre en la misma dirección. El generador corre en el propio nodo y tiene
# ruta al CIDR de servicios, así que ClusterIP es alcanzable para los dos.
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

run_level() {  # target protocol rate
  local t="$1" p="$2" rate="$3"
  local ns; ns="$(ns_for "$t")"
  local tag="${t}-${p}-${rate}"
  local snap="$OUTDIR/.thr-$tag.json"
  local res="$OUTDIR/$tag.json"

  echo "=============================================================="
  echo "  $t / $p / $rate msg/s   ($DEVICES dispositivos, ${DURATION}s)"
  echo "=============================================================="

  # Precalentamiento: JIT de la JVM, cachés, conexiones. Sin esto se mide el
  # arranque en frío del sistema, que penaliza sistemáticamente a la JVM.
  echo "-- precalentando ${WARMUP}s"
  # shellcheck disable=SC2046
  "$LOADGEN" run --target "$t" --protocol "$p" --devices "$DEVICES" \
      --rate "$((rate / 4))" --duration "$WARMUP" --qos 1 \
      $(target_args "$t" "$p") --run-id "warm-$tag" >/dev/null 2>&1 || true

  # El portón se abre DESPUÉS del precalentamiento: el throttling del arranque
  # es real pero no dice nada sobre el régimen permanente que se está midiendo.
  python3 "$GATE" snapshot "$ns" "$snap"

  # shellcheck disable=SC2046
  "$LOADGEN" run --target "$t" --protocol "$p" --devices "$DEVICES" \
      --rate "$rate" --duration "$DURATION" --qos 1 --ramp 15 \
      --verify-landed --settle-seconds "$SETTLE" \
      $(target_args "$t" "$p") --run-id "$tag" --out "$res" 2>&1 | tail -20

  # Holgura efectiva LEÍDA DEL CLUSTER, mientras la carga aún está caliente.
  # No se fía del YAML: un override puede no llegar (ya pasó con nats.resources).
  set +e
  python3 "$HERE/verify-effective-limits.py" "$ns" 3.0 2>&1 | tail -8
  set -e

  local gate_out gate_rc
  set +e
  gate_out="$(python3 "$GATE" check "$ns" "$snap" 2>&1)"; gate_rc=$?
  set -e
  echo "$gate_out"

  python3 - "$res" "$gate_rc" "$OUTDIR/verdicts.tsv" "$tag" <<'PY'
import json, os, sys
res, rc, tsv, tag = sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4]
try:
    d = json.load(open(res))
except Exception:
    d = {}

def dig(*names):
    """Los nombres cambiaron entre versiones del generador; se busca en plano."""
    stack = [d]
    while stack:
        cur = stack.pop()
        if isinstance(cur, dict):
            for k, v in cur.items():
                if k in names and isinstance(v, (int, float)):
                    return v
                stack.append(v)
        elif isinstance(cur, list):
            stack.extend(cur)
    return None

acc = dig("accepted") or 0
land = dig("landed", "landed_count")
fail = dig("failed_total", "errors") or 0
blocked = dig("blocked_inflight") or 0

reasons = []
if rc != 0:
    reasons.append("THROTTLED(jaula, no plataforma)")
if fail:
    reasons.append(f"errores={fail}")
if land is not None and acc and land < acc:
    reasons.append(f"perdida={acc - land}")
if acc and blocked > 0.02 * acc:
    reasons.append(f"generador-limitante(blocked={blocked})")

verdict = "LIMPIO" if not reasons else "INVALIDO"
line = f"{tag}\t{verdict}\t{acc}\t{land}\t{';'.join(reasons) or '-'}\n"
new = not os.path.exists(tsv)
with open(tsv, "a") as f:
    if new:
        f.write("nivel\tveredicto\taceptados\taterrizados\tmotivo\n")
    f.write(line)
print(f"\n>>> {tag}: {verdict}  {';'.join(reasons) or ''}")
PY
}

preflight() {
  # Comprobar los perfiles ANTES de gastar horas de rampa. La medición anterior
  # se perdió entera por una clave mal escrita que nadie validó.
  echo "=== preflight: claves de los perfiles ==="
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
    sleep 45   # que el sistema aislado alcance reposo antes de medir
    for p in $protos; do
      for r in $rates; do
        run_level "$t" "$p" "$r" || echo "nivel $t/$p/$r abortado, se continúa"
      done
    done
  done
  echo
  echo "=== veredictos ==="
  column -t -s$'\t' "$OUTDIR/verdicts.tsv" 2>/dev/null || cat "$OUTDIR/verdicts.tsv"
}

main "$@"
