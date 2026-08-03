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

# Sello único por ejecución. Sin él, el prefijo de clave se repite entre
# corridas del mismo nivel y el recuento de filas aterrizadas SUMA las corridas
# anteriores: una repetición de 33 744 filas dio landed=67 488 frente a
# expected=33 744 y se habría leído como "duplicación de datos" en vez de como
# lo que era, contaminación entre ejecuciones.
STAMP="${STAMP:-$(date -u +%m%d%H%M%S)}"

mkdir -p "$OUTDIR"

ns_for() { [[ "$1" == "thingsflow" ]] && echo "thingsflow-fresh" || echo "tb-classic"; }

# Ambas plataformas se alcanzan por ClusterIP, no por NodePort.
#
# Por qué importa: la rampa anterior llegaba a ThingsFlow por ClusterIP
# (<cluster-ip>:1883) y a ThingsBoard por NodePort (<node-ip>:30188). Un
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

# Espera a que la plataforma responda DE VERDAD, no a que sus pods estén Running.
#
# Por qué: un `sleep 45` tras escalar bastaba para ThingsFlow (binario Go) y no
# para ThingsBoard (JVM + Spring, minutos). Los diez niveles de ThingsBoard
# corrieron contra un login que devolvía "Connection refused" y se registraron
# como limpios. Pods Running != servicio listo.
wait_ready() {  # target
  local t="$1" url deadline=$((SECONDS + 900))
  if [[ "$t" == "thingsflow" ]]; then
    url="http://$(clusterip thingsflow-fresh flow-core):8080/api/auth/login"
  else
    url="http://$(clusterip tb-classic -tb-node):8080/api/auth/login"
  fi
  echo -n "-- esperando a que $t acepte peticiones"
  while (( SECONDS < deadline )); do
    # Un 400/401 ya demuestra que hay servicio escuchando y enrutando; solo un
    # fallo de conexión (000) significa que aún no está.
    # OJO con el `|| echo`: curl imprime "000" cuando falla la conexion Y ADEMAS
    # sale con codigo 7, asi que `$(curl ... || echo 000)` concatena y produce
    # "000000", que no es igual a "000" y hacia pasar la guarda. ThingsBoard se
    # dio por listo tras 3279s sin estarlo, y los 10 niveles corrieron contra un
    # login muerto. La asignacion en el `||` no concatena.
    local code
    code="$(curl -s -o /dev/null -m 10 -w '%{http_code}' -X POST "$url" \
              -H 'Content-Type: application/json' -d '{}' 2>/dev/null)" || code="000"
    if [[ -n "$code" && "$code" != "000" ]]; then
      echo " OK (HTTP $code tras ${SECONDS}s)"
      return 0
    fi
    echo -n "."
    sleep 10
  done
  echo " AGOTADO: $t no respondió en 900s" >&2
  return 1
}

# Espera a que NO quede backlog del nivel anterior antes de medir el siguiente.
#
# Por qué: sin esto un nivel mide el trabajo del anterior. Medido: tras la rampa
# v1, el consumidor `latest-kv` arrastraba 1 823 148 mensajes pendientes y
# quemaba 2 282 m drenándolos. El nivel de 1 000 msg/s que se midió encima
# reportó 6 659 m de CPU — casi todo era backlog ajeno.
#
# Se consulta el backlog REAL de cada plataforma, no un proxy de CPU: en NATS
# los mensajes pendientes por consumidor, en ThingsBoard el crecimiento de
# ts_kv. Un proxy de "CPU baja" confundiría un sistema drenado con uno atascado.
# Devuelve ThingsFlow al mismo estado antes de cada nivel: streams vacíos Y
# bucket KV vacío.
#
# El bucket importa tanto como los streams, y por una razón que no es obvia: el
# prefijo de clave de telemetría lleva el sello de ejecución, así que CADA nivel
# escribe 6 000 claves KV nuevas en vez de sobrescribir las del anterior. Tras
# seis corridas el bucket tenía 35 999 entradas con `history=1` y 6 000 claves
# únicas esperadas. Las escrituras KV se encarecen conforme crece el bucket, así
# que sin este reset los niveles tardíos salen artificialmente caros — y, peor,
# `latest-kv` podría marcarse como "consumidor retrasado" por un problema que
# fabricó el propio banco de pruebas.
#
# En producción las claves son estables (temperature, co2, ...) y el bucket queda
# acotado en dispositivos x claves. Vaciarlo entre niveles NO es maquillaje:
# restaura la condición que el sistema tiene de verdad.
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
  echo -n "-- esperando drenaje de $t"
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
      # ThingsBoard: el backlog vive en el heap de tb-node y en Kafka, y no hay
      # una cifra directa. Lo observable es que ts_kv deje de crecer.
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
      echo " OK (backlog=${pending} tras ${SECONDS}s)"
      return 0
    fi

    # Si el backlog es grande, PURGAR en vez de esperar.
    #
    # El retraso del nivel anterior ya quedó registrado en .lag-*.txt — ese es
    # el hallazgo y no se pierde. Lo que queda en el stream es contaminación
    # para el nivel siguiente, no información. Esperar a drenarlo a 644 msg/s
    # costaría 74 min tras un nivel de 16 000 msg/s: la rampa duraría un día y
    # el dato sería el mismo.
    if [[ "$t" == "thingsflow" && "${pending:-0}" -gt 50000 ]]; then
      echo -n " [purgando ${pending}]"
      tf_reset_state
      sleep 10
      continue
    fi
    echo -n " [${pending}]"
    sleep 30
  done
  echo " AVISO: $t no drenó en 3600s; el nivel siguiente saldrá contaminado" >&2
  return 1
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
      $(target_args "$t" "$p") --run-id "warm-$STAMP-$tag" >/dev/null 2>&1 || true

  # Drenar ANTES de abrir el portón: si queda backlog del nivel anterior, su
  # coste se cargaría a este nivel. Es la diferencia entre medir la plataforma
  # y medir lo que la plataforma aún le debe al experimento anterior.
  wait_drained "$t" || true
  # Reset incondicional, no solo cuando hay backlog: aunque los streams estén
  # vacíos, el bucket KV conserva las claves del nivel anterior.
  [[ "$t" == "thingsflow" ]] && tf_reset_state

  # El portón se abre DESPUÉS del precalentamiento y del drenaje: el throttling
  # del arranque es real pero no dice nada del régimen permanente que se mide.
  python3 "$GATE" snapshot "$ns" "$snap"

  # shellcheck disable=SC2046
  "$LOADGEN" run --target "$t" --protocol "$p" --devices "$DEVICES" \
      --rate "$rate" --duration "$DURATION" --qos 1 --ramp 15 \
      --verify-landed --settle-seconds "$SETTLE" \
      $(target_args "$t" "$p") --run-id "$STAMP-$tag" --out "$res" 2>&1 | tail -20

  # Retraso de consumidores AL TERMINAR el nivel.
  #
  # Por qué importa: la verificación de aterrizaje cuenta filas en GreptimeDB,
  # así que un nivel sale "limpio" aunque OTRO consumidor del mismo flujo se
  # haya quedado atrás. Ocurrió: a tasas altas el escritor de valores actuales
  # (twin state) acumuló 1,8 M de mensajes pendientes mientras el histórico iba
  # al día. El histórico estaba completo y la UI habría mostrado valores viejos.
  # Un nivel donde un consumidor no sigue el ritmo NO es un nivel sostenido.
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
    echo "-- retraso máximo de consumidor al cierre del nivel: $lag"
  fi
  echo "$lag" > "$OUTDIR/.lag-$tag.txt"

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

  python3 - "$res" "$gate_rc" "$OUTDIR/verdicts.tsv" "$tag" "$OUTDIR/.lag-$tag.txt" <<'PY'
import json, os, sys
res, rc, tsv, tag = sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4]
lag_path = sys.argv[5] if len(sys.argv) > 5 else None
try:
    d = json.load(open(res))
except Exception:
    d = {}

# Rutas EXACTAS del informe. Buscar la primera clave que coincida en cualquier
# nivel era un error: encontraba `per_second[0][0].accepted` (un cubo de un
# segundo) en vez de `delivery.accepted`, y el veredicto salía calculado sobre
# 125 mensajes en lugar de 11 248.
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

# PRIMERO: ¿se ejecutó siquiera algo?
#
# Sin esta comprobación una corrida que no envió NADA sale LIMPIO: cero errores
# porque no hubo intentos, cero pérdida porque no había qué perder, y el resto
# de guardas se saltan por valores nulos. Ocurrió: los diez niveles de
# ThingsBoard se registraron como limpios tras fallar todos en el login contra
# una plataforma que aún arrancaba. Un fallo total no puede parecerse a un
# éxito.
if not d:
    reasons.append("SIN INFORME (la corrida no produjo salida)")
elif not acc:
    reasons.append("NO SE EJECUTO (0 mensajes aceptados)")

# rc: 0 sin throttling, 1 leve, 2 grave.
#
# El leve solo descalifica si el nivel ADEMAS fallo. Razon: un contador absoluto
# castiga a la JVM, cuyas rafagas de GC superan la cuota en algun periodo de
# 100 ms aunque el promedio este al 43 %. Medido en ThingsBoard: 67 periodos de
# 2.324 (2,9 %), 4,4 s sobre 232 s, y el nivel aterrizo 1.035.000 filas EXACTAS.
# Invalidar eso seria penalizar un modelo de hilos, no un rendimiento.
#
# Si el nivel fallo Y hubo throttling, no se puede separar el techo de la
# plataforma del de la jaula: ahi si invalida.
if rc >= 2:
    reasons.append("THROTTLED GRAVE (la medicion de recursos no es fiable)")
throttled_mild = (rc == 1)
if fail:
    reasons.append(f"errores={fail}")
if verified is False or (rows is None and expected):
    reasons.append("aterrizaje NO VERIFICADO")
elif rows is not None and expected and rows != expected:
    reasons.append(f"perdida={expected - rows} de {expected} filas")
if acc and blocked > 0.02 * acc:
    reasons.append(f"generador-limitante(blocked={blocked})")

# Un consumidor que no sigue el ritmo descalifica el nivel aunque el histórico
# haya aterrizado entero: el sistema no sostuvo la tasa, solo una parte de él.
lag_n, lag_name = 0, ""
if lag_path and os.path.exists(lag_path):
    raw = open(lag_path).read().split()
    if raw and raw[0].lstrip("-").isdigit():
        lag_n = int(raw[0])
        lag_name = raw[1] if len(raw) > 1 else ""
if lag_n > 5000:
    reasons.append(f"consumidor retrasado: {lag_name or '?'} con {lag_n} pendientes")

# El throttling leve solo cuenta si algo mas fallo (ver nota arriba).
if throttled_mild and reasons:
    reasons.append("con throttling leve: techo indistinguible de la jaula")
verdict = "LIMPIO" if not reasons else "INVALIDO"
if not reasons and throttled_mild:
    verdict = "LIMPIO*"   # cumplio pese a rafagas breves; * = ver nota
line = f"{tag}\t{verdict}\t{acc}\t{rows}\t{expected}\t{lag_n}\t{';'.join(reasons) or '-'}\n"
new = not os.path.exists(tsv)
with open(tsv, "a") as f:
    if new:
        f.write("nivel\tveredicto\taceptados\taterrizados\tesperados\tretraso\tmotivo\n")
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
    wait_ready "$t" || { echo "se omite $t: no arrancó"; continue; }
    sleep 45   # ya listo: margen para que el arranque en caliente se asiente
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
