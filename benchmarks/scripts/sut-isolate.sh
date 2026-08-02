#!/usr/bin/env bash
# Aísla un sistema bajo prueba escalando el otro a cero.
#
# Por qué: ambas plataformas viven en el mismo nodo de 16 cores. En reposo
# TB Classic quema ~1 core de forma continua (2 JVM de tb-node + Kafka) y
# ThingsFlow ~0.1. Medir una mientras la otra respira mete ese ruido en el
# resultado y, peor, le quita CPU al sistema que sí se está midiendo.
#
# Uso:
#   sut-isolate.sh thingsflow   -> TB a 0, ThingsFlow arriba
#   sut-isolate.sh tb           -> ThingsFlow a 0, TB arriba
#   sut-isolate.sh both         -> ambos arriba (solo para la línea base en reposo)
#
# Los StatefulSets TAMBIÉN se escalan a cero. Medido: Kafka ocioso quema
# ~450m (2.8% del nodo) aun sin ningún tb-node vivo, y ese ruido entra
# directo en la medición del otro sistema. Vuelven a subir con su PVC
# intacto, así que no se pierde estado.

set -euo pipefail
K="kubectl --context=microk8s"
TF_NS=thingsflow-fresh
TB_NS=tb-classic

scale_deploys() {  # ns, up|down  — cubre Deployments y StatefulSets
  local ns="$1" mode="$2"
  if [[ "$mode" == "down" ]]; then
    $K -n "$ns" get deploy,statefulset -o name | while read -r d; do
      cur=$($K -n "$ns" get "$d" -o jsonpath='{.spec.replicas}')
      # No sobrescribir la anotación cuando ya está a 0: un segundo "down"
      # grabaría restore-replicas=0 y el "up" dejaría el sistema apagado.
      if [[ "$cur" != "0" ]]; then
        $K -n "$ns" annotate "$d" bench.restore-replicas="$cur" --overwrite >/dev/null
      fi
      $K -n "$ns" scale "$d" --replicas=0 >/dev/null
    done
  else
    $K -n "$ns" get deploy,statefulset -o name | while read -r d; do
      local r
      r=$($K -n "$ns" get "$d" -o jsonpath='{.metadata.annotations.bench\.restore-replicas}' 2>/dev/null || true)
      [[ -n "$r" && "$r" != "0" ]] && $K -n "$ns" scale "$d" --replicas="$r" >/dev/null || true
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
  echo "AVISO: quedan $n pods de deployment en $ns tras esperar" >&2
}

case "${1:-}" in
  thingsflow) scale_deploys "$TB_NS" down; wait_gone "$TB_NS"; scale_deploys "$TF_NS" up ;;
  tb)         scale_deploys "$TF_NS" down; wait_gone "$TF_NS"; scale_deploys "$TB_NS" up ;;
  both)       scale_deploys "$TF_NS" up;  scale_deploys "$TB_NS" up ;;
  *) echo "uso: $0 {thingsflow|tb|both}" >&2; exit 2 ;;
esac

echo "aislamiento aplicado: $1"
$K top nodes --no-headers 2>/dev/null | awk '{printf "nodo: CPU=%s (%s) MEM=%s (%s)\n", $2,$3,$4,$5}'
