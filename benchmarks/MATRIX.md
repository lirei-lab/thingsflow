# Matriz de escenarios

Escenarios de medición de ThingsFlow y qué se observa en cada uno. La regla de validez y
los defectos de método están en [METODOLOGIA.md](METODOLOGIA.md).

## Sujeto

| Objetivo | Pila |
|---|---|
| `thingsflow` | RMQTT (cluster raft de 3 nodos), ingest HTTP (Envoy + Bento), NATS JetStream, materializadores Bento, GreptimeDB, Postgres, Flow Core |

ThingsFlow se mide como middleware orientado a eventos: Flow Core es el plano de control
y el plano de datos es el camino de la telemetría. Flow Core **no está en el camino
caliente**, y por eso su réplica única no acota el throughput.

## Rampa de capacidad

El escenario principal. Se ofrece carga creciente hasta que un nivel deja de ser válido.

| Parámetro | Valor |
|---|---|
| Tasas | 1 000 → 2 000 → 4 000 → 8 000 → 16 000 msg/s |
| Dispositivos | 2 000 |
| Duración por nivel | 180 s de carga estable + 45 s de precalentamiento descartado |
| Payload | 3 claves de telemetría por mensaje |
| Protocolos | MQTT y HTTP, medidos por separado |
| Asentamiento | 30 s antes de contar filas aterrizadas |

Entre niveles se **vacían los streams y el bucket KV**: sin eso, el trabajo pendiente de
un nivel se carga al siguiente y la medición mide el experimento anterior.

La duración no es arbitraria: 180 s más el precalentamiento superan los 300 s de caché
de JWKS de Envoy, de modo que la rampa atraviesa al menos un refresco. Una corrida de
60 s pasaría en verde sin ejercitar ese camino.

## Escenarios de huella

| Escenario | Dispositivos | Propósito |
|---|---:|---|
| `idle` | 0 | Coste en reposo de la plataforma completa |
| `light` | 50 | Huella con carga baja sostenida |

Miden consumo, no capacidad. La distinción importa: sin verificación de aterrizaje, un
escenario de carga alta **no es una afirmación de capacidad** (ver
[results-instrumented/ALCANCE.md](results-instrumented/ALCANCE.md)).

## Qué se observa

| Métrica | Cómo |
|---|---|
| Aceptados y errores por tipo | del generador |
| **Filas aterrizadas en el almacén** | `count(*)` con el prefijo de la corrida; debe cuadrar exactamente |
| **Retraso de consumidores** | mensajes pendientes por consumidor al cierre del nivel |
| Latencia de cliente | p50 / p95 / p99 |
| CPU y memoria por contenedor | leídas del kernel (cgroup v2), no muestreadas |
| **Throttling CFS** | períodos estrangulados por contenedor durante la ventana |
| CPU del propio generador | para detectar cuándo el cliente es el límite |

Las tres en negrita son las que distinguen esta matriz de una que solo cuenta acuses.
Cada una nació de un fallo real que las otras no detectaron:

- **Filas aterrizadas** — un 200 en la ingesta no prueba que el dato se guardara.
- **Retraso de consumidores** — verificar solo el histórico deja pasar como limpio un
  nivel donde otra ruta del mismo flujo acumula retraso sin errores ni huecos visibles.
- **Throttling** — un nivel estrangulado por su propio límite de CPU mide la jaula.

## Publicación

Los resultados publicables van en `results-fair/`. Las corridas que contienen detalles
operativos del cluster, o mediciones de otras plataformas, quedan fuera del repositorio
público — ver las notas en `.gitignore`.
