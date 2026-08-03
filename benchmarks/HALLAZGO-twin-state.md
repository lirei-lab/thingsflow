# El escritor de valores actuales satura 8× antes que el de histórico

**Descubierto:** 2026-08-03, rampa justa v4 · **Estado:** medido, causa probable identificada

## El hecho

| tasa MQTT | histórico (GreptimeDB) | valores actuales (twin state) |
|---|---|---|
| 1 000 msg/s | al día | al día (0 pendientes) |
| 2 000 msg/s | al día | al día (0 pendientes) |
| **4 000 msg/s** | al día, **2 070 000 filas exactas** | **10 147 pendientes al cierre** |
| 16 000 msg/s | al día | (pendiente de medir) |

A 4 000 msg/s el histórico está **completo y verificado** —las filas aterrizadas cuadran
exactamente con lo aceptado— mientras `thingsflow-latest-kv-durable` acumula retraso.

**No es falta de CPU.** Las tres réplicas usan 717 m de una cuota de 2 000 m (36 %) con
cero throttling. Tienen recursos de sobra y aun así no siguen el ritmo.

## Por qué importa en producción

El estado gemelo es lo que la UI muestra como **valor actual** de cada dispositivo. Un
retraso ahí significa que el operador ve lecturas viejas mientras el histórico está
perfecto. Es la peor forma de fallo: **nada parece roto**. Los gráficos históricos están
completos, no hay errores en los logs, la ingesta responde, y el número grande de la
pantalla lleva minutos sin moverse.

Ningún benchmark anterior lo detectó porque todos verificaban el aterrizaje **solo contra
GreptimeDB**. El nivel salía «limpio» con el histórico completo y el twin state 25×
atrás.

## Causa probable (hipótesis, no confirmada)

La diferencia entre los dos escritores del mismo flujo:

| | histórico | valores actuales |
|---|---|---|
| salida Bento | `http_client` | `nats_kv` |
| agrupación | `batching: count 1000, period 500ms` | **ninguna** |
| unidad de escritura | 1 petición HTTP = 1 000 datos | 1 ida y vuelta = 1 clave |
| CPU/pod a 4 000 msg/s | 317 m | 717 m |

El histórico manda mil datos por petición. El KV hace una operación por clave, y cada
mensaje se abre en abanico a una clave por métrica (`DEVICE.<tenant>.<device>.telemetry.<clave>`),
así que 4 000 msg/s con 3 claves son **12 000 operaciones KV por segundo**.

`max_in_flight: 1024` está configurado, lo que *debería* encauzarlas en paralelo. Que aun
así no siga el ritmo sugiere que la concurrencia efectiva es menor de lo declarado —por el
`unarchive` que serializa el abanico dentro del lote, o porque el `nats_kv` de Bento no
encauza tanto como dice—. **Esto no está confirmado y no debe presentarse como diagnóstico.**

## Qué falta por hacer

1. **Acotar el techo.** Se sabe que aguanta 2 000 y no 4 000. Falta medir entre medias.
2. **Confirmar la causa** antes de tocar nada: instrumentar la concurrencia real del
   `nats_kv`, o probar con `max_in_flight` bajado a 1 para ver si el rendimiento cambia
   (si no cambia, la concurrencia declarada nunca fue efectiva).
3. **Evaluar alternativas** solo después: una entrada KV por dispositivo con todas sus
   claves reduciría las operaciones al número de mensajes en vez de al número de claves,
   pero cambia el contrato de lectura y no debe hacerse a ciegas.

## Lo que NO se debe concluir todavía

- Que ThingsFlow «solo aguanta 2 000 msg/s». El **histórico** sostiene ≥16 000 msg/s
  verificados con cero pérdida. Lo que satura antes es una ruta concreta.
- Que basta con subir réplicas o CPU. Están al 36 % de su cuota: el cuello no es ese.
