# El escritor de valores actuales colapsa por congestión sobre ~3 900 msg/s

**Descubierto:** 2026-08-03, rampa justa v4 · **Estado:** medido, causa probable identificada

## El hecho

Rendimiento real del escritor de twin state, calculado como
`(aceptados − pendientes al cierre) / duración`:

| ofrecido | procesa el KV | pendientes al cierre | histórico |
|---:|---:|---:|---|
| 958 msg/s | 958/s | 0 | completo |
| 1 917 msg/s | 1 917/s | 0 | completo |
| 3 833 msg/s | 3 777/s | 10 147 | completo, 2 070 000 filas exactas |
| 7 667 msg/s | **3 964/s** | **666 456** | completo, 4 140 000 filas exactas |

Con el doble de carga ofrecida procesa **lo mismo**: 3 964/s frente a 3 777/s. Es una
**meseta**, no una degradación progresiva — la firma de un cuello de botella duro.

Son unas **11 900 operaciones KV por segundo** (3 claves por mensaje). El escritor de
histórico sostiene ≥16 000 msg/s = 48 000 datos/s por la misma tubería: un factor de **4×**.

En los cuatro niveles el histórico aterrizó **completo y verificado**, con las filas
cuadrando exactamente con lo aceptado. La pérdida no existe; el desfase sí.

**No es falta de CPU.** Las tres réplicas usan 717 m de una cuota de 2 000 m (36 %) con
cero throttling. Tienen recursos de sobra y aun así se estancan.

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

## No es una meseta: es colapso por congestión

El quinto nivel cambia el diagnóstico. El rendimiento no se estanca — **cae**:

| ofrecido | procesa el KV | CPU/pod KV | CPU NATS | CPU/pod histórico |
|---:|---:|---:|---:|---:|
| 1 917/s | 1 917/s | 507 m | 1 480 m | 250 m |
| 3 833/s | 3 777/s | 717 m | 1 934 m | 314 m |
| 7 667/s | 3 964/s | 622 m | 1 877 m | 449 m |
| **15 333/s** | **2 914/s** | **509 m** | **1 646 m** | **733 m** |

Al doblar la carga de 7 667 a 15 333 msg/s el rendimiento **baja un 26 %**, de 3 964 a
2 914/s. Y la CPU baja con él: 717 → 622 → 509 m. **Menos CPU haciendo menos trabajo con
más carga ofrecida** significa que el proceso está *esperando*, no computando.

### Una hipótesis probada y DESCARTADA

La explicación obvia era contención en NATS: si el escritor de KV va bloqueado en idas y
vueltas, debería degradarse cuando NATS está más ocupado. **Los datos lo contradicen.**
La CPU de NATS también baja en el nivel de 15 333/s (1 646 m, el 27 % de su cuota de
6 000 m) — menos que a 3 833/s. NATS no está saturado.

El único componente que escala con la carga es el escritor de histórico (250 → 733 m),
que es precisamente el que agrupa.

**La causa sigue sin identificarse.** No es CPU del consumidor, no es su cuota, y no es
saturación de NATS. Cualquier arreglo que se intente sin averiguar qué es sería a ciegas.

## Qué falta por hacer

1. ~~Acotar el techo.~~ **Hecho: ~3 950 msg/s**, meseta confirmada con dos puntos por
   encima de la saturación (3 777/s con 3 833 ofrecidos, 3 964/s con 7 667 ofrecidos).
2. **Confirmar la causa** antes de tocar nada. La hipótesis de contención en NATS ya se
   probó y quedó DESCARTADA (NATS baja de CPU justo cuando el KV colapsa). Lo siguiente:
   bajar `max_in_flight` a 1 y ver si el rendimiento cambia — si no cambia, la
   concurrencia declarada nunca fue efectiva y ahí está el problema.
3. **Evaluar alternativas** solo después: una entrada KV por dispositivo con todas sus
   claves reduciría las operaciones al número de mensajes en vez de al número de claves,
   pero cambia el contrato de lectura y no debe hacerse a ciegas.

## Lo que NO se debe concluir todavía

- Que ThingsFlow «solo aguanta 3 950 msg/s». El **histórico** sostiene ≥16 000 msg/s
  verificados con cero pérdida, y la ingesta acepta 7 667/s sin un solo error. Lo que
  satura antes es una ruta concreta: la de valores actuales.
- Que haya pérdida de datos. **No la hay.** El histórico está completo en los cuatro
  niveles. Lo que se degrada es la *frescura* del valor actual, no su persistencia: los
  mensajes siguen en el stream y se procesan más tarde.
- Que basta con subir réplicas o CPU. Están al 36 % de su cuota: el cuello no es ese.
