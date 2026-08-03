# El escritor de valores actuales colapsa por congestión sobre ~3 900 msg/s

**Descubierto:** 2026-08-03, rampa justa v4
**Estado:** medido · **serializacion localizada en el consumidor Bento** · sin arreglo aplicado

## El hecho

Rendimiento real del escritor de twin state, calculado como
`(aceptados − pendientes al cierre del nivel) / duración`:

| ofrecido | procesa el KV | pendientes al cierre | CPU/pod KV | histórico |
|---:|---:|---:|---:|---|
| 958 msg/s | 958/s | 0 | 352 m | completo |
| 1 917 msg/s | 1 917/s | 0 | 507 m | completo |
| 3 833 msg/s | 3 777/s | 10 147 | 717 m | completo, 2 070 000 filas exactas |
| 7 667 msg/s | **3 964/s** | 666 456 | 622 m | completo, 4 140 000 filas exactas |
| 15 333 msg/s | **2 914/s** | 2 235 393 | 509 m | completo, 8 280 000 filas exactas |

Dos cosas, y la segunda es la grave:

1. **Satura sobre ~3 900 msg/s** (unas 11 700 operaciones KV/s a 3 claves por mensaje).
2. **Por encima de ese punto no se estanca: retrocede.** Al doblar la carga de 7 667 a
   15 333 msg/s el rendimiento **baja un 26 %**. Eso es colapso por congestión, no
   saturación: el sistema hace *menos* trabajo cuanto más se le pide.

**No es falta de CPU, y la propia CPU lo demuestra.** Las réplicas van al 36 % de su cuota
de 2 000 m sin throttling, y —lo revelador— **su CPU baja junto con el rendimiento**
(717 → 622 → 509 m). Menos CPU haciendo menos trabajo con más carga ofrecida significa
que el proceso está *esperando*, no computando.

En los cinco niveles el histórico aterrizó **completo y verificado**, con las filas
cuadrando exactamente con lo aceptado.

## Por qué importa en producción

El estado gemelo es lo que la UI muestra como **valor actual** de cada dispositivo. Un
retraso ahí deja al operador viendo lecturas viejas mientras el histórico está perfecto.
Es la peor forma de fallo: **nada parece roto**. Los gráficos históricos están completos,
no hay errores en los logs, la ingesta responde 200, y el número grande de la pantalla
lleva minutos sin moverse.

Y como colapsa en vez de degradarse suavemente, el problema **se agrava justo cuando más
carga hay** — el momento en que un operador más necesita ver datos frescos.

Ningún benchmark anterior lo detectó porque todos verificaban el aterrizaje **solo contra
GreptimeDB**. El nivel salía «limpio» con el histórico completo y el twin state atrás.

## Confirmado en dos vías de entrada independientes

El mismo techo aparece entrando por MQTT y por HTTP, que usan brokers, autenticación y
rutas de ingesta distintas. Solo comparten el consumidor:

| ofrecido | MQTT procesa | MQTT cpu/pod | HTTP procesa | HTTP cpu/pod |
|---:|---:|---:|---:|---:|
| 958/s | 958/s | 352 m | 958/s | 326 m |
| 1 917/s | 1 917/s | 507 m | 1 917/s | 340 m |
| 3 833/s | 3 777/s ✗ | 717 m | **3 833/s ✓** | 507 m |
| 7 667/s | 3 964/s ✗ | 622 m | 3 516/s ✗ | 515 m |

**Hipótesis descartada: que fuera específico del camino MQTT.** A 3 833 msg/s la vía HTTP
sigue el ritmo exacto y la MQTT se queda 56 msg/s corta, pero a 7 667 **las dos colapsan**
a un rendimiento equivalente (3 964 y 3 516/s). El techo es del escritor KV, no de la vía.

La vía MQTT es consistentemente un 20-40 % más cara por mensaje en el consumidor, lo que
la deja justo del lado malo del límite a 3 833. Coincide con que el mapeo decodifica el
JWT del `from_username` (base64url + `parse_json`) en cada mensaje MQTT, mientras que por
HTTP ese campo viene vacío y el decode se salta. **Pero eso no explica el colapso**: el
escritor de histórico paga exactamente el mismo decode y sostiene ≥16 000 msg/s.

## Qué se ha descartado ya

**Hipótesis: contención en NATS.** Si el escritor va bloqueado en idas y vueltas, debería
degradarse cuando NATS está más ocupado. **Los datos lo contradicen:**

| ofrecido | CPU NATS | CPU/pod KV | rendimiento KV |
|---:|---:|---:|---:|
| 3 833/s | 1 934 m | 717 m | 3 777/s |
| 15 333/s | **1 646 m** | 509 m | **2 914/s** |

NATS consume *menos* CPU en el nivel donde el KV colapsa, y está al 27 % de su cuota de
6 000 m. No está saturado. **Hipótesis descartada.**

**Descartado también:** CPU del consumidor, su cuota (36 %), throttling CFS (cero), y
número de réplicas (son 3 y todas van infrautilizadas).

## La diferencia estructural con el escritor que sí escala

| | histórico | valores actuales |
|---|---|---|
| salida Bento | `http_client` | `nats_kv` |
| agrupación | `batching: count 1000, period 500ms` | **ninguna** |
| unidad de escritura | 1 petición = 1 000 datos | 1 operación = 1 clave |
| CPU/pod a 15 333 msg/s | **733 m y subiendo** | 509 m y bajando |

El histórico es el **único componente que escala con la carga**, y es precisamente el que
agrupa en lotes. El KV hace una operación por clave: cada mensaje se abre en abanico a
`DEVICE.<tenant>.<device>.telemetry.<clave>`.

`max_in_flight: 1024` está configurado y *debería* encauzarlas en paralelo. **No lo
consigue**, y la sección siguiente lo mide.

## La concurrencia declarada es 1024. La efectiva es 1,4

Prueba ejecutada: mismo nivel de 8 000 msg/s, cambiando **solo** `max_in_flight`.

| `max_in_flight` | procesa el KV | pendientes al cierre | CPU/pod |
|---:|---:|---:|---:|
| 1024 | 4 058/s | 649 558 | 587 m |
| **1** | **2 936/s** | 851 492 | 480 m |

Bajar la concurrencia de 1024 a 1 cuesta **solo un 28 %** de rendimiento. Eso descarta
las dos hipótesis extremas a la vez:

- **No es que el ajuste no haga nada.** Hay diferencia medible, así que la concurrencia
  se aplica.
- **Pero no funciona como se declara.** Si 1024 operaciones fueran realmente paralelas,
  pasar a 1 debería hundir el rendimiento en órdenes de magnitud, no en un 28 %.

**Concurrencia efectiva = 4 058 / 2 936 ≈ 1,38×.** Se declaran 1024 y se obtiene menos
de 1,4. Ahí está el cuello: algo serializa las escrituras dentro del consumidor.

### Descartado: el almacén KV. Medido directamente

`nats bench --kv` contra un bucket limpio, **sin nuestra tubería en medio**:

| publicadores concurrentes | rendimiento |
|---:|---:|
| 1 | 4 634 ops/s |
| 8 | **31 568 ops/s** (6,8×, casi lineal) |

**El bucket KV paraleliza bien.** Aguanta ~31 500 operaciones/s mientras nuestra tubería
consigue unas 12 000 (4 058 msg/s × 3 claves). El almacén no es el cuello, y la
hipótesis de que un solo stream serializara las escrituras queda **descartada**.

### Dónde está entonces: dentro del consumidor

El dato que lo cierra: **cada pod de Bento rinde unas 4 000 ops/s — casi exactamente lo
que da UN publicador serial** (4 634/s). Con `max_in_flight: 1024` declarado.

Es decir, cada réplica se comporta como si emitiera las escrituras **de una en una**.
Eso encaja con todo lo medido: que subir la concurrencia declarada apenas ayude (+38 %),
que ampliar la ventana del consumidor no haga nada, y que añadir réplicas rinda
sublinealmente.

Candidato concreto a revisar en la tubería: el `unarchive` convierte un mensaje en N
(una por clave de telemetría), y esas N parecen despacharse en serie dentro del mismo
lote pese al `max_in_flight` global.

### Otras pruebas ejecutadas y su resultado

| prueba | resultado | conclusión |
|---|---|---|
| `max_in_flight` 1024 → 1 | 4 058 → 2 936/s (−28 %) | la concurrencia efectiva es ~1,4, no 1024 |
| réplicas 3 → 6 | 4 058 → 5 085/s (+25 %) | sublineal; por pod cae de 1 353 a 847/s |
| `maxAckPending` 1024 → 8192 | 4 058 → 4 101/s | **sin efecto**; la ventana del consumidor no ata |
| KV directo, 1 → 8 clientes | 4 634 → 31 568 ops/s | el almacén no es el cuello |

### Qué NO hacer todavía

Sustituir el modelo de una clave por entrada por uno de un documento por dispositivo
reduciría las operaciones al número de mensajes en vez de al de claves, y probablemente
resolvería el síntoma. **Pero cambia el contrato de lectura** de todo lo que consume twin
state, y hacerlo antes de confirmar la causa sería arreglar a ciegas algo que quizá se
resuelva con un ajuste de la tubería.

## Lo que NO se debe concluir

- **Que ThingsFlow «solo aguanta 3 900 msg/s».** El histórico sostiene ≥16 000 msg/s
  verificados con cero pérdida, y la ingesta acepta 15 333/s sin un solo error. Lo que
  colapsa es una ruta concreta.
- **Que haya pérdida de datos.** No la hay. El histórico está completo en los cinco
  niveles. Lo que se degrada es la *frescura* del valor actual, no su persistencia: los
  mensajes siguen en el stream y se procesan más tarde.
- **Que se arregle con más réplicas o más CPU.** Están al 36 % de su cuota y su consumo
  *baja* cuando el problema empeora.
