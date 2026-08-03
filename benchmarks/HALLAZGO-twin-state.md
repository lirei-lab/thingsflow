# El escritor de valores actuales colapsa por congestión sobre ~3 900 msg/s

**Descubierto:** 2026-08-03, rampa justa v4
**Estado:** medido y cuantificado · **causa NO identificada** · sin arreglo aplicado

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

`max_in_flight: 1024` está configurado y *debería* encauzarlas en paralelo. Que no lo
consiga apunta a que la concurrencia efectiva es menor que la declarada, pero **eso no
está comprobado y no debe presentarse como diagnóstico.**

## Siguiente paso para encontrar la causa

Bajar `max_in_flight` de 1024 a 1 y volver a medir el nivel de 8 000 msg/s. Es una prueba
que discrimina:

- Si el rendimiento **cae**, la concurrencia sí era efectiva y el problema está en otra
  parte.
- Si **no cambia**, la concurrencia declarada nunca se aplicó, y ahí está la causa.

Solo después tiene sentido evaluar alternativas —por ejemplo una entrada KV por
dispositivo con todas sus claves, que reduciría las operaciones al número de mensajes en
vez de al número de claves— porque eso cambia el contrato de lectura y no debe hacerse a
ciegas.

## Lo que NO se debe concluir

- **Que ThingsFlow «solo aguanta 3 900 msg/s».** El histórico sostiene ≥16 000 msg/s
  verificados con cero pérdida, y la ingesta acepta 15 333/s sin un solo error. Lo que
  colapsa es una ruta concreta.
- **Que haya pérdida de datos.** No la hay. El histórico está completo en los cinco
  niveles. Lo que se degrada es la *frescura* del valor actual, no su persistencia: los
  mensajes siguen en el stream y se procesan más tarde.
- **Que se arregle con más réplicas o más CPU.** Están al 36 % de su cuota y su consumo
  *baja* cuando el problema empeora.
