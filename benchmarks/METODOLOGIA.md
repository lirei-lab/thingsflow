# Cómo medimos, y los trece defectos que encontramos midiendo

Este documento es el residuo útil de cuatro intentos de medir ThingsFlow. Ninguno de los
tres primeros produjo datos publicables, y en cada caso el motivo fue el mismo: **el
instrumental mentía de una forma distinta, y siempre en silencio.**

Se publica porque el catálogo de formas de engañarse midiendo vale más que cualquier
tabla de resultados.

---

## Las tres condiciones para que un nivel cuente

Un nivel de carga solo se considera válido si se cumplen las tres a la vez:

1. **Cero errores y cero pérdida.** No basta con que la ingesta devuelva 200: se cuentan
   las filas que **aterrizaron en el almacén** y deben cuadrar exactamente con las
   aceptadas.
2. **El generador no fue el cuello de botella.** Si el cliente se quedó sin conexiones o
   sin margen, el techo medido es suyo, no de la plataforma.
3. **Ningún límite de CPU ató durante la ventana.** Se lee el throttling CFS real del
   kernel por contenedor. Un nivel estrangulado por su propio límite mide la jaula.

Y una cuarta que añadimos tarde, después de que su ausencia nos costara un hallazgo:

4. **Ningún consumidor se quedó atrás.** Verificar solo el almacén histórico deja pasar
   como «limpio» un nivel en el que otra ruta del mismo flujo acumula retraso.

---

## Los defectos que encontramos

### En el propio instrumental

| Defecto | Cómo se manifestaba |
|---|---|
| **Promediar CPU dividiendo por `nr_periods`** | `nr_periods` solo avanza cuando el cgroup tiene tareas ejecutables, así que un contenedor a ráfagas sale inflado. Dentro de una misma ventana las «duraciones» derivadas iban de 0,2 s a 177,9 s cuando deben ser idénticas. Lo delató una imposibilidad física: un consumidor gastando más CPU con ocho veces menos carga |
| **Una corrida vacía marcada como LIMPIA** | Cero errores porque no hubo intentos, cero pérdida porque no había qué perder, y el resto de guardas saltadas por valores nulos. Diez niveles inexistentes registrados como válidos |
| **Contaminación entre niveles** | Un consumidor arrastraba 1 823 148 mensajes del nivel anterior y quemaba 2 282 m drenándolos. El nivel medido encima reportó un 64 % más de CPU de la que le correspondía |
| **El banco inflando su propio estado** | El prefijo de clave llevaba un sello por ejecución, así que cada nivel escribía 6 000 claves KV nuevas en vez de sobrescribir. Tras seis corridas: 35 999 entradas donde debía haber 6 000. Podía fabricar veredictos falsos de «consumidor retrasado» |
| **Un parser leyendo el campo equivocado** | Buscaba la primera clave `accepted` en cualquier nivel del JSON y encontraba un cubo de un segundo: calculaba el veredicto sobre 125 mensajes en vez de 32 500 |
| **El informe describiendo mal su propio método** | La cadena que nombra la verificación de aterrizaje estaba cableada a un único almacén, así que una corrida contra otro salía etiquetada como contada donde no era |
| **`landed = None` colapsado con «sin pérdida»** | Una corrida sin verificar pasaba como válida |
| **Repetir un nivel sumaba las filas del anterior** | 33 744 esperadas dieron 67 488 |
| **El cliente muriendo en un punto fijo** | El servidor cierra la conexión HTTP tras ~100 peticiones; el pool no se reponía. Con 512 conexiones el cliente moría a las **51 200 peticiones exactas** y lo reportaba como saturación de la plataforma |
| **Una guarda que se saltaba a sí misma** | `curl -w '%{http_code}'` imprime `000` al fallar **y además** sale con error, así que `$(curl ... \|\| echo 000)` concatenaba a `"000000"` — distinto de `"000"`, y la comprobación pasaba |
| **Un portón que castigaba un modelo de hilos** | Contar períodos estrangulados en absoluto penaliza a la JVM, cuyas ráfagas de GC superan la cuota en algún período de 100 ms aunque el promedio esté al 43 %. Invalidaba corridas que habían aterrizado todo sin pérdida |

### En la configuración, no en el código

| Defecto | Consecuencia |
|---|---|
| **Una clave YAML que Helm ignora en silencio** | El perfil escribía `resources.nats` y el chart lee `nats.resources`. Helm no avisa: NATS corrió toda una rampa con su valor por defecto, y ese techo se publicó como límite de la plataforma. **Un override que no se aplica es peor que no ponerlo**: produce una medición que parece configurada y no lo está |
| **Un script de aislamiento pisando a Helm** | Guardaba las réplicas en una anotación antes de escalar a cero y al restaurar escribía el valor viejo, deshaciendo cualquier cambio hecho en medio. Invisible para el detector de claves, porque la clave existía y sí llegó |

---

## Las herramientas que quedaron

Todas nacieron de un defecto concreto, y todas están probadas **en las dos direcciones**
— una comprobación que solo se ha visto en verde no es evidencia de nada.

- **`scripts/throttle-gate.py`** — lee el throttling CFS del kernel por contenedor entre
  dos instantes y clasifica la severidad. Probado contra un pod estrangulado a propósito:
  `253/253 períodos, 100 %, exit=1`.
- **`scripts/verify-effective-limits.py`** — exige que cada contenedor tenga al menos 3×
  de holgura sobre su consumo, **leyéndolo del cluster desplegado**. Lo único que no
  miente es lo que el kubelet aplicó.
- **`scripts/check-values-keys.py`** — detecta overrides que no llegan al chart. Es una
  heurística y lo declara: da falsos positivos cuando la plantilla vuelca un subárbol con
  `toYaml`.
- **`scripts/fair-ramp.sh`** — la rampa: espera a que la plataforma responda de verdad
  (no a que sus pods estén `Running`), aísla cada nivel del anterior, y registra el
  veredicto con su motivo.

---

## Lo que no se puede igualar, y por eso se declara

- **Nodo único de 16 cores.** Ningún almacén puede replicarse de verdad. Las cifras
  **no representan un despliegue productivo con alta disponibilidad**.
- **`flow-core` fijado a 1 réplica** por diseño (throttle de login en memoria y escritor
  de auditoría síncrono). No está en el camino caliente de telemetría, así que no acota
  el throughput medido, pero es una limitación real y no una decisión de banco de pruebas.
- **GreptimeDB corre con `sync_write = false`** (verificado en el `/config` del proceso
  vivo). No hacemos `fsync` por escritura. Parte de la latencia baja es durabilidad
  renunciada, y decirlo importa. Matiz: el acuse al dispositivo ocurre tras persistir en
  JetStream, que sí está en fichero, así que la ventana de riesgo es entre el WAL y el
  disco, no el mensaje entero.

---

## Sobre comparar con otras plataformas

Se intentó y **se retiró**. Publicar cifras de rendimiento del producto de otra empresa,
medidas por nosotros, en nuestra infraestructura y con su backend de almacenamiento
elegido por nosotros, no es defendible por muy limpia que quede la metodología.

El detalle que zanjó la decisión: al auditar aquella comparación, **casi todas las
asimetrías encontradas nos favorecían**. Tiene una explicación inocente —instrumentamos
mucho mejor el sistema que conocemos— y es precisamente por eso que el resultado no era
publicable.
