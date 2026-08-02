# Simetría del banco de pruebas — qué se igualó y qué no se puede igualar

Este documento existe porque la primera rampa comparativa (`results-comparison/20260801T163702Z`)
**no era justa**, y el defecto no se detectó desde dentro: lo señaló una pregunta directa
sobre por qué nuestro MQTT no estaba en modo cluster. Se registran aquí todas las
asimetrías encontradas para que cualquiera pueda auditar la comparación en vez de
confiar en ella.

Regla general: **toda diferencia entre los dos bancos que no sea la arquitectura bajo
prueba es un defecto de medición**, aunque nos favorezca — y varias nos favorecían.

---

## Asimetrías corregidas

### 1. Presupuesto de CPU desigual (3×) — la más grave

| | antes | ahora |
|---|---|---|
| ThingsFlow | 29 500 m de límites | perfil `fair-thingsflow.yaml` |
| ThingsBoard | 10 000 m de límites | perfil `fair-thingsboard.yaml` |

El método declaraba que los límites eran «generosos a propósito para que NO sean
vinculantes». No lo eran para ninguno de los dos:

- `tb-node` llegó a **3 974 m de 4 000 m (99,3 %)** en el nivel donde «falló».
- `nats-alarms` llegó a **1 360 m de 1 500 m (90,7 %)** en el nivel publicado como limpio.

Un techo medido bajo estrangulamiento CFS es el techo de la jaula, no de la plataforma.
El fallo de ThingsBoard a 8 000 msg/s por MQTT queda por tanto **retirado como
caracterización del producto**: no sabemos aún dónde está su techo real.

### 2. El heap de la JVM era la jaula invisible

`tb-node` corría con `-Xmx3g`, y su modo de fallo observado fue precisamente
**~1 095 000 datos atascados en heap** con `totalSaved 0`. Medir un desbordamiento de
un heap de 3 GiB y llamarlo «límite de ThingsBoard» es el mismo error con otro nombre.
Ahora: `-Xms4g -Xmx8g` en un contenedor de 12 GiB, y Kafka de `-Xmx3g` a `-Xmx6g`.

### 3. Frente MQTT: 1 nodo contra 2 — y la asimetría iba **en nuestra contra**

La rampa comparaba **un** `rmqtt-edge` con 1 000 m contra **dos** `tb-mqtt-transport`
con 1 500 m en total. El chart tenía un `fail()` que bloqueaba `replicas > 1` porque
el modo cluster nunca se había configurado, aunque el plugin `rmqtt-cluster-raft`
**sí viene en la imagen** `rmqtt/rmqtt:0.20.0`. Ahora: 3 nodos en cluster raft.

Nótese que esta asimetría nos perjudicaba. Se corrige igual: el objetivo es que la
medición sea correcta, no que gane nadie.

### 4. Camino de red distinto

ThingsFlow se alcanzaba por **ClusterIP** (`10.152.183.46:1883`) y ThingsBoard por
**NodePort** (`172.16.128.7:30188`). El NodePort añade un DNAT de kube-proxy y, con
`externalTrafficPolicy: Cluster`, puede añadir SNAT. Diferencia pequeña, pero
sistemática y siempre en la misma dirección. Ahora ambos por ClusterIP
(`fair-ramp.sh:clusterip`).

### 5. Una clave YAML mal escrita que Helm ignoró en silencio — el peor de todos

El perfil anterior configuraba NATS así:

```yaml
resources:
  nats:
    limits: { cpu: "3000m" }     # <- el chart NO lee esto
```

El chart lee **`nats.resources`**, no `resources.nats`. Helm no avisa de claves
desconocidas: las ignora. **NATS corrió toda la rampa con su valor por defecto de
500 m**, y de ahí salió lo que se publicó como el techo MQTT de ThingsFlow:

> «ThingsFlow MQTT @8 000 — NATS, 501 m de un límite de 500 m, estrangulado por CFS
> en 1 819 periodos»

Eso no era la plataforma. Era una clave mal escrita. Un override que no se aplica es
**peor** que no ponerlo, porque produce una medición que parece configurada y no lo está.

Ahora hay dos comprobaciones, y ninguna se fía del YAML:

- `scripts/check-values-keys.py` — heurística de preflight; compara las claves del
  perfil contra el `values.yaml` del chart y sugiere la permutación invertida. Tiene
  falsos positivos cuando la plantilla usa `toYaml` sobre el subárbol padre, así que
  se declara como «revisar», no como «roto».
- `scripts/verify-effective-limits.py` — la prueba de verdad: lee del **cluster ya
  desplegado** el límite que el kubelet aplicó y el consumo real, y exige 3× de
  holgura. Lo único que no miente es lo que está corriendo.

De paso, la misma revisión encontró que el chart de ThingsBoard usa `replicaCount`
y el perfil decía `replicas`: también se ignoraba, y pasó desapercibido porque
coincidía con el valor por defecto del chart. Así es como sobreviven estos fallos.

### 6. Un método declarado que nadie verificaba

La corrección de fondo no es ninguna de las anteriores: es que **la afirmación de que
los límites no ataban vivía solo en un comentario**. Ahora existe
`scripts/throttle-gate.py`, que lee el throttling CFS real del kernel (cgroup v2,
`cpu.stat`) por contenedor entre dos instantes y marca el nivel **INVÁLIDO** si algún
contenedor fue estrangulado.

El portón está probado en las dos direcciones — un portón que solo se ha visto en verde
no es evidencia de nada:

```
$ throttle-gate.py check thr-test snap.json
PORTÓN FALLIDO — 1 contenedor(es) estrangulados:
burner/4036c04b0abe    253/253    100.0%    22.8s    99m/100m
exit=1
```

---

## Simetrías ya verificadas (no había defecto)

- **Payload y QoS**: ambos objetivos comparten `PayloadBuilder` y publican con
  **QoS 1**. Mismo número de claves, mismos bytes, misma semántica de acuse.
- **Durabilidad de la cola**: se comprobó por el endpoint `jsz` de JetStream que los
  cuatro streams (`TF_RAW`, `TF_ENTITY`, `TF_ALARMS`, `KV_twin_state`) están
  **`storage=file`**. No estamos comparando una cola volátil nuestra contra un Kafka
  en disco. *(En producción sí se detectaron streams en memoria; es un problema
  distinto, ya registrado, y no afecta a esta medición.)*
- **Simetría de métricas**: ambos lados cuentan datos *aterrizados en el almacén*
  (`landed_count`), no solo aceptados — ThingsFlow contra GreptimeDB y ThingsBoard
  contra `ts_kv JOIN key_dictionary`. Sin esto, «aceptado» puede significar
  «encolado en un heap que luego se pierde».
- **Aislamiento**: solo una plataforma corre a la vez (`sut-isolate.sh`), incluidos
  los StatefulSets — un Kafka ocioso quema ~450 m que se le restarían al otro sistema.

---

## Lo que NO se puede igualar (y por qué se declara en vez de ocultarse)

- **Modelo de almacenamiento**: ThingsBoard escribe telemetría a Postgres
  (`ts_kv` + `key_dictionary`); ThingsFlow a GreptimeDB, una base columnar de series
  temporales. No es una diferencia de configuración: es la arquitectura bajo prueba.
  Configurar ThingsBoard con Cassandra cambiaría el resultado y merece medirse aparte.
- **Nodo único**: ni NATS ni Kafka ni GreptimeDB pueden replicarse de verdad con
  R3 en un nodo. Ambos corren con réplica 1 en sus almacenes, así que la comparación
  es simétrica, pero **ninguna de las dos cifras representa un despliegue productivo
  con alta disponibilidad**.
- **`flow-core` fijado a 1 réplica** por diseño (throttle de login en memoria y
  escritor de auditoría síncrono). No está en el camino caliente de telemetría, así
  que no acota el throughput medido, pero es una limitación real del sistema y no
  debe presentarse como decisión de benchmark.

---

## Cómo repetirlo

```bash
# perfiles con límites >= 3x el pico observado
helm -n tb-classic        upgrade tbc benchmarks/helm/thingsboard-cluster \
     -f benchmarks/profiles/fair-thingsboard.yaml
helm -n thingsflow-fresh  upgrade tf  k8s/helm/thingsflow \
     -f benchmarks/profiles/fair-thingsflow.yaml

# rampa con portón activo: ningún nivel se publica sin pasar la verificación
benchmarks/scripts/fair-ramp.sh
```

Los veredictos quedan en `results-fair/verdicts.tsv`, con el motivo de cada nivel
inválido. Un nivel marcado `THROTTLED` no dice nada sobre la plataforma.
