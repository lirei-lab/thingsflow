# Benchmarks de ThingsFlow

Automedición de ThingsFlow bajo carga sostenida: cuántos recursos necesita para sostener
una tasa dada **sin errores y sin pérdida silenciosa**, y dónde deja de sostenerla.

No hay comparaciones con otras plataformas, y es deliberado — ver el final.

## La regla

Un nivel de carga solo cuenta como válido si se cumplen las cinco condiciones a la vez:

| condición | por qué |
|---|---|
| cero errores | obvio |
| **filas aterrizadas == aceptadas** | un 200 en la ingesta no prueba que el dato se guardara |
| el generador no fue el límite | si no, se mide el cliente, no la plataforma |
| ningún contenedor estrangulado | si no, se mide la jaula de CPU |
| ningún consumidor retrasado | verificar solo el histórico deja pasar rutas que se quedan atrás |

Cualquier otra combinación se registra como **INVÁLIDO con su motivo**, y los motivos
importan tanto como los veredictos: un nivel que falla por su propia jaula de recursos no
dice nada sobre la plataforma.

## Resultado

`results-fair/verdicts.tsv` — un nivel por fila, con veredicto y motivo.

Última corrida (nodo único de 16 cores, 2 000 dispositivos, 180 s por nivel, 3 claves por
mensaje):

| vía | tasa sostenida sin pérdida | p95 | CPU | memoria |
|---|---|---|---|---|
| MQTT | **15 333 msg/s** | 4,4 ms | 8 958 m | 5 325 MiB |
| HTTP | **7 667 msg/s** | 8,3 ms | 9 160 m | 2 550 MiB |

**Dos salvedades que deben acompañar siempre a esas cifras:**

1. **El techo MQTT no se encontró.** A 15 333 msg/s el nodo iba al 80 %: se acabó el
   hardware antes que la plataforma.
2. **Son cifras de ingesta y persistencia histórica, no de frescura.** Por encima de
   ~3 900 msg/s el escritor de valores actuales se queda atrás mientras el histórico
   sigue llegando completo y correcto. Ver [HALLAZGO-twin-state.md](HALLAZGO-twin-state.md):
   importa porque **falla en silencio** — sin errores, sin huecos en las gráficas, solo
   un número congelado en el panel.

## Cómo repetirlo

```bash
helm -n <ns> upgrade <release> k8s/helm/thingsflow \
     -f benchmarks/profiles/fair-thingsflow.yaml
benchmarks/scripts/fair-ramp.sh
python3 benchmarks/scripts/summarize-fair.py benchmarks/results-fair
```

Los perfiles fijan límites de CPU con al menos 3× de holgura sobre el pico observado, y
`verify-effective-limits.py` lo comprueba **contra el cluster desplegado**, no contra el
YAML: un override puede no llegar, y ya pasó.

## Documentos

- **[METODOLOGIA.md](METODOLOGIA.md)** — cómo se mide y los trece defectos que
  encontramos midiendo. Es lo más reutilizable de todo esto.
- **[HALLAZGO-twin-state.md](HALLAZGO-twin-state.md)** — el escritor de valores actuales
  colapsa 4× antes que el de histórico. Causa aún sin identificar: se documenta lo
  medido y lo descartado, no una explicación cómoda.
- **[MATRIX.md](MATRIX.md)** — escenarios y qué se observa en cada uno.
- **[results-instrumented/ALCANCE.md](results-instrumented/ALCANCE.md)** — medición
  anterior, con sus límites declarados.

## Sobre comparar con otras plataformas

Se intentó y **se retiró** (ver `results-comparison/*/RETRACTADO.md`). Publicar cifras de
rendimiento del producto de otra empresa, medidas por nosotros, en nuestra
infraestructura y con su backend de almacenamiento elegido por nosotros, no es defendible
por muy limpia que quede la metodología: quien use esa plataforma diría, con razón, que
elegimos su configuración menos favorable.

El detalle que zanjó la decisión: al auditar aquella comparación, **casi todas las
asimetrías encontradas nos favorecían**. Tiene una explicación inocente —instrumentamos
mucho mejor el sistema que conocemos— y es exactamente por eso que el resultado no se
publica.
