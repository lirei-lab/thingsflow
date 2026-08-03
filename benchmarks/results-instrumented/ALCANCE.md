# Alcance y límites de esta medición

**Fecha:** 2026-07-31 · **Sujeto:** ThingsFlow **únicamente**

## Qué mide

Huella de recursos de ThingsFlow en cuatro regímenes —`idle`, `light` (50 dispositivos),
`moderate` (200) y `heavy` (500)— con muestreo de CPU y memoria por pod, profundidad de
streams NATS y CPU del host.

## Qué NO mide, y por qué importa

**No verifica que los datos hayan aterrizado.** Ningún escenario cuenta filas en el
almacén para confirmar que lo enviado se persistió. Una medición de recursos sin
verificación de pérdida no puede distinguir «el sistema procesó todo con X CPU» de «el
sistema descartó parte y por eso gastó X CPU».

En reposo y con carga ligera eso es poco relevante —hay poco o nada que perder— pero
significa que **los escenarios `moderate` y `heavy` no son afirmaciones de capacidad**.
No deben citarse como «ThingsFlow sostiene N dispositivos».

**No es comparativa.** No hay ThingsBoard aquí. Cualquier comparación necesita la rampa
de `benchmarks/results-fair/`, que sí iguala presupuestos, verifica el aterrizaje y
comprueba que ningún límite de CPU ató durante la ventana medida.

## Para qué sirve entonces

Como línea base de la huella en reposo y con carga baja, que es lo que mide bien. Ese
dato sí es sólido: en reposo no hay tráfico que perder, así que la ausencia de
verificación no lo compromete.

## Contexto

Se tomó antes de que existieran `throttle-gate.py` (comprobación de estrangulamiento CFS)
y el aislamiento entre niveles. Los defectos de método que se corrigieron después están
documentados en `benchmarks/FAIRNESS.md`.
