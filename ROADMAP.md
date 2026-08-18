# ThingsFlow — One-Document-Per-Device Twin-State — Roadmap

## Milestones

| # | Milestone | Phases | Goal | Status |
|---|-----------|--------|------|--------|
| 1 | Public Release Polish | 1-5 | Instalable/operable por externos solo con lo publicado | Archived (2026-08-06) — `.planning/milestones/MILESTONE-1.md` |
| 2 | Twin-State Writer Scalability | 1-3 | Techo latest-KV ≥8.000 msg/s config-only | Archived (2026-08-13) — partially achieved: techo config-only alcanzado, target NO cumplido, causa localizada (`unarchive` fan-out). `.planning/milestones/MILESTONE-2.md` |
| 3 | Ditto-Style Twin Layer | 1-6 | Paridad con el núcleo Ditto + twin-graph estilo Azure DT, contrato TB y hot path intactos | Archived (2026-08-11) — `.planning/milestones/MILESTONE-3.md` |
| 4 | One-Document-Per-Device Twin-State | 1-3 | Eliminar la serialización del `unarchive` fan-out: latest-KV ≥8.000 msg/s con un documento por device, read contract unificado | **In Progress** (2026-08-15) |

## Phases

- [x] Phase 1: Spike the Merge Write Path *(review passed 2026-08-17, 2 cycles)*
- [ ] Phase 2: Unify the Read Contract
- [ ] Phase 3: Flip, Verify and Pin

## Phase Details

### Phase 1: Spike the Merge Write Path
**Goal**: Probar en el cluster de test que un merge writer config-only (Bento `branch` + `nats_kv` cache + CAS `expected_last_subject_seq` + retry, consumer single-replica) es race-safe y alcanza **≥8.000 msg/s sostenidos** con estado mergeado correcto — el gate que decide el enfoque Balanced vs el fallback Conservative.
**Requirements**: R1
**Recommended Agents**: engineering-infrastructure-devops, testing-performance-benchmarker, testing-qa-verification-specialist
**Success Criteria**:
- [ ] Consumer Bento config-only (sin Go custom) que lee el doc actual del device, mergea el batch nuevo (LWW por clave), escribe con CAS y reintenta en conflicto
- [ ] Consumer latest-KV single-replica (carreras cross-replica eliminadas); `LATEST_KV_MAX_IN_FLIGHT`-style parametrización para medibilidad
- [ ] Medido en fair-ramp: ≥8.000 msg/s sostenidos, cero claves perdidas, estado mergeado correcto, cero CFS throttling, 10 niveles GATE OK
- [ ] Verdict explícito: GO → Fase 2 (read-contract unification); FAIL → fallback Conservative (pre-split upstream) documentado con evidencia
- [ ] `$KV.` publish semantics re-verificadas para el doc write (read-back, watcher, write-twice)
**Plans**: 3

### Phase 2: Unify the Read Contract
**Goal**: Hacer el documento por device autoritativo en `twinstore` (doc-first reads) manteniendo fallback per-key durante la transición, y verificar los 5 read surfaces equivalentes en vivo — sin romper la interfaz `Store`.
**Requirements**: R2
**Recommended Agents**: engineering-senior-developer, engineering-infrastructure-devops, testing-qa-verification-specialist
**Success Criteria**:
- [ ] `GetLatestTelemetry` doc-first (1 `kv.Get` del documento), fallback per-key retenido durante la transición
- [ ] `GetTelemetryKeys` derivado de las claves `Telemetry` del documento (sin prefix-scan `kv.Keys()`)
- [ ] `Watch` doc-first; rama per-key `ParseTelemetryKey` removida tras el flip
- [ ] Interfaz `twinstore.Store` sin cambios de API (consumidores no tocan su código de llamada)
- [ ] 5 read surfaces verificados byte-por-byte equivalentes en vivo: WS (`ws.go` 830, 1791), twin projection (`twin.go` 435), deviceactivity (`activity.go` 34), entityquery (`entityquery.go` 393, 398), telemetry reader (`reader.go` 151, 369)
**Plans**: 2

### Phase 3: Flip, Verify and Pin
**Goal**: Cambiar el data plane a doc-only writes, deprecar las per-key entries, re-correr el fair-ramp a los 5 niveles × 2 protocolos y fijar los nuevos floors medidos.
**Requirements**: R3
**Recommended Agents**: testing-performance-benchmarker, testing-qa-verification-specialist
**Success Criteria**:
- [ ] Data plane flip a doc-only writes; per-key entries deprecadas (TTL o purge verificado, no destructivo sin verificación)
- [ ] Fair-ramp re-run: 5 niveles × 2 protocolos, ≥8.000 msg/s sostenidos, pending drenado al cierre de nivel, historial completo
- [ ] `## Post-Fix` section añadida a `benchmarks/FINDING-twin-state.md` (original preservado byte-identical; `verdicts.tsv` append-only)
- [ ] Floors de CPU re-pinned solo contra jaula de topología coincidente (lección Milestone 2 Fase 3 BLOCKED); nunca por debajo de lo medido
- [ ] Baseline `TF_TWIN_EVENTS` aislado/contabilizado (compartido desde Milestone 3)
**Plans**: 1

## Progress

| Phase | Plans | Completed | Status |
|-------|-------|-----------|--------|
| 1. Spike the Merge Write Path | 3 | 3 | **Complete** (2026-08-17, review passed 2 cycles) — CAS variant INFEASIBLE (01-01); Conservative pre-split measurement BLOCKED on environment (01-02); **single-writer merge FEASIBLE (01-03, 2/2 PASS)** — Balanced re-opened; throughput/sharding open. Finding: `benchmarks/FINDING-twin-state-merge-spike.md` (incl. `## Option A` section) |
| 2. Unify the Read Contract | 2 | 1 | **In progress** (2026-08-17) — 02-01 doc-first `GetLatestTelemetry`/`GetTelemetryKeys` implemented + unit-tested (full suite green, 39 packages); 02-02 live verification of 5 read surfaces pending (needs test-cluster flow-core redeploy) |
| 3. Flip, Verify and Pin | 1 | 0 | Not started |
