# Twin model fixture provenance

These three authored models pin the Phase 2 language to the SEM integration
without pretending that every model already exists in demo data.

- `energy_meter.json` follows `sem_transformer.py` `_transform_circuit`
  (lines 66-87) and the ThingsFlow publish projection (lines 186-201). The
  latter is why the authored attribute is `sem_device_id`, not `device_id`.
- Its location enum combines the YAML values in `circuit_config.yaml` lines
  8-24 with the hardcoded `main` and fallback `unknown` values in
  `sem_transformer.py` lines 66-74. Scaling factors and units come from YAML
  lines 34-38; property names come from the transformer.
- `main_total.json` reuses the electrical/energy wire shape and pins
  `circuit_index` 999 from `_transform_main_total` lines 89-101. That value is
  hardcoded Python behavior, not YAML-derived configuration.
- `building.json` is an intentionally new ASSET model for the legacy
  `Contains` relation. `flow-core/internal/bootstrap/demo.go` seeds its demo
  asset as type `default` and seeds only legacy `Contains` rows; it does not
  claim an existing building or panel model.

Current `_scale` always returns a number. Any all-null validator payload is
therefore synthetic and forward-looking, retained only to prove defensive
behavior for other producers and partial merges.
