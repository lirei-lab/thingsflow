# ThingsFlow benchmarks

Self-measurement of ThingsFlow under sustained load: how many resources it needs to
sustain a given rate **with no errors and no silent loss**, and where it stops sustaining
it.

There are no comparisons with other platforms, and that is deliberate — see the end.

## The rule

A load level only counts as valid if all five conditions hold at the same time:

| condition | why |
|---|---|
| zero errors | obvious |
| **landed rows == accepted** | a 200 from ingest does not prove the data was stored |
| the generator was not the limit | otherwise you measure the client, not the platform |
| no container throttled | otherwise you measure the CPU cage |
| no consumer lagging | verifying only the history lets routes that fall behind slip through |

Any other combination is recorded as **INVALID with its reason**, and the reasons matter
as much as the verdicts: a level that fails because of its own resource cage says nothing
about the platform.

## Result

`results-fair/verdicts.tsv` — one level per row, with verdict and reason.

Latest run (single 16-core node, 2,000 devices, 180 s per level, 3 keys per message):

| path | rate sustained with no loss | p95 | CPU | memory |
|---|---|---|---|---|
| MQTT | **15,333 msg/s** | 4.4 ms | 8,958 m | 5,325 MiB |
| HTTP | **7,667 msg/s** | 8.3 ms | 9,160 m | 2,550 MiB |

**Two caveats that must always accompany those figures:**

1. **The MQTT ceiling was not found.** At 15,333 msg/s the node was at 80 %: the hardware
   ran out before the platform did.
2. **They are ingest and history-persistence figures, not freshness figures.** Above
   ~3,900 msg/s the latest-values writer falls behind while the history keeps arriving
   complete and correct. See [FINDING-twin-state.md](FINDING-twin-state.md): it matters
   because **it fails silently** — no errors, no gaps in the charts, just a frozen number
   on the dashboard.

## How to repeat it

```bash
helm -n <ns> upgrade <release> k8s/helm/thingsflow \
     -f benchmarks/profiles/fair-thingsflow.yaml
benchmarks/scripts/fair-ramp.sh
python3 benchmarks/scripts/summarize-fair.py benchmarks/results-fair
```

The profiles set CPU limits with at least 3× headroom over the observed peak, and
`verify-effective-limits.py` checks that **against the deployed cluster**, not against the
YAML: an override may fail to arrive, and it already has.

## Documents

- **[METHODOLOGY.md](METHODOLOGY.md)** — how we measure and the thirteen defects we found
  while measuring. It is the most reusable part of all this.
- **[FINDING-twin-state.md](FINDING-twin-state.md)** — the latest-values writer collapses
  4× earlier than the history writer. Cause still not identified: what was measured and
  what was ruled out is documented, not a comfortable explanation.
- **[MATRIX.md](MATRIX.md)** — scenarios and what is observed in each one.
- **[results-instrumented/SCOPE.md](results-instrumented/SCOPE.md)** — an earlier
  measurement, with its limits declared.

## On comparing with other platforms

It was attempted and **withdrawn**, data included. Publishing performance figures for
another company's product, measured by us, on our infrastructure, and with its storage
backend chosen by us, is not defensible no matter how clean the methodology ends up being:
anyone who uses that platform would say, rightly, that we picked its least favorable
configuration.

The detail that settled the decision: when we audited that comparison, **almost all the
asymmetries we found favored us**. It has an innocent explanation — we instrument the
system we know far better — and that is exactly why the result is not published.

## Artifact policy

What gets tracked in this directory, and what never does:

- **Tracked**: findings (`FINDING-*.md` / `HALLAZGO-*.md`), the scripts that produce the
  measurements, and curated evidence referenced from docs or findings — today ~29
  JSON/PNG files (~1.3MB; everything tracked in this directory totals ~2.4MB).
- **Never tracked**: raw per-run results, virtualenvs (`**/.venv/` is gitignored), and
  any file over the ceiling defined in `tools/python/test_repo_hygiene.py` (currently
  500KB).

The limit is enforced, not aspirational: `tools/python/test_repo_hygiene.py` runs in the
OSS release gate and fails CI on vendored Python artifacts or oversized tracked files.
Tracking a file above the ceiling requires allowlisting its exact path in that test, so the
decision is visible in the PR diff instead of slipping in silently.
