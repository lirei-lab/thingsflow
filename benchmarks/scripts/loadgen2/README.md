# loadgen2 — open-loop, CPU-aware IoT load generator

A load generator for head-to-head benchmarking of **ThingsFlow** and **ThingsBoard**
that offers the load you asked for, and says so loudly when it cannot.

## Why a second generator

`benchmarks/scripts/instrumented-bench.py` is a **closed-loop** client: one request
in flight per device thread, and the next send waits for the previous response. As
soon as server latency rises, such a client stops offering the target rate — and
reports whatever it managed as if it were the platform's limit.

Measured, on this cluster: a **1000 msg/s** target actually offered **~385 msg/s**
and reported **234 msg/s** as a platform number. Nothing in the output said the
generator had backed off.

loadgen2 is built around three rules:

| Rule | How it is enforced |
|---|---|
| **Open loop** | Sends are scheduled off wall time (`stats.Schedule`), never off completions. A slow server grows the measured **lag**; it does not shrink the offered rate. |
| **Confess, don't absorb** | Every message the client could not place on the wire is counted: `schedule_deficit`, `blocked_inflight`, `behind_schedule`, `max_lag_ms`, `mqtt_queue_full`. The report carries a `client_was_bottleneck` verdict with reasons. |
| **Know your own ceiling** | `calibrate` ramps the rate against a trivial local sink and reports the highest rate **this host** can generate, and the CPU it took. Any platform number above that ceiling is not a platform number. |

## Install and run

Dependencies are bootstrapped into a local `.venv` on first use (uses `uv` if
present, otherwise `python -m venv`). `.venv/` is already gitignored.

```bash
cd benchmarks/scripts/loadgen2
./run.sh --selftest                 # offline unit tests, no cluster needed
./run.sh run --help
./run.sh calibrate --help
```

### Typical invocations

```bash
# 1. What can this host generate? (do this before designing a ramp)
./run.sh calibrate --protocol mqtt --devices 200 --max-workers 4 \
    --calibrate-rates 10000 20000 40000 80000 100000 140000 \
    --calibrate-step-seconds 15 --sink-workers 5

# 2. A real run against ThingsFlow, MQTT, with landed-row verification
./run.sh run --target thingsflow --protocol mqtt \
    --devices 100 --rate 500 --duration 60 --ramp 5 \
    --api-base http://<cluster-ip>:8080 \
    --ingest-base http://<cluster-ip>:8081 \
    --greptime-base http://<cluster-ip>:4000 \
    --mqtt-host <cluster-ip> --mqtt-port 1883 \
    --verify-landed --settle-seconds 30 --cleanup \
    --out results/tf-mqtt-500.json

# 3. The same shape against ThingsBoard classic (ACCESS_TOKEN credentials)
./run.sh run --target thingsboard --protocol http \
    --api-base http://<tb-node>:8080 --ingest-base http://<tb-http-transport>:8081 \
    --devices 100 --rate 500 --duration 60 --cleanup

# 4. Pure scheduler ceiling, no network, no sink
./run.sh calibrate --dry-run --protocol http --devices 200 \
    --calibrate-rates 100000 200000 400000 600000
```

## Targets (the adapter boundary)

Both platforms are first-class. A target owns provisioning **and** credentials
**and** endpoint shaping; the senders receive fully-resolved `DeviceEndpoint`
records and know nothing about either platform (`targets.py`).

| | ThingsFlow | ThingsBoard |
|---|---|---|
| credential | device JWT (ES256), `POST /api/device/{id}/jwt` | ACCESS_TOKEN, `GET /api/device/{id}/credentials` |
| HTTP | `POST /api/v1/telemetry`, `Authorization: Bearer <deviceJwt>` | `POST /api/v1/<token>/telemetry` |
| MQTT username | the device JWT (`--tf-mqtt-username-mode bearer` for the `Bearer …` spelling) | the access token |
| MQTT client id | **must** equal the JWT's `mqttId`/`clientid` claim — rmqtt rejects the CONNECT otherwise, and the ACL scopes publishes to `%c` | free |
| MQTT topic | `thingsflow/devices/<mqttId>/telemetry` | `v1/devices/me/telemetry` |
| landed check | yes — GreptimeDB `device_telemetry_kv` | not implemented (reported as unavailable, never faked) |

A third target, `sink`, points at the local calibration sinks and exists only to
measure the generator itself.

## Transport engines

`--engine raw` (default) uses the built-in asyncio HTTP/1.1 and MQTT 3.1.1
publishers in `rawtransport.py`. `--engine lib` uses `aiohttp` and `paho-mqtt`.
Both are open-loop and account identically; `lib` exists as a cross-check —
on the same target both must report the same accepted count and land the same
rows (verified, see below). The reason `raw` is the default is CPU:

| engine | protocol | ceiling on this host, ≤4 workers | generator CPU at ceiling |
|---|---|---|---|
| raw | MQTT | **~100 000 msg/s** | 2.89 cores |
| raw | HTTP | **~80 000 msg/s** | 2.77 cores |
| lib (paho) | MQTT | ~40 000 msg/s | 3.51 cores |
| lib (aiohttp) | HTTP | ~10 000 msg/s | 2.76 cores |
| — (`--dry-run`, scheduler only) | — | ~600 000 msg/s | 2.70 cores |

With `aiohttp`, the generator would have become the bottleneck at rates this
cluster can plausibly reach. That is the failure this tool exists to avoid.

## CPU budget

The generator runs on the same 16-core node as the system under test.
`--max-workers` (default **4**) caps the generator's processes, and every report
carries `generator_cpu` with:

* `worker_cpu_seconds_exact` / `worker_cores_avg_exact` — from `time.process_time()`
  inside each worker, charged to the measured window only (connection setup is
  excluded);
* `cores_mean` / `cores_peak` — psutil sampling of the whole process tree;
* `host_all_cores_mean` / `host_all_cores_peak` — the whole box, for context.

That is what backs the claim that the generator was not starving the SUT.

## Output

One JSON per run (default `results/<mode>-<protocol>-<runid>.json`). Key blocks:

```jsonc
"offered":  { "target_rate_msg_s", "scheduled_total", "dispatched_total",
              "offered_rate_msg_s", "schedule_deficit", "schedule_deficit_pct" },
"honesty":  { "client_was_bottleneck", "reasons", "notes",
              "behind_schedule", "behind_schedule_pct",
              "max_lag_ms", "mean_lag_ms", "blocked_inflight",
              "blocked_inflight_pct", "late_threshold_ms" },
"delivery": { "attempted", "accepted", "accepted_rate_msg_s",
              "failed": { "http_503": n, "mqtt_no_conn": n, "timeout": n, ... },
              "accept_ratio", "keys_per_message", "expected_rows" },
"latency_ms": { "p50_ms", "p95_ms", "p99_ms", "p999_ms", "max_ms", "mean_ms" },
"landed":   { "rows", "expected_rows", "delta", "match" },
"connections", "generator_cpu", "per_worker", "per_second",
"t_start_epoch", "t_end_epoch"    // for correlating with resource samples
```

`t_start_epoch` / `t_end_epoch` and the per-second series are there so the harness
can line a run up with `collect-metrics.sh` samples and with rows in the store.

### Why `accepted × keys_per_message == landed` is exact

Telemetry keys are **run-unique** (`lg2_<runid>_v0`, …), so landed rows are counted
with `LIKE '<prefix>%'` — no time window, no overlap with other traffic. The `ts`
is forced strictly increasing per device, because the store's primary key is
`(tenant, device, key, ts)`: two messages sharing a millisecond would UPSERT and a
row would silently vanish from the count. Every non-reserved key becomes exactly
one row (see `k8s/helm/thingsflow/files/bento-nats-greptimedb.yaml`).

## Verification evidence (2026-08-01, `thingsflow-fresh`)

All runs cleaned up their devices; the tenant was left with 0 leftover `lg2*` devices.

| Check | Result |
|---|---|
| provisions devices | 100/100 in 0.3 s, 0 errors; `cleanup {deleted: 100, delete_failed: 0}` |
| MQTT → GreptimeDB | 500 msg/s × 30 s (5 s ramp), 100 devices: dispatched 13 748, accepted 13 748, **0 failed**, landed **41 244** rows vs expected 41 244, delta **0** |
| HTTP → GreptimeDB | same shape: dispatched 13 748, accepted 13 748, **0 failed**, landed **41 244** vs 41 244, delta **0** |
| engine cross-check | `raw` and `lib`, MQTT and HTTP, all four combinations: accepted == dispatched, landed == expected, delta 0 |
| `--calibrate` | MQTT ceiling 99 973 msg/s @ 2.89 cores; HTTP ceiling 79 989 msg/s @ 2.77 cores; failure at the next step is reported with its reason, not hidden |
| generator CPU at 500 msg/s | **0.39 cores** of 16 (both protocols) |

Reports: `results/verify-mqtt-500.json`, `results/verify-http-500.json`,
`results/calibrate-mqtt-raw*.json`, `results/calibrate-http-raw.json`,
`results/calibrate-dryrun-scheduler.json`.

## Limitations to know before designing a ramp

1. **The calibration sink shares this host.** At the MQTT ceiling the sink itself
   burned ~3.1 cores. The reported ceiling is therefore a *lower bound* on the
   client's true capability, not an upper one. `sink_cpu_cores_avg` is in every
   calibration step so you can see it.
2. **`--max-inflight` and `--http-connections` are real ceilings.** The MQTT run
   failed at 140k because the in-flight cap (2048 total / 4 workers) filled, not
   because the client ran out of CPU. Raise them together with the rate; the
   report tells you which one bound you (`blocked_inflight`, `pool_full`).
3. **`blocked_inflight` is graded, not binary.** Any blocked send is reported, but
   the `client_was_bottleneck` verdict only trips above
   `--bottleneck-blocked-pct` (default 0.1% of scheduled). 95 blocks in 900 000
   messages is noise; 95 000 is a verdict.
4. **The ThingsBoard adapter is not yet live-verified.** `tb-classic` was deployed
   but scaled to zero when this was written, so that path is covered only by
   `selftest.py` against a stub control plane (URL shape, credential placement,
   topic, provisioning calls). Re-run a small `--target thingsboard` smoke test
   before trusting a comparison run.
5. **No landed-row check for ThingsBoard.** `--verify-landed` reports
   `{"available": false}` rather than guessing. The accepted-vs-landed gap — the
   silent-drop metric — is therefore only available on the ThingsFlow side today.
6. **`raw` HTTP needs one host:port for all devices.** True for both platforms'
   ingest, but it will refuse a mixed-host device list rather than silently
   splitting the pool.
7. **Latency is client-side.** HTTP = request→response; MQTT = publish→PUBACK.
   Neither is end-to-end ingest→queryable. Landed rows plus `t_start/t_end` are
   what cover durability.
8. **Device JWTs are minted once, at provisioning.** The chart default TTL is
   86 400 s; a run longer than the deployment's `DEVICE_JWT_TTL_SECONDS` (some
   overlays use 900 s) would see auth failures. There is no refresh loop yet —
   they would show up honestly as `mqtt_no_conn` / `http_401`, not as silence.

## Endpoints

NodePorts (30081 / 30808 / 30183 / 30400) are the documented path but were absent
after the 2026-08-01 redeploy. The ClusterIPs are reachable directly from this
node and are what the verification runs used:

```bash
kubectl --context microk8s -n thingsflow-fresh get svc
```

## Files

| file | role |
|---|---|
| `loadgen2.py` | CLI, multiprocessing orchestration, CPU sampling, merge, report |
| `senders.py` | the open loop; HTTP/MQTT senders for both engines |
| `rawtransport.py` | built-in asyncio HTTP/1.1 + MQTT 3.1.1 publishers |
| `targets.py` | ThingsFlow / ThingsBoard / sink adapters |
| `sink.py` | trivial local HTTP + MQTT sinks for calibration |
| `stats.py` | `Schedule` (open-loop arithmetic) and `Histogram` (mergeable percentiles) |
| `selftest.py` | offline tests: schedule, histogram, payload, MQTT framing, both adapters |
| `run.sh` | venv bootstrap + launcher |
