import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
BENCH = ROOT / "benchmarks"


class BenchmarkPackageTest(unittest.TestCase):
    def test_public_benchmark_package_has_methodology_chart_scenarios_and_scripts(self):
        expected = [
            BENCH / "README.md",
            BENCH / "helm" / "thingsboard-classic" / "Chart.yaml",
            BENCH / "helm" / "thingsboard-classic" / "values.yaml",
            BENCH / "scenarios" / "mqtt-100.env",
            BENCH / "scenarios" / "mqtt-1000.env",
            BENCH / "scenarios" / "http-1000.env",
            BENCH / "scripts" / "run-benchmark.sh",
            BENCH / "scripts" / "collect-metrics.sh",
            BENCH / "scripts" / "collect-thingsflow-run-evidence.sh",
            BENCH / "scripts" / "summarize-results.py",
        ]
        missing = [str(path.relative_to(ROOT)) for path in expected if not path.exists()]
        self.assertEqual(missing, [])

    def test_benchmark_docs_state_fairness_and_no_private_cluster_assumptions(self):
        readme = (BENCH / "README.md").read_text()
        self.assertIn("same cluster", readme)
        self.assertIn("same load generator", readme)
        self.assertIn("Flow Core manages the platform; ThingsFlow data plane moves device data", readme)
        private_terms = ["cluster" + ".yaml", "har" + "bor", "kani" + "ko", "cloud" + "." + "lirei", "lirei" + ".io"]
        for term in private_terms:
            self.assertNotIn(term, readme.lower())

    def test_tb_classic_chart_declares_official_images_and_benchmark_labels(self):
        chart = (BENCH / "helm" / "thingsboard-classic" / "Chart.yaml").read_text()
        values = (BENCH / "helm" / "thingsboard-classic" / "values.yaml").read_text()
        self.assertIn("thingsboard-classic", chart)
        self.assertIn("thingsboard/tb-node", values)
        self.assertIn("timescale/timescaledb", values)
        self.assertIn("DATABASE_TS_TYPE: timescale", values)
        self.assertIn("app.kubernetes.io/part-of: iot-benchmark", values)

    def test_scenarios_are_explicit_about_protocol_devices_rate_and_duration(self):
        for scenario in (BENCH / "scenarios").glob("*.env"):
            text = scenario.read_text()
            for key in ["PROTOCOL=", "DEVICE_COUNT=", "PUBLISH_INTERVAL_SECONDS=", "RUN_DURATION_SECONDS="]:
                self.assertIn(key, text, scenario.name)

    def test_thingsflow_benchmark_uses_native_rmqtt_edge(self):
        runner = (BENCH / "scripts" / "run-benchmark.sh").read_text()
        http_loadgen = (BENCH / "scripts" / "http-loadgen.py").read_text()
        mqtt_loadgen = (ROOT / "tools" / "python" / "telemetry_generator.py").read_text()

        self.assertIn("THINGSFLOW_NATIVE_EDGE", runner)
        self.assertIn("MQTT_AUTH_MODE", runner)
        self.assertIn("HTTP_TELEMETRY_SERVICE", runner)
        self.assertIn("$RELEASE-rmqtt-edge", runner)
        self.assertIn("1883", runner)
        self.assertIn("deviceJwtRaw", runner)
        self.assertIn("/api/device/{device_id}/jwt", http_loadgen)
        self.assertIn("/api/v1/devices/{mqtt_identity}/telemetry", http_loadgen)
        self.assertIn("Authorization", http_loadgen)
        self.assertIn("THINGSFLOW_NATIVE_EDGE", mqtt_loadgen)
        self.assertIn("MQTT_AUTH_MODE", mqtt_loadgen)
        self.assertIn("thingsflow/devices/{mqtt_identity}/telemetry", mqtt_loadgen)

    def test_runner_allows_environment_overrides_for_aggressive_smokes(self):
        runner = (BENCH / "scripts" / "run-benchmark.sh").read_text()
        self.assertIn("ENV_DEVICE_COUNT", runner)
        self.assertIn('DEVICE_COUNT="${ENV_DEVICE_COUNT:-${DEVICE_COUNT:-100}}"', runner)
        self.assertIn('RUN_DURATION_SECONDS="${ENV_RUN_DURATION_SECONDS:-${RUN_DURATION_SECONDS:-900}}"', runner)
        self.assertIn('PUBLISH_INTERVAL_SECONDS="${ENV_PUBLISH_INTERVAL_SECONDS:-${PUBLISH_INTERVAL_SECONDS:-5}}"', runner)

    def test_mqtt_loadgen_has_shutdown_guard_for_native_benchmark(self):
        mqtt_loadgen = (ROOT / "tools" / "python" / "telemetry_generator.py").read_text()
        self.assertIn('stopping = {"value": False}', mqtt_loadgen)
        self.assertIn('if stopping["value"]:', mqtt_loadgen)
        self.assertIn('stopping["value"] = True', mqtt_loadgen)

    def test_mqtt_loadgen_reports_machine_readable_done_summary(self):
        mqtt_loadgen = (ROOT / "tools" / "python" / "telemetry_generator.py").read_text()
        self.assertIn('"event": "done"', mqtt_loadgen)
        self.assertIn('"published_total": final_total', mqtt_loadgen)
        self.assertIn('"errors": errors', mqtt_loadgen)
        self.assertIn('"error_counts": error_counts', mqtt_loadgen)
        self.assertIn("record_error(", mqtt_loadgen)

    def test_runner_supports_sharded_loadgen_jobs(self):
        runner = (BENCH / "scripts" / "run-benchmark.sh").read_text()
        self.assertIn("LOADGEN_SHARDS", runner)
        self.assertIn("SHARD_COUNT", runner)
        self.assertIn("SHARD_INDEX", runner)
        self.assertIn("SHARD_DEVICE_COUNT", runner)
        self.assertIn('JOB="bench-$RUN_ID-s$SHARD_INDEX"', runner)
        self.assertIn('SHARD_DEVICE_PREFIX="$BASE_DEVICE_PREFIX-s$SHARD_INDEX"', runner)
        self.assertIn('$RESULTS_DIR/$RUN_ID-s$SHARD_INDEX.log', runner)

    def test_http_loadgen_max_workers_is_configurable_for_aggressive_tests(self):
        http_loadgen = (BENCH / "scripts" / "http-loadgen.py").read_text()
        runner = (BENCH / "scripts" / "run-benchmark.sh").read_text()
        self.assertIn("LOADGEN_MAX_WORKERS", http_loadgen)
        self.assertIn("MAX_WORKERS", http_loadgen)
        self.assertIn("ThreadPoolExecutor(max_workers=MAX_WORKERS)", http_loadgen)
        self.assertIn("ENV_LOADGEN_MAX_WORKERS", runner)
        self.assertIn("LOADGEN_MAX_WORKERS", runner)
        self.assertIn('name: LOADGEN_MAX_WORKERS', runner)

    def test_http_loadgen_reports_bounded_error_diagnostics(self):
        http_loadgen = (BENCH / "scripts" / "http-loadgen.py").read_text()
        self.assertIn("_error_counts", http_loadgen)
        self.assertIn("_error_samples", http_loadgen)
        self.assertIn("MAX_ERROR_SAMPLES", http_loadgen)
        self.assertIn("record_error(", http_loadgen)
        self.assertIn('"error_counts"', http_loadgen)
        self.assertIn('"error_samples"', http_loadgen)
        self.assertIn("requests.HTTPError", http_loadgen)

    def test_thingsflow_evidence_collector_records_industrial_metrics(self):
        collector = (BENCH / "scripts" / "collect-thingsflow-run-evidence.sh").read_text()
        for expected in [
            "/api/v1/brokers",
            "/api/v1/nodes",
            "/api/v1/clients",
            "/api/v1/plugins",
            "nats streams",
            "nats twin kv",
            "nats twin kv latest keys",
            "device_telemetry",
            "total_publishes",
            "published_total",
            "warning/error samples",
            "REDACTED_JWT",
        ]:
            self.assertIn(expected, collector)


if __name__ == "__main__":
    unittest.main()
