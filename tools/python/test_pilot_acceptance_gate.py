import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "tools" / "verify-pilot-acceptance.sh"
DOC = ROOT / "docs" / "RELEASE.md"


class PilotAcceptanceGateTest(unittest.TestCase):
    def test_gate_script_exists_and_is_public_portable(self):
        self.assertTrue(SCRIPT.exists(), "tools/verify-pilot-acceptance.sh should exist")
        text = SCRIPT.read_text()
        self.assertIn("KUBECONFIG", text)
        self.assertIn("NAMESPACE", text)
        self.assertIn("RELEASE", text)
        self.assertIn("values-pilot.example.yaml", text)
        for private in [
            "cluster" + ".yaml",
            "har" + "bor",
            "kani" + "ko",
            "cloud" + ".lirei",
            "lirei" + ".io",
        ]:
            self.assertNotIn(private, text.lower())

    def test_gate_checks_runtime_health_data_plane_and_ui(self):
        text = SCRIPT.read_text()
        for expected in [
            "helm template",
            "rollout status",
            "thingsflow-rmqtt-edge",
            "thingsflow-nats",
            "thingsflow-greptimedb",
            "thingsflow-postgres",
            "thingsflow-nats-latest-kv",
            "thingsflow-nats-greptimedb",
            "thingsflow-nats-alarms",
            "thingsflow-alarm-materializer",
            "thingsflow-thingsboard-ui",
            "api/noauth/device-jwks",
            "api/noauth/device-jwt-public.pem",
            "logs deploy/$RELEASE-rmqtt-edge",
            "kv ls twin_state",
            "nats_kv_latest_keys",
            "greptimedb_history_rows",
            "OIDC_USERINFO_URL",
            "api/noauth/oauth2Clients",
            "no obsolete Redpanda/Zilla/Flow Rules workloads",
            "required production secrets exist",
            "statefulset/$name",
        ]:
            self.assertIn(expected, text)

    def test_gate_runs_aggressive_benchmark_and_enforces_thresholds(self):
        text = SCRIPT.read_text()
        for expected in [
            "RUN_BENCHMARK",
            "EXPECTED_DEVICE_COUNT",
            "EXPECTED_PUBLISHED_MIN",
            "EXPECTED_ERRORS_MAX",
            "POST_BENCHMARK_DRAIN_SECONDS",
            'POST_BENCHMARK_DRAIN_SECONDS="${POST_BENCHMARK_DRAIN_SECONDS:-90}"',
            "sleep \"$POST_BENCHMARK_DRAIN_SECONDS\"",
            "benchmarks/scripts/run-benchmark.sh",
            "published_total",
            "error_counts",
            "benchmark result check skipped by RUN_BENCHMARK",
        ]:
            self.assertIn(expected, text)

    def test_gate_checks_production_security_posture(self):
        text = SCRIPT.read_text()
        for expected in [
            "thingsflow-platform-keys",
            "thingsflow-device-jwt",
            "thingsflow-postgres",
            "thingsflow-nats-auth",
            "thingsflow-oidc",
            "EVENT_BROKER",
            "FLOW_DATA_PLANE_CONSUMER_ENABLED",
            "FLOW_CORE_DEVICE_INGEST_ENABLED",
            "TELEMETRY_HISTORY_STORE",
            "ALLOWED_ORIGIN",
            "FLOW_ENV",
            "production",
        ]:
            self.assertIn(expected, text)

    def test_gate_rechecks_runtime_restarts_after_benchmark(self):
        text = SCRIPT.read_text()
        main_body = text.split("main() {", 1)[1]
        self.assertGreaterEqual(main_body.count("check_no_runtime_restarts"), 2)
        first = main_body.index("check_no_runtime_restarts")
        benchmark = main_body.index("check_benchmark_results")
        second = main_body.index("check_no_runtime_restarts", first + 1)
        self.assertLess(first, benchmark)
        self.assertGreater(second, benchmark)

    def test_pilot_readiness_docs_reference_acceptance_gate(self):
        self.assertTrue(DOC.exists(), "docs/RELEASE.md should exist")
        text = DOC.read_text()
        self.assertIn("tools/verify-pilot-acceptance.sh", text)
        self.assertIn("Pilot acceptance gate", text)
        self.assertIn("2,000", text)
        self.assertIn("120,000", text)

    def test_pilot_values_are_secure_and_public_portable(self):
        values_path = ROOT / "k8s/helm/thingsflow/values-pilot.example.yaml"
        self.assertTrue(values_path.exists(), "values-pilot.example.yaml should exist")
        text = values_path.read_text()

        for expected in [
            "production: true",
            "testAuth:",
            "enabled: false",
            "loadDemo: false",
            "nats:",
            "auth:",
            "persistence:",
            "storage: \"file\"",
            "oidc:",
            "clientSecretExistingSecret:",
            "stateSigningKeyExistingSecret:",
            "backup:",
            "existingSecret:",
            "thingsflow-backup-s3",
        ]:
            self.assertIn(expected, text)

        forbidden = [
            "cluster" + ".yaml",
            "har" + "bor",
            "kani" + "ko",
            "cloud" + ".lirei",
            "lirei" + ".io",
            "clientSecret: \"thingsflow",
            "stateSigningKey: \"thingsflow",
            "password: postgres",
        ]
        for private in forbidden:
            self.assertNotIn(private, text)


if __name__ == "__main__":
    unittest.main()
