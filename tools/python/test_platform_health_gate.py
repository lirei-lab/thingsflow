import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "tools" / "verify-platform-health.sh"
TOOLS_README = ROOT / "tools" / "README.md"


class PlatformHealthGateTest(unittest.TestCase):
    def test_health_script_exists_and_is_portable(self):
        self.assertTrue(SCRIPT.exists(), "tools/verify-platform-health.sh should exist")
        text = SCRIPT.read_text()
        for expected in [
            "KUBECONFIG",
            "--kubeconfig",
            "NAMESPACE",
            "RELEASE",
            "EXPECTED_GREPTIME_ROWS",
            "EXPECTED_NATS_KV_KEYS",
            "LOG_SINCE",
        ]:
            self.assertIn(expected, text)
        for private in [
            "cluster" + ".yaml",
            "har" + "bor",
            "cloud" + ".lirei",
            "lirei" + ".io",
        ]:
            self.assertNotIn(private, text.lower())

    def test_health_script_checks_databases_hot_state_and_logs(self):
        text = SCRIPT.read_text()
        for expected in [
            "pg_isready",
            "device_telemetry_kv",
            "greptime_timestamp",
            "kv info",
            "kv ls",
            "twin_state",
            "nats_kv_latest_keys",
            "http://$RELEASE-flow-core:8080/ready",
            "api/noauth/device-jwks",
            "api/noauth/oauth2Clients",
            "logs \"$target\" --since=\"$LOG_SINCE\"",
            "kubectl top pods",
            "ThingsFlow platform health check passed",
        ]:
            self.assertIn(expected, text)

    def test_health_script_uses_nats_secret_refs_without_inline_passwords(self):
        text = SCRIPT.read_text()
        self.assertIn("NATS_AUTH_SECRET", text)
        self.assertIn("secretKeyRef", text)
        self.assertIn("NATS_PASSWORD", text)
        self.assertNotIn("base64decode", text)
        self.assertNotIn("jsonpath='{.data.password}'", text)

    def test_tools_readme_mentions_health_gate(self):
        self.assertTrue(TOOLS_README.exists(), "tools/README.md should exist")
        text = TOOLS_README.read_text()
        self.assertIn("verify-platform-health.sh", text)
        self.assertIn("readiness", text.lower())


if __name__ == "__main__":
    unittest.main()
