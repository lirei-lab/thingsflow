import pathlib
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[2]
VALUES_DEMO = ROOT / "k8s" / "helm" / "thingsflow" / "values-demo.yaml"
DOCS_DEMO = ROOT / "docs" / "DEMO_PROFILE.md"
README = ROOT / "README.md"
MKDOCS = ROOT / "mkdocs.yml"
VERIFY_SCRIPT = ROOT / "tools" / "verify-demo-profile.sh"


class DemoProfileConfigTest(unittest.TestCase):
    def test_demo_values_enable_seed_and_simulator_without_private_cluster_settings(self):
        text = VALUES_DEMO.read_text()

        self.assertIn("flowCore:", text)
        self.assertIn("loadDemo: true", text)
        self.assertIn("demoSimulator:", text)
        self.assertIn("enabled: true", text)
        self.assertIn("publishIntervalSeconds: 5", text)

        private_terms = [
            "cluster" + ".yaml",
            "registry" + ".cloud" + ".lirei" + ".io",
            "har" + "bor",
            "kani" + "ko",
            "thingsflow" + ".cloud" + ".lirei" + ".io",
        ]
        for term in private_terms:
            self.assertNotIn(term, text)

    def test_demo_docs_show_install_and_verification_commands(self):
        text = DOCS_DEMO.read_text()

        self.assertIn("values-demo.yaml", text)
        self.assertIn("flowCore.loadDemo", text)
        self.assertIn("demoSimulator.enabled", text)
        self.assertIn("Thermostats", text)
        self.assertIn("SCADA Process Demo", text)
        self.assertIn("Smart Building Office Demo", text)
        self.assertIn("tools/verify-demo-profile.sh", text)
        self.assertIn("RELEASE=my-release", text)

    def test_demo_profile_is_linked_from_public_docs(self):
        self.assertIn("DEMO_PROFILE.md", README.read_text())
        self.assertIn("Demo Profile: DEMO_PROFILE.md", MKDOCS.read_text())

    def test_verifier_is_public_portable_and_ascii(self):
        text = VERIFY_SCRIPT.read_text()

        self.assertIn("RELEASE=", text)
        self.assertIn("${RELEASE}-postgres-0", text)
        self.assertIn("${RELEASE}-demo-simulator", text)
        text.encode("ascii")


if __name__ == "__main__":
    unittest.main()
