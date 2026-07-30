import pathlib
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[2]
OPERATIONS = ROOT / "docs" / "OPERATIONS.md"
ARCHITECTURE = ROOT / "docs" / "ARCHITECTURE.md"
INSTALL = ROOT / "docs" / "INSTALL.md"
MKDOCS = ROOT / "mkdocs.yml"


class PublicAccessDocsTest(unittest.TestCase):
    def test_public_access_docs_explain_default_private_posture(self):
        text = OPERATIONS.read_text()

        self.assertIn("ingress.enabled=true", text)
        self.assertIn("ingress.enabled=false", text)
        self.assertIn("ClusterIP", text)
        self.assertIn("rmqttEdge.serviceType=LoadBalancer", text)
        self.assertIn("RMQTT", text)
        self.assertIn("JWT-bearing traffic", text)

    def test_public_access_docs_are_linked(self):
        self.assertIn("Operations: OPERATIONS.md", MKDOCS.read_text())
        self.assertIn("OPERATIONS.md", ARCHITECTURE.read_text())
        self.assertIn("OPERATIONS.md", INSTALL.read_text())
        self.assertIn("docs/OPERATIONS.md", (ROOT / "README.md").read_text())

    def test_public_access_docs_do_not_publish_private_cluster_details(self):
        text = OPERATIONS.read_text()

        private_terms = [
            "cluster" + ".yaml",
            "registry" + ".cloud" + ".lirei" + ".io",
            "thingsflow" + ".cloud" + ".lirei" + ".io",
            "har" + "bor",
            "kani" + "ko",
        ]
        for term in private_terms:
            self.assertNotIn(term, text)


if __name__ == "__main__":
    unittest.main()
