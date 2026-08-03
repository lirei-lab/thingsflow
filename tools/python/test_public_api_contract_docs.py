import pathlib
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[2]


class PublicApiContractDocsTest(unittest.TestCase):
    def test_openapi_contract_covers_public_release_surface(self):
        spec = (ROOT / "docs" / "api" / "openapi.yaml").read_text()

        for expected in (
            "openapi: 3.0.3",
            "/api/auth/login:",
            "/api/auth/user:",
            "/api/device:",
            "/api/device/{deviceId}:",
            "/api/device/{deviceId}/credentials:",
            "/api/device/credentials:",
            "/api/device/{deviceId}/security:",
            "/api/v1/provision:",
            "/api/v1/telemetry:",
            "/api/v1/{token}/telemetry:",
            "/api/plugins/telemetry/{entityType}/{entityId}/values/timeseries:",
            "/api/twins/{entityType}/{entityId}:",
            "/api/relation:",
            "/api/relations:",
            "/health:",
            "/ready:",
            "/metrics:",
            "bearerAuth:",
            "deviceJwtAuth:",
            "DeviceSecurityUpdate:",
            "Twin:",
        ):
            self.assertIn(expected, spec)

        private_terms = [
            "cluster" + ".yaml",
            "har" + "bor",
            "kani" + "ko",
            "cloud" + "." + "lirei",
            "lirei" + ".io",
        ]
        lower = spec.lower()
        for term in private_terms:
            self.assertNotIn(term, lower)

    def test_api_reference_documents_generation_strategy(self):
        doc = (ROOT / "docs" / "API_REFERENCE.md").read_text()

        for expected in (
            "docs/api/openapi.yaml",
            "oapi-codegen",
            "swaggo/swag",
            "Huma",
            "Goa",
            "ThingsBoard compatibility APIs",
            "contract-first",
        ):
            self.assertIn(expected, doc)

    def test_public_docs_nav_links_new_contract_pages(self):
        mkdocs = (ROOT / "mkdocs.yml").read_text()
        readme = (ROOT / "docs" / "README.md").read_text()
        index = (ROOT / "docs" / "index.md").read_text()
        root_readme = (ROOT / "README.md").read_text()

        for expected in (
            "API_REFERENCE.md",
            "DATA_PLANE.md",
            "OPERATIONS.md",
        ):
            self.assertIn(expected, mkdocs)
            self.assertIn(expected, readme)
            self.assertIn(expected, index)
            self.assertIn(f"docs/{expected}", root_readme)

    def test_benchmark_matrix_defines_public_methodology_gates(self):
        matrix = (ROOT / "benchmarks" / "MATRIX.md").read_text()
        readme = (ROOT / "benchmarks" / "README.md").read_text()

        for expected in (
            "Capacity ramp",
            "idle",
            "light",
            "Rows landed in the store",
            "Consumer lag",
            "CFS throttling",
            "Flow Core is the control plane and\nthe data plane is the telemetry path",
        ):
            self.assertIn(expected, matrix)

        self.assertIn("[MATRIX.md](MATRIX.md)", readme)

        private_terms = [
            "cluster" + ".yaml",
            "har" + "bor",
            "kani" + "ko",
            "cloud" + "." + "lirei",
            "lirei" + ".io",
        ]
        lower = matrix.lower()
        for term in private_terms:
            self.assertNotIn(term, lower)


if __name__ == "__main__":
    unittest.main()
