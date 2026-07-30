import pathlib
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[2]


class OidcDemoAuthContractTest(unittest.TestCase):
    def test_compose_has_lightweight_dex_oidc_profile(self):
        compose = (ROOT / "docker" / "docker-compose-nats.yml").read_text()
        dex = (ROOT / "docker" / "dex" / "config.yaml").read_text()
        for expected in (
            "dex:",
            'profiles: ["oidc-dex"]',
            "ghcr.io/dexidp/dex",
            "OIDC_PROVIDER_ID: \"${OIDC_PROVIDER_ID:-dex}\"",
            "OIDC_PROVIDER_TITLE: \"${OIDC_PROVIDER_TITLE:-Dex}\"",
            "OIDC_ISSUER: \"${OIDC_ISSUER:-http://localhost:5556/dex}\"",
            "OIDC_AUTHORIZATION_URL: \"${OIDC_AUTHORIZATION_URL:-http://localhost:5556/dex/auth}\"",
            "OIDC_TOKEN_URL: \"${OIDC_TOKEN_URL:-http://dex:5556/dex/token}\"",
            "OIDC_JWKS_URL: \"${OIDC_JWKS_URL:-http://dex:5556/dex/keys}\"",
        ):
            self.assertIn(expected, compose)
        self.assertIn("tenant@thingsflow.local", dex)
        self.assertIn("http://localhost:8082/login/oauth2/code/dex", dex)
        self.assertNotIn("keycloak:", compose)
        self.assertNotIn("quay.io/keycloak/keycloak", compose)

    def test_docs_recommend_dex_for_automated_test_auth(self):
        install = (ROOT / "docs" / "INSTALL.md").read_text()
        security = (ROOT / "docs" / "SECURITY.md").read_text()

        self.assertIn("Dex", install)
        self.assertIn("testAuth.enabled=true", install)
        self.assertIn("oidc-dex", install)
        self.assertIn("Dex is a test/demo identity provider", security)
        self.assertIn("must stay disabled for production", security)


if __name__ == "__main__":
    unittest.main()
