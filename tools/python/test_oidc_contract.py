import pathlib
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[2]


class OidcContractTest(unittest.TestCase):
    def test_api_routes_mount_oidc_broker_endpoints(self):
        api = (ROOT / "flow-core" / "api.go").read_text()
        for expected in (
            "/api/noauth/oauth2Clients",
            "/api/noauth/oidc/authorize/",
            "/login/oauth2/code/",
        ):
            self.assertIn(expected, api)

    def test_oidc_docs_explain_broker_not_external_bearer_tokens(self):
        doc = (ROOT / "docs" / "API_REFERENCE.md").read_text()
        for expected in (
            "OIDC Auth Broker",
            "OIDC_PROVIDERS_JSON",
            "external_identity",
            "Flow Core still emits its own JWT",
            ".well-known/openid-configuration",
            "ZITADEL",
            "accessToken=...&refreshToken=...",
        ):
            self.assertIn(expected, doc)

    def test_install_docs_include_zitadel_trial(self):
        doc = (ROOT / "docs" / "INSTALL.md").read_text()
        for expected in (
            "ZITADEL",
            "http://localhost:8082/login/oauth2/code/zitadel",
            "OIDC_PROVIDER_ID=zitadel",
            "host.docker.internal",
            "k8s/helm/zitadel",
            "thingsflow-oidc",
            "oidc.userInfoUrl",
        ):
            self.assertIn(expected, doc)

    def test_helm_chart_exposes_userinfo_url_to_flow_core(self):
        values = (ROOT / "k8s" / "helm" / "thingsflow" / "values.yaml").read_text()
        configmap = (
            ROOT / "k8s" / "helm" / "thingsflow" / "templates" / "configmap.yaml"
        ).read_text()

        self.assertIn("userInfoUrl:", values)
        self.assertIn("OIDC_USERINFO_URL", configmap)

    def test_zitadel_chart_is_local_and_minimal(self):
        chart = (ROOT / "k8s" / "helm" / "zitadel" / "Chart.yaml").read_text()
        values = (ROOT / "k8s" / "helm" / "zitadel" / "values.yaml").read_text()
        deployment = (
            ROOT / "k8s" / "helm" / "zitadel" / "templates" / "deployment.yaml"
        ).read_text()
        setup = (
            ROOT / "k8s" / "helm" / "zitadel" / "templates" / "setup-job.yaml"
        ).read_text()

        self.assertIn("name: zitadel", chart)
        self.assertIn("kubeVersion: \">=1.24.0-0\"", chart)
        self.assertIn("useHelmHooks: false", values)
        self.assertIn("masterkey", values)
        self.assertIn("ghcr.io/zitadel/zitadel", values)
        self.assertIn("--masterkeyFromEnv", deployment)
        self.assertIn("--init-projections=true", values)
        self.assertNotIn("kubectl", setup)


if __name__ == "__main__":
    unittest.main()
