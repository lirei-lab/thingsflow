import pathlib
import subprocess
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[2]


class HelmProductionContractTest(unittest.TestCase):
    def test_production_true_sets_flow_env_to_production_when_secrets_are_valid(self):
        cmd = [
            "helm",
            "template",
            "thingsflow",
            "k8s/helm/thingsflow",
            "--show-only",
            "templates/flow-core.yaml",
            "--set",
            "production=true",
            "--set",
            "flowCore.allowedOrigin=https://thingsflow.example.com",
            "--set",
            "postgres.passwordExistingSecret.name=thingsflow-postgres",
            "--set",
            "nats.auth.enabled=true",
            "--set",
            "nats.auth.existingSecret.name=thingsflow-nats-auth",
            "--set",
            "flowCore.deviceJwt.privateKeyExistingSecret.name=thingsflow-device-jwt",
            "--set",
            "flowCore.deviceJwt.privateKeyExistingSecret.key=device-jwt-es256-private-key-pem-b64",
            "--set",
            "flowCore.jwtTokenSigningKey=dGVzdC1zaWduaW5nLWtleS1tdXN0LWJlLWxvbmdlci10aGFuLTMydiE=",
        ]
        result = subprocess.run(cmd, cwd=ROOT, text=True, capture_output=True, check=True)

        self.assertIn("name: FLOW_ENV", result.stdout)
        self.assertIn('value: "production"', result.stdout)

    def test_production_true_rejects_dev_defaults(self):
        cmd = [
            "helm",
            "template",
            "thingsflow",
            "k8s/helm/thingsflow",
            "--set",
            "production=true",
        ]
        result = subprocess.run(cmd, cwd=ROOT, text=True, capture_output=True)

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("postgres.password must not use the dev default", result.stderr + result.stdout)

    def test_production_true_rejects_non_base64_jwt_key(self):
        cmd = [
            "helm",
            "template",
            "thingsflow",
            "k8s/helm/thingsflow",
            "--set",
            "production=true",
            "--set",
            "flowCore.allowedOrigin=https://thingsflow.example.com",
            "--set",
            "postgres.passwordExistingSecret.name=thingsflow-postgres",
            "--set",
            "nats.auth.enabled=true",
            "--set",
            "nats.auth.existingSecret.name=thingsflow-nats-auth",
            "--set",
            "flowCore.jwtTokenSigningKey=raw_random_urlsafe_key_with_underscores",
        ]
        result = subprocess.run(cmd, cwd=ROOT, text=True, capture_output=True)

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("standard base64", result.stderr + result.stdout)


if __name__ == "__main__":
    unittest.main()
