import pathlib
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[2]


class ControlPlaneApiContractTest(unittest.TestCase):
    def test_contract_documents_api_first_device_lifecycle(self):
        doc = (ROOT / "docs" / "API_REFERENCE.md").read_text()

        for expected in (
            "POST /api/auth/login",
            "POST /api/device",
            "GET /api/device/{id}/credentials",
            "POST /api/device/credentials",
            "GET|POST /api/device/{id}/security",
            "POST /api/v1/provision",
            "POST /api/relation",
            "GET /api/relations",
            "GET /api/plugins/telemetry/{entityType}/{entityId}/values/timeseries",
            "GET /api/audit/logs/entity/{id}",
            "POST /api/v1/{token}/telemetry",
        ):
            self.assertIn(expected, doc)

        self.assertIn("without the ThingsBoard UI", doc)
        self.assertIn("tools/verify-control-plane-api.sh", doc)
        self.assertIn("Known Gaps", doc)

    def test_control_plane_smoke_exercises_security_without_printing_secrets(self):
        script = (ROOT / "tools" / "verify-control-plane-api.sh").read_text()

        for expected in (
            "/api/auth/login",
            "/api/v1/provision",
            "/api/device",
            "/api/device/$DEVICE_ID/credentials",
            "/api/device/$DEVICE_ID/security",
            "/api/device/credentials",
            "/api/deviceProfile",
            "/api/relation",
            "/api/relations?fromId=",
            "deviceJwt.token",
            "deviceJwt.mqttIdentity",
            "/api/audit/logs/entity/$DEVICE_ID",
            "CONTROL PLANE API SMOKE PASSED",
        ):
            self.assertIn(expected, script)

        self.assertNotIn("echo \"$TOKEN", script)
        self.assertNotIn("echo $TOKEN", script)
        self.assertNotIn("echo \"$DEVICE_TOKEN", script)
        self.assertNotIn("echo $DEVICE_TOKEN", script)
        self.assertNotIn("echo \"$PROVISIONED_DEVICE_TOKEN", script)
        self.assertNotIn("echo $PROVISIONED_DEVICE_TOKEN", script)
        self.assertNotIn("echo \"$PROVISION_SECRET", script)
        self.assertNotIn("echo $PROVISION_SECRET", script)


if __name__ == "__main__":
    unittest.main()
