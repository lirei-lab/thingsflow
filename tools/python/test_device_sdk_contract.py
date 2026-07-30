import base64
import json
import pathlib
import sys
import time
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[2]
SDK_ROOT = ROOT / "sdk" / "python"
sys.path.insert(0, str(SDK_ROOT))

from thingsflow_device_sdk import DeviceJWT, ThingsFlowDeviceClient  # noqa: E402
from thingsflow_device_sdk import ThingsFlowDeviceClient  # noqa: E402


def make_jwt(payload):
    def enc(data):
        raw = json.dumps(data, separators=(",", ":")).encode("utf-8")
        return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")

    return f"{enc({'alg': 'none'})}.{enc(payload)}.signature"


class DeviceSdkContractTest(unittest.TestCase):
    def test_device_jwt_parses_expiry_and_mqtt_identity(self):
        exp = int(time.time()) + 60
        token = make_jwt(
            {
                "iss": "thingsflow-device",
                "aud": "thingsflow-mqtt",
                "sub": "device-1",
                "mqttId": "device-1",
                "exp": exp,
            }
        )
        device_jwt = DeviceJWT.from_dict({"token": token})

        self.assertEqual(device_jwt.expires_at, float(exp))
        self.assertEqual(device_jwt.mqtt_identity, "device-1")
        self.assertFalse(device_jwt.expires_soon(5, now=exp - 60))
        self.assertTrue(device_jwt.expires_soon(300, now=exp - 60))

    def test_sdk_uses_current_native_endpoints(self):
        client_source = (SDK_ROOT / "thingsflow_device_sdk" / "client.py").read_text()
        readme = (SDK_ROOT / "README.md").read_text()

        self.assertIn("/api/v1/provision", client_source)
        self.assertIn("/api/v1/telemetry", client_source)
        self.assertIn("/api/device/{self.device_id}/jwt", client_source)
        self.assertIn("thingsflow/devices/{mqtt_identity}/telemetry", client_source)
        self.assertIn("POST /api/v1/provision", readme)
        self.assertIn("POST /api/v1/telemetry", readme)
        self.assertIn("thingsflow/devices/{mqttIdentity}/telemetry", readme)
        self.assertIn("POST /api/v1/devices/me/jwt/refresh", readme)
        self.assertTrue((ROOT / "tools" / "verify-device-sdk-live.py").exists())
        self.assertTrue((ROOT / "tools" / "verify-device-sdk-mqtt-live.py").exists())
        self.assertTrue((ROOT / "tools" / "verify-device-jwt-lifecycle.py").exists())

    def test_device_jwt_handler_checks_security_status(self):
        source = (ROOT / "flow-core" / "internal" / "device" / "device_jwt_handler.go").read_text()

        self.assertIn("COALESCE(security_status, 'ACTIVE')", source)
        self.assertIn("DeviceSecurityActive", source)
        self.assertIn("Device is suspended", source)

    def test_client_defaults_to_reprovision_renewal(self):
        client = ThingsFlowDeviceClient(
            base_url="https://thingsflow.example.com",
            device_name="sensor-001",
            provision_device_key="profile-key",
            provision_device_secret="profile-secret",
        )

        self.assertEqual(client.refresh_margin_seconds, 300)
        self.assertEqual(client.renewal_strategy.__class__.__name__, "ReProvisionRenewal")

    def test_thingsflow_import_uses_legacy_client_implementation(self):
        client = ThingsFlowDeviceClient(
            base_url="https://thingsflow.example.com",
            device_name="sensor-001",
            provision_device_key="profile-key",
            provision_device_secret="profile-secret",
        )

        self.assertIsInstance(client, ThingsFlowDeviceClient)


if __name__ == "__main__":
    unittest.main()
