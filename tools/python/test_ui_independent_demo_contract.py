import pathlib
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[2]


class UiIndependentDemoContractTest(unittest.TestCase):
    def test_demo_script_exists_and_uses_only_public_api(self):
        script_path = ROOT / "tools" / "demo-ui-independent-device-flow.sh"
        self.assertTrue(script_path.exists(), "missing UI-independent demo script")
        script = script_path.read_text()

        for expected in (
            "/api/auth/login",
            "/api/deviceProfile",
            "/api/v1/provision",
            "/api/v1/$DEVICE_TOKEN/telemetry",
            "/api/plugins/telemetry/DEVICE/$DEVICE_ID/values/timeseries",
            "/api/device/credentials",
            "/api/device/$DEVICE_ID/security",
            "UI-INDEPENDENT DEVICE FLOW PASSED",
        ):
            self.assertIn(expected, script)

        forbidden = (
            "thingsboard-ui",
            "tb-web-ui",
            "/dashboards/",
            "echo $TOKEN",
            "echo \"$TOKEN",
            "echo $DEVICE_TOKEN",
            "echo \"$DEVICE_TOKEN",
            "echo $PROVISION_SECRET",
            "echo \"$PROVISION_SECRET",
        )
        for text in forbidden:
            self.assertNotIn(text, script)

    def test_api_first_docs_reference_demo(self):
        doc = (ROOT / "docs" / "API_REFERENCE.md").read_text()
        self.assertIn("tools/demo-ui-independent-device-flow.sh", doc)
        self.assertIn("UI-independent device flow", doc)


if __name__ == "__main__":
    unittest.main()
