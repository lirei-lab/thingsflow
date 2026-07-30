import pathlib
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[2]


def yaml_block_has_value(text, key, expected):
    lines = text.splitlines()
    start = None
    for index, line in enumerate(lines):
        if line == f"{key}:":
            start = index + 1
            break
    if start is None:
        return False
    for line in lines[start:]:
        if line and not line.startswith(" "):
            break
        if line.strip() == expected:
            return True
    return False


class NativeRMQTTEdgeConfigTest(unittest.TestCase):
    def test_helm_exposes_rmqtt_edge_as_default(self):
        values = (ROOT / "k8s/helm/thingsflow/values.yaml").read_text()
        template = (ROOT / "k8s/helm/thingsflow/templates/rmqtt-edge.yaml").read_text()

        self.assertIn("rmqttEdge:", values)
        self.assertTrue(yaml_block_has_value(values, "rmqttEdge", "enabled: true"))
        self.assertNotIn("zil" + "laEdge:", values)
        self.assertIn(".Values.rmqttEdge.enabled", template)
        self.assertIn("rmqtt-bridge-egress-nats", template)
        self.assertIn("tf.ingest.mqtt.raw", template)
        self.assertIn("hmac_base64 = false", template)
        # Asserted in two parts: the init container builds the URL from a BASE
        # variable so it can also poll /api/noauth/device-jwks for the expected
        # key id. The contract is "fetch the public key from flow-core's noauth
        # endpoint", not "these characters appear consecutively".
        self.assertIn("/api/noauth", template)
        self.assertIn("device-jwt-public.pem", template)
        self.assertIn("fetch-device-jwt-public-key", template)

    def test_nats_event_broker_uses_native_rmqtt_nats_bridge(self):
        values = (ROOT / "k8s/helm/thingsflow/values.yaml").read_text()
        template = (ROOT / "k8s/helm/thingsflow/templates/rmqtt-edge.yaml").read_text()

        self.assertIn("natsBridge:", values)
        self.assertIn("natsSubject:", values)
        self.assertIn("rmqtt-bridge-egress-nats", template)
        self.assertIn("rmqtt-bridge-egress-nats.toml", template)
        self.assertIn("$natsBridgeEnabled", template)
        self.assertIn("remote.forward_all_from", template)
        self.assertIn("remote.forward_all_publish", template)
        self.assertIn("thingsflow-rmqtt-nats", template)
        self.assertIn("tf.ingest.mqtt.raw", template)
        self.assertIn('include "thingsflow.natsURLNoAuthTemplate"', template)
        self.assertIn("render-rmqtt-config", template)
        self.assertIn("s/__NATS_USER__/${user_escaped}/g", template)
        self.assertIn("s/__NATS_PASSWORD__/${pass_escaped}/g", template)

    def test_demo_simulator_defaults_to_rmqtt_device_jwt_raw(self):
        values = (ROOT / "k8s/helm/thingsflow/values.yaml").read_text()
        template = (ROOT / "k8s/helm/thingsflow/templates/demo-simulator.yaml").read_text()

        self.assertIn('mqttAuthMode: "deviceJwtRaw"', values)
        self.assertIn("%s-rmqtt-edge", template)
        self.assertIn('default "1883"', template)
        self.assertIn('default "deviceJwtRaw"', template)

    def test_public_docs_describe_compact_rmqtt_architecture(self):
        doc = (ROOT / "docs/DATA_PLANE.md").read_text()
        readme = (ROOT / "README.md").read_text()

        for expected in (
            "RMQTT is the principal MQTT device edge",
            "Flow Core stays out of the telemetry hot path",
            "ThingsBoard UI compatibility APIs",
            "tf.ingest.mqtt.raw",
            "device JWT",
        ):
            self.assertIn(expected, doc)

        self.assertIn("RMQTT", readme)


if __name__ == "__main__":
    unittest.main()
