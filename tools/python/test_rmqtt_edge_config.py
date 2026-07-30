import pathlib
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[2]


class RMQTTEdgeConfigTest(unittest.TestCase):
    def test_chart_uses_rmqtt_as_default_mqtt_edge(self):
        values = (ROOT / "k8s/helm/thingsflow/values.yaml").read_text()
        template = (ROOT / "k8s/helm/thingsflow/templates/rmqtt-edge.yaml").read_text()

        self.assertIn("rmqttEdge:", values)
        self.assertIn("enabled: true", values)
        self.assertIn(".Values.rmqttEdge.enabled", template)
        self.assertIn("rmqtt-bridge-egress-nats", template)
        self.assertIn("tf.ingest.mqtt.raw", template)
        self.assertIn("rmqtt/rmqtt:0.20.0", values)
        self.assertIn('logLevel: "warn"', values)
        self.assertIn("rmqtt.logLevel", template)
        self.assertIn("rmqttEdge.replicas > 1 requires a real RMQTT cluster profile", template)
        self.assertIn("rmqtt-cluster-raft", template)
        self.assertIn('"allow", "all", "publish"', template)
        self.assertIn("thingsflow/devices/+/telemetry", template)
        self.assertIn("thingsflow/devices/+/attributes", template)
        self.assertIn("hmac_base64 = false", template)
        self.assertIn("fetch-device-jwt-public-key", template)

    def test_demo_simulator_can_target_rmqtt_edge(self):
        values = (ROOT / "k8s/helm/thingsflow/values.yaml").read_text()
        template = (ROOT / "k8s/helm/thingsflow/templates/demo-simulator.yaml").read_text()

        self.assertIn("mqttHost:", values)
        self.assertIn("mqttPort:", values)
        self.assertIn('mqttAuthMode: "deviceJwtRaw"', values)
        self.assertIn("demoSimulator.mqttHost", template)
        self.assertIn("demoSimulator.mqttPort", template)
        simulator = (ROOT / "k8s/helm/thingsflow/files/demo_simulator.py").read_text()
        self.assertIn('MQTT_AUTH_MODE == "devicejwtraw"', simulator)
        self.assertIn('device["deviceJwt"]["token"]', simulator)

    def test_nats_bento_materializers_read_rmqtt_metadata(self):
        latest = (ROOT / "k8s/helm/thingsflow/files/bento-nats-latest-kv.yaml").read_text()
        greptime = (ROOT / "k8s/helm/thingsflow/files/bento-nats-greptimedb.yaml").read_text()
        questdb = (ROOT / "k8s/helm/thingsflow/files/bento-nats-questdb.yaml").read_text()

        for config in (latest, greptime, questdb):
            self.assertIn('metadata("topic")', config)
            self.assertIn('split("/")', config)
            self.assertIn('decode("base64url")', config)
            self.assertIn("DEFAULT_TENANT_ID", config)
            self.assertIn("device_id", config)


if __name__ == "__main__":
    unittest.main()
