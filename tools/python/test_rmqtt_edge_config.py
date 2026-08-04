import pathlib
import re
import subprocess
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
        # The guard message was rewritten when cluster mode was implemented; what
        # matters is that scaling the Deployment past one node stays unreachable,
        # because each pod would be an isolated broker with its own session table.
        self.assertIn("rmqttEdge.replicas > 1 is not an RMQTT cluster", template)
        self.assertIn("rmqttEdge.cluster.replicas must be >= 3", template)
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


class RenderedManifestSanityTest(unittest.TestCase):
    """Catches manifests that render but the API server rejects.

    `helm template` succeeding proves the templates parse, not that what they
    produce is admissible. An empty `image:` renders as a clean empty string and
    only fails at install time -- which is exactly how a fresh install broke
    after the flow-core image default moved into a template and one of its two
    consumers was left behind.
    """

    def _render(self, *extra):
        out = subprocess.run(
            ["helm", "template", "t", "k8s/helm/thingsflow", *extra],
            capture_output=True, text=True, cwd=ROOT,
        )
        self.assertEqual(out.returncode, 0, out.stderr[:600])
        return out.stdout

    def test_no_container_image_renders_empty(self):
        for extra in ([], ["--set", "images.flowCore="]):
            rendered = self._render(*extra)
            empty = [
                line for line in rendered.splitlines()
                if re.match(r'^\s*image:\s*(""|\'\')?\s*$', line)
            ]
            self.assertEqual(
                empty, [],
                f"empty image field rendered with {extra or 'defaults'}: {empty}",
            )
