import unittest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
import telemetry_generator as tg


class TelemetryGeneratorConfigTest(unittest.TestCase):
    def test_build_device_profiles_scales_evenly_with_shared_interval(self):
        profiles = tg.build_device_profiles(
            {
                "LOADGEN_DEVICE_COUNT": "12",
                "LOADGEN_INTERVAL_SECONDS": "5",
            }
        )

        self.assertEqual(sum(profile["count"] for profile in profiles), 12)
        self.assertEqual({profile["interval"] for profile in profiles}, {5.0})
        self.assertEqual([profile["count"] for profile in profiles], [3, 3, 2, 2, 2])

    def test_build_device_name_uses_prefix(self):
        self.assertEqual(
            tg.build_device_name("pilot", "Temperature Sensor", 0),
            "pilot-temperature-sensor-01",
        )

    def test_custom_mqtt_topic_template_supports_edge_gateway(self):
        previous = tg.MQTT_TOPIC_TEMPLATE
        tg.MQTT_TOPIC_TEMPLATE = "edge/devices/{device_name}/telemetry"
        try:
            self.assertEqual(
                tg.render_mqtt_topic(
                    "edge-sim-temperature-sensor-01",
                    "device-id",
                    "Temperature Sensor",
                    "mqtt-identity",
                ),
                "edge/devices/edge-sim-temperature-sensor-01/telemetry",
            )
        finally:
            tg.MQTT_TOPIC_TEMPLATE = previous

    def test_native_topic_still_uses_mqtt_identity_without_template(self):
        previous_template = tg.MQTT_TOPIC_TEMPLATE
        previous_native = tg.THINGSFLOW_NATIVE_EDGE
        tg.MQTT_TOPIC_TEMPLATE = ""
        tg.THINGSFLOW_NATIVE_EDGE = True
        try:
            self.assertEqual(
                tg.render_mqtt_topic(
                    "device-name",
                    "device-id",
                    "Temperature Sensor",
                    "mqtt-identity",
                ),
                "thingsflow/devices/mqtt-identity/telemetry",
            )
        finally:
            tg.MQTT_TOPIC_TEMPLATE = previous_template
            tg.THINGSFLOW_NATIVE_EDGE = previous_native


if __name__ == "__main__":
    unittest.main()
