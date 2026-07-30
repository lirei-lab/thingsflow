"""Minimal ThingsFlow MQTT telemetry device."""

import time

from thingsflow_device_sdk import ThingsFlowDeviceClient


device = ThingsFlowDeviceClient(
    base_url="https://thingsflow.example.com",
    device_name="sensor-001",
    device_type="temperature",
    provision_device_key="profile-key",
    provision_device_secret="profile-secret",
)

try:
    device.provision()
    device.connect_mqtt("mqtt.example.com", port=1883)
    for step in range(5):
        device.publish_mqtt({"temperature": 22.0 + step * 0.1})
        time.sleep(1)
finally:
    device.close()
