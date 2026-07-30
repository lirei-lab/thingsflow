"""Minimal ThingsFlow HTTP telemetry device."""

from thingsflow_device_sdk import ThingsFlowDeviceClient


device = ThingsFlowDeviceClient(
    base_url="https://thingsflow.example.com",
    device_name="sensor-001",
    device_type="temperature",
    provision_device_key="profile-key",
    provision_device_secret="profile-secret",
)

device.provision()
device.publish_http({"temperature": 22.4, "humidity": 48.2})
