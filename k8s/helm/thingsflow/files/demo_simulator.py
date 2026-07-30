"""ThingsBoard classic demo simulator.

Streams telemetry for the classic devices seeded by THINGSFLOW_LOAD_DEMO:
- DHT22 Demo Sensor
- Raspberry Pi Demo
- Thermostat Demo
- Energy Meter Demo
- Motion Sensor Demo
- Air Quality Demo
- Water Tank Demo
- Pump Demo
- Flow Meter Demo
- Pressure Sensor Demo
- Valve Demo
- Office Thermostat Demo
- Office Occupancy Demo
- Office IAQ Demo
- Office Smart Light Demo
- Office Door Access Demo
- Conference Room Meter Demo
- Elevator Monitor Demo

The Thermostat Demo publishes the stock dashboard keys
`temperature` and `humidity`, and the seeder sets its type to
`thermostat` so the original Thermostats dashboard can resolve it.
The SCADA devices publish process keys used by classic TB SCADA cards
and symbol widgets: level, flow, pressure, pump status, and valve
position.
The Smart Building devices publish office comfort, occupancy, lighting,
access, meeting-room, and elevator signals.
"""

import datetime as _dt
import base64
import json
import logging
import math
import os
import random
import sys
import threading
import time
import warnings

import paho.mqtt.client as mqtt
import requests

warnings.filterwarnings("ignore", category=DeprecationWarning)

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("thingsflow-demo-sim")

_publish_total = [0]
_publish_lock = threading.Lock()

TB_BASE_URL = os.environ.get("TB_BASE_URL", "http://thingsflow-flow-core:8080")
TB_USER = os.environ.get("TB_USER", "tenant@thingsboard.org")
TB_PASS = os.environ.get("TB_PASS", "tenant")
MQTT_HOST = os.environ.get("MQTT_HOST", "thingsflow-rmqtt-edge")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
MQTT_CA_CERT = os.environ.get("MQTT_CA_CERT", "/certs/thingsflow-broker-ca.crt")
MQTT_USE_TLS = os.environ.get("MQTT_USE_TLS", os.environ.get("MQTT_TLS", "false")).lower() in (
    "1",
    "true",
    "yes",
    "on",
)
MQTT_AUTH_MODE = os.environ.get("MQTT_AUTH_MODE", "deviceJwtRaw").strip().lower()
PUBLISH_INTERVAL_SEC = float(os.environ.get("PUBLISH_INTERVAL_SEC", "5"))
DEVICE_JWT_REFRESH_MARGIN_SEC = int(os.environ.get("DEVICE_JWT_REFRESH_MARGIN_SEC", "300"))

DEMO_DEVICES = [
    {
        "name": "DHT22 Demo Sensor",
        "type": "default",
        "token": "DEMO_DHT22_TOKEN",
    },
    {
        "name": "Raspberry Pi Demo",
        "type": "default",
        "token": "DEMO_RPI_TOKEN",
    },
    {
        "name": "Thermostat Demo",
        "type": "thermostat",
        "token": "DEMO_THERMOSTAT_TOKEN",
    },
    {
        "name": "Energy Meter Demo",
        "type": "energy_meter",
        "token": "DEMO_ENERGY_METER_TOKEN",
    },
    {
        "name": "Motion Sensor Demo",
        "type": "motion_sensor",
        "token": "DEMO_MOTION_SENSOR_TOKEN",
    },
    {
        "name": "Air Quality Demo",
        "type": "air_quality",
        "token": "DEMO_AIR_QUALITY_TOKEN",
    },
    {
        "name": "Water Tank Demo",
        "type": "water_tank",
        "token": "DEMO_WATER_TANK_TOKEN",
    },
    {
        "name": "Pump Demo",
        "type": "pump",
        "token": "DEMO_PUMP_TOKEN",
    },
    {
        "name": "Flow Meter Demo",
        "type": "flow_meter",
        "token": "DEMO_FLOW_METER_TOKEN",
    },
    {
        "name": "Pressure Sensor Demo",
        "type": "pressure_sensor",
        "token": "DEMO_PRESSURE_SENSOR_TOKEN",
    },
    {
        "name": "Valve Demo",
        "type": "valve",
        "token": "DEMO_VALVE_TOKEN",
    },
    {
        "name": "Office Thermostat Demo",
        "type": "office_thermostat",
        "token": "DEMO_OFFICE_THERMOSTAT_TOKEN",
    },
    {
        "name": "Office Occupancy Demo",
        "type": "office_occupancy",
        "token": "DEMO_OFFICE_OCCUPANCY_TOKEN",
    },
    {
        "name": "Office IAQ Demo",
        "type": "office_air_quality",
        "token": "DEMO_OFFICE_IAQ_TOKEN",
    },
    {
        "name": "Office Smart Light Demo",
        "type": "smart_light",
        "token": "DEMO_OFFICE_LIGHT_TOKEN",
    },
    {
        "name": "Office Door Access Demo",
        "type": "door_access",
        "token": "DEMO_OFFICE_DOOR_TOKEN",
    },
    {
        "name": "Conference Room Meter Demo",
        "type": "conference_room",
        "token": "DEMO_CONFERENCE_ROOM_TOKEN",
    },
    {
        "name": "Elevator Monitor Demo",
        "type": "elevator_monitor",
        "token": "DEMO_ELEVATOR_TOKEN",
    },
]


def _login_jwt() -> str:
    log.info("login %s on %s", TB_USER, TB_BASE_URL)
    r = _request_with_retry(
        "post",
        f"{TB_BASE_URL}/api/auth/login",
        json={"username": TB_USER, "password": TB_PASS},
        label="login",
    )
    return r.json()["token"]


def _fetch_device(jwt: str, name: str) -> dict:
    headers = {"X-Authorization": f"Bearer {jwt}"}
    r = _request_with_retry(
        "get",
        f"{TB_BASE_URL}/api/tenant/devices",
        params={"deviceName": name},
        headers=headers,
        label=f"fetch device {name}",
    )
    return r.json()


def _fetch_token(jwt: str, device_id: str) -> str:
    headers = {"X-Authorization": f"Bearer {jwt}"}
    r = _request_with_retry(
        "get",
        f"{TB_BASE_URL}/api/device/{device_id}/credentials",
        headers=headers,
        label=f"fetch token {device_id}",
    )
    return r.json()["credentialsId"]


def _fetch_device_jwt(jwt: str, device_id: str) -> dict:
    headers = {"X-Authorization": f"Bearer {jwt}"}
    r = _request_with_retry(
        "post",
        f"{TB_BASE_URL}/api/device/{device_id}/jwt",
        headers=headers,
        label=f"issue device jwt {device_id}",
    )
    body = r.json()
    if not body.get("token") or not body.get("mqttIdentity"):
        raise RuntimeError(f"device JWT response incomplete for {device_id}: {body}")
    return body


def _jwt_exp(token: str) -> int:
    try:
        payload = token.split(".")[1]
        payload += "=" * (-len(payload) % 4)
        claims = json.loads(base64.urlsafe_b64decode(payload.encode("ascii")))
        return int(claims.get("exp", 0))
    except Exception:
        return 0


def _request_with_retry(method: str, url: str, *, label: str, attempts: int = 60, **kwargs):
    timeout = kwargs.pop("timeout", 15)
    for attempt in range(1, attempts + 1):
        try:
            r = requests.request(method, url, timeout=timeout, **kwargs)
            r.raise_for_status()
            return r
        except requests.RequestException as e:
            if attempt == attempts:
                raise
            if attempt == 1 or attempt % 10 == 0:
                log.warning("%s attempt %s/%s failed: %s", label, attempt, attempts, e)
            time.sleep(3)


def _verify_demo_loaded() -> list[dict]:
    jwt = _login_jwt()
    fleet = []
    for expected in DEMO_DEVICES:
        body = _fetch_device(jwt, expected["name"])
        device_id = body["id"]["id"]
        device_type = body.get("type", "")
        token = _fetch_token(jwt, device_id)
        if token != expected["token"]:
            raise RuntimeError(
                f"{expected['name']} token mismatch: got {token}, want {expected['token']}"
            )
        if expected["type"] == "thermostat" and device_type != "thermostat":
            raise RuntimeError(
                f"{expected['name']} type mismatch: got {device_type}, want thermostat"
            )
        device_jwt = _fetch_device_jwt(jwt, device_id)
        fleet.append({**expected, "id": device_id, "deviceJwt": device_jwt})
    return fleet


def _daily_wave(now: _dt.datetime, center_hour: int, amplitude: float) -> float:
    minutes = now.hour * 60 + now.minute
    center = center_hour * 60
    return amplitude * math.exp(-((minutes - center) ** 2) / (2 * 210 * 210))


def _payload_for(device: dict, tick: int) -> dict:
    now = _dt.datetime.now(_dt.UTC)
    ambient = 21.5 + _daily_wave(now, 15, 2.0) + random.gauss(0, 0.2)
    humidity = 43.0 + _daily_wave(now, 6, 8.0) + random.gauss(0, 1.2)

    if device["name"] == "DHT22 Demo Sensor":
        return {
            "temperature": round(ambient + random.gauss(0, 0.15), 2),
            "humidity": round(max(20, min(80, humidity)), 1),
            "battery_pct": round(max(20, 98 - tick * 0.002), 2),
        }

    if device["name"] == "Raspberry Pi Demo":
        cpu = max(5, min(95, 35 + 18 * math.sin(tick / 12) + random.gauss(0, 3)))
        return {
            "cpu_usage": round(cpu, 1),
            "memory_usage": round(max(20, min(90, 48 + random.gauss(0, 4))), 1),
            "disk_usage": round(61 + 0.001 * tick, 2),
            "temperature": round(45 + cpu * 0.08 + random.gauss(0, 0.4), 2),
            "rssi_dbm": round(random.gauss(-58, 3), 1),
        }

    if device["name"] == "Energy Meter Demo":
        active_power = max(250, 1350 + 650 * math.sin(tick / 18) + random.gauss(0, 45))
        voltage = 120 + random.gauss(0, 0.8)
        return {
            "active_power_w": round(active_power, 1),
            "voltage_v": round(voltage, 2),
            "current_a": round(active_power / voltage, 2),
            "energy_kwh": round(250 + tick * active_power / 720000, 4),
            "power_factor": round(max(0.75, min(0.99, 0.92 + random.gauss(0, 0.02))), 2),
        }

    if device["name"] == "Motion Sensor Demo":
        occupied = random.random() < (0.22 if 7 <= now.hour <= 20 else 0.04)
        return {
            "motion_detected": occupied,
            "lux": round(max(0, 440 * math.sin(math.pi * ((now.hour * 60 + now.minute) / 1440)) + random.gauss(0, 18)), 1),
            "occupancy_count": random.randint(1, 9) if occupied else 0,
            "battery_pct": round(max(30, 96 - tick * 0.0015), 2),
        }

    if device["name"] == "Air Quality Demo":
        co2 = 520 + _daily_wave(now, 14, 310) + random.gauss(0, 22)
        pm25 = max(2, 9 + _daily_wave(now, 18, 14) + random.gauss(0, 2))
        return {
            "co2": round(co2, 1),
            "pm2_5": round(pm25, 2),
            "pm10": round(pm25 * 1.8 + random.gauss(0, 3), 2),
            "voc": round(max(40, 140 + _daily_wave(now, 11, 90) + random.gauss(0, 12)), 1),
            "temperature": round(ambient + random.gauss(0, 0.2), 2),
            "humidity": round(max(20, min(80, humidity)), 1),
        }

    if device["name"] == "Water Tank Demo":
        level = max(8, min(96, 56 + 28 * math.sin(tick / 32) + random.gauss(0, 1.1)))
        volume = 12500 * level / 100
        return {
            "level_pct": round(level, 1),
            "volume_l": round(volume, 0),
            "temperature": round(ambient - 1.5 + random.gauss(0, 0.12), 2),
            "leak_detected": level < 14 and random.random() < 0.08,
        }

    if device["name"] == "Pump Demo":
        running = (tick // 24) % 5 != 0
        rpm = max(0, 1760 + random.gauss(0, 25)) if running else 0
        vibration = max(0.3, 3.2 + 1.1 * math.sin(tick / 16) + random.gauss(0, 0.22)) if running else 0.2
        return {
            "running": running,
            "pump_status": "RUNNING" if running else "IDLE",
            "rpm": round(rpm, 0),
            "vibration_mm_s": round(vibration, 2),
            "motor_temp_c": round((62 + rpm * 0.006 + random.gauss(0, 0.6)) if running else ambient + 4, 2),
            "power_kw": round((5.8 + rpm * 0.002 + random.gauss(0, 0.15)) if running else 0, 2),
        }

    if device["name"] == "Flow Meter Demo":
        flow = max(0, 245 + 95 * math.sin(tick / 27) + random.gauss(0, 8))
        return {
            "flow_rate_l_min": round(flow, 1),
            "total_volume_l": round(15000 + tick * flow * PUBLISH_INTERVAL_SEC / 60, 1),
            "velocity_m_s": round(flow / 180, 2),
        }

    if device["name"] == "Pressure Sensor Demo":
        pressure_bar = max(1.5, 5.2 + 1.3 * math.sin(tick / 21) + random.gauss(0, 0.08))
        return {
            "pressure_bar": round(pressure_bar, 2),
            "pressure_psi": round(pressure_bar * 14.5038, 1),
            "temperature": round(ambient + 2.2 + random.gauss(0, 0.15), 2),
        }

    if device["name"] == "Valve Demo":
        phase = (math.sin(tick / 24) + 1) / 2
        position = max(0, min(100, 12 + phase * 82 + random.gauss(0, 1.5)))
        return {
            "valve_open": position > 10,
            "position_pct": round(position, 1),
            "command_state": "OPEN" if position > 70 else "CLOSED" if position < 20 else "MODULATING",
        }

    if device["name"] == "Office Thermostat Demo":
        setpoint = 22.0 if 7 <= now.hour <= 19 else 19.0
        temp = ambient + 0.5 * math.sin(tick / 18) + random.gauss(0, 0.15)
        return {
            "temperature": round(temp, 2),
            "humidity": round(max(25, min(70, humidity - 2)), 1),
            "setpoint": setpoint,
            "hvac_mode": "cool" if temp > setpoint + 0.5 else "heat" if temp < setpoint - 0.5 else "idle",
            "comfort_score": round(max(0, min(100, 100 - abs(temp - setpoint) * 18)), 1),
        }

    if device["name"] == "Office Occupancy Demo":
        workday_load = _daily_wave(now, 11, 72) + _daily_wave(now, 15, 58)
        occupants = int(max(0, min(120, workday_load + random.gauss(0, 5))))
        return {
            "occupied": occupants > 0,
            "occupancy_count": occupants,
            "occupancy_pct": round(occupants / 120 * 100, 1),
            "people_in": max(0, int(random.gauss(4, 2))) if 7 <= now.hour <= 10 else max(0, int(random.gauss(1, 1))),
            "people_out": max(0, int(random.gauss(4, 2))) if 16 <= now.hour <= 19 else max(0, int(random.gauss(1, 1))),
        }

    if device["name"] == "Office IAQ Demo":
        co2 = 450 + _daily_wave(now, 13, 430) + random.gauss(0, 24)
        return {
            "co2": round(co2, 1),
            "voc": round(max(45, 130 + _daily_wave(now, 12, 150) + random.gauss(0, 14)), 1),
            "pm2_5": round(max(2, 7 + _daily_wave(now, 18, 8) + random.gauss(0, 1.2)), 2),
            "iaq_score": round(max(0, min(100, 100 - max(0, co2 - 650) / 12)), 1),
            "temperature": round(ambient + random.gauss(0, 0.2), 2),
            "humidity": round(max(25, min(70, humidity)), 1),
        }

    if device["name"] == "Office Smart Light Demo":
        occupied = 7 <= now.hour <= 19 and random.random() < 0.82
        daylight = max(0, 520 * math.sin(math.pi * ((now.hour * 60 + now.minute) / 1440)))
        dimming = max(10, min(100, 85 - daylight / 10)) if occupied else 0
        return {
            "light_on": occupied,
            "dimming_pct": round(dimming, 1),
            "lux": round(daylight + dimming * 4.2 + random.gauss(0, 12), 1),
            "energy_w": round(dimming * 1.8 if occupied else 0, 1),
            "auto_mode": True,
        }

    if device["name"] == "Office Door Access Demo":
        busy = 7 <= now.hour <= 10 or 16 <= now.hour <= 19
        access_events = max(0, int(random.gauss(6 if busy else 1, 2)))
        denied = random.random() < (0.04 if busy else 0.01)
        return {
            "door_open": random.random() < (0.18 if busy else 0.03),
            "access_events": access_events,
            "access_denied": denied,
            "tailgate_detected": denied and random.random() < 0.15,
            "last_badge_status": "DENIED" if denied else "GRANTED",
        }

    if device["name"] == "Conference Room Meter Demo":
        in_meeting = 8 <= now.hour <= 18 and (tick // 18) % 3 != 0
        attendees = random.randint(2, 14) if in_meeting else 0
        return {
            "occupied": in_meeting,
            "attendees": attendees,
            "occupancy_pct": round(attendees / 16 * 100, 1),
            "noise_db": round((48 + attendees * 1.8 + random.gauss(0, 2)) if in_meeting else 34 + random.gauss(0, 1), 1),
            "screen_on": in_meeting and random.random() < 0.72,
            "booking_state": "IN_USE" if in_meeting else "AVAILABLE",
        }

    if device["name"] == "Elevator Monitor Demo":
        moving = (tick // 10) % 4 != 0
        load = max(0, min(100, 38 + _daily_wave(now, 9, 32) + _daily_wave(now, 17, 28) + random.gauss(0, 5)))
        return {
            "moving": moving,
            "floor": 1 + (tick // 3) % 12 if moving else 1 + (tick // 30) % 12,
            "load_pct": round(load, 1),
            "door_state": "CLOSED" if moving else "OPEN",
            "vibration_mm_s": round((2.1 + random.gauss(0, 0.18)) if moving else 0.35, 2),
            "motor_temp_c": round((48 + load * 0.18 + random.gauss(0, 0.6)) if moving else 36 + random.gauss(0, 0.3), 2),
        }

    setpoint = 22.0
    temp = ambient + random.gauss(0, 0.2)
    mode = "heat" if temp < setpoint - 0.4 else "cool" if temp > setpoint + 0.4 else "idle"
    return {
        "temperature": round(temp, 2),
        "humidity": round(max(20, min(80, humidity - 3)), 1),
        "setpoint": setpoint,
        "hvac_mode": mode,
    }


def _publish_loop(device: dict, idx: int):
    # The Client ID must equal the JWT's `clientid` claim: rmqtt's
    # validate_claims.clientid refuses the CONNECT otherwise, and the ACL scopes
    # publishes to %c (this same id). A synthetic id like "thingsflow-demo-<pod>"
    # is rejected at CONNECT while the publish loop keeps running against a
    # disconnected client — so the simulator reports publishing normally while
    # nothing reaches the broker. Derive it from the same claim the topic uses.
    client = mqtt.Client(
        client_id=device["deviceJwt"]["mqttIdentity"], clean_session=True
    )

    def apply_mqtt_auth():
        if MQTT_AUTH_MODE == "devicejwtraw":
            client.username_pw_set(device["deviceJwt"]["token"])
        elif MQTT_AUTH_MODE != "none":
            client.username_pw_set(device["deviceJwt"]["mqttUsername"])

    def refresh_device_jwt(reason: str):
        log.info("[%s] refreshing Device JWT (%s)", device["name"], reason)
        jwt = _login_jwt()
        device["deviceJwt"] = _fetch_device_jwt(jwt, device["id"])
        apply_mqtt_auth()
        try:
            client.disconnect()
        except Exception:
            pass
        time.sleep(1)
        client.reconnect()

    apply_mqtt_auth()
    if MQTT_USE_TLS:
        client.tls_set(ca_certs=MQTT_CA_CERT)

    connect_state = {"rc": None, "last_log": 0.0, "auth_rejected": False}
    disconnect_state = {"rc": None, "last_log": 0.0, "needs_reconnect": False, "backoff": 1.0}

    def on_connect(c, ud, flags, rc):
        now = time.monotonic()
        if rc == 0:
            if connect_state["rc"] != 0:
                log.info("[%s] MQTT connected", device["name"])
            connect_state["rc"] = 0
            return
        if connect_state["rc"] != rc or now - connect_state["last_log"] > 60:
            log.warning("[%s] MQTT connect rc=%s; retrying", device["name"], rc)
            connect_state["last_log"] = now
        connect_state["rc"] = rc
        if rc == 5 and MQTT_AUTH_MODE != "none":
            connect_state["auth_rejected"] = True

    def on_disconnect(c, ud, rc):
        if rc == 0:
            return
        now = time.monotonic()
        if disconnect_state["rc"] != rc or now - disconnect_state["last_log"] > 60:
            log.warning("[%s] MQTT disconnected rc=%s; will reconnect", device["name"], rc)
            disconnect_state["last_log"] = now
        disconnect_state["rc"] = rc
        # Ask the publish loop to reconnect. paho's background loop does not always
        # re-establish the session on its own, and when it does not the failure is
        # quiet: the threads stay alive, the loop keeps calling publish(), and every
        # call returns rc=4 into the void. Observed after a broker restart — 18
        # devices disconnected and none came back for eight minutes while the log
        # claimed it was retrying.
        disconnect_state["needs_reconnect"] = True

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect

    for attempt in range(12):
        try:
            client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
            break
        except Exception as e:
            log.warning("[%s] TCP connect attempt %s/12 failed: %s", device["name"], attempt + 1, e)
            time.sleep(5)
    else:
        raise RuntimeError(f"{device['name']} cannot connect to MQTT")

    client.loop_start()
    time.sleep(idx)
    tick = 0
    while True:
        # Re-establish the session before doing anything else. Without this a
        # broker restart is terminal for this thread: every later publish returns
        # rc=4 and the device goes silent while the process looks healthy.
        if disconnect_state["needs_reconnect"]:
            try:
                client.reconnect()
                disconnect_state["needs_reconnect"] = False
                disconnect_state["backoff"] = 1.0
                log.info("[%s] MQTT reconnected", device["name"])
            except Exception as e:
                # Capped backoff: a broker that is still coming up should not be
                # hammered by every simulated device at once.
                wait = min(disconnect_state["backoff"], 30.0)
                log.warning("[%s] reconnect failed (%s); retrying in %.0fs",
                            device["name"], e, wait)
                time.sleep(wait)
                disconnect_state["backoff"] = min(disconnect_state["backoff"] * 2, 30.0)
                continue

        exp = _jwt_exp(device["deviceJwt"]["token"])
        if MQTT_AUTH_MODE != "none" and exp and time.time() > exp - DEVICE_JWT_REFRESH_MARGIN_SEC:
            refresh_device_jwt("expiring")
        elif connect_state["auth_rejected"]:
            connect_state["auth_rejected"] = False
            refresh_device_jwt("auth rejected")
        payload = _payload_for(device, tick)
        # The data plane requires a deterministic timestamp: its idempotency key is
        # derived from `ts`, and a payload without one is dropped by the materializer
        # without an error anywhere. Unlike stock ThingsBoard there is no server-side
        # timestamping to fall back on, so a ts-less simulator publishes happily into
        # a void and the demo install comes up with every pod green and no data.
        payload["ts"] = int(time.time() * 1000)
        topic = f"thingsflow/devices/{device['deviceJwt']['mqttIdentity']}/telemetry"
        info = client.publish(topic, json.dumps(payload), qos=0)
        tick += 1
        # Count what the client actually accepted for delivery. Incrementing
        # unconditionally is how a rejected CONNECT stayed invisible: the loop kept
        # reporting a healthy publish rate while every message was discarded locally.
        if getattr(info, "rc", 0) == 0:
            with _publish_lock:
                _publish_total[0] += 1
        elif tick % 12 == 0:
            log.warning("[%s] publish rejected locally (rc=%s); is the client connected?",
                        device["name"], getattr(info, "rc", "?"))
        if tick % 12 == 0:
            log.debug("[%s] sent %s messages; last=%s", device["name"], tick, payload)
        time.sleep(PUBLISH_INTERVAL_SEC)


def main():
    fleet = _verify_demo_loaded()
    scheme = "mqtts" if MQTT_USE_TLS else "mqtt"
    log.info("demo loaded: %s/%s devices; streaming to %s://%s:%s", len(fleet), len(DEMO_DEVICES), scheme, MQTT_HOST, MQTT_PORT)

    threads = []
    for idx, device in enumerate(fleet):
        thread = threading.Thread(target=_publish_loop, args=(device, idx), daemon=True)
        thread.start()
        threads.append(thread)

    last_total = 0
    while True:
        time.sleep(30)
        alive = sum(1 for thread in threads if thread.is_alive())
        with _publish_lock:
            cur = _publish_total[0]
        delta = cur - last_total
        last_total = cur
        log.info(
            "[Heartbeat] demo %s/%s threads | published %s msgs in last 30s (%.1f/s) | total %s",
            alive,
            len(threads),
            delta,
            delta / 30,
            cur,
        )
        if alive != len(threads):
            sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.info("shutting down")
