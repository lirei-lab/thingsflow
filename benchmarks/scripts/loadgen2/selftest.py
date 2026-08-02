#!/usr/bin/env python3
"""Offline self-tests for loadgen2.

Covers the parts whose correctness the live smoke runs cannot prove:

  * the open-loop schedule's arithmetic (an off-by-one here reports a phantom
    100 ms of lag at 10 msg/s -- it did, once);
  * histogram percentiles and merging across worker processes;
  * payload shaping, key naming, and the strictly-increasing `ts` that makes
    "landed rows == accepted * keys" an exact identity rather than a hope;
  * the hand-built MQTT packets, decoded by the sink's independent parser;
  * BOTH target adapters' provisioning and endpoint shaping, driven against a
    stub control plane. This is the only coverage the ThingsBoard adapter has
    while `tb-classic` is scaled to zero, so it asserts the exact contract:
    ACCESS_TOKEN in the URL path, `v1/devices/me/telemetry`, token as MQTT
    username -- versus ThingsFlow's device JWT, Bearer header, and the
    mqttId-pinned client id and topic.

Run:  ./run.sh --selftest      (or:  .venv/bin/python selftest.py)
"""

from __future__ import annotations

import http.server
import json
import os
import sys
import threading
import uuid

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from rawtransport import build_connect  # noqa: E402
from senders import PayloadBuilder  # noqa: E402
from sink import MqttSinkProtocol, _BatchCounter, _varint  # noqa: E402
from stats import Histogram, Schedule  # noqa: E402
from targets import ThingsBoardTarget, ThingsFlowTarget  # noqa: E402

FAILED = []


def check(name, cond, detail=""):
    if cond:
        print(f"  ok   {name}")
    else:
        print(f"  FAIL {name} {detail}")
        FAILED.append(name)


# ---------------------------------------------------------------------------


def test_schedule():
    print("schedule")
    s = Schedule(rate=10, ramp=0)
    check("message 0 is due at t=0", s.due(0.0) == 1)
    check("sched_time(k) == k/rate", abs(s.sched_time(7) - 0.7) < 1e-9)
    # the bug this guards: due() must release k once t >= k/rate, not k+1/rate
    check("no self-inflicted lag", s.due(0.70001) == 8, f"got {s.due(0.70001)}")
    check("total over 10s", s.total(10.0) == 100, f"got {s.total(10.0)}")

    r = Schedule(rate=1000, ramp=5)
    check("ramp halves the first window", r.total(5.0) == 2500, f"got {r.total(5.0)}")
    check("post-ramp is linear", r.total(10.0) == 7500, f"got {r.total(10.0)}")
    for k in (0, 1, 100, 2499, 2500, 5000):
        t = r.sched_time(k)
        check(f"sched_time/due inverse at k={k}", r.due(t) >= k + 1, f"due={r.due(t)}")


def test_histogram():
    print("histogram")
    h = Histogram()
    for v in range(1, 10001):
        h.record(v * 100)  # 0.1ms .. 1000ms
    s = h.summary()
    check("count", s["count"] == 10000)
    check("p50 within 1%", abs(s["p50_ms"] - 500.0) / 500.0 < 0.01, s["p50_ms"])
    check("p99 within 1%", abs(s["p99_ms"] - 990.0) / 990.0 < 0.01, s["p99_ms"])
    check("max is exact", s["max_ms"] == 1000.0, s["max_ms"])
    a, b = Histogram(), Histogram()
    for v in range(1, 5001):
        a.record(v * 100)
    for v in range(5001, 10001):
        b.record(v * 100)
    m = Histogram.merge([a.to_dict(), b.to_dict()])
    check("merge preserves count", m.count == 10000)
    check("merge preserves p50", abs(m.percentile(0.5) - s["p50_ms"]) < 1.0)


def test_payload():
    print("payload")
    p = PayloadBuilder("lg2_ab12", 3, 0)
    check("keys per message", p.keys_per_message == 3)
    body = json.loads(p.build(1785597729757, 0))
    check("ts present", body["ts"] == 1785597729757)
    check("key naming", sorted(k for k in body if k != "ts") ==
          ["lg2_ab12_v0", "lg2_ab12_v1", "lg2_ab12_v2"])
    pad = PayloadBuilder("lg2_ab12", 2, 512)
    check("padding adds one row-producing key", pad.keys_per_message == 3)
    check("padded size near target", abs(len(pad.build(1785597729757, 3)) - 512) <= 2,
          len(pad.build(1785597729757, 3)))

    # every variant must be valid JSON and the same length family
    check("all variants parse", all(json.loads(p.build(1785597729757, i)) for i in range(64)))


def test_mqtt_packets():
    print("mqtt wire format")
    pkt = build_connect("dev001", "the-jwt", "", 60)
    check("CONNECT type", pkt[0] == 0x10)
    val, i = _varint(pkt, 1)
    check("CONNECT length", val == len(pkt) - i, f"{val} vs {len(pkt) - i}")
    check("protocol name", pkt[i : i + 6] == b"\x00\x04MQTT")
    check("protocol level 4 (3.1.1)", pkt[i + 6] == 4)
    check("clean session + username flags", pkt[i + 7] == 0x82, hex(pkt[i + 7]))

    # feed a real PUBLISH through the sink's independent decoder
    class T:
        def __init__(self):
            self.out = b""

        def write(self, b):
            self.out += b

        def get_extra_info(self, _):
            return None

        def close(self):
            pass

    from rawtransport import MqttConnection

    class Owner:
        acks = []

        def on_puback(self, lat):
            Owner.acks.append(lat)

        def on_connack(self, c):
            pass

        def on_conn_lost(self, *a):
            pass

    class Dev:
        mqtt_topic = "thingsflow/devices/abc/telemetry"

    conn = MqttConnection(Owner(), 0, Dev.mqtt_topic)
    t = T()
    conn.connection_made(t)
    pid = conn.publish(b'{"ts":1,"x":2}', 1)
    check("packet id starts at 1", pid == 1)

    class SharedInt:
        def __init__(self):
            self.value = 0

        def get_lock(self):
            class L:
                def __enter__(self_):
                    return None

                def __exit__(self_, *a):
                    return False

            return L()

    shared = SharedInt()
    counter = _BatchCounter(shared, flush_every=1)
    sink = MqttSinkProtocol(counter)
    st = T()
    sink.connection_made(st)
    sink.data_received(t.out)
    check("sink decoded the PUBLISH", shared.value == 1, shared.value)
    check("sink replied PUBACK for pid 1", st.out == b"\x40\x02\x00\x01", st.out)
    conn.data_received(st.out)
    check("client matched the PUBACK", len(Owner.acks) == 1)
    check("pending cleared", conn.pending == {})


# ---------------------------------------------------------------------------
# stub control planes
# ---------------------------------------------------------------------------

TENANT = "aaaaaaaa-1dd2-11b2-8080-808080808080"


class StubHandler(http.server.BaseHTTPRequestHandler):
    flavour = "thingsflow"
    devices = {}

    def log_message(self, *a):
        pass

    def _send(self, obj, code=200):
        b = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def do_GET(self):
        if "/api/tenant/devices" in self.path:
            self._send({"status": 404, "message": "not found"}, 404)
            return
        if self.path.endswith("/credentials"):
            did = self.path.split("/")[3]
            self._send({"deviceId": {"id": did}, "credentialsType": "ACCESS_TOKEN",
                        "credentialsId": "TOKEN" + did[:15].replace("-", "")})
            return
        self._send({}, 404)

    def do_POST(self):
        n = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(n) or b"{}")
        if self.path == "/api/auth/login":
            self._send({"token": "tenant-jwt", "refreshToken": "r"})
        elif self.path == "/api/device":
            did = str(uuid.uuid4())
            StubHandler.devices[did] = body["name"]
            self._send({"id": {"entityType": "DEVICE", "id": did}, "name": body["name"],
                        "tenantId": {"id": TENANT}})
        elif self.path.endswith("/jwt"):
            did = self.path.split("/")[3]
            mid = did.replace("-", "")
            self._send({"token": "device-jwt-" + mid, "tokenType": "Bearer",
                        "mqttIdentity": mid, "mqttUsername": "Bearer device-jwt-" + mid})
        else:
            self._send({}, 404)

    def do_DELETE(self):
        self.send_response(200)
        self.send_header("Content-Length", "0")
        self.end_headers()


def _stub_server():
    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), StubHandler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv, f"http://127.0.0.1:{srv.server_address[1]}"


def test_targets():
    print("target adapters (stub control plane)")
    srv, base = _stub_server()
    try:
        tf = ThingsFlowTarget(base, "http://ingest:8081", "mqtt-host", 1883, "u", "p")
        devs, errs = tf.provision(3, "st", parallelism=3)
        check("thingsflow provisioned 3", len(devs) == 3 and not errs, errs)
        d = devs[0]
        check("tf http url", d.http_url == "http://ingest:8081/api/v1/telemetry", d.http_url)
        check("tf uses Bearer device JWT",
              d.http_headers["Authorization"].startswith("Bearer device-jwt-"),
              d.http_headers)
        check("tf client id == mqttId claim", d.mqtt_client_id == d.device_id.replace("-", ""))
        check("tf mqtt username is the raw JWT", d.mqtt_username.startswith("device-jwt-"))
        check("tf topic is pinned to its own mqttId",
              d.mqtt_topic == f"thingsflow/devices/{d.mqtt_client_id}/telemetry", d.mqtt_topic)

        tfb = ThingsFlowTarget(base, "http://i:1", "h", 1883, "u", "p",
                               mqtt_username_mode="bearer")
        db = tfb.provision(1, "st2", parallelism=1)[0][0]
        check("tf bearer mode", db.mqtt_username.startswith("Bearer device-jwt-"), db.mqtt_username)

        tb = ThingsBoardTarget(base, "http://tbhttp:8081", "tbmqtt", 1883, "u", "p")
        tdevs, terrs = tb.provision(2, "st", parallelism=2)
        check("thingsboard provisioned 2", len(tdevs) == 2 and not terrs, terrs)
        t = tdevs[0]
        check("tb token is in the URL path",
              t.http_url.startswith("http://tbhttp:8081/api/v1/TOKEN")
              and t.http_url.endswith("/telemetry"), t.http_url)
        check("tb sends no Authorization header", "Authorization" not in t.http_headers)
        check("tb mqtt username is the access token", t.mqtt_username.startswith("TOKEN"))
        check("tb topic is v1/devices/me/telemetry", t.mqtt_topic == "v1/devices/me/telemetry")
        check("tb has no landed check", tb.supports_landed_verification is False)
        check("tf has a landed check", tf.supports_landed_verification is True)
        check("cleanup deletes", tf.cleanup(devs)["deleted"] == 3)
    finally:
        srv.shutdown()


def main():
    test_schedule()
    test_histogram()
    test_payload()
    test_mqtt_packets()
    test_targets()
    print()
    if FAILED:
        print(f"FAILED {len(FAILED)}: {FAILED}")
        return 1
    print("all self-tests passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
