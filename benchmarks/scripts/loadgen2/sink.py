"""Trivial local sinks used to calibrate the generator against itself.

Calibration needs an endpoint that is, as far as possible, *not* the bottleneck,
so that whatever rate the client stops being able to offer is the client's own
ceiling. These sinks do the minimum legal work:

  * HTTP: read the request, answer `204 No Content`, keep the connection alive.
  * MQTT: answer CONNECT with CONNACK, QoS1 PUBLISH with PUBACK, PINGREQ with
    PINGRESP. Nothing is stored, routed or parsed beyond what the framing needs.

They still cost CPU on this host, which is why the sink runs in its own
process(es) and the harness reports the sink's CPU separately. A calibration
number is only honest next to the CPU the sink took to produce it.
"""

from __future__ import annotations

import asyncio
import multiprocessing as mp
import os
import socket
import time

HTTP_204 = b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n"
CONNACK = b"\x20\x02\x00\x00"
PINGRESP = b"\xd0\x00"


class HttpSinkProtocol(asyncio.Protocol):
    __slots__ = ("transport", "buf", "counter")

    def __init__(self, counter):
        self.buf = b""
        self.counter = counter
        self.transport = None

    def connection_made(self, transport):
        try:
            transport.get_extra_info("socket").setsockopt(
                socket.IPPROTO_TCP, socket.TCP_NODELAY, 1
            )
        except Exception:  # noqa: BLE001
            pass
        self.transport = transport

    def data_received(self, data):
        self.buf += data
        n = 0
        while True:
            head_end = self.buf.find(b"\r\n\r\n")
            if head_end < 0:
                break
            head = self.buf[:head_end]
            clen = 0
            idx = head.lower().find(b"content-length:")
            if idx >= 0:
                end = head.find(b"\r\n", idx)
                seg = head[idx + 15 : end if end > 0 else len(head)]
                try:
                    clen = int(seg.strip())
                except ValueError:
                    clen = 0
            total = head_end + 4 + clen
            if len(self.buf) < total:
                break
            self.buf = self.buf[total:]
            n += 1
        if n:
            self.transport.write(HTTP_204 * n)
            self.counter.add(n)


def _varint(buf, i):
    """Decode an MQTT remaining-length varint. Returns (value, next_index) or None."""
    mult = 1
    value = 0
    for _ in range(4):
        if i >= len(buf):
            return None
        b = buf[i]
        i += 1
        value += (b & 0x7F) * mult
        if not (b & 0x80):
            return value, i
        mult *= 128
    return None


class MqttSinkProtocol(asyncio.Protocol):
    __slots__ = ("transport", "buf", "counter")

    def __init__(self, counter):
        self.buf = b""
        self.counter = counter
        self.transport = None

    def connection_made(self, transport):
        try:
            transport.get_extra_info("socket").setsockopt(
                socket.IPPROTO_TCP, socket.TCP_NODELAY, 1
            )
        except Exception:  # noqa: BLE001
            pass
        self.transport = transport

    def data_received(self, data):
        self.buf += data
        out = bytearray()
        pubs = 0
        while self.buf:
            if len(self.buf) < 2:
                break
            hdr = self.buf[0]
            dec = _varint(self.buf, 1)
            if dec is None:
                break
            rem, body_start = dec
            total = body_start + rem
            if len(self.buf) < total:
                break
            ptype = hdr >> 4
            body = self.buf[body_start:total]
            self.buf = self.buf[total:]
            if ptype == 3:  # PUBLISH
                qos = (hdr >> 1) & 0x03
                pubs += 1
                if qos == 1 and len(body) >= 2:
                    tlen = (body[0] << 8) | body[1]
                    pid_off = 2 + tlen
                    if len(body) >= pid_off + 2:
                        out += b"\x40\x02" + body[pid_off : pid_off + 2]
            elif ptype == 1:  # CONNECT
                out += CONNACK
            elif ptype == 12:  # PINGREQ
                out += PINGRESP
            elif ptype == 8:  # SUBSCRIBE
                if len(body) >= 2:
                    out += b"\x90\x03" + body[0:2] + b"\x01"
            elif ptype == 14:  # DISCONNECT
                self.transport.close()
                return
        if out:
            self.transport.write(bytes(out))
        if pubs:
            self.counter.add(pubs)


class _BatchCounter:
    """Batch shared-memory increments; per-message contention would dominate."""

    __slots__ = ("shared", "n", "flush_every")

    def __init__(self, shared, flush_every=500):
        self.shared = shared
        self.n = 0
        self.flush_every = flush_every

    def add(self, k):
        self.n += k
        if self.n >= self.flush_every:
            with self.shared.get_lock():
                self.shared.value += self.n
            self.n = 0

    def flush(self):
        if self.n:
            with self.shared.get_lock():
                self.shared.value += self.n
            self.n = 0


def _reuseport_socket(host, port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    s.bind((host, port))
    s.listen(4096)
    s.setblocking(False)
    return s


def _sink_worker(host, http_port, mqtt_port, http_shared, mqtt_shared, stop_evt):
    async def amain():
        hc = _BatchCounter(http_shared)
        mc = _BatchCounter(mqtt_shared)
        loop = asyncio.get_running_loop()
        servers = []
        if http_port:
            servers.append(
                await loop.create_server(
                    lambda: HttpSinkProtocol(hc), sock=_reuseport_socket(host, http_port)
                )
            )
        if mqtt_port:
            servers.append(
                await loop.create_server(
                    lambda: MqttSinkProtocol(mc), sock=_reuseport_socket(host, mqtt_port)
                )
            )
        while not stop_evt.is_set():
            await asyncio.sleep(0.2)
            hc.flush()
            mc.flush()
        for s in servers:
            s.close()
        hc.flush()
        mc.flush()

    asyncio.run(amain())


class SinkFleet:
    """Owns the sink processes and reports their CPU cost."""

    def __init__(self, host="127.0.0.1", http_port=18081, mqtt_port=18883, workers=2):
        self.host = host
        self.http_port = http_port
        self.mqtt_port = mqtt_port
        self.workers = workers
        self.http_count = mp.Value("q", 0)
        self.mqtt_count = mp.Value("q", 0)
        self.stop = mp.Event()
        self.procs = []
        self._cpu0 = None
        self._t0 = None

    def start(self):
        ctx = mp.get_context("fork")
        for _ in range(self.workers):
            p = ctx.Process(
                target=_sink_worker,
                args=(
                    self.host,
                    self.http_port,
                    self.mqtt_port,
                    self.http_count,
                    self.mqtt_count,
                    self.stop,
                ),
                daemon=True,
            )
            p.start()
            self.procs.append(p)
        # wait for the listeners to accept
        deadline = time.time() + 15
        for port in (self.http_port, self.mqtt_port):
            while time.time() < deadline:
                try:
                    with socket.create_connection((self.host, port), timeout=0.5):
                        break
                except OSError:
                    time.sleep(0.05)
            else:
                raise RuntimeError(f"sink port {port} never came up")
        self._cpu0 = self.cpu_seconds()
        self._t0 = time.time()

    def cpu_seconds(self):
        try:
            import psutil
        except ImportError:
            return None
        total = 0.0
        for p in self.procs:
            try:
                t = psutil.Process(p.pid).cpu_times()
                total += t.user + t.system
            except Exception:  # noqa: BLE001
                pass
        return total

    def mark(self):
        """Reset the CPU/message baseline (call between calibration steps)."""
        self._cpu0 = self.cpu_seconds()
        self._t0 = time.time()
        return {"http": self.http_count.value, "mqtt": self.mqtt_count.value}

    def report(self, baseline=None):
        cpu = self.cpu_seconds()
        wall = time.time() - (self._t0 or time.time())
        base = baseline or {"http": 0, "mqtt": 0}
        return {
            "sink_workers": self.workers,
            "sink_http_received": self.http_count.value - base["http"],
            "sink_mqtt_publishes_received": self.mqtt_count.value - base["mqtt"],
            "sink_cpu_seconds": round(cpu - self._cpu0, 3) if cpu is not None else None,
            "sink_cpu_cores_avg": round((cpu - self._cpu0) / wall, 3)
            if cpu is not None and wall > 0
            else None,
        }

    def stop_all(self):
        self.stop.set()
        for p in self.procs:
            p.join(timeout=5)
            if p.is_alive():
                p.terminate()


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(description="Run the loadgen2 calibration sinks standalone.")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--http-port", type=int, default=18081)
    ap.add_argument("--mqtt-port", type=int, default=18883)
    ap.add_argument("--workers", type=int, default=2)
    a = ap.parse_args()
    fleet = SinkFleet(a.host, a.http_port, a.mqtt_port, a.workers)
    fleet.start()
    print(f"sink up: http={a.host}:{a.http_port} mqtt={a.host}:{a.mqtt_port} pid={os.getpid()}")
    try:
        while True:
            time.sleep(5)
            print(f"http={fleet.http_count.value} mqtt={fleet.mqtt_count.value}", flush=True)
    except KeyboardInterrupt:
        fleet.stop_all()
