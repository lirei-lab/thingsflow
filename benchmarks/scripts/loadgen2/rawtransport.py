"""Minimal asyncio HTTP/1.1 and MQTT 3.1.1 publishers.

`paho-mqtt` and `aiohttp` are correct and are kept as the default-comparable
engines (`--engine lib`), but they cost roughly 0.4 ms of CPU per message on this
host, which puts the generator's own ceiling around 10k msg/s inside a 4-core
budget. That is close enough to the numbers we want to measure that the client
could become the bottleneck -- the exact failure this tool exists to avoid.

These engines do only what the wire needs:

  * HTTP: pre-rendered request head per device, one outstanding request per
    connection (no pipelining -- that would hide head-of-line blocking and is not
    what a device fleet does), bounded keep-alive pool.
  * MQTT: hand-built CONNECT/PUBLISH/PUBACK/PINGREQ over one socket per device,
    which is what MQTT actually is.

Both count what they could not place on the wire instead of absorbing it, same
as the library engines. `--engine lib` exists to cross-check them: on the same
target both must report the same accepted count and land the same rows.
"""

from __future__ import annotations

import asyncio
import socket
import time
from urllib.parse import urlsplit


# ---------------------------------------------------------------------------
# HTTP/1.1
# ---------------------------------------------------------------------------


def build_request_head(url: str, headers: dict) -> tuple[str, int, bytes]:
    """Return (host, port, head-template) where the template lacks Content-Length."""
    u = urlsplit(url)
    host = u.hostname
    port = u.port or (443 if u.scheme == "https" else 80)
    path = u.path or "/"
    if u.query:
        path += "?" + u.query
    lines = [f"POST {path} HTTP/1.1", f"Host: {u.netloc}"]
    for k, v in headers.items():
        if k.lower() == "content-length":
            continue
        lines.append(f"{k}: {v}")
    return host, port, ("\r\n".join(lines) + "\r\nContent-Length: ").encode()


class HttpConnection(asyncio.Protocol):
    """One keep-alive connection carrying one request at a time."""

    __slots__ = ("owner", "transport", "buf", "sent_at", "busy", "closed", "_need", "_chunked")

    def __init__(self, owner):
        self.owner = owner
        self.transport = None
        self.buf = b""
        self.sent_at = 0.0
        self.busy = False
        self.closed = False

    def connection_made(self, transport):
        try:
            transport.get_extra_info("socket").setsockopt(
                socket.IPPROTO_TCP, socket.TCP_NODELAY, 1
            )
        except Exception:  # noqa: BLE001
            pass
        self.transport = transport

    def send(self, head: bytes, body: bytes):
        self.busy = True
        self.sent_at = time.perf_counter()
        self.transport.write(head + str(len(body)).encode() + b"\r\n\r\n" + body)

    def data_received(self, data):
        self.buf += data
        while True:
            he = self.buf.find(b"\r\n\r\n")
            if he < 0:
                return
            head = self.buf[:he]
            nl = head.find(b"\r\n")
            first = head[:nl] if nl > 0 else head
            try:
                status = int(first.split(b" ")[1])
            except Exception:  # noqa: BLE001
                status = 0
            low = head.lower()
            body_len = 0
            chunked = b"transfer-encoding: chunked" in low
            if chunked:
                # Walk the chunk framing; ingest replies are tiny so this is rare.
                i = he + 4
                total = 0
                while True:
                    e = self.buf.find(b"\r\n", i)
                    if e < 0:
                        return
                    try:
                        n = int(self.buf[i:e].split(b";")[0], 16)
                    except ValueError:
                        n = 0
                    if n == 0:
                        end = self.buf.find(b"\r\n\r\n", e)
                        if end < 0:
                            return
                        total = end + 4 - (he + 4)
                        break
                    i = e + 2 + n + 2
                    if i > len(self.buf):
                        return
                body_len = total
            else:
                ci = low.find(b"content-length:")
                if ci >= 0:
                    ce = low.find(b"\r\n", ci)
                    try:
                        body_len = int(head[ci + 15 : ce if ce > 0 else len(head)].strip())
                    except ValueError:
                        body_len = 0
            total = he + 4 + body_len
            if len(self.buf) < total:
                return
            self.buf = self.buf[total:]
            self.busy = False
            self.owner.on_response(self, status, time.perf_counter() - self.sent_at)
            if b"connection: close" in low:
                self.transport.close()
                return

    def connection_lost(self, exc):
        self.closed = True
        self.owner.on_conn_lost(self)


class RawHttpClient:
    """Bounded keep-alive pool. A full pool is reported, never queued away."""

    def __init__(self, host, port, pool_size, on_result, on_lost=None):
        self.host = host
        self.port = port
        self.pool_size = pool_size
        self.on_result = on_result
        self._on_lost = on_lost
        self.free = []
        self.all = []
        self.inflight = 0
        self.lost = 0
        self.reconnects = 0
        self.reconnect_failed = 0
        self._closing = False

    async def connect(self):
        loop = asyncio.get_running_loop()
        conns = await asyncio.gather(
            *[
                loop.create_connection(lambda: HttpConnection(self), self.host, self.port)
                for _ in range(self.pool_size)
            ],
            return_exceptions=True,
        )
        for c in conns:
            if isinstance(c, BaseException):
                continue
            proto = c[1]
            self.all.append(proto)
            self.free.append(proto)
        return len(self.all)

    def send(self, head, body):
        # Descartar las cerradas y seguir probando, en vez de rendirse con la
        # primera: devolver False con conexiones sanas en la lista contaba como
        # "pool lleno" un fallo que no lo era.
        while self.free:
            c = self.free.pop()
            if c.closed:
                continue
            c.send(head, body)
            self.inflight += 1
            return True
        return False

    def on_response(self, conn, status, latency):
        self.inflight -= 1
        self.free.append(conn)
        self.on_result(status, latency)

    def on_conn_lost(self, conn):
        self.lost += 1
        if conn.busy:
            self.inflight -= 1
            self.on_result(-1, 0.0)
        for lst in (self.free, self.all):
            try:
                lst.remove(conn)
            except ValueError:
                pass
        # REPONER la conexión. Sin esto el pool se vacía y no se rellena nunca.
        #
        # ThingsBoard cierra la conexión HTTP tras ~100 peticiones (keep-alive
        # máximo del transporte). Con 512 conexiones eso da exactamente 51 200
        # peticiones y después TODO se reporta como `pool_full`. Los cinco
        # niveles HTTP de ThingsBoard salieron con ese número idéntico —
        # 51 200 aceptados, 512 caídas, 0 reconexiones— y habrían sido leídos
        # como el techo de ThingsBoard cuando eran el techo del cliente.
        if not self._closing:
            asyncio.ensure_future(self._replace())
        if self._on_lost:
            self._on_lost()

    async def _replace(self):
        """Repone una conexión perdida para mantener el tamaño del pool."""
        if self._closing or len(self.all) >= self.pool_size:
            return
        loop = asyncio.get_running_loop()
        try:
            _, proto = await loop.create_connection(
                lambda: HttpConnection(self), self.host, self.port)
        except Exception:  # noqa: BLE001
            self.reconnect_failed += 1
            return
        self.all.append(proto)
        self.free.append(proto)
        self.reconnects += 1

    def close(self):
        self._closing = True
        for c in self.all:
            try:
                c.transport.close()
            except Exception:  # noqa: BLE001
                pass


# ---------------------------------------------------------------------------
# MQTT 3.1.1
# ---------------------------------------------------------------------------


def _varint(n: int) -> bytes:
    out = bytearray()
    while True:
        b = n % 128
        n //= 128
        if n:
            b |= 0x80
        out.append(b)
        if not n:
            return bytes(out)


def _mstr(s) -> bytes:
    b = s.encode() if isinstance(s, str) else s
    return len(b).to_bytes(2, "big") + b


def build_connect(client_id, username="", password="", keepalive=60, clean=True) -> bytes:
    vh = _mstr("MQTT") + bytes([4])
    flags = 0x02 if clean else 0x00
    if username:
        flags |= 0x80
    if password:
        flags |= 0x40
    vh += bytes([flags]) + int(keepalive).to_bytes(2, "big")
    pl = _mstr(client_id)
    if username:
        pl += _mstr(username)
    if password:
        pl += _mstr(password)
    rem = vh + pl
    return b"\x10" + _varint(len(rem)) + rem


PINGREQ = b"\xc0\x00"
DISCONNECT = b"\xe0\x00"


class MqttConnection(asyncio.Protocol):
    """One device, one socket. Tracks QoS1 PUBACKs by packet id."""

    __slots__ = (
        "owner",
        "idx",
        "transport",
        "buf",
        "connected",
        "connack_rc",
        "pid",
        "pending",
        "topic_prefix",
        "closed",
    )

    def __init__(self, owner, idx, topic):
        self.owner = owner
        self.idx = idx
        self.transport = None
        self.buf = b""
        self.connected = False
        self.connack_rc = None
        self.pid = 0
        self.pending = {}
        # PUBLISH varies only in packet id and payload, so cache the constant part
        self.topic_prefix = _mstr(topic)
        self.closed = False

    def connection_made(self, transport):
        try:
            s = transport.get_extra_info("socket")
            s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1 << 20)
        except Exception:  # noqa: BLE001
            pass
        self.transport = transport

    def publish(self, payload: bytes, qos: int = 1):
        if qos == 0:
            rem = self.topic_prefix + payload
            self.transport.write(b"\x30" + _varint(len(rem)) + rem)
            return 0
        self.pid = (self.pid % 65535) + 1
        pid = self.pid
        if pid in self.pending:
            return -1  # packet-id space exhausted: real backpressure, report it
        rem = self.topic_prefix + pid.to_bytes(2, "big") + payload
        self.transport.write(b"\x32" + _varint(len(rem)) + rem)
        self.pending[pid] = time.perf_counter()
        return pid

    def data_received(self, data):
        self.buf += data
        while len(self.buf) >= 2:
            hdr = self.buf[0]
            mult, value, i = 1, 0, 1
            ok = False
            for _ in range(4):
                if i >= len(self.buf):
                    return
                b = self.buf[i]
                i += 1
                value += (b & 0x7F) * mult
                if not (b & 0x80):
                    ok = True
                    break
                mult *= 128
            if not ok:
                return
            total = i + value
            if len(self.buf) < total:
                return
            body = self.buf[i:total]
            self.buf = self.buf[total:]
            t = hdr >> 4
            if t == 4 and len(body) >= 2:  # PUBACK
                pid = (body[0] << 8) | body[1]
                sent = self.pending.pop(pid, None)
                if sent is not None:
                    self.owner.on_puback(time.perf_counter() - sent)
            elif t == 2:  # CONNACK
                self.connack_rc = body[1] if len(body) >= 2 else -1
                self.connected = self.connack_rc == 0
                self.owner.on_connack(self)

    def connection_lost(self, exc):
        self.closed = True
        was = self.connected
        self.connected = False
        self.owner.on_conn_lost(self, was, len(self.pending))
        self.pending.clear()


class RawMqttClient:
    def __init__(self, host, port, keepalive, on_puback, on_event):
        self.host = host
        self.port = port
        self.keepalive = keepalive
        self.on_puback = on_puback
        self.on_event = on_event
        self.conns = []
        self._connack_waiters = 0

    async def connect_all(self, devices, connect_rate=0.0):
        loop = asyncio.get_running_loop()
        for i, dev in enumerate(devices):
            conn = None
            try:
                _, conn = await loop.create_connection(
                    lambda idx=i, d=dev: MqttConnection(self, idx, d.mqtt_topic),
                    self.host,
                    self.port,
                )
                conn.transport.write(
                    build_connect(
                        dev.mqtt_client_id, dev.mqtt_username, dev.mqtt_password, self.keepalive
                    )
                )
            except Exception:  # noqa: BLE001
                self.on_event("connect_failed")
            self.conns.append(conn)
            if connect_rate > 0:
                await asyncio.sleep(1.0 / connect_rate)
        deadline = time.perf_counter() + 60
        while time.perf_counter() < deadline:
            if all(c is None or c.connack_rc is not None or c.closed for c in self.conns):
                break
            await asyncio.sleep(0.05)
        return self.conns

    def on_connack(self, conn):
        self.on_event("connack", conn.connack_rc)

    def on_conn_lost(self, conn, was_connected, pending):
        if was_connected:
            self.on_event("disconnect")
        if pending:
            self.on_event("lost_pending", pending)

    def inflight(self):
        return sum(len(c.pending) for c in self.conns if c is not None)

    def ping_all(self):
        for c in self.conns:
            if c is not None and c.connected:
                try:
                    c.transport.write(PINGREQ)
                except Exception:  # noqa: BLE001
                    pass

    def close(self):
        for c in self.conns:
            if c is None:
                continue
            try:
                c.transport.write(DISCONNECT)
                c.transport.close()
            except Exception:  # noqa: BLE001
                pass
