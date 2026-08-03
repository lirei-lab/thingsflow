"""Open-loop senders.

The contract every sender in this module honours:

  * The send schedule is driven by wall time (`stats.Schedule`), never by
    completions. A slow server makes the *lag* grow; it does not make the
    offered rate shrink.
  * When the client genuinely cannot offer a message -- the in-flight cap is
    full, the MQTT queue is full, the socket is gone -- that message is
    COUNTED, in its own bucket, and shows up in the report as offered-load
    deficit. It is never silently skipped and never quietly re-paced.
  * Connections are established before t0 and kept for the whole run, so
    connection churn is not part of what is being measured.
"""

from __future__ import annotations

import asyncio
import os
import socket
import time
from dataclasses import dataclass, field

from stats import Histogram, Schedule


@dataclass
class WorkerConfig:
    protocol: str = "http"
    duration: float = 60.0
    rate: float = 100.0
    ramp: float = 0.0
    max_inflight: int = 512
    late_threshold_ms: float = 50.0
    payload_keys: int = 3
    payload_bytes: int = 0
    key_prefix: str = "lg2"
    drain_seconds: float = 15.0
    dry_run: bool = False
    tick_ms: float = 1.0
    # mqtt
    mqtt_host: str = "127.0.0.1"
    mqtt_port: int = 1883
    mqtt_keepalive: int = 60
    mqtt_qos: int = 1
    mqtt_max_inflight: int = 200
    mqtt_max_queued: int = 200
    mqtt_connect_rate: float = 400.0
    # http
    http_timeout: float = 30.0
    http_warmup: bool = True
    extra: dict = field(default_factory=dict)


class PayloadBuilder:
    """Pre-renders payload variants so the hot path is one bytes concat.

    Telemetry keys are run-unique (`<prefix>_v0`...), which is what lets the
    harness count landed rows with an exact `LIKE '<prefix>%'` and no time-window
    guessing. `ts` is forced strictly increasing per device: the store's primary
    key is (tenant, device, key, ts), so two messages sharing a millisecond would
    UPSERT and one row would silently vanish from the landed count.
    """

    VARIANTS = 64

    def __init__(self, key_prefix: str, n_keys: int, payload_bytes: int = 0):
        self.key_prefix = key_prefix
        self.n_keys = max(1, n_keys)
        keys = [f"{key_prefix}_v{i}" for i in range(self.n_keys)]
        self.pad_key = None
        self.variants = []
        for r in range(self.VARIANTS):
            parts = [f'"{k}":{20.0 + r * 0.37 + i:.3f}' for i, k in enumerate(keys)]
            tail = "," + ",".join(parts)
            if payload_bytes:
                # 13 digits of ts + '{"ts":' + tail + '}'
                base_len = 6 + 13 + len(tail) + 1
                overhead = len(f',"{key_prefix}_pad":""')
                pad = payload_bytes - base_len - overhead
                if pad > 0:
                    self.pad_key = f"{key_prefix}_pad"
                    tail += f',"{key_prefix}_pad":"' + ("x" * pad) + '"'
            self.variants.append((tail + "}").encode())
        # every non-reserved key becomes exactly one row in the store
        self.keys_per_message = self.n_keys + (1 if self.pad_key else 0)
        self.size_bytes = len(b'{"ts":1785597729757') + len(self.variants[0])

    def build(self, ts_ms: int, seq: int) -> bytes:
        return b'{"ts":%d%s' % (ts_ms, self.variants[seq & (self.VARIANTS - 1)])


class BaseSender:
    def __init__(self, cfg: WorkerConfig, devices, worker_index: int):
        self.cfg = cfg
        self.devices = devices
        self.worker_index = worker_index
        self.payload = PayloadBuilder(cfg.key_prefix, cfg.payload_keys, cfg.payload_bytes)
        self.schedule = Schedule(cfg.rate, cfg.ramp)
        self.hist = Histogram()

        self.attempted = 0  # messages actually handed to the transport
        self.accepted = 0  # 2xx / PUBACK
        self.failed: dict[str, int] = {}
        self.dispatched = 0  # == attempted + transport-refused
        self.blocked_inflight = 0  # schedule said send, client had no slot
        self.behind_schedule = 0  # dispatched later than its deadline
        self.max_lag_ms = 0.0
        self.lag_sum_ms = 0.0
        self.due_total = 0

        self.connects_ok = 0
        self.connects_failed = 0
        self.disconnects = 0
        self.reconnects = 0

        self._last_ts = {}
        self._seq = 0
        self._n_dev = len(devices)
        self.per_second = []  # [(sec, dispatched, accepted, lag_ms)]

    # -- helpers ----------------------------------------------------------
    def _fail(self, reason, n=1):
        self.failed[reason] = self.failed.get(reason, 0) + n

    def _next_ts(self, idx):
        ts = int(time.time() * 1000)
        last = self._last_ts.get(idx, 0)
        if ts <= last:
            ts = last + 1
        self._last_ts[idx] = ts
        return ts

    @property
    def inflight(self):
        raise NotImplementedError

    async def setup(self):
        raise NotImplementedError

    def dispatch(self, idx, ts_ms, seq):
        raise NotImplementedError

    async def drain(self):
        raise NotImplementedError

    async def teardown(self):
        pass

    # -- the open loop ----------------------------------------------------
    async def run_schedule(self):
        cfg = self.cfg
        tick = cfg.tick_ms / 1000.0
        late = cfg.late_threshold_ms / 1000.0
        t0 = time.perf_counter()
        self.t_start_epoch = time.time()
        self._cpu0 = time.process_time()
        k = 0
        next_sec = 1.0
        sec_disp0 = sec_acc0 = 0
        sec_lag = 0.0
        while True:
            now = time.perf_counter()
            el = now - t0
            if el >= cfg.duration:
                break
            due = min(self.schedule.due(el), self.schedule.total(cfg.duration))
            while k < due:
                if self.inflight >= cfg.max_inflight:
                    self.blocked_inflight += 1
                    break
                sched = self.schedule.sched_time(k)
                lag = el - sched
                if lag > late:
                    self.behind_schedule += 1
                if lag > 0:
                    self.lag_sum_ms += lag * 1000.0
                    if lag * 1000.0 > self.max_lag_ms:
                        self.max_lag_ms = lag * 1000.0
                        sec_lag = max(sec_lag, self.max_lag_ms)
                idx = k % self._n_dev
                self.dispatch(idx, self._next_ts(idx), k)
                self.dispatched += 1
                k += 1
            if el >= next_sec:
                self.per_second.append(
                    {
                        "t": round(next_sec, 1),
                        "dispatched": self.dispatched - sec_disp0,
                        "accepted": self.accepted - sec_acc0,
                        "max_lag_ms": round(sec_lag, 2),
                        "inflight": self.inflight,
                    }
                )
                sec_disp0 = self.dispatched
                sec_acc0 = self.accepted
                sec_lag = 0.0
                next_sec += 1.0
            await asyncio.sleep(tick)
        # Final catch-up: the tick loop exits at el >= duration, so up to one
        # tick's worth of scheduled messages would otherwise be counted as
        # deficit purely because of where the loop happened to break. The
        # schedule asked for due(duration) messages in this window; send the
        # remainder rather than report a phantom deficit. A real deficit (the
        # client could not place them) still shows up, because this pass obeys
        # the same in-flight cap.
        self.due_total = self.schedule.total(cfg.duration)
        while k < self.due_total:
            if self.inflight >= cfg.max_inflight:
                self.blocked_inflight += 1
                break
            lag = (time.perf_counter() - t0) - self.schedule.sched_time(k)
            if lag > late:
                self.behind_schedule += 1
            if lag > 0:
                self.lag_sum_ms += lag * 1000.0
                self.max_lag_ms = max(self.max_lag_ms, lag * 1000.0)
            idx = k % self._n_dev
            self.dispatch(idx, self._next_ts(idx), k)
            self.dispatched += 1
            k += 1
        self.t_end_epoch = time.time()
        self.wall_seconds = time.perf_counter() - t0
        # CPU charged to the measured window only, not to connection setup.
        self.cpu_seconds = time.process_time() - self._cpu0

    def result(self):
        deficit = max(0, self.due_total - self.dispatched)
        return {
            "worker": self.worker_index,
            "devices": self._n_dev,
            "target_rate": self.cfg.rate,
            "wall_seconds": round(self.wall_seconds, 3),
            "t_start_epoch": self.t_start_epoch,
            "t_end_epoch": self.t_end_epoch,
            "due_total": self.due_total,
            "dispatched": self.dispatched,
            "schedule_deficit": deficit,
            "blocked_inflight": self.blocked_inflight,
            "behind_schedule": self.behind_schedule,
            "max_lag_ms": round(self.max_lag_ms, 3),
            "mean_lag_ms": round(self.lag_sum_ms / self.dispatched, 3) if self.dispatched else 0.0,
            "attempted": self.attempted,
            "accepted": self.accepted,
            "failed": self.failed,
            "latency": self.hist.to_dict(),
            "connects_ok": self.connects_ok,
            "connects_failed": self.connects_failed,
            "disconnects": self.disconnects,
            "reconnects": self.reconnects,
            "cpu_seconds": round(self.cpu_seconds, 3),
            "cpu_cores_avg": round(self.cpu_seconds / self.wall_seconds, 3)
            if self.wall_seconds > 0
            else None,
            "pid": os.getpid(),
            "keys_per_message": self.payload.keys_per_message,
            "payload_bytes": self.payload.size_bytes,
            "per_second": self.per_second,
        }


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------


class HttpSender(BaseSender):
    def __init__(self, cfg, devices, worker_index):
        super().__init__(cfg, devices, worker_index)
        self._inflight = 0
        self._session = None

    @property
    def inflight(self):
        return self._inflight

    async def setup(self):
        if self.cfg.dry_run:
            self.connects_ok = self._n_dev
            return
        import aiohttp

        conn = aiohttp.TCPConnector(
            limit=self.cfg.max_inflight,
            limit_per_host=self.cfg.max_inflight,
            keepalive_timeout=120,
            force_close=False,
            enable_cleanup_closed=False,
            ttl_dns_cache=600,
        )
        self._session = aiohttp.ClientSession(
            connector=conn,
            timeout=aiohttp.ClientTimeout(total=self.cfg.http_timeout),
            skip_auto_headers=["User-Agent", "Accept-Encoding"],
        )
        if self.cfg.http_warmup:
            # Establish the keep-alive pool before t0 so TCP/TLS setup is not
            # inside the measurement. The body is `{}` on purpose: it carries no
            # telemetry key, so it cannot land a row and cannot skew
            # accepted-vs-landed. Status is deliberately ignored -- all we need
            # from it is an open, reusable connection.
            n = min(self.cfg.max_inflight, 64, max(1, self._n_dev))
            async def warm(d):
                try:
                    async with self._session.post(
                        d.http_url, data=b"{}", headers=d.http_headers
                    ) as r:
                        await r.read()
                    return True
                except Exception:  # noqa: BLE001
                    return False

            res = await asyncio.gather(*[warm(self.devices[i % self._n_dev]) for i in range(n)])
            self.connects_ok = sum(1 for x in res if x)
            self.connects_failed = sum(1 for x in res if not x)
        else:
            self.connects_ok = self._n_dev

    def dispatch(self, idx, ts_ms, seq):
        body = self.payload.build(ts_ms, seq)
        if self.cfg.dry_run:
            self.attempted += 1
            self.accepted += 1
            self.hist.record(0)
            return
        dev = self.devices[idx]
        self._inflight += 1
        self.attempted += 1
        asyncio.get_running_loop().create_task(self._post(dev, body))

    async def _post(self, dev, body):
        t0 = time.perf_counter()
        try:
            async with self._session.post(dev.http_url, data=body, headers=dev.http_headers) as r:
                await r.read()
                st = r.status
            self.hist.record((time.perf_counter() - t0) * 1e6)
            if 200 <= st < 300:
                self.accepted += 1
            else:
                self._fail(f"http_{st}")
        except asyncio.TimeoutError:
            self._fail("timeout")
        except Exception as exc:  # noqa: BLE001
            self._fail(f"exc_{type(exc).__name__}")
        finally:
            self._inflight -= 1

    async def drain(self):
        deadline = time.perf_counter() + self.cfg.drain_seconds
        while self._inflight > 0 and time.perf_counter() < deadline:
            await asyncio.sleep(0.05)
        if self._inflight > 0:
            self._fail("drain_incomplete", self._inflight)

    async def teardown(self):
        if self._session:
            await self._session.close()


# ---------------------------------------------------------------------------
# MQTT
# ---------------------------------------------------------------------------


class _AsyncioMqttGlue:
    """Drive a paho client from an asyncio loop -- no thread per device.

    paho's threaded `loop_start()` costs one OS thread per connection, which at a
    few hundred devices makes the generator's own scheduler the bottleneck. paho
    exposes socket callbacks precisely so an external event loop can own the
    readiness notifications; that is what this does.
    """

    __slots__ = ("loop", "client")

    def __init__(self, loop, client):
        self.loop = loop
        self.client = client
        client.on_socket_open = self._on_open
        client.on_socket_close = self._on_close
        client.on_socket_register_write = self._on_reg_write
        client.on_socket_unregister_write = self._on_unreg_write

    def _on_open(self, client, userdata, sock):
        try:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1 << 21)
        except Exception:  # noqa: BLE001
            pass
        self.loop.add_reader(sock, client.loop_read)

    def _on_close(self, client, userdata, sock):
        try:
            self.loop.remove_reader(sock)
        except Exception:  # noqa: BLE001
            pass

    def _on_reg_write(self, client, userdata, sock):
        self.loop.add_writer(sock, client.loop_write)

    def _on_unreg_write(self, client, userdata, sock):
        try:
            self.loop.remove_writer(sock)
        except Exception:  # noqa: BLE001
            pass


class MqttSender(BaseSender):
    def __init__(self, cfg, devices, worker_index):
        super().__init__(cfg, devices, worker_index)
        self.clients = []
        self.pending = {}  # (client_idx, mid) -> perf_counter
        self.connected = []
        self._misc_task = None
        self._stop_misc = False

    @property
    def inflight(self):
        return len(self.pending)

    async def setup(self):
        if self.cfg.dry_run:
            self.connects_ok = self._n_dev
            self.connected = [True] * self._n_dev
            return
        import paho.mqtt.client as mqtt

        self._mqtt = mqtt
        loop = asyncio.get_running_loop()
        self.connected = [False] * self._n_dev
        connack = [None] * self._n_dev

        def on_connect(client, userdata, flags, reason_code, properties=None):
            rc = int(reason_code) if not hasattr(reason_code, "value") else reason_code.value
            connack[userdata] = rc
            if rc == 0:
                self.connected[userdata] = True

        def on_disconnect(client, userdata, flags=None, reason_code=None, properties=None):
            if self.connected[userdata]:
                self.disconnects += 1
            self.connected[userdata] = False

        def on_publish(client, userdata, mid, reason_code=None, properties=None):
            t = self.pending.pop((userdata, mid), None)
            if t is not None:
                self.hist.record((time.perf_counter() - t) * 1e6)
                self.accepted += 1

        interval = 1.0 / self.cfg.mqtt_connect_rate if self.cfg.mqtt_connect_rate > 0 else 0.0
        for i, dev in enumerate(self.devices):
            c = mqtt.Client(
                mqtt.CallbackAPIVersion.VERSION2,
                client_id=dev.mqtt_client_id,
                clean_session=True,
                protocol=mqtt.MQTTv311,
            )
            c.user_data_set(i)
            if dev.mqtt_username:
                c.username_pw_set(dev.mqtt_username, dev.mqtt_password or None)
            c.max_inflight_messages_set(self.cfg.mqtt_max_inflight)
            # A bounded queue is deliberate: when it fills, publish() returns
            # MQTT_ERR_QUEUE_SIZE and we count it as offered-load we could not
            # place. An unbounded queue would absorb the overload and report a
            # rate the wire never carried.
            c.max_queued_messages_set(self.cfg.mqtt_max_queued)
            c.on_connect = on_connect
            c.on_disconnect = on_disconnect
            c.on_publish = on_publish
            _AsyncioMqttGlue(loop, c)
            try:
                await loop.run_in_executor(
                    None, lambda cc=c: cc.connect(self.cfg.mqtt_host, self.cfg.mqtt_port, self.cfg.mqtt_keepalive)
                )
            except Exception:  # noqa: BLE001
                self.connects_failed += 1
                connack[i] = -1
            self.clients.append(c)
            if interval:
                await asyncio.sleep(interval)

        deadline = time.perf_counter() + 60
        while time.perf_counter() < deadline:
            if all(v is not None for v in connack):
                break
            await asyncio.sleep(0.05)
        self.connects_ok = sum(1 for v in self.connected if v)
        self.connects_failed = self._n_dev - self.connects_ok
        self.connack_codes = {}
        for v in connack:
            key = "timeout" if v is None else str(v)
            self.connack_codes[key] = self.connack_codes.get(key, 0) + 1
        self._misc_task = loop.create_task(self._misc_loop())

    async def _misc_loop(self):
        """One shared misc/keepalive pump for every client in this worker."""
        while not self._stop_misc:
            await asyncio.sleep(0.5)
            for i, c in enumerate(self.clients):
                try:
                    rc = c.loop_misc()
                except Exception:  # noqa: BLE001
                    rc = -1
                if rc != 0 and not self.connected[i]:
                    try:
                        c.reconnect()
                        self.reconnects += 1
                    except Exception:  # noqa: BLE001
                        pass

    def dispatch(self, idx, ts_ms, seq):
        body = self.payload.build(ts_ms, seq)
        if self.cfg.dry_run:
            self.attempted += 1
            self.accepted += 1
            self.hist.record(0)
            return
        c = self.clients[idx]
        dev = self.devices[idx]
        try:
            info = c.publish(dev.mqtt_topic, body, qos=self.cfg.mqtt_qos)
        except Exception as exc:  # noqa: BLE001
            self._fail(f"exc_{type(exc).__name__}")
            return
        rc = info.rc
        if rc == 0:
            self.attempted += 1
            if self.cfg.mqtt_qos == 0:
                self.accepted += 1
                self.hist.record(0)
            else:
                self.pending[(idx, info.mid)] = time.perf_counter()
        elif rc == self._mqtt.MQTT_ERR_NO_CONN:
            self._fail("mqtt_no_conn")
        elif rc == self._mqtt.MQTT_ERR_QUEUE_SIZE:
            self._fail("mqtt_queue_full")
        else:
            self._fail(f"mqtt_rc_{rc}")

    async def drain(self):
        deadline = time.perf_counter() + self.cfg.drain_seconds
        while self.pending and time.perf_counter() < deadline:
            await asyncio.sleep(0.05)
        if self.pending:
            self._fail("puback_missing", len(self.pending))

    async def teardown(self):
        self._stop_misc = True
        if self._misc_task:
            self._misc_task.cancel()
        for c in self.clients:
            try:
                c.disconnect()
            except Exception:  # noqa: BLE001
                pass

    def result(self):
        r = super().result()
        r["connack_codes"] = getattr(self, "connack_codes", {})
        return r


# ---------------------------------------------------------------------------
# raw engines (see rawtransport.py for why they exist)
# ---------------------------------------------------------------------------


class RawHttpSender(BaseSender):
    def __init__(self, cfg, devices, worker_index):
        super().__init__(cfg, devices, worker_index)
        self._client = None
        self._heads = []

    @property
    def inflight(self):
        return self._client.inflight if self._client else 0

    async def setup(self):
        from rawtransport import RawHttpClient, build_request_head

        host = port = None
        for d in self.devices:
            h, p, head = build_request_head(d.http_url, d.http_headers)
            self._heads.append(head)
            if host is None:
                host, port = h, p
            elif (h, p) != (host, port):
                raise RuntimeError("raw http engine needs one host:port for all devices")
        if self.cfg.dry_run:
            self.connects_ok = self._n_dev
            return
        pool = max(1, min(self.cfg.max_inflight, self.cfg.extra.get("http_connections", 64)))
        self._client = RawHttpClient(host, port, pool, self._on_result)
        opened = await self._client.connect()
        self.connects_ok = opened
        self.connects_failed = pool - opened

    def _on_result(self, status, latency):
        if status < 0:
            self._fail("conn_lost")
            return
        self.hist.record(latency * 1e6)
        if 200 <= status < 300:
            self.accepted += 1
        else:
            self._fail(f"http_{status}")

    def dispatch(self, idx, ts_ms, seq):
        body = self.payload.build(ts_ms, seq)
        if self.cfg.dry_run:
            self.attempted += 1
            self.accepted += 1
            self.hist.record(0)
            return
        if not self._client.send(self._heads[idx], body):
            self.blocked_inflight += 1
            self._fail("pool_full")
            return
        self.attempted += 1

    async def drain(self):
        deadline = time.perf_counter() + self.cfg.drain_seconds
        while self.inflight > 0 and time.perf_counter() < deadline:
            await asyncio.sleep(0.05)
        if self.inflight > 0:
            self._fail("drain_incomplete", self.inflight)
        if self._client:
            self.disconnects += self._client.lost
            # Reconexiones REPORTADAS: sin este numero no hay forma de ver que
            # el pool se repuso. ThingsBoard cierra tras ~100 peticiones, asi
            # que en una corrida sana este contador debe ser ALTO, no cero.
            self.reconnects += getattr(self._client, "reconnects", 0)
            self.reconnect_failed = getattr(self._client, "reconnect_failed", 0)

    async def teardown(self):
        if self._client:
            self._client.close()


class RawMqttSender(BaseSender):
    def __init__(self, cfg, devices, worker_index):
        super().__init__(cfg, devices, worker_index)
        self._client = None
        self._ping_task = None
        self._stop_ping = False
        self.connack_codes = {}

    @property
    def inflight(self):
        return self._client.inflight() if self._client else 0

    def _on_puback(self, latency):
        self.hist.record(latency * 1e6)
        self.accepted += 1

    def _on_event(self, kind, value=None):
        if kind == "connack":
            k = str(value)
            self.connack_codes[k] = self.connack_codes.get(k, 0) + 1
        elif kind == "disconnect":
            self.disconnects += 1
        elif kind == "connect_failed":
            self.connects_failed += 1
        elif kind == "lost_pending":
            self._fail("puback_lost_on_disconnect", value)

    async def setup(self):
        if self.cfg.dry_run:
            self.connects_ok = self._n_dev
            return
        from rawtransport import RawMqttClient

        self._client = RawMqttClient(
            self.cfg.mqtt_host,
            self.cfg.mqtt_port,
            self.cfg.mqtt_keepalive,
            self._on_puback,
            self._on_event,
        )
        conns = await self._client.connect_all(
            self.devices, connect_rate=self.cfg.mqtt_connect_rate
        )
        self.connects_ok = sum(1 for c in conns if c is not None and c.connected)
        self.connects_failed = self._n_dev - self.connects_ok
        loop = asyncio.get_running_loop()
        self._ping_task = loop.create_task(self._ping_loop())

    async def _ping_loop(self):
        period = max(5.0, self.cfg.mqtt_keepalive * 0.5)
        while not self._stop_ping:
            await asyncio.sleep(period)
            if self._client:
                self._client.ping_all()

    def dispatch(self, idx, ts_ms, seq):
        body = self.payload.build(ts_ms, seq)
        if self.cfg.dry_run:
            self.attempted += 1
            self.accepted += 1
            self.hist.record(0)
            return
        conn = self._client.conns[idx]
        if conn is None or not conn.connected:
            self._fail("mqtt_no_conn")
            return
        try:
            pid = conn.publish(body, self.cfg.mqtt_qos)
        except Exception as exc:  # noqa: BLE001
            self._fail(f"exc_{type(exc).__name__}")
            return
        if pid < 0:
            self._fail("mqtt_pid_exhausted")
            return
        self.attempted += 1
        if self.cfg.mqtt_qos == 0:
            self.accepted += 1
            self.hist.record(0)

    async def drain(self):
        deadline = time.perf_counter() + self.cfg.drain_seconds
        while self.inflight > 0 and time.perf_counter() < deadline:
            await asyncio.sleep(0.05)
        if self.inflight > 0:
            self._fail("puback_missing", self.inflight)

    async def teardown(self):
        self._stop_ping = True
        if self._ping_task:
            self._ping_task.cancel()
        if self._client:
            self._client.close()

    def result(self):
        r = super().result()
        r["connack_codes"] = self.connack_codes
        return r


# ---------------------------------------------------------------------------
# worker entry point (runs in its own process)
# ---------------------------------------------------------------------------


def _make_sender(cfg, devices, idx):
    raw = cfg.extra.get("engine", "raw") == "raw"
    if cfg.protocol == "mqtt":
        return (RawMqttSender if raw else MqttSender)(cfg, devices, idx)
    if cfg.protocol == "http":
        return (RawHttpSender if raw else HttpSender)(cfg, devices, idx)
    raise ValueError(f"unknown protocol {cfg.protocol}")


def worker_main(cfg: WorkerConfig, devices, worker_index: int, barrier, out_q):
    async def amain():
        sender = _make_sender(cfg, devices, worker_index)
        try:
            await sender.setup()
        except Exception as exc:  # noqa: BLE001
            out_q.put({"worker": worker_index, "fatal": f"setup: {type(exc).__name__}: {exc}"})
            try:
                barrier.wait(timeout=5)
            except Exception:  # noqa: BLE001
                pass
            return
        try:
            barrier.wait(timeout=300)
        except Exception:  # noqa: BLE001
            pass
        await sender.run_schedule()
        await sender.drain()
        res = sender.result()
        await sender.teardown()
        out_q.put(res)

    try:
        import uvloop  # noqa: F401

        uvloop.install()
    except Exception:  # noqa: BLE001
        pass
    asyncio.run(amain())
