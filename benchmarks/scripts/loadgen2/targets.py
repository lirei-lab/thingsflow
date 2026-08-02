"""Pluggable benchmark targets.

A target owns everything platform-specific: how devices are created, how they
authenticate, where telemetry is posted or published, and (where possible) how
many rows actually landed in the store. The senders in `senders.py` know none of
that -- they receive a list of fully-resolved `DeviceEndpoint` records and drive
them, so ThingsFlow and ThingsBoard are both first-class and neither is a
special case bolted onto the other.

Provisioning deliberately uses stdlib `urllib` rather than the async stack: it
is a control-plane operation that happens once, before t0, and must not share an
event loop with the measured path.
"""

from __future__ import annotations

import json
import ssl
import urllib.error
import subprocess
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field


@dataclass
class DeviceEndpoint:
    """Everything a sender needs for one device. Must stay picklable."""

    name: str
    device_id: str = ""
    # HTTP path
    http_url: str = ""
    http_headers: dict = field(default_factory=dict)
    # MQTT path
    mqtt_client_id: str = ""
    mqtt_username: str = ""
    mqtt_password: str = ""
    mqtt_topic: str = ""


class HttpError(Exception):
    def __init__(self, status, body):
        super().__init__(f"HTTP {status}: {body[:300]}")
        self.status = status
        self.body = body


_SSL_CTX = ssl.create_default_context()
_SSL_CTX.check_hostname = False
_SSL_CTX.verify_mode = ssl.CERT_NONE


def _request(method, url, *, headers=None, body=None, timeout=30):
    data = None
    hdrs = dict(headers or {})
    if body is not None:
        data = json.dumps(body).encode()
        hdrs.setdefault("Content-Type", "application/json")
    req = urllib.request.Request(url, data=data, headers=hdrs, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=_SSL_CTX) as r:
            raw = r.read().decode("utf-8", "replace")
            return r.status, raw
    except urllib.error.HTTPError as e:  # noqa: PERF203
        raise HttpError(e.code, e.read().decode("utf-8", "replace")) from None


def _json_request(method, url, **kw):
    _, raw = _request(method, url, **kw)
    return json.loads(raw) if raw.strip() else None


class Target:
    """Adapter interface. Both platforms implement all of it."""

    name = "abstract"
    # Does this target expose a way to count rows that actually landed?
    supports_landed_verification = False

    def provision(self, count: int, prefix: str, parallelism: int = 16):
        raise NotImplementedError

    def cleanup(self, devices) -> dict:
        raise NotImplementedError

    def landed_count(self, key_prefix: str) -> int | None:
        return None

    def landed_method(self, key_prefix: str) -> str:
        """Descripción del recuento que ESTE target hace realmente.

        Existe porque el informe traía la cadena "greptime count(*)" cableada
        para todos los targets: una corrida de ThingsBoard salía etiquetada
        como si se hubiera contado en GreptimeDB (la base de ThingsFlow) aunque
        el recuento fuese correcto contra ts_kv. Un informe que describe mal su
        propio método no sirve como evidencia, aunque el número sea bueno.
        """
        return "sin verificación de aterrizaje"

    def describe(self) -> dict:
        return {"target": self.name}


# ---------------------------------------------------------------------------
# ThingsFlow: device JWT
# ---------------------------------------------------------------------------


class ThingsFlowTarget(Target):
    name = "thingsflow"
    supports_landed_verification = True

    def __init__(
        self,
        api_base,
        ingest_base,
        mqtt_host,
        mqtt_port,
        user,
        password,
        greptime_base=None,
        greptime_table="device_telemetry_kv",
        greptime_db="public",
        mqtt_username_mode="raw",
    ):
        self.api_base = api_base.rstrip("/")
        self.ingest_base = ingest_base.rstrip("/")
        self.mqtt_host = mqtt_host
        self.mqtt_port = int(mqtt_port)
        self.user = user
        self.password = password
        self.greptime_base = (greptime_base or "").rstrip("/")
        self.greptime_table = greptime_table
        self.greptime_db = greptime_db
        # rmqtt's auth-jwt plugin reads the JWT from the MQTT username. flow-core
        # hands back both the bare token and a "Bearer <token>" spelling
        # (`mqttUsername`); which one the broker accepts is deployment config, so
        # it stays a knob instead of a guess.
        self.mqtt_username_mode = mqtt_username_mode
        self._tenant_jwt = None

    def _login(self):
        if self._tenant_jwt:
            return self._tenant_jwt
        body = _json_request(
            "POST",
            f"{self.api_base}/api/auth/login",
            body={"username": self.user, "password": self.password},
        )
        self._tenant_jwt = body["token"]
        return self._tenant_jwt

    def _auth_headers(self):
        return {"X-Authorization": f"Bearer {self._login()}"}

    def _find_device(self, name):
        q = urllib.parse.urlencode({"deviceName": name})
        try:
            return _json_request(
                "GET", f"{self.api_base}/api/tenant/devices?{q}", headers=self._auth_headers()
            )
        except HttpError:
            return None

    def _provision_one(self, name):
        dev = self._find_device(name)
        if not dev:
            dev = _json_request(
                "POST",
                f"{self.api_base}/api/device",
                headers=self._auth_headers(),
                body={"name": name, "type": "benchmark"},
            )
        did = dev["id"]["id"]
        jwt = _json_request(
            "POST", f"{self.api_base}/api/device/{did}/jwt", headers=self._auth_headers()
        )
        token = jwt["token"]
        mqtt_id = jwt.get("mqttIdentity") or did.replace("-", "")
        username = token if self.mqtt_username_mode == "raw" else jwt.get("mqttUsername", token)
        return DeviceEndpoint(
            name=name,
            device_id=did,
            http_url=f"{self.ingest_base}/api/v1/telemetry",
            http_headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            # The Client ID MUST equal the JWT's clientid/mqttId claim: rmqtt's
            # validate_claims.clientid rejects the CONNECT otherwise, and the ACL
            # scopes publishes to %c (this same id).
            mqtt_client_id=mqtt_id,
            mqtt_username=username,
            mqtt_password="",
            mqtt_topic=f"thingsflow/devices/{mqtt_id}/telemetry",
        )

    def provision(self, count, prefix, parallelism=16):
        self._login()
        names = [f"{prefix}-{i + 1:05d}" for i in range(count)]
        out = [None] * count
        errors = []

        def work(i):
            for attempt in range(3):
                try:
                    out[i] = self._provision_one(names[i])
                    return
                except Exception as exc:  # noqa: BLE001
                    if attempt == 2:
                        errors.append(f"{names[i]}: {exc}")

        with ThreadPoolExecutor(max_workers=parallelism) as pool:
            list(pool.map(work, range(count)))
        devices = [d for d in out if d is not None]
        return devices, errors


    def find_by_prefix(self, prefix, page_size=1000):
        """List existing devices whose name starts with `prefix`.

        `cleanup` must not go through `provision`: that would CREATE any device
        the prefix is missing just to delete it again.
        """
        q = urllib.parse.urlencode({"pageSize": page_size, "page": 0})
        body = _json_request(
            "GET", f"{self.api_base}/api/tenant/devices?{q}", headers=self._auth_headers()
        )
        out = []
        for d in (body or {}).get("data", []):
            if d.get("name", "").startswith(prefix):
                out.append(DeviceEndpoint(name=d["name"], device_id=d["id"]["id"]))
        return out

    def cleanup(self, devices):
        ok, failed = 0, 0

        def work(d):
            nonlocal ok, failed
            try:
                _request(
                    "DELETE",
                    f"{self.api_base}/api/device/{d.device_id}",
                    headers=self._auth_headers(),
                )
                return True
            except Exception:  # noqa: BLE001
                return False

        with ThreadPoolExecutor(max_workers=16) as pool:
            for r in pool.map(work, devices):
                if r:
                    ok += 1
                else:
                    failed += 1
        return {"deleted": ok, "delete_failed": failed}

    def gsql(self, sql, timeout=120):
        if not self.greptime_base:
            return None
        data = urllib.parse.urlencode({"sql": sql}).encode()
        req = urllib.request.Request(
            f"{self.greptime_base}/v1/sql?db={self.greptime_db}",
            data=data,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=timeout, context=_SSL_CTX) as r:
            out = json.loads(r.read().decode())["output"][0]
        if "records" not in out:
            return []
        return out["records"]["rows"]

    def landed_count(self, key_prefix):
        if not self.greptime_base:
            return None
        safe = key_prefix.replace("'", "''")
        rows = self.gsql(
            f"SELECT count(*) FROM {self.greptime_table} WHERE telemetry_key LIKE '{safe}%'"
        )
        return int(rows[0][0]) if rows else 0

    def landed_method(self, key_prefix):
        if not self.greptime_base:
            return "no disponible: falta --greptime-base"
        return (f"GreptimeDB: SELECT count(*) FROM {self.greptime_table} "
                f"WHERE telemetry_key LIKE '{key_prefix}%'")

    def describe(self):
        return {
            "target": self.name,
            "api_base": self.api_base,
            "ingest_base": self.ingest_base,
            "mqtt": f"{self.mqtt_host}:{self.mqtt_port}",
            "greptime_base": self.greptime_base or None,
            "credential": "device JWT (ES256)",
            "landed_verification": self.landed_method("<prefijo>"),
        }


# ---------------------------------------------------------------------------
# ThingsBoard: ACCESS_TOKEN
# ---------------------------------------------------------------------------


class ThingsBoardTarget(Target):
    name = "thingsboard"
    # Landed verification IS available: ThingsBoard with DATABASE_TS_TYPE=sql
    # writes timeseries into Postgres ts_kv, with the key normalised through
    # key_dictionary. Without this the silent-drop metric would only exist on
    # the ThingsFlow side and the comparison would not be symmetric — the
    # whole point is counting rows that actually landed, not acks.
    supports_landed_verification = True

    def __init__(self, api_base, ingest_base, mqtt_host, mqtt_port, user, password,
                 pg_namespace="tb-classic", pg_pod=None, pg_user="thingsboard", pg_db="thingsboard"):
        self.api_base = api_base.rstrip("/")
        self.ingest_base = ingest_base.rstrip("/")
        self.mqtt_host = mqtt_host
        self.mqtt_port = int(mqtt_port)
        self.user = user
        self.password = password
        self._tenant_jwt = None
        self.pg_namespace = pg_namespace
        self.pg_pod = pg_pod
        self.pg_user = pg_user
        self.pg_db = pg_db

    def _resolve_pg_pod(self):
        if self.pg_pod:
            return self.pg_pod
        out = subprocess.run(
            ["kubectl", "--context=microk8s", "-n", self.pg_namespace, "get", "pods",
             "-o", "jsonpath={.items[*].metadata.name}"],
            capture_output=True, text=True, timeout=30,
        )
        for name in out.stdout.split():
            if "postgres" in name:
                self.pg_pod = name
                return name
        return None

    def landed_count(self, key_prefix):
        """Rows actually stored, counted in Postgres ts_kv.

        ts_kv.key is an integer FK into key_dictionary, so the telemetry key
        name has to be joined back. Reached over `kubectl exec` because
        Postgres is ClusterIP-only and exposing it just for the benchmark
        would change the system under test.
        """
        pod = self._resolve_pg_pod()
        if not pod:
            return None
        safe = key_prefix.replace("'", "''")
        sql = (
            "SELECT count(*) FROM ts_kv t "
            "JOIN key_dictionary k ON k.key_id = t.key "
            f"WHERE k.key LIKE '{safe}%'"
        )
        try:
            out = subprocess.run(
                ["kubectl", "--context=microk8s", "-n", self.pg_namespace, "exec", pod, "--",
                 "psql", "-U", self.pg_user, "-d", self.pg_db, "-tAc", sql],
                capture_output=True, text=True, timeout=120,
            )
            if out.returncode != 0:
                return None
            return int(out.stdout.strip().splitlines()[-1])
        except Exception:
            return None

    def landed_method(self, key_prefix):
        return ("Postgres ts_kv: SELECT count(*) FROM ts_kv t "
                "JOIN key_dictionary k ON k.key_id = t.key "
                f"WHERE k.key LIKE '{key_prefix}%'")

    def _login(self):
        if self._tenant_jwt:
            return self._tenant_jwt
        body = _json_request(
            "POST",
            f"{self.api_base}/api/auth/login",
            body={"username": self.user, "password": self.password},
        )
        self._tenant_jwt = body["token"]
        return self._tenant_jwt

    def _auth_headers(self):
        return {"X-Authorization": f"Bearer {self._login()}"}

    def _find_device(self, name):
        q = urllib.parse.urlencode({"deviceName": name})
        try:
            return _json_request(
                "GET", f"{self.api_base}/api/tenant/devices?{q}", headers=self._auth_headers()
            )
        except HttpError:
            return None

    def _provision_one(self, name):
        dev = self._find_device(name)
        if not dev:
            dev = _json_request(
                "POST",
                f"{self.api_base}/api/device",
                headers=self._auth_headers(),
                body={"name": name, "type": "benchmark"},
            )
        did = dev["id"]["id"]
        creds = _json_request(
            "GET", f"{self.api_base}/api/device/{did}/credentials", headers=self._auth_headers()
        )
        token = creds["credentialsId"]
        return DeviceEndpoint(
            name=name,
            device_id=did,
            http_url=f"{self.ingest_base}/api/v1/{token}/telemetry",
            http_headers={"Content-Type": "application/json"},
            mqtt_client_id=f"tb-{did.replace('-', '')}",
            mqtt_username=token,
            mqtt_password="",
            mqtt_topic="v1/devices/me/telemetry",
        )

    def provision(self, count, prefix, parallelism=16):
        self._login()
        names = [f"{prefix}-{i + 1:05d}" for i in range(count)]
        out = [None] * count
        errors = []

        def work(i):
            for attempt in range(3):
                try:
                    out[i] = self._provision_one(names[i])
                    return
                except Exception as exc:  # noqa: BLE001
                    if attempt == 2:
                        errors.append(f"{names[i]}: {exc}")

        with ThreadPoolExecutor(max_workers=parallelism) as pool:
            list(pool.map(work, range(count)))
        return [d for d in out if d is not None], errors


    def find_by_prefix(self, prefix, page_size=1000):
        """List existing devices whose name starts with `prefix`.

        `cleanup` must not go through `provision`: that would CREATE any device
        the prefix is missing just to delete it again.
        """
        q = urllib.parse.urlencode({"pageSize": page_size, "page": 0})
        body = _json_request(
            "GET", f"{self.api_base}/api/tenant/devices?{q}", headers=self._auth_headers()
        )
        out = []
        for d in (body or {}).get("data", []):
            if d.get("name", "").startswith(prefix):
                out.append(DeviceEndpoint(name=d["name"], device_id=d["id"]["id"]))
        return out

    def cleanup(self, devices):
        ok, failed = 0, 0

        def work(d):
            try:
                _request(
                    "DELETE",
                    f"{self.api_base}/api/device/{d.device_id}",
                    headers=self._auth_headers(),
                )
                return True
            except Exception:  # noqa: BLE001
                return False

        with ThreadPoolExecutor(max_workers=16) as pool:
            for r in pool.map(work, devices):
                ok += 1 if r else 0
                failed += 0 if r else 1
        return {"deleted": ok, "delete_failed": failed}

    def describe(self):
        return {
            "target": self.name,
            "api_base": self.api_base,
            "ingest_base": self.ingest_base,
            "mqtt": f"{self.mqtt_host}:{self.mqtt_port}",
            "credential": "device ACCESS_TOKEN",
            # Etiqueta obsoleta corregida: SÍ está implementado (landed_count
            # cuenta contra ts_kv). Decía "not implemented" mientras el recuento
            # funcionaba, que es la peor combinación posible: quien auditara el
            # informe descartaría un dato correcto.
            "landed_verification": self.landed_method("<prefijo>"),
        }


# ---------------------------------------------------------------------------
# Sink: a trivial local endpoint, used only to calibrate the client itself
# ---------------------------------------------------------------------------


class SinkTarget(Target):
    name = "sink"
    supports_landed_verification = False

    def __init__(self, http_host, http_port, mqtt_host, mqtt_port):
        self.http_host = http_host
        self.http_port = int(http_port)
        self.mqtt_host = mqtt_host
        self.mqtt_port = int(mqtt_port)

    def provision(self, count, prefix, parallelism=16):
        devs = []
        for i in range(count):
            name = f"{prefix}-{i + 1:05d}"
            devs.append(
                DeviceEndpoint(
                    name=name,
                    device_id=f"sink-{i:05d}",
                    http_url=f"http://{self.http_host}:{self.http_port}/api/v1/telemetry",
                    http_headers={"Content-Type": "application/json"},
                    mqtt_client_id=f"sink{i:06d}",
                    mqtt_username="sink",
                    mqtt_password="",
                    mqtt_topic=f"sink/devices/{i}/telemetry",
                )
            )
        return devs, []

    def find_by_prefix(self, prefix, page_size=1000):
        return []

    def cleanup(self, devices):
        return {"deleted": 0, "delete_failed": 0}

    def describe(self):
        return {
            "target": self.name,
            "http": f"{self.http_host}:{self.http_port}",
            "mqtt": f"{self.mqtt_host}:{self.mqtt_port}",
            "note": "local trivial sink -- measures the GENERATOR ceiling, not a platform",
        }


def build_target(args):
    if args.target == "thingsflow":
        return ThingsFlowTarget(
            api_base=args.api_base,
            ingest_base=args.ingest_base,
            mqtt_host=args.mqtt_host,
            mqtt_port=args.mqtt_port,
            user=args.user,
            password=args.password,
            greptime_base=args.greptime_base,
            greptime_table=args.greptime_table,
            mqtt_username_mode=args.tf_mqtt_username_mode,
        )
    if args.target == "thingsboard":
        return ThingsBoardTarget(
            api_base=args.api_base,
            ingest_base=args.ingest_base,
            mqtt_host=args.mqtt_host,
            mqtt_port=args.mqtt_port,
            user=args.user,
            password=args.password,
        )
    if args.target == "sink":
        return SinkTarget(
            http_host=args.sink_host,
            http_port=args.sink_http_port,
            mqtt_host=args.sink_host,
            mqtt_port=args.sink_mqtt_port,
        )
    raise ValueError(f"unknown target {args.target}")
