"""Device-side client for ThingsFlow provisioning and telemetry.

The SDK intentionally keeps telemetry publishing at the edge endpoints. Flow
Core is used for provisioning and control-plane renewal only.
"""

from __future__ import annotations

import base64
import json
import ssl
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any, Callable, Dict, Mapping, Optional, Protocol, Tuple, Union

JsonObject = Mapping[str, Any]
JsonPayload = Union[JsonObject, list[JsonObject]]


class ThingsFlowDeviceError(RuntimeError):
    """Raised when provisioning, renewal, or telemetry publishing fails."""


def _strip_url(url: str) -> str:
    return url.rstrip("/")


def _b64url_decode(segment: str) -> bytes:
    padded = segment + "=" * (-len(segment) % 4)
    return base64.urlsafe_b64decode(padded.encode("ascii"))


def _jwt_payload(token: str) -> Dict[str, Any]:
    """Decode JWT payload without validating it.

    This is only used to read local metadata such as `exp`. Edge services still
    validate signatures, issuer, audience, and ACLs.
    """

    parts = token.split(".")
    if len(parts) != 3:
        raise ThingsFlowDeviceError("deviceJwt token is not a compact JWT")
    try:
        payload = json.loads(_b64url_decode(parts[1]).decode("utf-8"))
    except (ValueError, UnicodeDecodeError) as exc:
        raise ThingsFlowDeviceError("deviceJwt payload is not valid JSON") from exc
    if not isinstance(payload, dict):
        raise ThingsFlowDeviceError("deviceJwt payload must be a JSON object")
    return payload


def _parse_expires_at(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        # Accept seconds or milliseconds.
        return float(value / 1000 if value > 10_000_000_000 else value)
    if isinstance(value, str):
        text = value.strip()
        if text.isdigit():
            return _parse_expires_at(int(text))
    return None


@dataclass(frozen=True)
class DeviceJWT:
    """Native ThingsFlow Device JWT response."""

    token: str
    token_type: str = "Bearer"
    expires_at: Optional[float] = None
    issuer: Optional[str] = None
    audience: Optional[str] = None
    subject: Optional[str] = None
    mqtt_identity: Optional[str] = None
    mqtt_username: Optional[str] = None

    @classmethod
    def from_dict(cls, body: Mapping[str, Any]) -> "DeviceJWT":
        token = str(body.get("token") or "")
        if not token:
            raise ThingsFlowDeviceError("deviceJwt.token is missing")
        payload = _jwt_payload(token)
        expires_at = _parse_expires_at(body.get("expiresAt"))
        if expires_at is None and isinstance(payload.get("exp"), (int, float)):
            expires_at = float(payload["exp"])
        mqtt_identity = body.get("mqttIdentity") or payload.get("mqttId")
        return cls(
            token=token,
            token_type=str(body.get("tokenType") or "Bearer"),
            expires_at=expires_at,
            issuer=body.get("issuer") or payload.get("iss"),
            audience=body.get("audience") or payload.get("aud"),
            subject=body.get("subject") or payload.get("sub"),
            mqtt_identity=str(mqtt_identity) if mqtt_identity else None,
            mqtt_username=str(body.get("mqttUsername") or token),
        )

    def expires_in(self, now: Optional[float] = None) -> Optional[float]:
        if self.expires_at is None:
            return None
        return self.expires_at - (time.time() if now is None else now)

    def expires_soon(self, margin_seconds: int, now: Optional[float] = None) -> bool:
        remaining = self.expires_in(now=now)
        return remaining is not None and remaining <= margin_seconds


@dataclass(frozen=True)
class ProvisioningResponse:
    """Response returned by `POST /api/v1/provision`."""

    status: str
    credentials_type: Optional[str] = None
    credentials_value: Optional[str] = None
    device_id: Optional[str] = None
    tenant_id: Optional[str] = None
    device_jwt: Optional[DeviceJWT] = None
    error_msg: Optional[str] = None

    @classmethod
    def from_dict(cls, body: Mapping[str, Any]) -> "ProvisioningResponse":
        device_jwt_body = body.get("deviceJwt")
        device_jwt = None
        if isinstance(device_jwt_body, Mapping):
            device_jwt = DeviceJWT.from_dict(device_jwt_body)
        return cls(
            status=str(body.get("status") or ""),
            credentials_type=body.get("credentialsType"),
            credentials_value=body.get("credentialsValue"),
            device_id=body.get("deviceId"),
            tenant_id=body.get("tenantId"),
            device_jwt=device_jwt,
            error_msg=body.get("errorMsg"),
        )

    def require_success(self) -> "ProvisioningResponse":
        if self.status != "SUCCESS":
            raise ThingsFlowDeviceError(self.error_msg or "device provisioning failed")
        if self.device_jwt is None:
            raise ThingsFlowDeviceError("provisioning did not return deviceJwt")
        return self


class RenewalStrategy(Protocol):
    """Strategy used by the SDK to refresh Device JWTs."""

    def renew(self, client: "ThingsFlowDeviceClient") -> DeviceJWT:
        ...


class ReProvisionRenewal:
    """Refresh Device JWT by calling `POST /api/v1/provision` again."""

    def renew(self, client: "ThingsFlowDeviceClient") -> DeviceJWT:
        response = client.provision()
        if response.device_jwt is None:
            raise ThingsFlowDeviceError("re-provisioning did not return deviceJwt")
        return response.device_jwt


class ControlPlaneRenewal:
    """Refresh Device JWT through an authenticated fleet/control-plane service."""

    def __init__(self, token_supplier: Callable[[], str]):
        self._token_supplier = token_supplier

    def renew(self, client: "ThingsFlowDeviceClient") -> DeviceJWT:
        if not client.device_id:
            raise ThingsFlowDeviceError("device_id is required for control-plane renewal")
        token = self._token_supplier()
        if not token:
            raise ThingsFlowDeviceError("control-plane token supplier returned empty token")
        return client.renew_with_control_plane(token)


class ThingsFlowDeviceClient:
    """ThingsFlow device client.

    `renewal_strategy` defaults to re-provisioning because it works with the
    current public device API. For production field devices, use per-device
    bootstrap credentials or inject a `ControlPlaneRenewal` backed by a fleet
    service instead of storing human/operator credentials.
    """

    def __init__(
        self,
        *,
        base_url: str,
        device_name: str,
        provision_device_key: str,
        provision_device_secret: str,
        device_type: Optional[str] = None,
        refresh_margin_seconds: int = 300,
        timeout_seconds: float = 10.0,
        renewal_strategy: Optional[RenewalStrategy] = None,
    ) -> None:
        self.base_url = _strip_url(base_url)
        self.device_name = device_name
        self.device_type = device_type
        self.provision_device_key = provision_device_key
        self.provision_device_secret = provision_device_secret
        self.refresh_margin_seconds = refresh_margin_seconds
        self.timeout_seconds = timeout_seconds
        self.renewal_strategy: RenewalStrategy = renewal_strategy or ReProvisionRenewal()
        self.provisioning: Optional[ProvisioningResponse] = None
        self.device_jwt: Optional[DeviceJWT] = None
        self.device_id: Optional[str] = None
        self.tenant_id: Optional[str] = None
        self._mqtt_client: Any = None
        self._mqtt_connection: Optional[Tuple[str, int, bool, Optional[str]]] = None

    def provision(self) -> ProvisioningResponse:
        payload: Dict[str, Any] = {
            "deviceName": self.device_name,
            "provisionDeviceKey": self.provision_device_key,
            "provisionDeviceSecret": self.provision_device_secret,
        }
        if self.device_type:
            payload["deviceType"] = self.device_type
        body = self._request_json("POST", "/api/v1/provision", payload=payload)
        response = ProvisioningResponse.from_dict(body).require_success()
        self.provisioning = response
        self.device_jwt = response.device_jwt
        self.device_id = response.device_id
        self.tenant_id = response.tenant_id
        return response

    def ensure_fresh_jwt(self) -> DeviceJWT:
        if self.device_jwt is None:
            self.provision()
        assert self.device_jwt is not None
        if self.device_jwt.expires_soon(self.refresh_margin_seconds):
            previous_token = self.device_jwt.token
            fresh = self.renewal_strategy.renew(self)
            self.device_jwt = fresh
            if self._mqtt_client is not None and fresh.token != previous_token:
                self._reconnect_mqtt()
        return self.device_jwt

    def renew_with_control_plane(self, control_plane_token: str) -> DeviceJWT:
        if not self.device_id:
            raise ThingsFlowDeviceError("device_id is required for control-plane renewal")
        body = self._request_json(
            "POST",
            f"/api/device/{self.device_id}/jwt",
            payload={},
            bearer_token=control_plane_token,
        )
        fresh = DeviceJWT.from_dict(body)
        self.device_jwt = fresh
        return fresh

    def publish_http(self, payload: JsonPayload) -> Dict[str, Any]:
        token = self.ensure_fresh_jwt().token
        return self._request_json(
            "POST",
            "/api/v1/telemetry",
            payload=payload,
            bearer_token=token,
            accept_empty=True,
        )

    def connect_mqtt(
        self,
        host: str,
        *,
        port: int = 1883,
        tls: bool = False,
        ca_cert: Optional[str] = None,
        client_id: Optional[str] = None,
    ) -> Any:
        try:
            import paho.mqtt.client as mqtt
        except ImportError as exc:
            raise ThingsFlowDeviceError(
                "MQTT support requires installing thingsflow-device-sdk[mqtt]"
            ) from exc
        device_jwt = self.ensure_fresh_jwt()
        token = device_jwt.token
        # The broker pins the MQTT Client ID to the JWT's `clientid` claim (rmqtt
        # validate_claims.clientid) and its ACL scopes publishes to that same id, so a
        # client announcing anything else is refused at CONNECT. The claim is the
        # topic-safe device identity, which is also what publish_mqtt() uses for the
        # topic — never the human-readable device name.
        if not client_id:
            client_id = device_jwt.mqtt_identity or self.device_id
        if not client_id:
            raise ThingsFlowDeviceError(
                "deviceJwt.mqttIdentity is required as the MQTT client id"
            )
        mqtt_client = mqtt.Client(client_id=client_id)
        connected = threading.Event()
        connection: Dict[str, int] = {"rc": -1}

        def on_connect(_client: Any, _userdata: Any, _flags: Any, reason_code: Any, *_args: Any) -> None:
            connection["rc"] = int(getattr(reason_code, "value", reason_code))
            connected.set()

        mqtt_client.on_connect = on_connect
        mqtt_client.username_pw_set(token)
        if tls:
            mqtt_client.tls_set(ca_certs=ca_cert, tls_version=ssl.PROTOCOL_TLS_CLIENT)
        mqtt_client.connect(host, port=port)
        mqtt_client.loop_start()
        if not connected.wait(timeout=self.timeout_seconds):
            mqtt_client.loop_stop()
            mqtt_client.disconnect()
            raise ThingsFlowDeviceError("MQTT connect timed out")
        if connection["rc"] != 0:
            mqtt_client.loop_stop()
            mqtt_client.disconnect()
            raise ThingsFlowDeviceError(f"MQTT connect failed with rc={connection['rc']}")
        self._mqtt_client = mqtt_client
        self._mqtt_connection = (host, port, tls, ca_cert)
        return mqtt_client

    def publish_mqtt(
        self,
        payload: JsonObject,
        *,
        topic: Optional[str] = None,
        qos: int = 0,
        retain: bool = False,
    ) -> None:
        if self._mqtt_client is None:
            raise ThingsFlowDeviceError("connect_mqtt must be called before publish_mqtt")
        device_jwt = self.ensure_fresh_jwt()
        mqtt_identity = device_jwt.mqtt_identity
        if not mqtt_identity and self.device_id:
            mqtt_identity = self.device_id
        if not mqtt_identity:
            raise ThingsFlowDeviceError("deviceJwt.mqttIdentity is required for MQTT publish")
        target = topic or f"thingsflow/devices/{mqtt_identity}/telemetry"
        result = self._mqtt_client.publish(
            target,
            json.dumps(payload, separators=(",", ":")),
            qos=qos,
            retain=retain,
        )
        if getattr(result, "rc", 0) != 0:
            raise ThingsFlowDeviceError(f"MQTT publish failed with rc={result.rc}")
        result.wait_for_publish()

    def on_rpc(self, handler: Callable[[str, JsonObject], Any]) -> None:
        """Subscribe to server-to-device RPC and answer each command with `handler`.

        `handler(method, params)` returns whatever should be sent back; return None
        for a command that needs no reply body. Exceptions are caught and reported
        to the caller as {"error": "..."} rather than being swallowed — a device
        that raises would otherwise look identical to one that is offline, and the
        operator would be left waiting out the timeout with no explanation.

        Requires connect_mqtt() first. The broker ACL only permits subscribing to
        this device's own request topic and publishing on its own response topic,
        both pinned to the connection's Client ID.
        """
        if self._mqtt_client is None:
            raise ThingsFlowDeviceError("connect_mqtt must be called before on_rpc")
        device_jwt = self.ensure_fresh_jwt()
        mqtt_identity = device_jwt.mqtt_identity or self.device_id
        if not mqtt_identity:
            raise ThingsFlowDeviceError("deviceJwt.mqttIdentity is required for RPC")

        def on_message(_client: Any, _userdata: Any, message: Any) -> None:
            request_id = message.topic.rsplit("/", 1)[-1]
            try:
                request = json.loads(message.payload.decode("utf-8"))
                method = request.get("method", "")
                params = request.get("params") or {}
            except (ValueError, UnicodeDecodeError) as exc:
                self._publish_rpc_response(
                    mqtt_identity, request_id, {"error": f"unparseable request: {exc}"}
                )
                return
            try:
                result = handler(method, params)
            except Exception as exc:  # noqa: BLE001 - reported, not swallowed
                self._publish_rpc_response(
                    mqtt_identity, request_id, {"error": str(exc)}
                )
                return
            self._publish_rpc_response(
                mqtt_identity, request_id, result if result is not None else {}
            )

        self._mqtt_client.on_message = on_message
        # QoS 1: a command delivered once matters more than one delivered fast.
        self._mqtt_client.subscribe(
            f"thingsflow/devices/{mqtt_identity}/rpc/request/+", qos=1
        )

    def _publish_rpc_response(
        self, mqtt_identity: str, request_id: str, body: Any
    ) -> None:
        if self._mqtt_client is None:
            return
        self._mqtt_client.publish(
            f"thingsflow/devices/{mqtt_identity}/rpc/response/{request_id}",
            json.dumps(body, separators=(",", ":")),
            qos=1,
            retain=False,
        )

    def on_desired(self, handler: Callable[[JsonObject], Any]) -> None:
        """Subscribe to server-to-device desired state and apply it with `handler`.

        The control plane publishes desired state retained on
        `thingsflow/devices/<mqtt_identity>/desired` (R5). A retained message is
        delivered on subscribe, so a device that connects or reconnects receives
        the current desired state (replay) without a second request. The payload
        is a Ditto-style per-feature map:
        `{"features": {"energy": {"desiredProperties": {...}}}}`.

        Requires connect_mqtt() first. The broker ACL permits subscribing to this
        device's own desired topic only (pinned to the connection's Client ID).
        """
        if self._mqtt_client is None:
            raise ThingsFlowDeviceError("connect_mqtt must be called before on_desired")
        device_jwt = self.ensure_fresh_jwt()
        mqtt_identity = device_jwt.mqtt_identity or self.device_id
        if not mqtt_identity:
            raise ThingsFlowDeviceError("deviceJwt.mqttIdentity is required for desired state")

        def on_message(_client: Any, _userdata: Any, message: Any) -> None:
            try:
                desired = json.loads(message.payload.decode("utf-8"))
            except (ValueError, UnicodeDecodeError) as exc:
                self._desired_error(mqtt_identity, f"unparseable desired state: {exc}")
                return
            try:
                handler(desired)
            except Exception as exc:  # noqa: BLE001 - reported, not swallowed
                self._desired_error(mqtt_identity, str(exc))

        # QoS 1: a retained desired message delivered once matters more than one
        # delivered fast. Retained delivery means the last published desired state
        # is replayed to this subscriber on every (re)connect.
        self._mqtt_client.subscribe(
            f"thingsflow/devices/{mqtt_identity}/desired", qos=1
        )
        # Route desired messages through the same callback; paho calls every
        # registered callback, so layer ours on top of the existing on_message.
        prev_on_message = self._mqtt_client.on_message

        def dispatch(_client: Any, _userdata: Any, message: Any) -> None:
            if message.topic == f"thingsflow/devices/{mqtt_identity}/desired":
                on_message(_client, _userdata, message)
                return
            if prev_on_message is not None:
                prev_on_message(_client, _userdata, message)

        self._mqtt_client.on_message = dispatch

    def _desired_error(self, mqtt_identity: str, message: str) -> None:
        """Log a desired-state handling failure. There is no response topic for
        desired state (it is a push, not a request/response), so failures are
        surfaced through the standard logging path rather than a reply."""
        import logging

        logging.getLogger(__name__).warning(
            "desired_state_error mqtt_id=%s message=%s", mqtt_identity, message
        )

    def close(self) -> None:
        if self._mqtt_client is not None:
            self._mqtt_client.loop_stop()
            self._mqtt_client.disconnect()
            self._mqtt_client = None

    def _reconnect_mqtt(self) -> None:
        if self._mqtt_client is None or self._mqtt_connection is None:
            return
        host, port, tls, ca_cert = self._mqtt_connection
        client_id = getattr(self._mqtt_client, "_client_id", b"")
        if isinstance(client_id, bytes):
            client_id = client_id.decode("utf-8", errors="ignore")
        self.close()
        self.connect_mqtt(host, port=port, tls=tls, ca_cert=ca_cert, client_id=client_id)

    def _request_json(
        self,
        method: str,
        path: str,
        *,
        payload: Optional[JsonPayload] = None,
        bearer_token: Optional[str] = None,
        accept_empty: bool = False,
    ) -> Dict[str, Any]:
        url = f"{self.base_url}{path}"
        body = None if payload is None else json.dumps(payload).encode("utf-8")
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        if bearer_token:
            headers["Authorization"] = f"Bearer {bearer_token}"
        request = urllib.request.Request(url, data=body, headers=headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                raw = response.read()
                if not raw:
                    if accept_empty:
                        return {}
                    raise ThingsFlowDeviceError(f"{method} {path} returned an empty response")
                text = raw.decode("utf-8")
                if accept_empty and not text.strip():
                    return {}
                try:
                    return json.loads(text)
                except json.JSONDecodeError:
                    if accept_empty:
                        return {}
                    raise
        except urllib.error.HTTPError as exc:
            raw = exc.read().decode("utf-8", errors="replace")
            raise ThingsFlowDeviceError(f"{method} {path} failed with HTTP {exc.code}: {raw}") from exc
        except urllib.error.URLError as exc:
            raise ThingsFlowDeviceError(f"{method} {path} failed: {exc.reason}") from exc
        except json.JSONDecodeError as exc:
            raise ThingsFlowDeviceError(f"{method} {path} returned invalid JSON") from exc


ThingsFlowDeviceClient = ThingsFlowDeviceClient
ThingsFlowDeviceError = ThingsFlowDeviceError
