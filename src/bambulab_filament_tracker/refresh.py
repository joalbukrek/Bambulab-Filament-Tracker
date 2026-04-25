from __future__ import annotations

import ssl
import time
from dataclasses import dataclass
from typing import Any

from .config import PrinterConfig
from .tracker import push_all


@dataclass
class RefreshResult:
    connected: bool
    message: str


def request_status_refresh(config: PrinterConfig, seconds: float = 2.0) -> RefreshResult:
    try:
        import paho.mqtt.client as mqtt
    except ImportError as exc:
        raise RuntimeError("paho-mqtt is required. Install it with `.venv/bin/python -m pip install paho-mqtt`.") from exc

    connected = {"value": False, "message": ""}
    client = make_client(mqtt, "btref%s%s" % (config.serial[-4:], int(time.time()) % 100000))
    client.username_pw_set(config.username, config.access_code)
    client.tls_set(cert_reqs=ssl.CERT_NONE)
    if config.tls_insecure:
        client.tls_insecure_set(True)

    def on_connect(client: Any, userdata: Any, flags: Any, rc: int, *extra: Any) -> None:
        connected["value"] = rc == 0
        if rc == 0:
            push_all(client, config.serial)
            connected["message"] = "Requested status push from printer"
        else:
            connected["message"] = "MQTT connection failed with code %s" % rc

    client.on_connect = on_connect
    try:
        client.connect(config.host, config.mqtt_port, keepalive=60)
    except OSError as exc:
        return RefreshResult(False, "Connection failed: %s" % exc)

    client.loop_start()
    try:
        time.sleep(seconds)
    finally:
        client.loop_stop()
        client.disconnect()

    return RefreshResult(bool(connected["value"]), connected["message"] or "No connection callback received")


def make_client(mqtt: Any, client_id: str) -> Any:
    try:
        return mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, client_id=client_id)
    except (AttributeError, TypeError):
        return mqtt.Client(client_id=client_id)
