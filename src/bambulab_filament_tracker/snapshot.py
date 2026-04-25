from __future__ import annotations

import json
import ssl
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from .config import PrinterConfig
from .db import Store
from .tracker import (
    MQTT_REPORT_TOPIC,
    MQTT_REQUEST_TOPIC,
    FilamentTracker,
    extract_active_slot,
    extract_print_weight_g,
    push_all,
)


@dataclass
class PrinterSnapshot:
    connected: bool
    messages: int
    events: List[str]
    state: str
    job: str
    percent: Optional[Any]
    remaining_min: Optional[Any]
    active_ams_slot: Optional[int]
    print_weight_g: Optional[float]


@dataclass
class MqttDump:
    connected: bool
    messages: int
    events: List[str]
    report: Dict[str, Any]
    reports: List[Dict[str, Any]]


def take_snapshot(
    config: PrinterConfig,
    seconds: float = 12.0,
    max_messages: int = 5,
    wildcard: bool = False,
) -> PrinterSnapshot:
    try:
        import paho.mqtt.client as mqtt
    except ImportError as exc:
        raise RuntimeError("paho-mqtt is required. Install it with `.venv/bin/python -m pip install paho-mqtt`.") from exc

    store = Store(config.db_path)
    tracker = FilamentTracker(store, printer_serial=config.serial)
    report_topic = MQTT_REPORT_TOPIC.format(serial=config.serial)
    request_topic = MQTT_REQUEST_TOPIC.format(serial=config.serial)
    state: Dict[str, Any] = {
        "connected": False,
        "messages": 0,
        "events": [],
        "last_report": None,
    }

    client = make_client(mqtt, "btsnap%s%s" % (config.serial[-4:], int(time.time()) % 100000))
    client.username_pw_set(config.username, config.access_code)
    client.tls_set(cert_reqs=ssl.CERT_NONE)
    if config.tls_insecure:
        client.tls_insecure_set(True)

    def on_connect(client: Any, userdata: Any, flags: Any, rc: int, *extra: Any) -> None:
        state["connected"] = rc == 0
        if rc != 0:
            state["events"].append("MQTT connection failed with code %s" % rc)
            return
        topic = "#" if wildcard else report_topic
        result, mid = client.subscribe(topic)
        state["events"].append("subscribe topic=%s result=%s mid=%s" % (topic, result, mid))
        if config.monitor_requests:
            result, mid = client.subscribe(request_topic)
            state["events"].append("subscribe topic=%s result=%s mid=%s" % (request_topic, result, mid))
        push_all(client, config.serial)
        state["events"].append("published pushall request to %s" % request_topic)

    def on_subscribe(client: Any, userdata: Any, mid: int, granted_qos: Any, *extra: Any) -> None:
        state["events"].append("suback mid=%s qos=%s" % (mid, granted_qos))

    def on_disconnect(client: Any, userdata: Any, rc: int, *extra: Any) -> None:
        state["events"].append("disconnect rc=%s" % rc)

    def on_message(client: Any, userdata: Any, message: Any) -> None:
        state["messages"] += 1
        if wildcard:
            state["events"].append("topic=%s" % message.topic)
        state["events"].extend(tracker.handle_mqtt_message(message.topic, message.payload))
        try:
            payload = json.loads(message.payload.decode("utf-8", errors="replace"))
        except json.JSONDecodeError:
            return
        report = payload.get("print")
        if isinstance(report, dict):
            state["last_report"] = report

    client.on_connect = on_connect
    client.on_subscribe = on_subscribe
    client.on_disconnect = on_disconnect
    client.on_message = on_message
    try:
        client.connect(config.host, config.mqtt_port, keepalive=60)
    except OSError as exc:
        return PrinterSnapshot(
            connected=False,
            messages=0,
            events=["Connection failed: %s" % exc],
            state="unknown",
            job="",
            percent=None,
            remaining_min=None,
            active_ams_slot=None,
            print_weight_g=None,
        )
    client.loop_start()
    deadline = time.time() + seconds
    try:
        while time.time() < deadline and int(state["messages"]) < max_messages:
            time.sleep(0.2)
    finally:
        client.loop_stop()
        client.disconnect()

    report = state["last_report"] or {}
    return PrinterSnapshot(
        connected=bool(state["connected"]),
        messages=int(state["messages"]),
        events=list(state["events"]),
        state=str(report.get("gcode_state") or report.get("print_state") or "unknown"),
        job=str(report.get("subtask_name") or report.get("gcode_file") or report.get("mc_print_file") or ""),
        percent=report.get("mc_percent"),
        remaining_min=report.get("mc_remaining_time"),
        active_ams_slot=extract_active_slot(report),
        print_weight_g=extract_print_weight_g(report),
    )


def dump_mqtt_report(
    config: PrinterConfig,
    seconds: float = 12.0,
    max_messages: int = 5,
    output_path: Optional[Path] = None,
) -> MqttDump:
    try:
        import paho.mqtt.client as mqtt
    except ImportError as exc:
        raise RuntimeError("paho-mqtt is required. Install it with `.venv/bin/python -m pip install paho-mqtt`.") from exc

    report_topic = MQTT_REPORT_TOPIC.format(serial=config.serial)
    request_topic = MQTT_REQUEST_TOPIC.format(serial=config.serial)
    state: Dict[str, Any] = {
        "connected": False,
        "messages": 0,
        "events": [],
        "last_report": {},
        "merged_report": {},
        "reports": [],
    }

    client = make_client(mqtt, "btdump%s%s" % (config.serial[-4:], int(time.time()) % 100000))
    client.username_pw_set(config.username, config.access_code)
    client.tls_set(cert_reqs=ssl.CERT_NONE)
    if config.tls_insecure:
        client.tls_insecure_set(True)

    def on_connect(client: Any, userdata: Any, flags: Any, rc: int, *extra: Any) -> None:
        state["connected"] = rc == 0
        if rc != 0:
            state["events"].append("MQTT connection failed with code %s" % rc)
            return
        result, mid = client.subscribe(report_topic)
        state["events"].append("subscribe topic=%s result=%s mid=%s" % (report_topic, result, mid))
        push_all(client, config.serial)
        state["events"].append("published pushall request to %s" % request_topic)

    def on_message(client: Any, userdata: Any, message: Any) -> None:
        state["messages"] += 1
        try:
            payload = json.loads(message.payload.decode("utf-8", errors="replace"))
        except json.JSONDecodeError as exc:
            state["events"].append("invalid json: %s" % exc)
            return
        report = payload.get("print")
        if isinstance(report, dict):
            state["last_report"] = report
            state["reports"].append(report)
            deep_merge(state["merged_report"], report)

    client.on_connect = on_connect
    client.on_message = on_message
    try:
        client.connect(config.host, config.mqtt_port, keepalive=60)
    except OSError as exc:
        return MqttDump(False, 0, ["Connection failed: %s" % exc], {}, [])

    client.loop_start()
    deadline = time.time() + seconds
    try:
        while time.time() < deadline and int(state["messages"]) < max_messages:
            time.sleep(0.2)
    finally:
        client.loop_stop()
        client.disconnect()

    report = dict(state["merged_report"] or state["last_report"] or {})
    if output_path is not None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "merged_report": report,
            "reports": list(state["reports"]),
        }
        output_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return MqttDump(bool(state["connected"]), int(state["messages"]), list(state["events"]), report, list(state["reports"]))


def deep_merge(target: Dict[str, Any], source: Dict[str, Any]) -> Dict[str, Any]:
    for key, value in source.items():
        existing = target.get(key)
        if isinstance(existing, dict) and isinstance(value, dict):
            deep_merge(existing, value)
        else:
            target[key] = value
    return target


def make_client(mqtt: Any, client_id: str) -> Any:
    try:
        return mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, client_id=client_id)
    except (AttributeError, TypeError):
        return mqtt.Client(client_id=client_id)
