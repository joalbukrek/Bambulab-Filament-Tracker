from __future__ import annotations

import json
import ssl
import time
from pathlib import Path
from itertools import permutations
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

from .cloud import CloudFilamentUsage, CloudTaskDetail
from .config import PrinterConfig
from .db import Store
from .parser import (
    bambu_ams_mapping_to_slots,
    parse_usage_file,
    usage_by_slot,
)
from .sheets_sync import SheetsSyncer

MQTT_REPORT_TOPIC = "device/{serial}/report"
MQTT_REQUEST_TOPIC = "device/{serial}/request"

FINISHED_STATES = {"FINISH", "FAILED", "CANCELLED", "CANCEL", "STOPPED"}
RUNNING_STATES = {"RUNNING", "PREPARE", "PAUSE", "PAUSED"}


class FilamentTracker:
    def __init__(self, store: Store, printer_serial: str = "", cloud_client: Optional[Any] = None) -> None:
        self.store = store
        self.printer_serial = printer_serial
        self.cloud_client = cloud_client
        self.current_job_id: Optional[int] = None
        self.current_active_slot: Optional[int] = None

    def handle_mqtt_message(self, topic: str, payload: bytes) -> List[str]:
        try:
            message = json.loads(payload.decode("utf-8", errors="replace"))
        except json.JSONDecodeError as exc:
            return ["Ignored invalid JSON payload: %s" % exc]

        if topic.endswith("/request"):
            return self._handle_request(message)
        return self._handle_report(message)

    def _handle_request(self, message: Dict[str, Any]) -> List[str]:
        print_command = message.get("print")
        if not isinstance(print_command, dict):
            return []

        command = str(print_command.get("command", ""))
        if command != "project_file":
            return []

        mapping = normalize_ams_mapping(print_command.get("ams_mapping"))
        plate_index = first_int(print_command, "plate_index", "plate_id", "curr_plate_index")
        subtask_name = str(print_command.get("subtask_name") or print_command.get("project_name") or "")
        gcode_file = request_gcode_file(print_command)
        job_key = job_key_from_request(self.printer_serial, print_command)

        job_id = self.store.start_job(
            printer_serial=self.printer_serial,
            job_key=job_key,
            subtask_name=subtask_name,
            gcode_file=gcode_file,
            plate_index=plate_index,
            ams_mapping=mapping,
            cloud_task_id=extract_cloud_task_id(print_command),
        )
        self.current_job_id = job_id
        return ["Started job %s from print request; AMS mapping=%s" % (job_id, mapping)]

    def _handle_report(self, message: Dict[str, Any]) -> List[str]:
        report = message.get("print")
        if not isinstance(report, dict):
            return []

        events: List[str] = []
        active_slot = extract_active_slot(report)
        ams_slots = extract_ams_slots(report)
        for slot, tray in ams_slots.items():
            self.store.upsert_ams_slot(slot, tray, is_active=slot == active_slot)
        if ams_slots:
            events.append(
                "AMS slots seen: %s%s"
                % (
                    ", ".join(str(slot) for slot in sorted(ams_slots)),
                    "; active=%s" % active_slot if active_slot is not None else "",
                )
            )

        if active_slot is not None:
            self.current_active_slot = active_slot

        state = str(report.get("gcode_state") or report.get("print_state") or "").upper()
        if state and state not in RUNNING_STATES and state not in FINISHED_STATES:
            events.append("Printer state=%s; active AMS slot=%s" % (state, active_slot or "?"))
        if state in RUNNING_STATES:
            job_id = self._ensure_running_job(report)
            if job_id:
                if active_slot is not None:
                    self.store.record_job_ams_slot(job_id, active_slot)
                events.append("Job %s is %s; active AMS slot=%s" % (job_id, state, active_slot or "?"))

        if state in FINISHED_STATES:
            events.extend(finish_diagnostics(report))
            job = self.store.latest_running_job(self.printer_serial)
            if job is None and self.current_job_id is not None:
                job = self.store.get_job(self.current_job_id)
            if job is None:
                return events

            usage_g = extract_print_weight_g(report)
            usage_source = "mqtt_print_weight" if usage_g is not None else None
            if usage_g is None:
                events.append("No final filament weight field found in MQTT finish report")
                cloud_usage_g, cloud_events = self._store_cloud_usage(int(job["id"]), job, report, state)
                events.extend(cloud_events)
                if cloud_usage_g is not None:
                    usage_g = cloud_usage_g
                    usage_source = "bambu_cloud"
            if usage_g is not None and not self.store.job_has_usage(int(job["id"])):
                stored = self._store_single_slot_usage(int(job["id"]), job, usage_g, report)
                if stored:
                    events.append("Stored %.2fg usage for finished job %s" % (usage_g, int(job["id"])))
                else:
                    events.append(
                        "Printer reported %.2fg total, but multiple AMS slots were observed; not assigning grams to one spool"
                        % usage_g
                    )
            self.store.finish_job(
                int(job["id"]),
                "finished" if state == "FINISH" else state.lower(),
                total_used_g=usage_g,
                usage_source=usage_source,
                completion_percent=extract_completion_percent(report),
            )
            events.append("Finished job %s with state %s" % (int(job["id"]), state))
            self.current_job_id = None
        return events

    def _ensure_running_job(self, report: Dict[str, Any]) -> Optional[int]:
        existing = self.store.latest_running_job(self.printer_serial)
        if existing:
            self.current_job_id = int(existing["id"])
            task_id = extract_cloud_task_id(report)
            if task_id:
                self.store.update_job_cloud_details(int(existing["id"]), cloud_task_id=task_id)
            return int(existing["id"])

        subtask_name = str(report.get("subtask_name") or report.get("project_name") or "")
        gcode_file = str(report.get("gcode_file") or report.get("mc_print_file") or "")
        if not subtask_name and not gcode_file:
            return None

        job_key = job_key_from_report(self.printer_serial, report)
        job_id = self.store.start_job(
            printer_serial=self.printer_serial,
            job_key=job_key,
            subtask_name=subtask_name,
            gcode_file=gcode_file,
            plate_index=first_int(report, "plate_index", "plate_id", "curr_plate_index"),
            ams_mapping=normalize_ams_mapping(report.get("ams_mapping")),
            cloud_task_id=extract_cloud_task_id(report),
        )
        self.current_job_id = job_id
        return job_id

    def _store_cloud_usage(
        self,
        job_id: int,
        job: Any,
        report: Dict[str, Any],
        state: str,
    ) -> Tuple[Optional[float], List[str]]:
        if self.cloud_client is None:
            return None, []

        events: List[str] = []
        task_id = extract_cloud_task_id(report) or cloud_task_id_from_job(job)
        if not task_id:
            events.append("No Bambu Cloud task_id available for this job")
            return None, events

        try:
            detail = self.cloud_client.get_task_detail_for_task_id(task_id)
        except Exception as exc:
            events.append("Bambu Cloud usage lookup failed for task %s: %s" % (task_id, exc))
            return None, events

        if detail is None:
            events.append("Bambu Cloud did not return task detail for task %s" % task_id)
            return None, events

        self.store.update_job_cloud_details(
            job_id,
            cloud_task_id=detail.task_id,
            cloud_job_id=detail.job_id,
            cloud_payload=detail.raw,
        )
        multiplier = cloud_completion_multiplier(state, extract_completion_percent(report))
        total_used_g = detail.total_weight_g * multiplier
        if total_used_g <= 0:
            events.append("Bambu Cloud task %s had no positive filament weight" % task_id)
            return None, events

        if self.store.job_has_usage(job_id):
            events.append("Bambu Cloud found %.2fg for task %s; usage rows already exist" % (total_used_g, task_id))
            return total_used_g, events

        stored = self._store_cloud_filament_rows(job_id, detail, multiplier)
        if stored:
            events.append("Stored %.2fg usage from Bambu Cloud task %s" % (total_used_g, task_id))
        else:
            events.append(
                "Bambu Cloud found %.2fg for task %s, but could not safely map it to observed AMS slot(s)"
                % (total_used_g, task_id)
            )
        return total_used_g, events

    def _store_cloud_filament_rows(
        self,
        job_id: int,
        detail: CloudTaskDetail,
        multiplier: float,
    ) -> bool:
        observed = self.store.list_job_ams_slots(job_id)
        if not observed:
            return False

        mapped = self._cloud_rows_from_job_mapping(job_id, detail)
        if mapped is not None:
            for index, filament, row in mapped:
                self._add_cloud_usage_row(job_id, row, filament, index, multiplier)
            return True

        if len(detail.filaments) == 1 and len(observed) == 1:
            self._add_cloud_usage_row(job_id, observed[0], detail.filaments[0], 0, multiplier)
            return True

        if not detail.filaments and len(observed) == 1 and detail.total_weight_g > 0:
            filament = CloudFilamentUsage(filament_id="", weight_g=detail.total_weight_g)
            self._add_cloud_usage_row(job_id, observed[0], filament, 0, multiplier)
            return True

        matches = match_cloud_filaments_to_observed_slots(detail.filaments, observed)
        if not matches:
            return False
        for index, filament, row in matches:
            self._add_cloud_usage_row(job_id, row, filament, index, multiplier)
        return True

    def _cloud_rows_from_job_mapping(
        self,
        job_id: int,
        detail: CloudTaskDetail,
    ) -> Optional[List[Tuple[int, Any, Any]]]:
        if not detail.filaments:
            return None
        job = self.store.get_job(job_id)
        if job is None:
            return None
        slicer_to_slot = bambu_ams_mapping_to_slots(mapping_from_job(job))
        if not slicer_to_slot:
            return None
        observed_by_slot = {int(row["ams_slot"]): row for row in self.store.list_job_ams_slots(job_id)}
        matches: List[Tuple[int, Any, Any]] = []
        used_slots = set()
        for index, filament in enumerate(detail.filaments):
            slot = slicer_to_slot.get(index)
            if slot is None:
                return None
            row = observed_by_slot.get(slot)
            if row is None or slot in used_slots:
                return None
            used_slots.add(slot)
            matches.append((index, filament, row))
        return matches

    def _add_cloud_usage_row(
        self,
        job_id: int,
        row: Any,
        filament: Any,
        slicer_index: int,
        multiplier: float,
    ) -> None:
        used_g = float(filament.weight_g) * multiplier
        if used_g <= 0:
            return
        self.store.add_usage(
            print_job_id=job_id,
            spool_id=int(row["spool_id"]) if row["spool_id"] is not None else None,
            ams_slot=int(row["ams_slot"]),
            slicer_filament_index=slicer_index,
            filament_name=filament.name or row["filament_name"] or row["spool_name"] or "",
            material=filament.material or row["material"] or "",
            color_hex=filament.color_hex or row["color_hex"] or "",
            used_g=used_g,
            source="bambu_cloud",
        )

    def sync_cloud_usage(self, job_id: int, replace_existing: bool = False) -> List[str]:
        if self.cloud_client is None:
            raise ValueError("Bambu Cloud is not configured")
        job = self.store.get_job(job_id)
        if job is None:
            raise ValueError("No print job found with id %s" % job_id)
        if self.store.job_has_usage(job_id):
            if replace_existing:
                self.store.replace_job_usage(job_id)
            else:
                raise ValueError("Job %s already has usage rows" % job_id)
        usage_g, events = self._store_cloud_usage(job_id, job, {}, "FINISH")
        if usage_g is not None:
            self.store.finish_job(
                job_id,
                "finished",
                total_used_g=usage_g,
                usage_source="bambu_cloud",
                completion_percent=100.0,
            )
        return events

    def _store_single_slot_usage(
        self,
        job_id: int,
        job: Any,
        usage_g: float,
        report: Dict[str, Any],
    ) -> bool:
        observed = self.store.list_job_ams_slots(job_id)
        if len(observed) > 1:
            return False
        if len(observed) == 1:
            row = observed[0]
            self.store.add_usage(
                print_job_id=job_id,
                spool_id=int(row["spool_id"]) if row["spool_id"] is not None else None,
                ams_slot=int(row["ams_slot"]),
                slicer_filament_index=0,
                filament_name=row["filament_name"] or row["spool_name"] or "",
                material=row["material"] or "",
                color_hex=row["color_hex"] or "",
                used_g=usage_g,
                source="mqtt_print_weight",
            )
            return True

        mapping = mapping_from_job(job)
        slots = sorted({slot for slot in bambu_ams_mapping_to_slots(mapping).values() if slot is not None})
        if len(slots) == 1:
            slot = slots[0]
        elif len(slots) > 1:
            return False
        else:
            slot = extract_active_slot(report) or self.current_active_slot
        if slot is None:
            return False

        spool = self.store.active_spool_for_slot(slot)
        self.store.add_usage(
            print_job_id=job_id,
            spool_id=int(spool["id"]) if spool else None,
            ams_slot=slot,
            slicer_filament_index=0 if len(slots) <= 1 else None,
            filament_name=spool["name"] if spool else "",
            material=spool["material"] if spool else "",
            color_hex=spool["color_hex"] if spool else "",
            used_g=usage_g,
            source="mqtt_print_weight",
        )
        return True

    def import_usage(
        self,
        path: Path,
        job_id: Optional[int] = None,
        job_name: str = "",
        manual_mapping: Optional[Dict[int, int]] = None,
        plate_index: Optional[int] = None,
        fallback_slot: Optional[int] = None,
        replace_existing: bool = False,
    ) -> int:
        report = parse_usage_file(path, plate_index=plate_index)
        if job_id is None:
            job_key = "manual:%s:%s" % (path.resolve(), report.plate_index)
            job_id = self.store.start_job(
                printer_serial=self.printer_serial,
                job_key=job_key,
                subtask_name=job_name or path.stem,
                gcode_file=str(path),
                plate_index=report.plate_index,
                ams_mapping=None,
            )
        job = self.store.get_job(job_id)
        if job is None:
            raise ValueError("No print job found with id %s" % job_id)
        if self.store.job_has_usage(job_id):
            if replace_existing:
                self.store.replace_job_usage(job_id)
            else:
                raise ValueError(
                    "Job %s already has usage rows. Re-run with --replace-existing to overwrite them." % job_id
                )

        slicer_to_slot: Dict[int, Optional[int]]
        if manual_mapping is not None:
            slicer_to_slot = dict(manual_mapping)
        else:
            slicer_to_slot = bambu_ams_mapping_to_slots(mapping_from_job(job))
            observed = self.store.list_job_ams_slots(job_id)
            if not slicer_to_slot and len(report.filaments) == 1 and len(observed) == 1:
                fallback_slot = int(observed[0]["ams_slot"])

        for filament, slot in usage_by_slot(report.filaments, slicer_to_slot, fallback_slot=fallback_slot):
            spool = self.store.active_spool_for_slot(slot)
            self.store.add_usage(
                print_job_id=job_id,
                spool_id=int(spool["id"]) if spool else None,
                ams_slot=slot,
                slicer_filament_index=filament.slicer_index,
                filament_name=filament.name or (spool["name"] if spool else ""),
                material=filament.material or (spool["material"] if spool else ""),
                color_hex=filament.color_hex or (spool["color_hex"] if spool else ""),
                used_g=filament.used_g,
                source="3mf" if ".3mf" in [suffix.lower() for suffix in path.suffixes] else "gcode",
            )

        self.store.finish_job(
            job_id,
            "finished",
            total_used_g=report.total_used_g,
            usage_source="3mf" if ".3mf" in [suffix.lower() for suffix in path.suffixes] else "gcode",
        )
        return job_id


class BambuMqttListener:
    def __init__(
        self,
        config: PrinterConfig,
        tracker: FilamentTracker,
        on_event: Optional[Callable[[str], None]] = None,
        syncer: Optional[SheetsSyncer] = None,
    ) -> None:
        self.config = config
        self.tracker = tracker
        self.on_event = on_event or (lambda event: None)
        self.syncer = syncer

    def run_forever(self) -> None:
        try:
            import paho.mqtt.client as mqtt
        except ImportError as exc:
            raise RuntimeError("paho-mqtt is required. Install with `pip install -e .`.") from exc

        client = make_mqtt_client(mqtt, "btlisten%s" % self.config.serial[-6:])
        state = {"connected": False, "messages": 0}
        client.username_pw_set(self.config.username, self.config.access_code)
        client.tls_set(cert_reqs=ssl.CERT_NONE)
        if self.config.tls_insecure:
            client.tls_insecure_set(True)

        def on_connect(client: Any, userdata: Any, flags: Any, rc: int, *extra: Any) -> None:
            if rc != 0:
                self.on_event("MQTT connection failed with code %s" % rc)
                return
            state["connected"] = True
            report_topic = MQTT_REPORT_TOPIC.format(serial=self.config.serial)
            client.subscribe(report_topic)
            self.on_event("Subscribed to %s" % report_topic)
            if self.config.monitor_requests:
                request_topic = MQTT_REQUEST_TOPIC.format(serial=self.config.serial)
                client.subscribe(request_topic)
                self.on_event("Subscribed to %s" % request_topic)
            push_all(client, self.config.serial)
            self.on_event("Requested printer status refresh")

        def on_message(client: Any, userdata: Any, message: Any) -> None:
            state["messages"] += 1
            events = self.tracker.handle_mqtt_message(message.topic, message.payload)
            for event in events:
                self.on_event(event)
            if events and self.syncer is not None:
                try:
                    result = self.syncer.sync(force=any("Finished job" in event for event in events))
                except Exception as exc:
                    self.on_event("Google Sheets sync failed without stopping listener: %s" % exc)
                else:
                    if not result.ok or "throttled" not in result.message:
                        self.on_event(result.message)

        def on_subscribe(client: Any, userdata: Any, mid: int, granted_qos: Any, *extra: Any) -> None:
            self.on_event("MQTT subscription acknowledged: mid=%s qos=%s" % (mid, granted_qos))

        def on_disconnect(client: Any, userdata: Any, rc: int, *extra: Any) -> None:
            state["connected"] = False
            self.on_event("MQTT disconnected: rc=%s" % rc)

        client.on_connect = on_connect
        client.on_subscribe = on_subscribe
        client.on_message = on_message
        client.on_disconnect = on_disconnect
        client.connect(self.config.host, self.config.mqtt_port, keepalive=60)
        client.loop_start()
        try:
            while True:
                time.sleep(30)
                if state["connected"]:
                    push_all(client, self.config.serial)
                    if state["messages"] == 0:
                        self.on_event("No MQTT reports received yet; requested another printer status refresh")
                    else:
                        self.on_event("Requested printer status refresh; messages received=%s" % state["messages"])
        finally:
            client.loop_stop()
            client.disconnect()


def push_all(client: Any, serial: str) -> None:
    topic = MQTT_REQUEST_TOPIC.format(serial=serial)
    payload = {"pushing": {"sequence_id": "0", "command": "pushall"}}
    client.publish(topic, json.dumps(payload))


def make_mqtt_client(mqtt: Any, client_id: str) -> Any:
    try:
        return mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, client_id=client_id)
    except (AttributeError, TypeError):
        return mqtt.Client(client_id=client_id)


def extract_ams_slots(report: Dict[str, Any]) -> Dict[int, Dict[str, Any]]:
    ams = report.get("ams")
    if not isinstance(ams, dict):
        return {}
    slots: Dict[int, Dict[str, Any]] = {}

    ams_units = ams.get("ams")
    if isinstance(ams_units, list):
        for unit in ams_units:
            if not isinstance(unit, dict):
                continue
            trays = unit.get("tray")
            if not isinstance(trays, list):
                continue
            for tray in trays:
                if not isinstance(tray, dict):
                    continue
                slot = slot_from_raw_tray(
                    first_present(tray, "id", "tray_id", "tray_idx", "slot")
                )
                if slot is not None:
                    slots[slot] = tray

    tray_now = ams.get("tray_now")
    if isinstance(tray_now, dict):
        slot = slot_from_raw_tray(first_present(tray_now, "id", "tray_id", "tray_idx", "slot"))
        if slot is not None:
            slots[slot] = tray_now
    return slots


def extract_active_slot(report: Dict[str, Any]) -> Optional[int]:
    ams = report.get("ams")
    if isinstance(ams, dict):
        for key in ("tray_now", "tray_tar", "tray_pre"):
            slot = slot_from_raw_tray(ams.get(key))
            if slot is not None:
                return slot
    for key in ("tray_now", "tray_tar", "vt_tray"):
        slot = slot_from_raw_tray(report.get(key))
        if slot is not None:
            return slot
    return None


def slot_from_raw_tray(value: Any) -> Optional[int]:
    if isinstance(value, dict):
        value = first_present(value, "id", "tray_id", "tray_idx", "slot")
    if value is None:
        return None
    try:
        raw = int(value)
    except (TypeError, ValueError):
        return None
    if raw < 0 or raw >= 254:
        return None
    if 0 <= raw <= 3:
        return raw + 1
    if raw == 4:
        return raw
    return None


def extract_print_weight_g(report: Dict[str, Any]) -> Optional[float]:
    for key in (
        "print_weight",
        "filament_used_g",
        "filament_used",
        "filament_used_gram",
        "filament_used_weight",
        "total_used_g",
        "total_filament_used",
        "total_filament_used_g",
        "total_filament_weight",
    ):
        value = report.get(key)
        if value is None:
            continue
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            continue
        if parsed > 0:
            return parsed
    for key, value in flatten_dict(report):
        normalized = key.lower()
        if not any(token in normalized for token in ("filament", "print_weight", "weight")):
            continue
        if any(blocked in normalized for blocked in ("time", "remain", "percent", "length", "diameter")):
            continue
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            continue
        if 0 < parsed < 10000:
            return parsed
    return None


def extract_cloud_task_id(report: Dict[str, Any]) -> Optional[str]:
    value = first_present(report, "task_id", "taskId")
    if value is None:
        return None
    text = str(value).strip()
    if not text or text == "0":
        return None
    return text


def cloud_task_id_from_job(job: Any) -> Optional[str]:
    if "cloud_task_id" in job.keys() and job["cloud_task_id"]:
        return str(job["cloud_task_id"])
    job_key = str(job["job_key"] or "") if "job_key" in job.keys() else ""
    marker = ":task_id:"
    if marker in job_key:
        task_id = job_key.rsplit(marker, 1)[-1].strip()
        if task_id and task_id != "0":
            return task_id
    return None


def extract_completion_percent(report: Dict[str, Any]) -> Optional[float]:
    value = first_present(report, "mc_percent", "progress", "print_percent")
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def cloud_completion_multiplier(state: str, percent: Optional[float]) -> float:
    if state.upper() == "FINISH":
        return 1.0
    if percent is None:
        return 1.0
    if percent <= 0:
        return 0.0
    if percent >= 100:
        return 1.0
    return percent / 100.0


def match_cloud_filaments_to_observed_slots(
    filaments: List[CloudFilamentUsage],
    observed: List[Any],
) -> Optional[List[Tuple[int, CloudFilamentUsage, Any]]]:
    if not filaments or not observed or len(filaments) != len(observed):
        return None

    for filament in filaments:
        has_candidate = False
        for row in observed:
            score = cloud_filament_slot_score(filament, row)
            if score >= 60:
                has_candidate = True
                break
        if not has_candidate:
            return None

    best_score = -1
    best_matches: Optional[List[Tuple[int, CloudFilamentUsage, Any]]] = None
    best_count = 0
    for rows in permutations(observed, len(filaments)):
        total = 0
        valid = True
        matches: List[Tuple[int, CloudFilamentUsage, Any]] = []
        for index, row in enumerate(rows):
            score = cloud_filament_slot_score(filaments[index], row)
            if score < 60:
                valid = False
                break
            total += score
            matches.append((index, filaments[index], row))
        if not valid:
            continue
        if total > best_score:
            best_score = total
            best_matches = matches
            best_count = 1
        elif total == best_score:
            best_count += 1

    if best_matches is None or best_count != 1:
        return None
    return best_matches


def cloud_filament_slot_score(filament: CloudFilamentUsage, row: Any) -> int:
    score = 0
    filament_id = normalize_match_text(filament.filament_id)
    row_ids = normalized_row_values(row, "tray_info_idx", "filament_name", "spool_name")
    if filament_id and filament_id in row_ids:
        score += 80

    cloud_color = normalize_match_text(filament.color_hex)
    row_color = normalize_match_text(row["color_hex"] if "color_hex" in row.keys() else "")
    if cloud_color and row_color and cloud_color == row_color:
        score += 70

    cloud_material = normalize_match_text(filament.material)
    row_material = normalize_match_text(row["material"] if "material" in row.keys() else "")
    if cloud_material and row_material and cloud_material == row_material:
        score += 20

    cloud_name = normalize_match_text(filament.name)
    if cloud_name:
        row_names = normalized_row_values(row, "filament_name", "spool_name")
        if cloud_name in row_names:
            score += 35
        elif any(cloud_name in value or value in cloud_name for value in row_names if len(value) >= 4):
            score += 15

    if filament_id and cloud_color and row_color and cloud_color == row_color:
        score += 20
    return score


def normalized_row_values(row: Any, *keys: str) -> set:
    values = set()
    for key in keys:
        if key in row.keys():
            normalized = normalize_match_text(row[key])
            if normalized:
                values.add(normalized)
    return values


def normalize_match_text(value: Any) -> str:
    return str(value or "").strip().lower()


def finish_diagnostics(report: Dict[str, Any]) -> List[str]:
    candidates = []
    for key, value in flatten_dict(report):
        normalized = key.lower()
        if any(token in normalized for token in ("filament", "weight", "meter", "length", "print_weight")):
            candidates.append("%s=%s" % (key, value))
    if not candidates:
        return ["Finish report had no obvious filament/weight fields"]
    return ["Finish report candidate usage fields: " + "; ".join(candidates[:20])]


def flatten_dict(value: Any, prefix: str = "") -> Iterable[Tuple[str, Any]]:
    if isinstance(value, dict):
        for key, nested in value.items():
            next_prefix = "%s.%s" % (prefix, key) if prefix else str(key)
            yield from flatten_dict(nested, next_prefix)
    elif isinstance(value, list):
        for index, nested in enumerate(value):
            next_prefix = "%s[%s]" % (prefix, index)
            yield from flatten_dict(nested, next_prefix)
    else:
        yield prefix, value


def normalize_ams_mapping(value: Any) -> Optional[List[int]]:
    if value is None:
        return None
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return None
    if not isinstance(value, list):
        return None
    result: List[int] = []
    for item in value:
        try:
            result.append(int(item))
        except (TypeError, ValueError):
            result.append(-1)
    return result


def mapping_from_job(job: Any) -> Optional[List[int]]:
    raw = job["raw_ams_mapping"] if "raw_ams_mapping" in job.keys() else None
    if not raw:
        return None
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return None
    return normalize_ams_mapping(data)


def job_key_from_request(serial: str, command: Dict[str, Any]) -> str:
    for key in ("task_id", "project_id", "subtask_id"):
        if is_real_identifier(command.get(key)):
            return "%s:%s:%s" % (serial, key, command[key])
    return "%s:request:%s:%s" % (serial, request_gcode_file(command), int(time.time()))


def job_key_from_report(serial: str, report: Dict[str, Any]) -> str:
    for key in ("task_id", "project_id", "subtask_id"):
        if is_real_identifier(report.get(key)):
            return "%s:%s:%s" % (serial, key, report[key])
    name = str(report.get("subtask_name") or report.get("gcode_file") or report.get("mc_print_file") or "unknown")
    return "%s:report:%s:%s" % (serial, name, int(time.time()))


def is_real_identifier(value: Any) -> bool:
    if value is None:
        return False
    text = str(value).strip()
    return bool(text and text != "0")


def request_gcode_file(command: Dict[str, Any]) -> str:
    for key in ("gcode_file", "file", "filename", "url"):
        value = command.get(key)
        if value:
            return str(value)
    return ""


def first_present(data: Dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in data:
            return data[key]
    return None


def first_int(data: Dict[str, Any], *keys: str) -> Optional[int]:
    for key in keys:
        value = data.get(key)
        if value is None or value == "":
            continue
        try:
            return int(value)
        except (TypeError, ValueError):
            continue
    return None
