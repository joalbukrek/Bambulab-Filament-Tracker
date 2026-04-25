from __future__ import annotations

import tempfile
import unittest
import zipfile
import json
from pathlib import Path

from bambulab_filament_tracker.cloud import CloudFilamentUsage, CloudTaskDetail, parse_task_detail
from bambulab_filament_tracker.db import Store
from bambulab_filament_tracker.parser import (
    bambu_ams_mapping_to_slots,
    parse_gcode_usage,
    parse_usage_file,
)
from bambulab_filament_tracker.sheets_sync import build_sheets_payload
from bambulab_filament_tracker.tracker import FilamentTracker, match_cloud_filaments_to_observed_slots


SAMPLE_GCODE = """; filament used [mm] = 1000.0, 250.0
; filament used [g] = 3.00, 0.75
; filament_type = PLA;PETG
; filament_colour: #ff0000;#00ff00ff
M83
T0
G1 E1.0
T1
G1 E2.0
"""


class ParserTests(unittest.TestCase):
    def test_parse_gcode_weight_vectors(self) -> None:
        usage = parse_gcode_usage(SAMPLE_GCODE)
        self.assertEqual(len(usage), 2)
        self.assertEqual(usage[0].slicer_index, 0)
        self.assertAlmostEqual(usage[0].used_g, 3.0)
        self.assertEqual(usage[0].material, "PLA")
        self.assertEqual(usage[0].color_hex, "FF0000")
        self.assertEqual(usage[1].slicer_index, 1)
        self.assertAlmostEqual(usage[1].used_g, 0.75)
        self.assertEqual(usage[1].material, "PETG")
        self.assertEqual(usage[1].color_hex, "00FF00")

    def test_parse_3mf_selects_plate(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "project.3mf"
            with zipfile.ZipFile(path, "w") as archive:
                archive.writestr("Metadata/plate_1.gcode", "; filament used [g] = 1.0\n")
                archive.writestr("Metadata/plate_2.gcode", "; filament used [g] = 2.0\n")
            report = parse_usage_file(path, plate_index=1)
            self.assertEqual(report.plate_index, 1)
            self.assertAlmostEqual(report.total_used_g, 2.0)

    def test_bambu_mapping_is_converted_to_user_slots(self) -> None:
        self.assertEqual(bambu_ams_mapping_to_slots([1, 3, -1]), {0: 2, 1: 4, 2: None})

    def test_import_usage_subtracts_assigned_spool(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            gcode = root / "part.gcode"
            gcode.write_text("; filament used [g] = 12.5\n; filament_type = PLA\n", encoding="utf-8")
            store = Store(root / "filament.sqlite3")
            spool_id = store.add_spool("Red PLA", material="PLA", initial_weight_g=1000.0, ams_slot=2)

            tracker = FilamentTracker(store, printer_serial="TEST")
            job_id = tracker.import_usage(gcode, fallback_slot=2)

            spools = store.list_spools()
            self.assertEqual(spools[0]["id"], spool_id)
            self.assertAlmostEqual(spools[0]["remaining_weight_g"], 987.5)
            usage = store.list_usage()
            self.assertEqual(len(usage), 1)
            self.assertEqual(usage[0]["ams_slot"], 2)
            self.assertAlmostEqual(usage[0]["used_g"], 12.5)

            with self.assertRaises(ValueError):
                tracker.import_usage(gcode, job_id=job_id, fallback_slot=2)

            tracker.import_usage(gcode, job_id=job_id, fallback_slot=2, replace_existing=True)
            spools = store.list_spools()
            self.assertAlmostEqual(spools[0]["remaining_weight_g"], 987.5)

    def test_set_slot_spool_weight_updates_current_slot_spool(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = Store(Path(tmp) / "filament.sqlite3")
            spool_id = store.add_spool("Green PLA", material="PLA", initial_weight_g=1000.0, ams_slot=4)

            updated_id = store.set_slot_spool_weights(4, remaining_weight_g=750.0)

            self.assertEqual(updated_id, spool_id)
            spools = store.list_spools()
            self.assertAlmostEqual(spools[0]["remaining_weight_g"], 750.0)

    def test_mqtt_ams_metadata_creates_printer_spool_and_records_job_slot(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = Store(Path(tmp) / "filament.sqlite3")
            tracker = FilamentTracker(store, printer_serial="TEST")

            report = {
                "print": {
                    "gcode_state": "RUNNING",
                    "subtask_name": "cube",
                    "ams": {
                        "tray_now": "1",
                        "ams": [
                            {
                                "id": "0",
                                "tray": [
                                    {"id": "0"},
                                    {
                                        "id": "1",
                                        "tray_type": "PLA",
                                        "tray_color": "00AE2FFF",
                                        "tray_sub_brands": "PLA Basic",
                                        "tray_id_name": "00-G6",
                                        "tray_info_idx": "GFA00",
                                        "tray_uuid": "77FDADEE154E3E9AFBCF8CB5F6",
                                        "tray_weight": "1000",
                                    },
                                ],
                            }
                        ],
                    },
                }
            }
            events = tracker.handle_mqtt_message("device/TEST/report", bytes(json.dumps(report), "utf-8"))
            self.assertTrue(any("active AMS slot=2" in event for event in events))

            ams_slots = store.list_ams_slots()
            active = [row for row in ams_slots if row["slot"] == 2][0]
            self.assertEqual(active["filament_name"], "PLA Basic")
            self.assertEqual(active["material"], "PLA")
            self.assertEqual(active["color_hex"], "00AE2F")
            self.assertEqual(active["is_active"], 1)

            spools = store.list_spools()
            self.assertEqual(len(spools), 1)
            self.assertEqual(spools[0]["source"], "printer")
            self.assertEqual(spools[0]["name"], "PLA Basic")
            self.assertEqual(spools[0]["ams_slot"], 2)

            jobs = store.list_jobs()
            self.assertEqual(jobs[0]["observed_ams_slots"], "[2]")
            job_slots = store.list_job_ams_slots(jobs[0]["id"])
            self.assertEqual(job_slots[0]["ams_slot"], 2)
            self.assertEqual(job_slots[0]["filament_name"], "PLA Basic")

    def test_new_printer_tray_key_in_same_slot_creates_new_spool(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = Store(Path(tmp) / "filament.sqlite3")
            store.upsert_ams_slot(
                2,
                {
                    "id": "1",
                    "tray_type": "PLA",
                    "tray_color": "FF0000FF",
                    "tray_sub_brands": "PLA Red",
                    "tray_uuid": "AAAAAAAAAAAAAAAAAAAAAAAAAA",
                },
            )
            store.upsert_ams_slot(
                2,
                {
                    "id": "1",
                    "tray_type": "PLA",
                    "tray_color": "0000FFFF",
                    "tray_sub_brands": "PLA Blue",
                    "tray_uuid": "BBBBBBBBBBBBBBBBBBBBBBBBBB",
                },
            )

            spools = store.list_spools()
            self.assertEqual(len(spools), 2)
            self.assertEqual(spools[0]["ams_slot"], 2)
            self.assertEqual(spools[0]["name"], "PLA Blue")
            self.assertIsNone(spools[1]["ams_slot"])

    def test_sheets_payload_contains_dashboard_tables(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = Store(Path(tmp) / "filament.sqlite3")
            store.add_spool("Yellow PLA", material="PLA", color_hex="FFFF00", initial_weight_g=1000, ams_slot=1)
            payload = build_sheets_payload(store)
            self.assertIn("ams_slots", payload)
            self.assertIn("spools", payload)
            self.assertIn("print_jobs", payload)
            self.assertIn("usage", payload)
            self.assertEqual(payload["spools"]["headers"][0], "ID")
            self.assertEqual(payload["spools"]["rows"][0][3], "Yellow PLA")

    def test_parse_bambu_cloud_task_detail(self) -> None:
        detail = parse_task_detail(
            task_id="123",
            job_id="job-1",
            payload={
                "id": "job-1",
                "title": "cube",
                "weight": 15.5,
                "amsDetailMapping": [
                    {"filamentId": "GFL03", "weight": 15.5, "filamentType": "PLA", "color": "FFFFFFFF"}
                ],
            },
        )
        self.assertEqual(detail.task_id, "123")
        self.assertEqual(detail.job_id, "job-1")
        self.assertAlmostEqual(detail.total_weight_g, 15.5)
        self.assertEqual(detail.filaments[0].filament_id, "GFL03")
        self.assertEqual(detail.filaments[0].color_hex, "FFFFFF")

    def test_cloud_usage_is_stored_for_finished_single_slot_job(self) -> None:
        class FakeCloudClient:
            def get_task_detail_for_task_id(self, task_id: str) -> CloudTaskDetail:
                return CloudTaskDetail(
                    task_id=task_id,
                    job_id="job-123",
                    title="cube",
                    cover_url="",
                    total_weight_g=12.5,
                    filaments=[CloudFilamentUsage(filament_id="GFA00", weight_g=12.5, material="PLA")],
                    raw={"id": "job-123"},
                )

        with tempfile.TemporaryDirectory() as tmp:
            store = Store(Path(tmp) / "filament.sqlite3")
            tracker = FilamentTracker(store, printer_serial="TEST", cloud_client=FakeCloudClient())
            running = {
                "print": {
                    "gcode_state": "RUNNING",
                    "task_id": "123",
                    "subtask_name": "cube",
                    "mc_percent": 10,
                    "ams": {
                        "tray_now": "1",
                        "ams": [
                            {
                                "tray": [
                                    {"id": "1", "tray_type": "PLA", "tray_info_idx": "GFA00", "tray_color": "FFFFFFFF"}
                                ]
                            }
                        ],
                    },
                }
            }
            tracker.handle_mqtt_message("device/TEST/report", bytes(json.dumps(running), "utf-8"))

            finished = {"print": {"gcode_state": "FINISH", "task_id": "123", "mc_percent": 100}}
            events = tracker.handle_mqtt_message("device/TEST/report", bytes(json.dumps(finished), "utf-8"))
            self.assertTrue(any("Stored 12.50g usage from Bambu Cloud task 123" in event for event in events))

            usage = store.list_usage()
            self.assertEqual(len(usage), 1)
            self.assertEqual(usage[0]["source"], "bambu_cloud")
            self.assertEqual(usage[0]["ams_slot"], 2)
            self.assertAlmostEqual(usage[0]["used_g"], 12.5)

            jobs = store.list_jobs()
            self.assertEqual(jobs[0]["cloud_task_id"], "123")
            self.assertEqual(jobs[0]["cloud_job_id"], "job-123")
            self.assertAlmostEqual(jobs[0]["total_used_g"], 12.5)

    def test_cloud_multi_spool_matching_uses_unique_color_when_filament_id_is_generic(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = Store(Path(tmp) / "filament.sqlite3")
            store.upsert_ams_slot(
                1,
                {"id": "0", "tray_type": "PLA", "tray_info_idx": "GFL03", "tray_color": "FF0000FF"},
            )
            store.upsert_ams_slot(
                2,
                {"id": "1", "tray_type": "PLA", "tray_info_idx": "GFL03", "tray_color": "0000FFFF"},
            )
            job_id = store.start_job("TEST", "job")
            store.record_job_ams_slot(job_id, 1)
            store.record_job_ams_slot(job_id, 2)

            observed = store.list_job_ams_slots(job_id)
            matches = match_cloud_filaments_to_observed_slots(
                [
                    CloudFilamentUsage(filament_id="GFL03", weight_g=4.0, material="PLA", color_hex="0000FF"),
                    CloudFilamentUsage(filament_id="GFL03", weight_g=5.0, material="PLA", color_hex="FF0000"),
                ],
                observed,
            )

            self.assertIsNotNone(matches)
            assert matches is not None
            self.assertEqual([int(match[2]["ams_slot"]) for match in matches], [2, 1])

    def test_cloud_multi_spool_matching_refuses_ambiguous_generic_filaments(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = Store(Path(tmp) / "filament.sqlite3")
            store.upsert_ams_slot(1, {"id": "0", "tray_type": "PLA", "tray_info_idx": "GFL03"})
            store.upsert_ams_slot(2, {"id": "1", "tray_type": "PLA", "tray_info_idx": "GFL03"})
            job_id = store.start_job("TEST", "job")
            store.record_job_ams_slot(job_id, 1)
            store.record_job_ams_slot(job_id, 2)

            matches = match_cloud_filaments_to_observed_slots(
                [
                    CloudFilamentUsage(filament_id="GFL03", weight_g=4.0, material="PLA"),
                    CloudFilamentUsage(filament_id="GFL03", weight_g=5.0, material="PLA"),
                ],
                store.list_job_ams_slots(job_id),
            )

            self.assertIsNone(matches)


if __name__ == "__main__":
    unittest.main()
