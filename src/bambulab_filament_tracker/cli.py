from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from .cloud import BambuCloudClient, BambuCloudError
from .config import CloudConfig, PrinterConfig, SheetsConfig, _load_dotenv
from .db import Store
from .parser import parse_manual_mapping
from .refresh import request_status_refresh
from .service import install_service, log_paths, read_logs, start_service, status_service, stop_service
from .sheets_sync import APPS_SCRIPT, SheetsSyncer
from .snapshot import dump_mqtt_report, take_snapshot
from .tracker import BambuMqttListener, FilamentTracker, finish_diagnostics, flatten_dict


DEFAULT_DB_PATH = Path("./data/filament.sqlite3")


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not hasattr(args, "func"):
        parser.print_help()
        return 1
    return int(args.func(args))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="bambu-track")
    parser.add_argument("--db", help="SQLite database path. Defaults to BAMBU_DB_PATH or ./data/filament.sqlite3")
    subparsers = parser.add_subparsers(dest="command")

    init_parser = subparsers.add_parser("init-db", help="Create the SQLite database")
    init_parser.set_defaults(func=cmd_init_db)

    doctor_parser = subparsers.add_parser("doctor", help="Check local setup, service, and latest tracking data")
    doctor_parser.add_argument("--env-file", default=".env", help="Path to .env config")
    doctor_parser.set_defaults(func=cmd_doctor)

    listen_parser = subparsers.add_parser("listen", help="Listen to the printer over local MQTT")
    listen_parser.add_argument("--env-file", default=".env", help="Path to .env config")
    listen_parser.set_defaults(func=cmd_listen)

    snapshot_parser = subparsers.add_parser("snapshot", help="Read a short live printer status snapshot over MQTT")
    snapshot_parser.add_argument("--env-file", default=".env", help="Path to .env config")
    snapshot_parser.add_argument("--seconds", type=float, default=12.0)
    snapshot_parser.add_argument("--messages", type=int, default=5)
    snapshot_parser.add_argument("--wildcard", action="store_true", help="Subscribe to # briefly to discover actual printer topics")
    snapshot_parser.set_defaults(func=cmd_snapshot)

    dump_parser = subparsers.add_parser("dump-mqtt", help="Dump and flatten the latest raw MQTT report")
    dump_parser.add_argument("--env-file", default=".env", help="Path to .env config")
    dump_parser.add_argument("--seconds", type=float, default=12.0)
    dump_parser.add_argument("--messages", type=int, default=5)
    dump_parser.add_argument("--output", default="logs/latest_mqtt_report.json")
    dump_parser.add_argument("--all", action="store_true", help="Print all flattened fields, not only likely usage fields")
    dump_parser.set_defaults(func=cmd_dump_mqtt)

    refresh_parser = subparsers.add_parser("refresh", help="Ask the printer to publish a fresh MQTT status report")
    refresh_parser.add_argument("--env-file", default=".env", help="Path to .env config")
    refresh_parser.set_defaults(func=cmd_refresh)

    cloud_login_parser = subparsers.add_parser("cloud-login", help="Log in to Bambu Cloud and save an access token")
    cloud_login_parser.add_argument("--env-file", default=".env", help="Path to .env config")
    cloud_login_parser.add_argument("--email", help="Bambu Cloud email. Defaults to BAMBU_CLOUD_EMAIL")
    cloud_login_parser.add_argument("--password", help="Bambu Cloud password. Defaults to BAMBU_CLOUD_PASSWORD")
    cloud_login_parser.add_argument("--code", help="Email verification code, if Bambu Cloud requires one")
    cloud_login_parser.add_argument("--no-save", action="store_true", help="Print the token instead of saving it to .env")
    cloud_login_parser.set_defaults(func=cmd_cloud_login)

    cloud_task_parser = subparsers.add_parser("cloud-task", help="Fetch one Bambu Cloud task by MQTT task_id")
    cloud_task_parser.add_argument("--env-file", default=".env", help="Path to .env config")
    cloud_task_parser.add_argument("--task-id", required=True)
    cloud_task_parser.set_defaults(func=cmd_cloud_task)

    cloud_sync_parser = subparsers.add_parser("cloud-sync-job", help="Fetch cloud usage for a tracked print job")
    cloud_sync_parser.add_argument("--env-file", default=".env", help="Path to .env config")
    cloud_sync_parser.add_argument("--job-id", type=int, required=True)
    cloud_sync_parser.add_argument("--task-id", help="Set the Bambu Cloud task_id before syncing")
    cloud_sync_parser.add_argument("--replace-existing", action="store_true")
    cloud_sync_parser.set_defaults(func=cmd_cloud_sync_job)

    ams_parser = subparsers.add_parser("ams", help="List AMS Lite slots last seen from the printer")
    ams_parser.set_defaults(func=cmd_ams)

    spools_parser = subparsers.add_parser("spools", help="Manage filament spools")
    spool_sub = spools_parser.add_subparsers(dest="spools_command")

    spool_add = spool_sub.add_parser("add", help="Add a spool and optionally assign it to an AMS Lite slot")
    spool_add.add_argument("--name", required=True)
    spool_add.add_argument("--vendor", default="")
    spool_add.add_argument("--material", default="")
    spool_add.add_argument("--color", default="")
    spool_add.add_argument("--initial-g", type=float)
    spool_add.add_argument("--remaining-g", type=float)
    spool_add.add_argument("--empty-spool-g", type=float)
    spool_add.add_argument("--slot", type=int, choices=[1, 2, 3, 4])
    spool_add.set_defaults(func=cmd_spool_add)

    spool_list = spool_sub.add_parser("list", help="List spools")
    spool_list.set_defaults(func=cmd_spool_list)

    spool_assign = spool_sub.add_parser("assign", help="Assign an existing spool to an AMS Lite slot")
    spool_assign.add_argument("--id", type=int, required=True)
    spool_assign.add_argument("--slot", type=int, choices=[1, 2, 3, 4], required=True)
    spool_assign.set_defaults(func=cmd_spool_assign)

    spool_clear = spool_sub.add_parser("clear-slot", help="Clear a spool assignment from an AMS Lite slot")
    spool_clear.add_argument("--slot", type=int, choices=[1, 2, 3, 4], required=True)
    spool_clear.set_defaults(func=cmd_spool_clear)

    spool_weight = spool_sub.add_parser("set-weight", help="Set known spool weights")
    spool_weight.add_argument("--id", type=int, required=True)
    spool_weight.add_argument("--initial-g", type=float)
    spool_weight.add_argument("--remaining-g", type=float)
    spool_weight.add_argument("--empty-spool-g", type=float)
    spool_weight.set_defaults(func=cmd_spool_set_weight)

    slot_weight = spool_sub.add_parser("set-slot-weight", help="Set weights for the spool currently in an AMS slot")
    slot_weight.add_argument("--slot", type=int, choices=[1, 2, 3, 4], required=True)
    slot_weight.add_argument("--initial-g", type=float)
    slot_weight.add_argument("--remaining-g", type=float)
    slot_weight.add_argument("--empty-spool-g", type=float)
    slot_weight.set_defaults(func=cmd_spool_set_slot_weight)

    import_parser = subparsers.add_parser("import", help="Import usage from a Bambu Studio .3mf or G-code file")
    import_parser.add_argument("path")
    import_parser.add_argument("--job-id", type=int, help="Attach usage to an existing tracked print job")
    import_parser.add_argument("--job-name", default="", help="Name used when creating a manual job")
    import_parser.add_argument("--mapping", help="Slicer filament to AMS slot mapping, for example 0:2,1:4")
    import_parser.add_argument("--plate-index", type=int, help="Zero-based plate index inside a .3mf")
    import_parser.add_argument("--slot", type=int, choices=[1, 2, 3, 4], help="Fallback AMS slot for single-filament imports")
    import_parser.add_argument("--serial", default="", help="Printer serial stored on manual jobs")
    import_parser.add_argument("--replace-existing", action="store_true", help="Replace existing usage rows for the job")
    import_parser.set_defaults(func=cmd_import)

    jobs_parser = subparsers.add_parser("jobs", help="List recent print jobs")
    jobs_parser.add_argument("--limit", type=int, default=20)
    jobs_parser.set_defaults(func=cmd_jobs)

    usage_parser = subparsers.add_parser("usage", help="List recent filament usage rows")
    usage_parser.add_argument("--limit", type=int, default=50)
    usage_parser.set_defaults(func=cmd_usage)

    usage_add_parser = subparsers.add_parser("add-usage", help="Record printer-screen grams for a finished single-spool job")
    usage_add_parser.add_argument("--job-id", type=int, required=True)
    usage_add_parser.add_argument("--grams", type=float, required=True)
    usage_add_parser.add_argument("--replace-existing", action="store_true")
    usage_add_parser.set_defaults(func=cmd_add_usage)

    sync_parser = subparsers.add_parser("sync-sheets", help="Sync current local data to Google Sheets")
    sync_parser.add_argument("--env-file", default=".env", help="Path to .env config")
    sync_parser.set_defaults(func=cmd_sync_sheets)

    script_parser = subparsers.add_parser("sheet-script", help="Print the Google Apps Script webhook code")
    script_parser.set_defaults(func=cmd_sheet_script)

    install_service_parser = subparsers.add_parser("install-service", help="Install the macOS background listener")
    install_service_parser.set_defaults(func=cmd_install_service)

    start_service_parser = subparsers.add_parser("start-service", help="Start the macOS background listener")
    start_service_parser.set_defaults(func=cmd_start_service)

    stop_service_parser = subparsers.add_parser("stop-service", help="Stop the macOS background listener")
    stop_service_parser.set_defaults(func=cmd_stop_service)

    status_service_parser = subparsers.add_parser("status-service", help="Show macOS background listener status")
    status_service_parser.set_defaults(func=cmd_status_service)

    logs_parser = subparsers.add_parser("logs", help="Show recent background listener logs")
    logs_parser.add_argument("--lines", type=int, default=80)
    logs_parser.set_defaults(func=cmd_logs)

    return parser


def cmd_init_db(args: argparse.Namespace) -> int:
    store = Store(resolve_db_path(args))
    print("Initialized %s" % store.path)
    return 0


def cmd_doctor(args: argparse.Namespace) -> int:
    print("Bambu Filament Tracker doctor")
    try:
        printer_config = PrinterConfig.from_env(args.env_file)
    except Exception as exc:
        print("config: error - %s" % exc)
        return 1

    sheets_config = SheetsConfig.from_env(args.env_file)
    cloud_config = CloudConfig.from_env(args.env_file)
    store = Store(Path(args.db) if args.db else printer_config.db_path)
    service = status_service()

    print("config: ok")
    print("printer_host: %s" % printer_config.host)
    print("printer_serial: %s" % mask_identifier(printer_config.serial))
    print("database: %s" % store.path)
    print("service: %s" % ("running" if service.ok else "not running"))
    print("google_sheets: %s" % ("enabled" if sheets_config.sync_on_event else "disabled"))
    if sheets_config.sync_on_event:
        print("google_sheets_interval_seconds: %s" % sheets_config.min_interval_seconds)
    print("bambu_cloud: %s" % ("enabled" if cloud_config.enabled and cloud_config.access_token else "disabled"))

    ams_rows = store.list_ams_slots()
    active_slots = [str(row["slot"]) for row in ams_rows if row["is_active"]]
    print("ams_slots_seen: %s" % (", ".join(str(row["slot"]) for row in ams_rows) or "none"))
    print("active_ams_slot: %s" % (", ".join(active_slots) or "none"))

    jobs = store.list_jobs(limit=1)
    if jobs:
        job = jobs[0]
        print(
            "latest_job: #%s %s %s slots=%s total_g=%s source=%s"
            % (
                job["id"],
                job["state"],
                job["subtask_name"] or job["gcode_file"] or "",
                job["observed_ams_slots"] or "",
                fmt_weight(job["total_used_g"]),
                job["usage_source"] or "",
            )
        )
    else:
        print("latest_job: none")

    usage = store.list_usage(limit=1)
    if usage:
        row = usage[0]
        print(
            "latest_usage: job=%s slot=%s grams=%s source=%s"
            % (row["print_job_id"], row["ams_slot"] or "", fmt_weight(row["used_g"]), row["source"])
        )
    else:
        print("latest_usage: none")
    return 0


def cmd_listen(args: argparse.Namespace) -> int:
    config = PrinterConfig.from_env(args.env_file)
    if args.db:
        config = PrinterConfig(
            host=config.host,
            serial=config.serial,
            access_code=config.access_code,
            username=config.username,
            mqtt_port=config.mqtt_port,
            db_path=Path(args.db),
            monitor_requests=config.monitor_requests,
            tls_insecure=config.tls_insecure,
        )
    store = Store(config.db_path)
    cloud_client = cloud_client_from_env(args.env_file, quiet=True)
    tracker = FilamentTracker(store, printer_serial=config.serial, cloud_client=cloud_client)
    sheets_config = SheetsConfig.from_env(args.env_file)
    syncer = SheetsSyncer(store, sheets_config) if sheets_config.sync_on_event else None
    listener = BambuMqttListener(config, tracker, on_event=print, syncer=syncer)
    print("Listening to %s for printer %s. Press Ctrl-C to stop." % (config.host, config.serial))
    if syncer is not None:
        print("Google Sheets sync is enabled")
    if cloud_client is not None:
        print("Bambu Cloud usage lookup is enabled")
    try:
        listener.run_forever()
    except KeyboardInterrupt:
        print("Stopped")
    return 0


def cmd_snapshot(args: argparse.Namespace) -> int:
    config = PrinterConfig.from_env(args.env_file)
    if args.db:
        config = PrinterConfig(
            host=config.host,
            serial=config.serial,
            access_code=config.access_code,
            username=config.username,
            mqtt_port=config.mqtt_port,
            db_path=Path(args.db),
            monitor_requests=config.monitor_requests,
            tls_insecure=config.tls_insecure,
        )
    snapshot = take_snapshot(config, seconds=args.seconds, max_messages=args.messages, wildcard=args.wildcard)
    print("connected=%s" % snapshot.connected)
    print("messages=%s" % snapshot.messages)
    for event in snapshot.events[-5:]:
        print("event=%s" % event)
    print("state=%s" % snapshot.state)
    print("job=%s" % (snapshot.job or "none reported"))
    print("percent=%s" % (snapshot.percent if snapshot.percent is not None else "unknown"))
    print("remaining_min=%s" % (snapshot.remaining_min if snapshot.remaining_min is not None else "unknown"))
    print("active_ams_slot=%s" % (snapshot.active_ams_slot if snapshot.active_ams_slot is not None else "unknown"))
    print("print_weight_g=%s" % (snapshot.print_weight_g if snapshot.print_weight_g is not None else "not reported"))
    return 0


def cmd_dump_mqtt(args: argparse.Namespace) -> int:
    config = PrinterConfig.from_env(args.env_file)
    if args.db:
        config = PrinterConfig(
            host=config.host,
            serial=config.serial,
            access_code=config.access_code,
            username=config.username,
            mqtt_port=config.mqtt_port,
            db_path=Path(args.db),
            monitor_requests=config.monitor_requests,
            tls_insecure=config.tls_insecure,
        )
    dump = dump_mqtt_report(
        config,
        seconds=args.seconds,
        max_messages=args.messages,
        output_path=Path(args.output),
    )
    print("connected=%s" % dump.connected)
    print("messages=%s" % dump.messages)
    print("print_reports=%s" % len(dump.reports))
    for event in dump.events:
        print("event=%s" % event)
    print("saved=%s" % args.output)
    print("saved_format=merged_report plus raw reports list")
    print("candidate_fields:")
    for event in finish_diagnostics(dump.report):
        print(event)
    print("flattened_fields:")
    rows = list(flatten_dict(dump.report))
    for key, value in rows:
        if args.all or is_interesting_mqtt_key(key):
            print("%s=%s" % (key, value))
    print("field_count=%s" % len(rows))
    return 0 if dump.connected else 1


def cmd_refresh(args: argparse.Namespace) -> int:
    config = PrinterConfig.from_env(args.env_file)
    result = request_status_refresh(config)
    print("connected=%s" % result.connected)
    print(result.message)
    return 0 if result.connected else 1


def cmd_cloud_login(args: argparse.Namespace) -> int:
    config = CloudConfig.from_env(args.env_file)
    email = args.email or config.email
    password = args.password or config.password
    client = BambuCloudClient(base_url=config.base_url)
    try:
        result = client.login(email=email, password=password, verification_code=args.code or "")
    except BambuCloudError as exc:
        print("Bambu Cloud login failed: %s" % exc)
        return 1

    print(result.message or result.status)
    if result.access_token:
        if args.no_save:
            print("BAMBU_CLOUD_ACCESS_TOKEN=%s" % result.access_token)
        else:
            updates = {
                "BAMBU_CLOUD_ACCESS_TOKEN": result.access_token,
                "BAMBU_CLOUD_ENABLED": "true",
            }
            if email:
                updates["BAMBU_CLOUD_EMAIL"] = email
            update_env_file(Path(args.env_file), updates)
            print("Saved Bambu Cloud token to %s" % args.env_file)
    return 0 if result.status in {"success", "needs_verification_code"} else 1


def cmd_cloud_task(args: argparse.Namespace) -> int:
    client = cloud_client_from_env(args.env_file, quiet=False)
    if client is None:
        return 1
    try:
        detail = client.get_task_detail_for_task_id(args.task_id)
    except BambuCloudError as exc:
        print("Bambu Cloud task lookup failed: %s" % exc)
        return 1
    if detail is None:
        print("No Bambu Cloud task detail found for task_id %s" % args.task_id)
        return 1
    print("task_id=%s" % detail.task_id)
    print("job_id=%s" % detail.job_id)
    print("title=%s" % detail.title)
    print("total_weight_g=%.2f" % detail.total_weight_g)
    print_table(
        ["index", "filament_id", "name", "material", "color", "weight_g"],
        [
            [
                index,
                filament.filament_id,
                filament.name,
                filament.material,
                filament.color_hex,
                fmt_weight(filament.weight_g),
            ]
            for index, filament in enumerate(detail.filaments)
        ],
    )
    return 0


def cmd_cloud_sync_job(args: argparse.Namespace) -> int:
    printer_config = PrinterConfig.from_env(args.env_file)
    client = cloud_client_from_env(args.env_file, quiet=False)
    if client is None:
        return 1
    store = Store(Path(args.db) if args.db else printer_config.db_path)
    if args.task_id:
        store.update_job_cloud_details(args.job_id, cloud_task_id=args.task_id)
    tracker = FilamentTracker(store, printer_serial=printer_config.serial, cloud_client=client)
    try:
        events = tracker.sync_cloud_usage(args.job_id, replace_existing=args.replace_existing)
    except ValueError as exc:
        print(str(exc))
        return 1
    for event in events:
        print(event)
    return 0


def cmd_spool_add(args: argparse.Namespace) -> int:
    store = Store(resolve_db_path(args))
    spool_id = store.add_spool(
        name=args.name,
        vendor=args.vendor,
        material=args.material,
        color_hex=args.color,
        initial_weight_g=args.initial_g,
        remaining_weight_g=args.remaining_g,
        empty_spool_weight_g=args.empty_spool_g,
        ams_slot=args.slot,
    )
    print("Added spool %s" % spool_id)
    return 0


def cmd_spool_list(args: argparse.Namespace) -> int:
    store = Store(resolve_db_path(args))
    rows = store.list_spools()
    print_table(
        ["id", "slot", "source", "name", "material", "color", "remaining_g"],
        [
            [
                row["id"],
                row["ams_slot"] or "",
                row["source"] or "",
                row["name"],
                row["material"] or "",
                row["color_hex"] or "",
                fmt_weight(row["remaining_weight_g"]),
            ]
            for row in rows
        ],
    )
    return 0


def cmd_ams(args: argparse.Namespace) -> int:
    store = Store(resolve_db_path(args))
    rows = store.list_ams_slots()
    print_table(
        ["slot", "active", "name", "material", "color", "tray_code", "spool_id", "remaining_g"],
        [
            [
                row["slot"],
                "yes" if row["is_active"] else "",
                row["filament_name"] or row["spool_name"] or "",
                row["material"] or "",
                row["color_hex"] or "",
                row["tray_info_idx"] or row["tray_id_name"] or "",
                row["spool_id"] or "",
                fmt_weight(row["remaining_weight_g"]),
            ]
            for row in rows
        ],
    )
    return 0


def cmd_spool_assign(args: argparse.Namespace) -> int:
    store = Store(resolve_db_path(args))
    store.assign_spool_to_slot(args.id, args.slot)
    print("Assigned spool %s to AMS slot %s" % (args.id, args.slot))
    return 0


def cmd_spool_clear(args: argparse.Namespace) -> int:
    store = Store(resolve_db_path(args))
    store.clear_slot(args.slot)
    print("Cleared AMS slot %s" % args.slot)
    return 0


def cmd_spool_set_weight(args: argparse.Namespace) -> int:
    if args.initial_g is None and args.remaining_g is None and args.empty_spool_g is None:
        print("Provide at least one of --initial-g, --remaining-g, or --empty-spool-g")
        return 1
    store = Store(resolve_db_path(args))
    store.set_spool_weights(
        args.id,
        initial_weight_g=args.initial_g,
        remaining_weight_g=args.remaining_g,
        empty_spool_weight_g=args.empty_spool_g,
    )
    print("Updated weights for spool %s" % args.id)
    return 0


def cmd_spool_set_slot_weight(args: argparse.Namespace) -> int:
    if args.initial_g is None and args.remaining_g is None and args.empty_spool_g is None:
        print("Provide at least one of --initial-g, --remaining-g, or --empty-spool-g")
        return 1
    store = Store(resolve_db_path(args))
    try:
        spool_id = store.set_slot_spool_weights(
            args.slot,
            initial_weight_g=args.initial_g,
            remaining_weight_g=args.remaining_g,
            empty_spool_weight_g=args.empty_spool_g,
        )
    except ValueError as exc:
        print(str(exc))
        return 1
    print("Updated weights for spool %s in AMS slot %s" % (spool_id, args.slot))
    return 0


def cmd_import(args: argparse.Namespace) -> int:
    store = Store(resolve_db_path(args))
    tracker = FilamentTracker(store, printer_serial=args.serial)
    mapping = parse_manual_mapping(args.mapping) if args.mapping else None
    job_id = tracker.import_usage(
        path=Path(args.path),
        job_id=args.job_id,
        job_name=args.job_name,
        manual_mapping=mapping,
        plate_index=args.plate_index,
        fallback_slot=args.slot,
        replace_existing=args.replace_existing,
    )
    print("Imported usage into job %s" % job_id)
    return 0


def cmd_jobs(args: argparse.Namespace) -> int:
    store = Store(resolve_db_path(args))
    rows = store.list_jobs(limit=args.limit)
    print_table(
        ["id", "state", "name", "slots", "total_g", "source", "started", "ended"],
        [
            [
                row["id"],
                row["state"],
                row["subtask_name"] or row["gcode_file"] or "",
                row["observed_ams_slots"] or "",
                fmt_weight(row["total_used_g"]),
                row["usage_source"] or "",
                row["started_at"] or "",
                row["ended_at"] or "",
            ]
            for row in rows
        ],
    )
    return 0


def cmd_usage(args: argparse.Namespace) -> int:
    store = Store(resolve_db_path(args))
    rows = store.list_usage(limit=args.limit)
    print_table(
        ["id", "job", "slot", "filament", "material", "color", "grams", "source", "ended"],
        [
            [
                row["id"],
                row["subtask_name"] or "",
                row["ams_slot"] or "",
                row["filament_name"] or row["spool_name"] or "",
                row["material"] or "",
                row["color_hex"] or "",
                fmt_weight(row["used_g"]),
                row["source"],
                row["ended_at"] or "",
            ]
            for row in rows
        ],
    )
    return 0


def cmd_add_usage(args: argparse.Namespace) -> int:
    store = Store(resolve_db_path(args))
    usage_id = store.add_manual_job_usage(args.job_id, args.grams, replace_existing=args.replace_existing)
    print("Recorded usage %s for job %s" % (usage_id, args.job_id))
    return 0


def cmd_sync_sheets(args: argparse.Namespace) -> int:
    printer_config = PrinterConfig.from_env(args.env_file)
    sheets_config = SheetsConfig.from_env(args.env_file)
    store = Store(printer_config.db_path if not args.db else Path(args.db))
    result = SheetsSyncer(store, sheets_config).sync(force=True)
    print(result.message)
    return 0 if result.ok else 1


def cmd_sheet_script(args: argparse.Namespace) -> int:
    print(APPS_SCRIPT)
    return 0


def cmd_install_service(args: argparse.Namespace) -> int:
    result = install_service()
    print(result.message)
    print("Logs:")
    for path in log_paths():
        print(path)
    return 0 if result.ok else 1


def cmd_start_service(args: argparse.Namespace) -> int:
    result = start_service()
    print(result.message)
    return 0 if result.ok else 1


def cmd_stop_service(args: argparse.Namespace) -> int:
    result = stop_service()
    print(result.message)
    return 0 if result.ok else 1


def cmd_status_service(args: argparse.Namespace) -> int:
    result = status_service()
    print(result.message)
    return 0 if result.ok else 1


def cmd_logs(args: argparse.Namespace) -> int:
    print(read_logs(lines=args.lines))
    return 0


def resolve_db_path(args: argparse.Namespace) -> Path:
    if args.db:
        return Path(args.db)
    dotenv = _load_dotenv(Path(".env"))
    return Path(os.environ.get("BAMBU_DB_PATH", dotenv.get("BAMBU_DB_PATH", str(DEFAULT_DB_PATH))))


def cloud_client_from_env(env_file: str, quiet: bool = False) -> Optional[BambuCloudClient]:
    config = CloudConfig.from_env(env_file)
    if not config.enabled:
        return None
    if not config.access_token:
        if not quiet:
            print("Bambu Cloud is enabled but BAMBU_CLOUD_ACCESS_TOKEN is missing")
        return None
    return BambuCloudClient(access_token=config.access_token, base_url=config.base_url)


def update_env_file(path: Path, updates: Dict[str, str]) -> None:
    existing_lines = path.read_text(encoding="utf-8").splitlines() if path.exists() else []
    seen = set()
    new_lines = []
    for raw_line in existing_lines:
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            new_lines.append(raw_line)
            continue
        key = stripped.split("=", 1)[0].strip()
        if key in updates:
            new_lines.append("%s=%s" % (key, updates[key]))
            seen.add(key)
        else:
            new_lines.append(raw_line)
    for key, value in updates.items():
        if key not in seen:
            new_lines.append("%s=%s" % (key, value))
    path.write_text("\n".join(new_lines) + "\n", encoding="utf-8")


def mask_identifier(value: str) -> str:
    if len(value) <= 6:
        return "*" * len(value)
    return "%s...%s" % (value[:4], value[-4:])


def print_table(headers: List[str], rows: Iterable[Iterable[Any]]) -> None:
    rows_list = [[str(value) for value in row] for row in rows]
    widths = [len(header) for header in headers]
    for row in rows_list:
        for index, value in enumerate(row):
            widths[index] = max(widths[index], len(value))
    header_line = "  ".join(header.ljust(widths[index]) for index, header in enumerate(headers))
    print(header_line)
    print("  ".join("-" * width for width in widths))
    for row in rows_list:
        print("  ".join(value.ljust(widths[index]) for index, value in enumerate(row)))


def fmt_weight(value: Any) -> str:
    if value is None or value == "":
        return ""
    return "%.2f" % float(value)


def is_interesting_mqtt_key(key: str) -> bool:
    normalized = key.lower()
    return any(
        token in normalized
        for token in (
            "filament",
            "weight",
            "meter",
            "length",
            "gcode",
            "subtask",
            "percent",
            "remaining",
            "state",
            "tray",
            "ams",
        )
    )


if __name__ == "__main__":
    raise SystemExit(main())
