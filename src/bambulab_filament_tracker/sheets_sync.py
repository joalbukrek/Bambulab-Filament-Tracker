from __future__ import annotations

import json
import socket
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover - Python < 3.9 fallback.
    ZoneInfo = None  # type: ignore

from .config import SheetsConfig
from .db import Store, utc_now
from .http import urlopen_with_certifi


APPS_SCRIPT = r"""const SECRET = 'CHANGE_ME_TO_A_RANDOM_SECRET';

const SHEETS = {
  ams_slots: 'AMS Slots',
  spools: 'Spools',
  print_jobs: 'Print Jobs',
  usage: 'Usage',
};

function doPost(e) {
  const payload = JSON.parse(e.postData.contents);
  if (payload.secret !== SECRET) {
    return jsonResponse({ ok: false, error: 'invalid secret' }, 403);
  }

  const spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
  for (const key of Object.keys(SHEETS)) {
    const data = payload.tables[key];
    if (!data) continue;
    writeTable(spreadsheet, SHEETS[key], data.headers, data.rows);
  }
  return jsonResponse({ ok: true, synced_at: new Date().toISOString() });
}

function writeTable(spreadsheet, sheetName, headers, rows) {
  const sheet = spreadsheet.getSheetByName(sheetName) || spreadsheet.insertSheet(sheetName);
  sheet.clearContents();
  const values = [headers].concat(rows || []);
  if (values.length === 0 || headers.length === 0) return;
  const colorColumn = headers.indexOf('Color') + 1;
  if (colorColumn > 0) {
    sheet.getRange(1, colorColumn, Math.max(values.length, 1), 1).setNumberFormat('@');
  }
  sheet.getRange(1, 1, values.length, headers.length).setValues(values);
  sheet.setFrozenRows(1);
  sheet.autoResizeColumns(1, headers.length);
}

function jsonResponse(body, statusCode) {
  return ContentService
    .createTextOutput(JSON.stringify(body))
    .setMimeType(ContentService.MimeType.JSON);
}
"""


@dataclass
class SyncResult:
    ok: bool
    message: str


class SheetsSyncer:
    def __init__(self, store: Store, config: SheetsConfig) -> None:
        self.store = store
        self.config = config
        self.last_sync_at = 0.0

    def sync(self, force: bool = False) -> SyncResult:
        if not self.config.webhook_url:
            return SyncResult(False, "GOOGLE_SHEETS_WEBHOOK_URL is not configured")
        if not self.config.secret:
            return SyncResult(False, "GOOGLE_SHEETS_SECRET is not configured")
        if not force and time.time() - self.last_sync_at < self.config.min_interval_seconds:
            return SyncResult(True, "Skipped Google Sheets sync; throttled")

        payload = {
            "secret": self.config.secret,
            "synced_at": utc_now(),
            "tables": build_sheets_payload(self.store),
        }
        data = json.dumps(payload).encode("utf-8")
        request = urllib.request.Request(
            self.config.webhook_url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urlopen_with_certifi(request, timeout=20) as response:
                response_body = response.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as exc:
            response_body = exc.read().decode("utf-8", errors="replace")
            return SyncResult(False, "Google Sheets sync failed: HTTP %s %s" % (exc.code, response_body))
        except (urllib.error.URLError, TimeoutError, socket.timeout) as exc:
            return SyncResult(False, "Google Sheets sync failed: %s" % exc)

        self.last_sync_at = time.time()
        return SyncResult(True, "Google Sheets synced: %s" % response_body)


def build_sheets_payload(store: Store) -> Dict[str, Dict[str, List[List[Any]]]]:
    return {
        "ams_slots": table(
            [
                "Slot",
                "Active",
                "Filament",
                "Material",
                "Color",
                "Tray Code",
                "Spool ID",
                "Remaining g",
                "Last Seen",
            ],
            [
                [
                    row["slot"],
                    "yes" if row["is_active"] else "",
                    row["filament_name"] or row["spool_name"] or "",
                    row["material"] or "",
                    hex_text(row["color_hex"]),
                    row["tray_info_idx"] or row["tray_id_name"] or "",
                    row["spool_id"] or "",
                    value_or_blank(row["remaining_weight_g"]),
                    turkey_time(row["last_seen_at"]),
                ]
                for row in store.list_ams_slots()
            ],
        ),
        "spools": table(
            [
                "ID",
                "Slot",
                "Source",
                "Name",
                "Vendor",
                "Material",
                "Color",
                "Initial g",
                "Remaining g",
                "Empty Spool g",
                "Tray Code",
                "Updated",
            ],
            [
                [
                    row["id"],
                    row["ams_slot"] or "",
                    row["source"] or "",
                    row["name"] or "",
                    row["vendor"] or "",
                    row["material"] or "",
                    hex_text(row["color_hex"]),
                    value_or_blank(row["initial_weight_g"]),
                    value_or_blank(row["remaining_weight_g"]),
                    value_or_blank(row["empty_spool_weight_g"]),
                    row["tray_info_idx"] or row["tray_id_name"] or "",
                    turkey_time(row["updated_at"]),
                ]
                for row in store.list_spools()
            ],
        ),
        "print_jobs": table(
            ["ID", "State", "Name", "Observed Slots", "Total g", "Source", "Started", "Ended"],
            [
                [
                    row["id"],
                    row["state"] or "",
                    row["subtask_name"] or row["gcode_file"] or "",
                    row["observed_ams_slots"] or "",
                    value_or_blank(row["total_used_g"]),
                    row["usage_source"] or "",
                    turkey_time(row["started_at"]),
                    turkey_time(row["ended_at"]),
                ]
                for row in store.list_jobs(limit=500)
            ],
        ),
        "usage": table(
            ["ID", "Job", "Slot", "Filament", "Material", "Color", "Used g", "Source", "Ended"],
            [
                [
                    row["id"],
                    row["subtask_name"] or "",
                    row["ams_slot"] or "",
                    row["filament_name"] or row["spool_name"] or "",
                    row["material"] or "",
                    hex_text(row["color_hex"]),
                    value_or_blank(row["used_g"]),
                    row["source"] or "",
                    turkey_time(row["ended_at"]),
                ]
                for row in store.list_usage(limit=1000)
            ],
        ),
    }


def table(headers: List[str], rows: List[List[Any]]) -> Dict[str, List[List[Any]]]:
    return {"headers": headers, "rows": rows}


def value_or_blank(value: Optional[Any]) -> Any:
    if value is None:
        return ""
    return value


def hex_text(value: Optional[Any]) -> str:
    if value is None:
        return ""
    text = str(value).strip().upper()
    if not text:
        return ""
    return "'" + text.zfill(6)


def turkey_time(value: Optional[Any]) -> str:
    if not value:
        return ""
    text = str(value)
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return text
    if ZoneInfo is None:
        return text
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=ZoneInfo("UTC"))
    local = parsed.astimezone(ZoneInfo("Europe/Istanbul"))
    return local.strftime("%Y-%m-%d %H:%M:%S")
