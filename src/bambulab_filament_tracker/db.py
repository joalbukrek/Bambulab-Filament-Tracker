from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class Store:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.init()

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(str(self.path))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def init(self) -> None:
        with self.connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS spools (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    vendor TEXT,
                    material TEXT,
                    color_hex TEXT,
                    source TEXT DEFAULT 'manual',
                    printer_tray_key TEXT UNIQUE,
                    tray_uuid TEXT,
                    tag_uid TEXT,
                    tray_info_idx TEXT,
                    tray_id_name TEXT,
                    tray_sub_brands TEXT,
                    initial_weight_g REAL,
                    remaining_weight_g REAL,
                    empty_spool_weight_g REAL,
                    ams_slot INTEGER UNIQUE,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS ams_slots (
                    slot INTEGER PRIMARY KEY,
                    spool_id INTEGER REFERENCES spools(id) ON DELETE SET NULL,
                    filament_name TEXT,
                    material TEXT,
                    color_hex TEXT,
                    tray_uuid TEXT,
                    tag_uid TEXT,
                    tray_info_idx TEXT,
                    tray_id_name TEXT,
                    tray_sub_brands TEXT,
                    tray_weight_g REAL,
                    remain_percent REAL,
                    is_active INTEGER DEFAULT 0,
                    last_seen_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS print_jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    printer_serial TEXT,
                    job_key TEXT UNIQUE,
                    subtask_name TEXT,
                    gcode_file TEXT,
                    plate_index INTEGER,
                    state TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    ended_at TEXT,
                    raw_ams_mapping TEXT,
                    observed_ams_slots TEXT,
                    cloud_task_id TEXT,
                    cloud_job_id TEXT,
                    cloud_payload TEXT,
                    completion_percent REAL,
                    usage_source TEXT,
                    total_used_g REAL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS print_job_ams_slots (
                    print_job_id INTEGER NOT NULL REFERENCES print_jobs(id) ON DELETE CASCADE,
                    ams_slot INTEGER NOT NULL,
                    spool_id INTEGER REFERENCES spools(id) ON DELETE SET NULL,
                    filament_name TEXT,
                    material TEXT,
                    color_hex TEXT,
                    tray_uuid TEXT,
                    tray_info_idx TEXT,
                    first_seen_at TEXT NOT NULL,
                    last_seen_at TEXT NOT NULL,
                    PRIMARY KEY (print_job_id, ams_slot)
                );

                CREATE TABLE IF NOT EXISTS filament_usage (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    print_job_id INTEGER NOT NULL REFERENCES print_jobs(id) ON DELETE CASCADE,
                    spool_id INTEGER REFERENCES spools(id) ON DELETE SET NULL,
                    ams_slot INTEGER,
                    slicer_filament_index INTEGER,
                    filament_name TEXT,
                    material TEXT,
                    color_hex TEXT,
                    used_g REAL NOT NULL,
                    source TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                """
            )
            self._migrate(conn)

    def _migrate(self, conn: sqlite3.Connection) -> None:
        self._ensure_columns(
            conn,
            "spools",
            {
                "source": "TEXT DEFAULT 'manual'",
                "printer_tray_key": "TEXT",
                "tray_uuid": "TEXT",
                "tag_uid": "TEXT",
                "tray_info_idx": "TEXT",
                "tray_id_name": "TEXT",
                "tray_sub_brands": "TEXT",
            },
        )
        self._ensure_columns(
            conn,
            "ams_slots",
            {
                "filament_name": "TEXT",
                "tag_uid": "TEXT",
                "tray_id_name": "TEXT",
                "tray_sub_brands": "TEXT",
                "is_active": "INTEGER DEFAULT 0",
            },
        )
        self._ensure_columns(
            conn,
            "print_jobs",
            {
                "observed_ams_slots": "TEXT",
                "cloud_task_id": "TEXT",
                "cloud_job_id": "TEXT",
                "cloud_payload": "TEXT",
                "completion_percent": "REAL",
            },
        )
        self._ensure_columns(conn, "filament_usage", {"filament_name": "TEXT"})
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_spools_printer_tray_key ON spools(printer_tray_key)"
        )

    def _ensure_columns(self, conn: sqlite3.Connection, table: str, columns: Dict[str, str]) -> None:
        existing = {row["name"] for row in conn.execute("PRAGMA table_info(%s)" % table).fetchall()}
        for name, ddl in columns.items():
            if name not in existing:
                conn.execute("ALTER TABLE %s ADD COLUMN %s %s" % (table, name, ddl))

    def add_spool(
        self,
        name: str,
        vendor: str = "",
        material: str = "",
        color_hex: str = "",
        initial_weight_g: Optional[float] = None,
        remaining_weight_g: Optional[float] = None,
        empty_spool_weight_g: Optional[float] = None,
        ams_slot: Optional[int] = None,
    ) -> int:
        now = utc_now()
        with self.connect() as conn:
            if ams_slot is not None:
                self._clear_slot_assignment(conn, ams_slot)
            cursor = conn.execute(
                """
                INSERT INTO spools (
                    name, vendor, material, color_hex, source, initial_weight_g,
                    remaining_weight_g, empty_spool_weight_g, ams_slot,
                    created_at, updated_at
                )
                VALUES (?, ?, ?, ?, 'manual', ?, ?, ?, ?, ?, ?)
                """,
                (
                    name,
                    vendor,
                    material,
                    normalize_color(color_hex),
                    initial_weight_g,
                    remaining_weight_g if remaining_weight_g is not None else initial_weight_g,
                    empty_spool_weight_g,
                    ams_slot,
                    now,
                    now,
                ),
            )
            spool_id = int(cursor.lastrowid)
            if ams_slot is not None:
                conn.execute(
                    """
                    INSERT INTO ams_slots (slot, spool_id, last_seen_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT(slot) DO UPDATE SET spool_id=excluded.spool_id, last_seen_at=excluded.last_seen_at
                    """,
                    (ams_slot, spool_id, now),
                )
            return spool_id

    def assign_spool_to_slot(self, spool_id: int, slot: int) -> None:
        now = utc_now()
        with self.connect() as conn:
            current = conn.execute("SELECT ams_slot FROM spools WHERE id=?", (spool_id,)).fetchone()
            if current and current["ams_slot"] is not None:
                conn.execute(
                    "UPDATE ams_slots SET spool_id=NULL, last_seen_at=? WHERE slot=?",
                    (now, int(current["ams_slot"])),
                )
            self._clear_slot_assignment(conn, slot)
            conn.execute("UPDATE spools SET ams_slot=NULL, updated_at=? WHERE id=?", (now, spool_id))
            conn.execute("UPDATE spools SET ams_slot=?, updated_at=? WHERE id=?", (slot, now, spool_id))
            conn.execute(
                """
                INSERT INTO ams_slots (slot, spool_id, last_seen_at)
                VALUES (?, ?, ?)
                ON CONFLICT(slot) DO UPDATE SET spool_id=excluded.spool_id, last_seen_at=excluded.last_seen_at
                """,
                (slot, spool_id, now),
            )

    def clear_slot(self, slot: int) -> None:
        now = utc_now()
        with self.connect() as conn:
            self._clear_slot_assignment(conn, slot)
            conn.execute(
                """
                INSERT INTO ams_slots (slot, spool_id, last_seen_at)
                VALUES (?, NULL, ?)
                ON CONFLICT(slot) DO UPDATE SET spool_id=NULL, last_seen_at=excluded.last_seen_at
                """,
                (slot, now),
            )

    def _clear_slot_assignment(self, conn: sqlite3.Connection, slot: int) -> None:
        now = utc_now()
        conn.execute("UPDATE spools SET ams_slot=NULL, updated_at=? WHERE ams_slot=?", (now, slot))
        conn.execute("UPDATE ams_slots SET spool_id=NULL, last_seen_at=? WHERE slot=?", (now, slot))

    def upsert_ams_slot(self, slot: int, tray: Dict[str, Any], is_active: bool = False) -> Optional[int]:
        now = utc_now()
        with self.connect() as conn:
            if is_active:
                conn.execute("UPDATE ams_slots SET is_active=0")
            metadata = tray_metadata(tray)
            conn.execute(
                """
                INSERT INTO ams_slots (
                    slot, spool_id, filament_name, material, color_hex, tray_uuid,
                    tag_uid, tray_info_idx, tray_id_name, tray_sub_brands,
                    tray_weight_g, remain_percent, is_active, last_seen_at
                )
                VALUES (?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(slot) DO UPDATE SET
                    filament_name=excluded.filament_name,
                    material=excluded.material,
                    color_hex=excluded.color_hex,
                    tray_uuid=excluded.tray_uuid,
                    tag_uid=excluded.tag_uid,
                    tray_info_idx=excluded.tray_info_idx,
                    tray_id_name=excluded.tray_id_name,
                    tray_sub_brands=excluded.tray_sub_brands,
                    tray_weight_g=excluded.tray_weight_g,
                    remain_percent=excluded.remain_percent,
                    is_active=excluded.is_active,
                    last_seen_at=excluded.last_seen_at
                """,
                (
                    slot,
                    metadata["filament_name"],
                    metadata["material"],
                    metadata["color_hex"],
                    metadata["tray_uuid"],
                    metadata["tag_uid"],
                    metadata["tray_info_idx"],
                    metadata["tray_id_name"],
                    metadata["tray_sub_brands"],
                    metadata["tray_weight_g"],
                    metadata["remain_percent"],
                    1 if is_active else 0,
                    now,
                ),
            )
            spool_id = self._sync_spool_from_ams_slot(conn, slot, metadata, now)
            conn.execute("UPDATE ams_slots SET spool_id=? WHERE slot=?", (spool_id, slot))
            return spool_id

    def _sync_spool_from_ams_slot(
        self,
        conn: sqlite3.Connection,
        slot: int,
        metadata: Dict[str, Any],
        now: str,
    ) -> Optional[int]:
        if not has_filament_metadata(metadata):
            self._clear_slot_assignment(conn, slot)
            return None

        key = printer_tray_key(slot, metadata)
        existing = conn.execute("SELECT * FROM spools WHERE printer_tray_key=?", (key,)).fetchone()
        if existing is None:
            slot_existing = conn.execute("SELECT * FROM spools WHERE ams_slot=?", (slot,)).fetchone()
            if slot_existing is not None and not slot_existing["printer_tray_key"]:
                existing = slot_existing

        if existing is not None:
            spool_id = int(existing["id"])
            source = existing["source"] or "printer"
            self._clear_slot_assignment(conn, slot)
            conn.execute("UPDATE ams_slots SET spool_id=NULL, last_seen_at=? WHERE spool_id=?", (now, spool_id))
            conn.execute(
                """
                UPDATE spools
                SET name=?, material=?, color_hex=?, source=?,
                    printer_tray_key=COALESCE(printer_tray_key, ?),
                    tray_uuid=?, tag_uid=?, tray_info_idx=?,
                    tray_id_name=?, tray_sub_brands=?,
                    ams_slot=?, updated_at=?
                WHERE id=?
                """,
                (
                    metadata["filament_name"],
                    metadata["material"],
                    metadata["color_hex"],
                    source,
                    key,
                    metadata["tray_uuid"],
                    metadata["tag_uid"],
                    metadata["tray_info_idx"],
                    metadata["tray_id_name"],
                    metadata["tray_sub_brands"],
                    slot,
                    now,
                    spool_id,
                ),
            )
            return spool_id

        self._clear_slot_assignment(conn, slot)
        cursor = conn.execute(
            """
            INSERT INTO spools (
                name, vendor, material, color_hex, source, printer_tray_key,
                tray_uuid, tag_uid, tray_info_idx, tray_id_name, tray_sub_brands,
                initial_weight_g, remaining_weight_g, empty_spool_weight_g,
                ams_slot, created_at, updated_at
            )
            VALUES (?, '', ?, ?, 'printer', ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?)
            """,
            (
                metadata["filament_name"],
                metadata["material"],
                metadata["color_hex"],
                key,
                metadata["tray_uuid"],
                metadata["tag_uid"],
                metadata["tray_info_idx"],
                metadata["tray_id_name"],
                metadata["tray_sub_brands"],
                metadata["tray_weight_g"],
                metadata["tray_weight_g"],
                slot,
                now,
                now,
            ),
        )
        return int(cursor.lastrowid)

    def set_spool_weights(
        self,
        spool_id: int,
        initial_weight_g: Optional[float] = None,
        remaining_weight_g: Optional[float] = None,
        empty_spool_weight_g: Optional[float] = None,
    ) -> None:
        now = utc_now()
        updates = ["updated_at=?"]
        values: List[Any] = [now]
        if initial_weight_g is not None:
            updates.append("initial_weight_g=?")
            values.append(initial_weight_g)
        if remaining_weight_g is not None:
            updates.append("remaining_weight_g=?")
            values.append(remaining_weight_g)
        if empty_spool_weight_g is not None:
            updates.append("empty_spool_weight_g=?")
            values.append(empty_spool_weight_g)
        values.append(spool_id)
        with self.connect() as conn:
            conn.execute("UPDATE spools SET %s WHERE id=?" % ", ".join(updates), values)

    def set_slot_spool_weights(
        self,
        slot: int,
        initial_weight_g: Optional[float] = None,
        remaining_weight_g: Optional[float] = None,
        empty_spool_weight_g: Optional[float] = None,
    ) -> int:
        spool = self.active_spool_for_slot(slot)
        if spool is None:
            raise ValueError("No spool is assigned to AMS slot %s" % slot)
        spool_id = int(spool["id"])
        self.set_spool_weights(
            spool_id,
            initial_weight_g=initial_weight_g,
            remaining_weight_g=remaining_weight_g,
            empty_spool_weight_g=empty_spool_weight_g,
        )
        return spool_id

    def active_spool_for_slot(self, slot: Optional[int]) -> Optional[sqlite3.Row]:
        if slot is None:
            return None
        with self.connect() as conn:
            return conn.execute("SELECT * FROM spools WHERE ams_slot=?", (slot,)).fetchone()

    def start_job(
        self,
        printer_serial: str,
        job_key: str,
        subtask_name: str = "",
        gcode_file: str = "",
        plate_index: Optional[int] = None,
        ams_mapping: Optional[Sequence[int]] = None,
        cloud_task_id: Optional[str] = None,
    ) -> int:
        now = utc_now()
        with self.connect() as conn:
            existing = conn.execute("SELECT id FROM print_jobs WHERE job_key=?", (job_key,)).fetchone()
            if existing:
                conn.execute(
                    """
                    UPDATE print_jobs SET
                        state='running',
                        subtask_name=COALESCE(NULLIF(?, ''), subtask_name),
                        gcode_file=COALESCE(NULLIF(?, ''), gcode_file),
                        plate_index=COALESCE(?, plate_index),
                        raw_ams_mapping=COALESCE(?, raw_ams_mapping),
                        cloud_task_id=COALESCE(?, cloud_task_id),
                        updated_at=?
                    WHERE id=?
                    """,
                    (
                        subtask_name,
                        gcode_file,
                        plate_index,
                        json_dumps_or_none(ams_mapping),
                        cloud_task_id,
                        now,
                        int(existing["id"]),
                    ),
                )
                return int(existing["id"])

            cursor = conn.execute(
                """
                INSERT INTO print_jobs (
                    printer_serial, job_key, subtask_name, gcode_file,
                    plate_index, state, started_at, raw_ams_mapping, cloud_task_id,
                    created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, 'running', ?, ?, ?, ?, ?)
                """,
                (
                    printer_serial,
                    job_key,
                    subtask_name,
                    gcode_file,
                    plate_index,
                    now,
                    json_dumps_or_none(ams_mapping),
                    cloud_task_id,
                    now,
                    now,
                ),
            )
            return int(cursor.lastrowid)

    def latest_running_job(self, printer_serial: str) -> Optional[sqlite3.Row]:
        with self.connect() as conn:
            return conn.execute(
                """
                SELECT * FROM print_jobs
                WHERE printer_serial=? AND state='running'
                ORDER BY id DESC
                LIMIT 1
                """,
                (printer_serial,),
            ).fetchone()

    def update_job_mapping(self, job_id: int, ams_mapping: Sequence[int], plate_index: Optional[int] = None) -> None:
        now = utc_now()
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE print_jobs
                SET raw_ams_mapping=?, plate_index=COALESCE(?, plate_index), updated_at=?
                WHERE id=?
                """,
                (json.dumps(list(ams_mapping)), plate_index, now, job_id),
            )

    def record_job_ams_slot(self, print_job_id: int, ams_slot: int) -> None:
        now = utc_now()
        with self.connect() as conn:
            slot_row = conn.execute("SELECT * FROM ams_slots WHERE slot=?", (ams_slot,)).fetchone()
            spool_id = int(slot_row["spool_id"]) if slot_row and slot_row["spool_id"] is not None else None
            conn.execute(
                """
                INSERT INTO print_job_ams_slots (
                    print_job_id, ams_slot, spool_id, filament_name, material,
                    color_hex, tray_uuid, tray_info_idx, first_seen_at, last_seen_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(print_job_id, ams_slot) DO UPDATE SET
                    spool_id=excluded.spool_id,
                    filament_name=excluded.filament_name,
                    material=excluded.material,
                    color_hex=excluded.color_hex,
                    tray_uuid=excluded.tray_uuid,
                    tray_info_idx=excluded.tray_info_idx,
                    last_seen_at=excluded.last_seen_at
                """,
                (
                    print_job_id,
                    ams_slot,
                    spool_id,
                    slot_row["filament_name"] if slot_row else "",
                    slot_row["material"] if slot_row else "",
                    slot_row["color_hex"] if slot_row else "",
                    slot_row["tray_uuid"] if slot_row else "",
                    slot_row["tray_info_idx"] if slot_row else "",
                    now,
                    now,
                ),
            )
            slots = [
                int(row["ams_slot"])
                for row in conn.execute(
                    "SELECT ams_slot FROM print_job_ams_slots WHERE print_job_id=? ORDER BY ams_slot",
                    (print_job_id,),
                )
            ]
            conn.execute(
                "UPDATE print_jobs SET observed_ams_slots=?, updated_at=? WHERE id=?",
                (json.dumps(slots), now, print_job_id),
            )

    def finish_job(
        self,
        job_id: int,
        state: str,
        total_used_g: Optional[float] = None,
        usage_source: Optional[str] = None,
        completion_percent: Optional[float] = None,
    ) -> None:
        now = utc_now()
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE print_jobs
                SET state=?, ended_at=?, total_used_g=COALESCE(?, total_used_g),
                    usage_source=COALESCE(?, usage_source),
                    completion_percent=COALESCE(?, completion_percent),
                    updated_at=?
                WHERE id=?
                """,
                (state, now, total_used_g, usage_source, completion_percent, now, job_id),
            )

    def update_job_cloud_details(
        self,
        job_id: int,
        cloud_task_id: Optional[str] = None,
        cloud_job_id: Optional[str] = None,
        cloud_payload: Optional[Dict[str, Any]] = None,
    ) -> None:
        now = utc_now()
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE print_jobs
                SET cloud_task_id=COALESCE(?, cloud_task_id),
                    cloud_job_id=COALESCE(?, cloud_job_id),
                    cloud_payload=COALESCE(?, cloud_payload),
                    updated_at=?
                WHERE id=?
                """,
                (
                    cloud_task_id,
                    cloud_job_id,
                    json.dumps(cloud_payload) if cloud_payload is not None else None,
                    now,
                    job_id,
                ),
            )

    def add_usage(
        self,
        print_job_id: int,
        used_g: float,
        source: str,
        spool_id: Optional[int] = None,
        ams_slot: Optional[int] = None,
        slicer_filament_index: Optional[int] = None,
        filament_name: str = "",
        material: str = "",
        color_hex: str = "",
    ) -> int:
        if used_g <= 0:
            raise ValueError("used_g must be greater than zero")

        now = utc_now()
        with self.connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO filament_usage (
                    print_job_id, spool_id, ams_slot, slicer_filament_index,
                    filament_name, material, color_hex, used_g, source, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    print_job_id,
                    spool_id,
                    ams_slot,
                    slicer_filament_index,
                    filament_name,
                    material,
                    normalize_color(color_hex),
                    used_g,
                    source,
                    now,
                ),
            )
            if spool_id is not None:
                conn.execute(
                    """
                    UPDATE spools
                    SET remaining_weight_g = CASE
                            WHEN remaining_weight_g IS NULL THEN NULL
                            ELSE remaining_weight_g - ?
                        END,
                        updated_at=?
                    WHERE id=?
                    """,
                    (used_g, now, spool_id),
            )
            return int(cursor.lastrowid)

    def job_has_usage(self, print_job_id: int) -> bool:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM filament_usage WHERE print_job_id=? LIMIT 1",
                (print_job_id,),
            ).fetchone()
            return row is not None

    def replace_job_usage(self, print_job_id: int) -> None:
        now = utc_now()
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT spool_id, used_g FROM filament_usage WHERE print_job_id=?",
                (print_job_id,),
            ).fetchall()
            for row in rows:
                if row["spool_id"] is None:
                    continue
                conn.execute(
                    """
                    UPDATE spools
                    SET remaining_weight_g = CASE
                            WHEN remaining_weight_g IS NULL THEN NULL
                            ELSE remaining_weight_g + ?
                        END,
                        updated_at=?
                    WHERE id=?
                    """,
                    (float(row["used_g"]), now, int(row["spool_id"])),
                )
            conn.execute("DELETE FROM filament_usage WHERE print_job_id=?", (print_job_id,))

    def add_manual_job_usage(self, print_job_id: int, used_g: float, replace_existing: bool = False) -> int:
        if replace_existing and self.job_has_usage(print_job_id):
            self.replace_job_usage(print_job_id)
        elif self.job_has_usage(print_job_id):
            raise ValueError("Job %s already has usage rows" % print_job_id)

        observed = self.list_job_ams_slots(print_job_id)
        if len(observed) != 1:
            raise ValueError(
                "Job %s has %s observed AMS slots; manual usage requires exactly one observed slot"
                % (print_job_id, len(observed))
            )
        row = observed[0]
        usage_id = self.add_usage(
            print_job_id=print_job_id,
            spool_id=int(row["spool_id"]) if row["spool_id"] is not None else None,
            ams_slot=int(row["ams_slot"]),
            slicer_filament_index=0,
            filament_name=row["filament_name"] or row["spool_name"] or "",
            material=row["material"] or "",
            color_hex=row["color_hex"] or "",
            used_g=used_g,
            source="manual_printer_screen",
        )
        self.finish_job(print_job_id, "finished", total_used_g=used_g, usage_source="manual_printer_screen")
        return usage_id

    def list_spools(self) -> List[sqlite3.Row]:
        with self.connect() as conn:
            return list(conn.execute("SELECT * FROM spools ORDER BY COALESCE(ams_slot, 999), id"))

    def list_ams_slots(self) -> List[sqlite3.Row]:
        with self.connect() as conn:
            return list(
                conn.execute(
                    """
                    SELECT a.*, s.name AS spool_name, s.remaining_weight_g
                    FROM ams_slots a
                    LEFT JOIN spools s ON s.id = a.spool_id
                    ORDER BY a.slot
                    """
                )
            )

    def list_jobs(self, limit: int = 20) -> List[sqlite3.Row]:
        with self.connect() as conn:
            return list(
                conn.execute(
                    """
                    SELECT * FROM print_jobs
                    ORDER BY id DESC
                    LIMIT ?
                    """,
                    (limit,),
                )
            )

    def list_usage(self, limit: int = 50) -> List[sqlite3.Row]:
        with self.connect() as conn:
            return list(
                conn.execute(
                    """
                    SELECT
                        u.*,
                        j.subtask_name,
                        j.ended_at,
                        s.name AS spool_name
                    FROM filament_usage u
                    JOIN print_jobs j ON j.id = u.print_job_id
                    LEFT JOIN spools s ON s.id = u.spool_id
                    ORDER BY u.id DESC
                    LIMIT ?
                    """,
                    (limit,),
                )
            )

    def get_job(self, job_id: int) -> Optional[sqlite3.Row]:
        with self.connect() as conn:
            return conn.execute("SELECT * FROM print_jobs WHERE id=?", (job_id,)).fetchone()

    def list_job_ams_slots(self, print_job_id: int) -> List[sqlite3.Row]:
        with self.connect() as conn:
            return list(
                conn.execute(
                    """
                    SELECT p.*, s.name AS spool_name
                    FROM print_job_ams_slots p
                    LEFT JOIN spools s ON s.id = p.spool_id
                    WHERE p.print_job_id=?
                    ORDER BY p.ams_slot
                    """,
                    (print_job_id,),
                )
            )


def json_dumps_or_none(value: Optional[Sequence[int]]) -> Optional[str]:
    if value is None:
        return None
    return json.dumps(list(value))


def number_or_none(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def text_or_none(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def tray_metadata(tray: Dict[str, Any]) -> Dict[str, Any]:
    material = text_or_none(tray.get("tray_type")) or ""
    color_hex = normalize_color(text_or_none(tray.get("tray_color")) or text_or_none(tray.get("color")) or "")
    tray_sub_brands = text_or_none(tray.get("tray_sub_brands")) or ""
    tray_id_name = text_or_none(tray.get("tray_id_name")) or ""
    tray_info_idx = text_or_none(tray.get("tray_info_idx")) or ""
    tray_uuid = normalize_identifier(text_or_none(tray.get("tray_uuid")) or "")
    tag_uid = normalize_identifier(text_or_none(tray.get("tag_uid")) or "")
    filament_name = (
        tray_sub_brands
        or tray_id_name
        or tray_info_idx
        or material
        or ("AMS slot filament" if color_hex else "")
    )
    return {
        "filament_name": filament_name,
        "material": material,
        "color_hex": color_hex,
        "tray_uuid": tray_uuid,
        "tag_uid": tag_uid,
        "tray_info_idx": tray_info_idx,
        "tray_id_name": tray_id_name,
        "tray_sub_brands": tray_sub_brands,
        "tray_weight_g": number_or_none(tray.get("tray_weight")),
        "remain_percent": number_or_none(tray.get("remain")),
    }


def has_filament_metadata(metadata: Dict[str, Any]) -> bool:
    return any(
        metadata.get(key)
        for key in ("filament_name", "material", "color_hex", "tray_uuid", "tag_uid", "tray_info_idx")
    )


def printer_tray_key(slot: int, metadata: Dict[str, Any]) -> str:
    if metadata.get("tray_uuid"):
        return "uuid:%s" % metadata["tray_uuid"]
    if metadata.get("tag_uid"):
        return "tag:%s" % metadata["tag_uid"]
    parts = [
        str(slot),
        str(metadata.get("tray_info_idx") or ""),
        str(metadata.get("filament_name") or ""),
        str(metadata.get("material") or ""),
        str(metadata.get("color_hex") or ""),
    ]
    return "slot-meta:" + ":".join(parts)


def normalize_identifier(value: str) -> str:
    text = value.strip()
    if not text or set(text) <= {"0"}:
        return ""
    return text


def normalize_color(color: str) -> str:
    text = (color or "").strip().upper()
    if text.startswith("#"):
        text = text[1:]
    if len(text) == 8 and text.endswith("FF"):
        text = text[:6]
    return text


def rows_to_dicts(rows: Iterable[sqlite3.Row]) -> List[Dict[str, Any]]:
    return [dict(row) for row in rows]
