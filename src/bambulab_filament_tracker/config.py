from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional

from .cloud import DEFAULT_BASE_URL


def _load_dotenv(path: Path) -> Dict[str, str]:
    values: Dict[str, str] = {}
    if not path.exists():
        return values

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def _env(name: str, dotenv: Dict[str, str], default: Optional[str] = None) -> Optional[str]:
    return os.environ.get(name, dotenv.get(name, default))


def _env_bool(name: str, dotenv: Dict[str, str], default: bool = False) -> bool:
    value = _env(name, dotenv)
    if value is None:
        return default
    return value.lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class PrinterConfig:
    host: str
    serial: str
    access_code: str
    username: str = "bblp"
    mqtt_port: int = 8883
    db_path: Path = Path("./data/filament.sqlite3")
    monitor_requests: bool = False
    tls_insecure: bool = True

    @classmethod
    def from_env(cls, env_file: str = ".env") -> "PrinterConfig":
        dotenv = _load_dotenv(Path(env_file))
        host = _env("BAMBU_HOST", dotenv)
        serial = _env("BAMBU_SERIAL", dotenv)
        access_code = _env("BAMBU_ACCESS_CODE", dotenv)

        missing = [
            name
            for name, value in {
                "BAMBU_HOST": host,
                "BAMBU_SERIAL": serial,
                "BAMBU_ACCESS_CODE": access_code,
            }.items()
            if not value
        ]
        if missing:
            raise ValueError("Missing required config: " + ", ".join(missing))

        return cls(
            host=str(host),
            serial=str(serial),
            access_code=str(access_code),
            username=str(_env("BAMBU_USERNAME", dotenv, "bblp")),
            mqtt_port=int(str(_env("BAMBU_MQTT_PORT", dotenv, "8883"))),
            db_path=Path(str(_env("BAMBU_DB_PATH", dotenv, "./data/filament.sqlite3"))),
            monitor_requests=_env_bool("BAMBU_MONITOR_REQUESTS", dotenv, False),
            tls_insecure=_env_bool("BAMBU_TLS_INSECURE", dotenv, True),
        )


@dataclass(frozen=True)
class SheetsConfig:
    webhook_url: str = ""
    secret: str = ""
    sync_on_event: bool = False
    min_interval_seconds: int = 60

    @classmethod
    def from_env(cls, env_file: str = ".env") -> "SheetsConfig":
        dotenv = _load_dotenv(Path(env_file))
        return cls(
            webhook_url=str(_env("GOOGLE_SHEETS_WEBHOOK_URL", dotenv, "") or ""),
            secret=str(_env("GOOGLE_SHEETS_SECRET", dotenv, "") or ""),
            sync_on_event=_env_bool("GOOGLE_SHEETS_SYNC_ON_EVENT", dotenv, False),
            min_interval_seconds=int(str(_env("GOOGLE_SHEETS_MIN_INTERVAL_SECONDS", dotenv, "60") or "60")),
        )


@dataclass(frozen=True)
class CloudConfig:
    enabled: bool = False
    email: str = ""
    password: str = ""
    access_token: str = ""
    base_url: str = DEFAULT_BASE_URL

    @classmethod
    def from_env(cls, env_file: str = ".env") -> "CloudConfig":
        dotenv = _load_dotenv(Path(env_file))
        access_token = str(_env("BAMBU_CLOUD_ACCESS_TOKEN", dotenv, "") or "")
        return cls(
            enabled=_env_bool("BAMBU_CLOUD_ENABLED", dotenv, bool(access_token)),
            email=str(_env("BAMBU_CLOUD_EMAIL", dotenv, "") or ""),
            password=str(_env("BAMBU_CLOUD_PASSWORD", dotenv, "") or ""),
            access_token=access_token,
            base_url=str(_env("BAMBU_CLOUD_BASE_URL", dotenv, DEFAULT_BASE_URL) or DEFAULT_BASE_URL),
        )
