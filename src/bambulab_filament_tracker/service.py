from __future__ import annotations

import os
import plistlib
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import List


LABEL = "com.joalbukrek.bambu-filament-tracker"
PROJECT_DIR = Path(__file__).resolve().parents[2]
APP_DIR = Path.home() / "Library" / "Application Support" / "BambuFilamentTracker"
RUNTIME_SRC_DIR = APP_DIR / "src"
RUNTIME_VENV_DIR = APP_DIR / ".venv"
RUNTIME_ENV_PATH = APP_DIR / ".env"
RUNTIME_DB_PATH = APP_DIR / "data" / "filament.sqlite3"
PLIST_PATH = Path.home() / "Library" / "LaunchAgents" / ("%s.plist" % LABEL)
LOG_DIR = APP_DIR / "logs"
OUT_LOG = LOG_DIR / "listener.log"
ERR_LOG = LOG_DIR / "listener.err.log"


@dataclass
class ServiceResult:
    ok: bool
    message: str


def install_service() -> ServiceResult:
    prepare_runtime()
    PLIST_PATH.parent.mkdir(parents=True, exist_ok=True)
    plist = {
        "Label": LABEL,
        "ProgramArguments": [
            str(RUNTIME_VENV_DIR / "bin" / "python"),
            "-u",
            "-m",
            "bambulab_filament_tracker.cli",
            "listen",
            "--env-file",
            str(RUNTIME_ENV_PATH),
        ],
        "WorkingDirectory": str(APP_DIR),
        "EnvironmentVariables": {
            "PYTHONPATH": str(RUNTIME_SRC_DIR),
        },
        "RunAtLoad": True,
        "KeepAlive": True,
        "StandardOutPath": str(OUT_LOG),
        "StandardErrorPath": str(ERR_LOG),
    }
    with PLIST_PATH.open("wb") as handle:
        plistlib.dump(plist, handle)
    return ServiceResult(True, "Installed %s" % PLIST_PATH)


def prepare_runtime() -> None:
    APP_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    (APP_DIR / "data").mkdir(parents=True, exist_ok=True)

    if RUNTIME_SRC_DIR.exists():
        shutil.rmtree(RUNTIME_SRC_DIR)
    shutil.copytree(PROJECT_DIR / "src", RUNTIME_SRC_DIR)

    project_venv = PROJECT_DIR / ".venv"
    if not project_venv.exists():
        raise RuntimeError("Missing project virtualenv: %s" % project_venv)
    if RUNTIME_VENV_DIR.exists():
        shutil.rmtree(RUNTIME_VENV_DIR)
    shutil.copytree(project_venv, RUNTIME_VENV_DIR, symlinks=True)

    project_db = PROJECT_DIR / "data" / "filament.sqlite3"
    if project_db.exists() and not RUNTIME_DB_PATH.exists():
        shutil.copy2(project_db, RUNTIME_DB_PATH)

    write_runtime_env()


def write_runtime_env() -> None:
    source_env = PROJECT_DIR / ".env"
    values = read_env_file(source_env)
    values["BAMBU_DB_PATH"] = str(RUNTIME_DB_PATH)
    lines = ["%s=%s" % (key, value) for key, value in values.items()]
    RUNTIME_ENV_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")


def read_env_file(path: Path) -> dict:
    values = {}
    if not path.exists():
        return values
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def start_service() -> ServiceResult:
    install_service()
    stop_service(ignore_errors=True)
    result = run_launchctl(["bootstrap", "gui/%s" % os.getuid(), str(PLIST_PATH)])
    if result.returncode != 0:
        return ServiceResult(False, result.stderr.strip() or result.stdout.strip())
    return ServiceResult(True, "Started %s" % LABEL)


def stop_service(ignore_errors: bool = False) -> ServiceResult:
    result = run_launchctl(["bootout", "gui/%s" % os.getuid(), str(PLIST_PATH)])
    if result.returncode != 0 and not ignore_errors:
        return ServiceResult(False, result.stderr.strip() or result.stdout.strip())
    return ServiceResult(True, "Stopped %s" % LABEL)


def status_service() -> ServiceResult:
    result = run_launchctl(["print", "gui/%s/%s" % (os.getuid(), LABEL)])
    if result.returncode != 0:
        return ServiceResult(False, "Not running: %s" % (result.stderr.strip() or result.stdout.strip()))
    lines = []
    for line in result.stdout.splitlines():
        stripped = line.strip()
        if stripped.startswith("pid =") or stripped.startswith("state =") or stripped.startswith("last exit code ="):
            lines.append(stripped)
    return ServiceResult(True, "\n".join(lines) or "Running")


def log_paths() -> List[Path]:
    return [OUT_LOG, ERR_LOG]


def read_logs(lines: int = 80) -> str:
    output = []
    for path in log_paths():
        output.append("==> %s <==" % path)
        if not path.exists():
            output.append("(missing)")
            continue
        text = path.read_text(encoding="utf-8", errors="replace").splitlines()
        output.extend(text[-lines:])
    return "\n".join(output)


def run_launchctl(args: List[str]) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["launchctl"] + args,
        cwd=str(APP_DIR if APP_DIR.exists() else PROJECT_DIR),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
