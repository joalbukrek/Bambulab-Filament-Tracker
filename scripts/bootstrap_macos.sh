#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="$ROOT_DIR/.env"

cd "$ROOT_DIR"

if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 is required. Install Xcode Command Line Tools or Python 3 first."
  exit 1
fi

if [ ! -d "$ROOT_DIR/.venv" ]; then
  python3 -m venv .venv
fi

. "$ROOT_DIR/.venv/bin/activate"
python -m ensurepip --upgrade
python -m pip install --upgrade pip
python -m pip install -e .

if [ ! -f "$ENV_FILE" ]; then
  cp .env.example "$ENV_FILE"
fi

get_env() {
  python3 - "$ENV_FILE" "$1" <<'PY'
from pathlib import Path
import sys

path = Path(sys.argv[1])
key = sys.argv[2]
for raw in path.read_text(encoding="utf-8").splitlines():
    line = raw.strip()
    if not line or line.startswith("#") or "=" not in line:
        continue
    name, value = line.split("=", 1)
    if name.strip() == key:
        print(value.strip().strip('"').strip("'"))
        break
PY
}

set_env() {
  python3 - "$ENV_FILE" "$1" "$2" <<'PY'
from pathlib import Path
import sys

path = Path(sys.argv[1])
key = sys.argv[2]
value = sys.argv[3]
lines = path.read_text(encoding="utf-8").splitlines() if path.exists() else []
seen = False
result = []
for raw in lines:
    stripped = raw.strip()
    if not stripped or stripped.startswith("#") or "=" not in stripped:
        result.append(raw)
        continue
    name = stripped.split("=", 1)[0].strip()
    if name == key:
        result.append(f"{key}={value}")
        seen = True
    else:
        result.append(raw)
if not seen:
    result.append(f"{key}={value}")
path.write_text("\n".join(result) + "\n", encoding="utf-8")
PY
}

prompt_env() {
  local key="$1"
  local label="$2"
  local secret="${3:-false}"
  local current
  current="$(get_env "$key" || true)"
  if [ -n "$current" ] && [[ "$current" != YOUR_* ]] && [[ "$current" != "change-me" ]]; then
    return
  fi
  local value
  if [ "$secret" = "true" ]; then
    printf "%s: " "$label"
    read -r -s value
    printf "\n"
  else
    printf "%s: " "$label"
    read -r value
  fi
  if [ -n "$value" ]; then
    set_env "$key" "$value"
  fi
}

echo "Configuring Bambu Filament Tracker"
prompt_env "BAMBU_HOST" "Printer IP address"
prompt_env "BAMBU_SERIAL" "Printer serial number"
prompt_env "BAMBU_ACCESS_CODE" "LAN access code" true
username="$(get_env BAMBU_USERNAME || true)"
if [ -z "$username" ]; then
  username="bblp"
fi
mqtt_port="$(get_env BAMBU_MQTT_PORT || true)"
if [ -z "$mqtt_port" ]; then
  mqtt_port="8883"
fi
set_env "BAMBU_USERNAME" "$username"
set_env "BAMBU_MQTT_PORT" "$mqtt_port"
set_env "GOOGLE_SHEETS_SYNC_ON_EVENT" "true"

if [ -z "$(get_env GOOGLE_SHEETS_WEBHOOK_URL || true)" ]; then
  printf "Google Sheets webhook URL (optional, press Enter to skip): "
  read -r sheets_url
  if [ -n "$sheets_url" ]; then
    set_env "GOOGLE_SHEETS_WEBHOOK_URL" "$sheets_url"
    prompt_env "GOOGLE_SHEETS_SECRET" "Google Sheets secret"
  fi
fi

cloud_enabled="$(get_env BAMBU_CLOUD_ENABLED || true)"
if [ "$cloud_enabled" != "true" ]; then
  printf "Enable Bambu Cloud usage lookup? [Y/n]: "
  read -r enable_cloud
  if [ -z "$enable_cloud" ] || [[ "$enable_cloud" =~ ^[Yy]$ ]]; then
    set_env "BAMBU_CLOUD_ENABLED" "true"
  fi
fi

if [ "$(get_env BAMBU_CLOUD_ENABLED || true)" = "true" ] && [ -z "$(get_env BAMBU_CLOUD_ACCESS_TOKEN || true)" ]; then
  prompt_env "BAMBU_CLOUD_EMAIL" "Bambu Cloud email"
  printf "Bambu Cloud password (not saved, press Enter to skip login): "
  read -r -s cloud_password
  printf "\n"
  if [ -n "$cloud_password" ]; then
    bambu-track cloud-login --password "$cloud_password" || true
    if [ -z "$(get_env BAMBU_CLOUD_ACCESS_TOKEN || true)" ]; then
      printf "Verification code from Bambu email (press Enter to skip): "
      read -r cloud_code
      if [ -n "$cloud_code" ]; then
        bambu-track cloud-login --code "$cloud_code"
      fi
    fi
  fi
fi

bambu-track init-db

if [ -n "$(get_env GOOGLE_SHEETS_WEBHOOK_URL || true)" ]; then
  bambu-track sync-sheets || true
fi

if command -v launchctl >/dev/null 2>&1; then
  bambu-track install-service
  bambu-track start-service
fi

bambu-track doctor

echo "Setup complete. Keep this Mac awake and on the same network as the printer."
