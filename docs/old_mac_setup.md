# Old Mac Setup

Use this when moving the tracker to a Mac that will stay on near the printer.

## 1. Install

```bash
git clone https://github.com/YOUR_USERNAME/bambulab-filament-tracker.git
cd bambulab-filament-tracker
./scripts/bootstrap_macos.sh
```

The script creates the virtual environment, installs the CLI, prompts for config, optionally logs in to Bambu Cloud, installs the macOS background service, starts it, and runs `bambu-track doctor`.

Manual setup is below if you prefer to do each step yourself.

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -e .
cp .env.example .env
```

Edit `.env` with:

```bash
BAMBU_HOST=192.168.1.50
BAMBU_SERIAL=YOUR_PRINTER_SERIAL
BAMBU_ACCESS_CODE=YOUR_LAN_ACCESS_CODE
BAMBU_CLOUD_ENABLED=true
BAMBU_CLOUD_EMAIL=you@example.com
GOOGLE_SHEETS_WEBHOOK_URL=https://script.google.com/macros/s/...
GOOGLE_SHEETS_SECRET=your-secret
GOOGLE_SHEETS_SYNC_ON_EVENT=true
```

Do not commit `.env`.

## 2. Log In To Bambu Cloud

```bash
bambu-track cloud-login --password "your-bambu-password"
```

If Bambu sends an email code:

```bash
bambu-track cloud-login --code 123456
```

## 3. Start Tracking

```bash
bambu-track init-db
bambu-track sync-sheets
bambu-track install-service
bambu-track start-service
bambu-track doctor
```

The service will restart automatically after reboots.

## 4. Day-To-Day Commands

```bash
bambu-track doctor
bambu-track logs
bambu-track jobs
bambu-track usage
bambu-track ams
```

Set the current remaining spool weight by AMS slot:

```bash
bambu-track spools set-slot-weight --slot 4 --remaining-g 1000
bambu-track sync-sheets
```

## Notes

- Keep the Mac awake.
- Keep the Mac on the same local network as the printer.
- Google Sheets is append-only. New unique rows are added to the bottom and old rows are preserved.
- Usage is recorded at print finish, not continuously during the print.
