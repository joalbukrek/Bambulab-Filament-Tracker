# Bambu Lab Filament Tracker

Local filament usage tracker for a Bambu Lab A1 with AMS Lite.

The reliable approach is:

1. Use the printer's local MQTT feed to detect print start/end and the active AMS Lite tray.
2. Use Bambu Cloud task details for grams when a cloud `task_id` is available.
3. Fall back to Bambu Studio/OrcaSlicer `.3mf` or G-code metadata for local-only prints.
4. Store print jobs, AMS slot assignments, spools, and per-spool usage in SQLite.

This matters because the printer can report state and tray data over MQTT, but local MQTT often does not expose the final grams used. The app combines local MQTT, optional Bambu Cloud task weights, and optional slicer-file imports.

## Quick Start

```bash
git clone https://github.com/YOUR_USERNAME/bambulab-filament-tracker.git
cd bambulab-filament-tracker
./scripts/bootstrap_macos.sh
```

The installer creates `.venv`, installs the CLI, creates `.env`, asks for printer settings, optionally logs in to Bambu Cloud, installs the macOS background service, starts it, and runs `bambu-track doctor`.

Manual setup is still available:

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -e .
cp .env.example .env
bambu-track init-db
bambu-track cloud-login
bambu-track sync-sheets
bambu-track install-service
bambu-track start-service
bambu-track doctor
```

Keep the Mac awake and connected to the same network as the printer.

## Can This Run On A Phone?

Not reliably as the always-on tracker. The tracker needs a long-running background process that keeps an MQTT connection open on the local network and writes to a local database. iOS does not allow this kind of continuous background process for a normal app, and Android may kill it unless it is built as a foreground service or run through Termux with battery optimizations disabled.

Recommended setup: run the tracker on an old Mac, Raspberry Pi, or small always-on computer, then view the Google Sheet from your phone.

## Setup

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -e .
cp .env.example .env
```

Edit `.env`:

```bash
BAMBU_HOST=192.168.1.50
BAMBU_SERIAL=YOUR_PRINTER_SERIAL
BAMBU_ACCESS_CODE=YOUR_LAN_ACCESS_CODE
```

The local MQTT username is usually `bblp`. The password is the LAN access code shown on the printer.

## Initialize

```bash
bambu-track init-db
```

Add the spools currently loaded in the AMS Lite. Slots are user-facing AMS Lite slots `1` through `4`.

```bash
bambu-track spools add --name "Bambu PLA Matte Red" --material PLA --color FF0000 --initial-g 1000 --slot 2
bambu-track spools list
```

## Automatic Printer Tracking

Run this while printing:

```bash
bambu-track listen
```

To keep tracking in the background on macOS, install and start the LaunchAgent:

```bash
bambu-track install-service
bambu-track start-service
```

Check it later with:

```bash
bambu-track status-service
bambu-track logs
```

Stop it with:

```bash
bambu-track stop-service
```

For a quick one-time status check:

```bash
bambu-track snapshot
```

The listener subscribes to:

```text
device/<serial>/report
```

The `report` topic captures printer state and AMS tray state. The app still publishes Bambu's `pushall` request to ask the printer for a fresh status report. Some firmware disconnects clients that subscribe to `device/<serial>/request`, so request-topic monitoring is disabled by default.

AMS Lite slots are learned automatically from the printer report. The app stores the active tray and the filament metadata the printer exposes, including material, color, RFID/tray code, and the name-like fields such as `tray_sub_brands` and `tray_id_name`.

```bash
bambu-track ams
bambu-track spools list
```

The printer does not weigh spools. If AMS/RFID metadata says `1000g`, treat that as nominal spool capacity, not measured remaining weight. Correct known weights manually:

```bash
bambu-track spools set-weight --id 3 --remaining-g 810
```

You can also update by AMS slot, which is easier for day-to-day use:

```bash
bambu-track spools set-slot-weight --slot 4 --initial-g 1000 --remaining-g 1000
```

During a running print, the listener records every AMS slot that becomes active for that print job. If the printer reports a final total gram value and exactly one AMS slot was observed, that usage is assigned to the observed spool automatically. If multiple AMS slots were observed, the app records which spools were used but does not split a single total gram value across them unless slicer metadata is imported.

## Optional Bambu Cloud Usage Lookup

Local MQTT does not expose the printer-screen grams for every print. Bambulab Spoolman works around that by using MQTT only to get the print `task_id` and progress, then calling Bambu Cloud task endpoints for the task weight and `amsDetailMapping`.

Enable the same optional method here:

```bash
BAMBU_CLOUD_ENABLED=true
BAMBU_CLOUD_EMAIL=you@example.com
BAMBU_CLOUD_PASSWORD=your-password
```

Then log in and save a token:

```bash
bambu-track cloud-login
```

If Bambu requires an email code, run the command once to send the code, then run:

```bash
bambu-track cloud-login --code 123456
```

After this, restart the background service:

```bash
bambu-track stop-service
bambu-track start-service
```

When a cloud-started print reports a real `task_id`, the listener will fetch Bambu Cloud task details at print finish. For one-spool jobs, it records the cloud weight against the observed AMS slot automatically. For multi-spool jobs, it first uses captured slicer-to-AMS mapping if available, then tries a conservative unique match by filament ID, color, material, and name. If the match is ambiguous, it refuses to guess and you can import the `.3mf`/G-code metadata manually.

Useful diagnostics:

```bash
bambu-track cloud-task --task-id 123456
bambu-track cloud-sync-job --job-id 7 --task-id 123456
```

## Exact Usage Import

After slicing or printing, import the `.3mf`, `.gcode.3mf`, or `.gcode` file. For a single-filament job that used AMS slot 2:

```bash
bambu-track import path/to/print.3mf --slot 2
```

For multi-filament jobs, map slicer filament indexes to AMS slots:

```bash
bambu-track import path/to/print.3mf --mapping 0:2,1:4
```

That means slicer filament `0` used AMS slot `2`, and slicer filament `1` used AMS slot `4`.

If a print job was already captured by MQTT, attach the imported usage to it:

```bash
bambu-track jobs
bambu-track import path/to/print.3mf --job-id 7
```

When the print request's `ams_mapping` was captured, `--job-id` is enough because the app already knows the slicer-filament to AMS-slot mapping. For one-filament jobs, if the listener observed exactly one AMS slot during that job, `--job-id` is also enough.

Imports are protected against double-counting. If you need to re-import a job, use:

```bash
bambu-track import path/to/print.3mf --job-id 7 --replace-existing
```

## View Data

```bash
bambu-track jobs
bambu-track usage
bambu-track spools list
```

## Merge History From Another Mac

If you moved tracking from one Mac to another, import historical jobs and usage into the current database:

```bash
bambu-track merge-db /path/to/old/filament.sqlite3
bambu-track sync-sheets
```

The merge imports print jobs, observed AMS slots, and usage rows. It does not import active spool assignments from the other Mac, so it will not overwrite the current AMS/spool state on the Mac that is now running the tracker.

## Google Sheets Sync

The simplest cloud dashboard is Google Sheets via a small Google Apps Script web app. This avoids installing Google API client libraries or storing OAuth tokens on the Mac.

1. Create a Google Sheet.
2. In the sheet, open Extensions -> Apps Script.
3. Paste the code from `docs/google_apps_script.gs`.
4. Change `SECRET` in the script to a random value.
5. Deploy -> New deployment -> Web app.
6. Set "Execute as" to "Me" and "Who has access" to "Anyone with the link".
7. Copy the Web app URL into `.env` as `GOOGLE_SHEETS_WEBHOOK_URL`.
8. Put the same random value into `.env` as `GOOGLE_SHEETS_SECRET`.

Test a manual sync:

```bash
bambu-track sync-sheets
```

Enable automatic sync from the background listener:

```bash
GOOGLE_SHEETS_SYNC_ON_EVENT=true
```

Then restart the service:

```bash
bambu-track stop-service
bambu-track start-service
```

The Google Sheet will be rewritten with four tabs: `AMS Slots`, `Spools`, `Print Jobs`, and `Usage`. Timestamps are exported in Turkey time (`Europe/Istanbul`). Hex colors are exported as text so values like `000000` keep their leading zeros.

Treat Google Sheets as a dashboard. Manual edits in the sheet are overwritten by the next sync. Update spool weights through `bambu-track`, then run `bambu-track sync-sheets` if you want the sheet updated immediately.

If the printer screen shows grams but MQTT does not expose that value, record the screen value manually for the captured job:

```bash
bambu-track jobs
bambu-track add-usage --job-id 1 --grams 12.34
bambu-track sync-sheets
```

## Notes

- AMS Lite tray IDs in Bambu MQTT are treated as zero-based internally and shown as AMS slots `1` through `4`.
- If direct MQTT `print_weight` data is present at the end of a print, the app stores it automatically. Multi-material attribution still needs a slicer file or captured mapping.
- If the printer firmware or LAN mode blocks the request topic, use `--slot` or `--mapping` during import.
- The SQLite database defaults to `./data/filament.sqlite3`.
- Run `bambu-track doctor` to check whether config, service, Google Sheets, Bambu Cloud, AMS slots, and recent jobs look healthy.
