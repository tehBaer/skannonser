# Interactive Map Setup (Sheets + Apps Script)

This guide keeps almost all logic in this repo and uses Apps Script only as a thin bridge.

## 1. What was added in this repo

- Apps Script project files:
  - `apps_script/map/Code.gs`
  - `apps_script/map/map.html`
  - `apps_script/map/appsscript.json`
  - `apps_script/map/.clasp.example.json`
- Sync safeguard to preserve custom map columns:
  - `main/sync/update_rows_in_sheet.py`
- DB coordinate support and reporting:
  - `main/database/db.py` (`LAT`/`LNG` in `eiendom_processed` + sheets export)
  - `main/tools/report_missing_coordinates.py`

The sync safeguard prevents DB update sync from overwriting sheet-only columns if you add custom columns.

## 2. Prepare your Google Sheet

1. Open your `Eie` sheet (same spreadsheet used by current sync).
2. Ensure these columns exist in row 1 (they are now exported from DB):
   - `LAT`
   - `LNG`
3. Create a new sheet tab named `Stations` with headers in row 1:
   - `Name`
   - `LAT`
   - `LNG`
   - `RadiusM`
   - `Type`
  - `Lines` (optional, comma-separated line IDs like `L1, R14, RE11`)
4. Add train stations (example rows):
  - `Oslo S`, `59.9109`, `10.7531`, `1200`, `train`, `L1, L2, R10, R11, R12, R13, R14, R21, RE11`
  - `Nationaltheatret`, `59.9146`, `10.7303`, `900`, `train`, `L1, L2, R10, R11, R12, R14, RE11`
  - `Skoyen`, `59.9226`, `10.6795`, `900`, `train`, `L1, L2, R10, R11, R12, R14, RE11`

Notes:
- `RadiusM` controls immediate vicinity for that station.
- `Lines` enables line-specific station toggles/colors in the map sidebar.
- If a station has multiple lines, the map creates one station overlay per line.
- Disabling a line hides that line's station overlays and removes it from `Only near station` filtering.
- Listings inside station radius are highlighted on the map.

## 3. Keep repo sync workflow as-is

Use your existing sync to keep Sheets updated:

```bash
make sheet
```

Your current filters still apply in Python first:
- `main/config/filters.py` (`MAX_PRICE`, etc.)

And the web map then uses only currently visible rows from Google Sheets (after manual sheet filters).
Map plotting is strict: only rows with saved `LAT/LNG` are plotted.

## 4. Create Apps Script project and connect with clasp

1. Install clasp (once):

```bash
npm install -g @google/clasp
```

2. Login:

```bash
clasp login
```

3. Create a standalone Apps Script project:

```bash
mkdir -p ~/tmp/skannonser-map-script && cd ~/tmp/skannonser-map-script
clasp create --type standalone --title "SKAnnonser Property Map"
```

4. Copy the generated `scriptId` into this repo:

```bash
cp /Users/tehbaer/kode/skannonser/apps_script/map/.clasp.example.json /Users/tehbaer/kode/skannonser/apps_script/map/.clasp.json
```

Then edit `apps_script/map/.clasp.json` and set real `scriptId`.

5. Push files from repo to Apps Script:

```bash
cd /Users/tehbaer/kode/skannonser/apps_script/map
clasp push
```

## 5. Set Script Properties (required)

In Apps Script editor:
1. Open Project Settings.
2. Under Script Properties, add:
   - `MAPS_API_KEY` = your Google Maps JavaScript API key
  - `SPREADSHEET_ID` = your target Google Sheet ID (required for standalone script)
   - `LISTINGS_SHEET` = `Eie`
   - `STATIONS_SHEET` = `Stations`

Required Google Cloud APIs for that key:
- Maps JavaScript API

## 6. Deploy the web app

From `apps_script/map`:

```bash
clasp deploy --description "Initial interactive map"
```

Then in Apps Script UI:
1. Deploy -> Manage deployments -> Web app
2. Execute as: `Me`
3. Who has access: choose suitable scope (for private use, your account/domain)
4. Copy Web app URL

## 7. Daily usage flow

1. Update listings to Sheets:

```bash
make sheet
```

2. In Google Sheets, apply filter conditions on `Eie`.
3. Open Web app URL.
4. Click `Refresh` in map.
5. Map now shows only rows visible after filter.

Optional DB health check:

```bash
make coords-missing
```

This reports listings still missing DB coordinates.

## 8. Features already implemented

- Reads only visible rows from filtered `Eie` sheet.
- Plots listings and station overlays with radius circles.
- Highlights listings in immediate station vicinity.
- Does not geocode in browser and does not write coordinates from map UI.
- Listing popup:
  - Clickable `Open FINN ad` link (`URL` column)
  - Clickable `Open Google Maps` link (`GOOGLE_MAPS_URL`)
  - Expandable details section from full sheet row
- Missing coordinate visibility:
  - Sidebar shows visible rows that are missing `LAT/LNG`
  - `make coords-missing` reports missing coordinates directly from DB

## 9. Important implementation notes

- Keep logic in repo Python where possible.
- Keep Apps Script thin:
  - Read visible rows
  - Serve map data/UI
- Do not put price/business filtering logic into script unless intentionally needed.

## 10. Troubleshooting

- `Missing MAPS_API_KEY script property`:
  - Add `MAPS_API_KEY` in Script Properties.
- `Sorry, unable to open the file at this time`:
  - Ensure you are opening the web app URL ending in `/exec`.
  - Ensure Script Property `SPREADSHEET_ID` is set correctly.
  - Ensure the deploying Google account has access to that spreadsheet.
  - Redeploy after updates and use the latest deployment URL.
- Empty map but rows exist:
  - Ensure `LAT/LNG` exist in DB and were synced to sheet.
  - Run `make coords-missing` to see which listings need coordinates.
  - Ensure rows are not hidden by filter.
- No station highlighting:
  - Check `Stations` sheet has valid numeric `LAT/LNG/RadiusM`.
- Custom columns overwritten after sync:
  - Already patched in `main/sync/update_rows_in_sheet.py` to preserve non-DB columns.
