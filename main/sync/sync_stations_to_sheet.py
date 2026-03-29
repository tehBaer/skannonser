"""
Export station data from the SQLite DB back to the Stations Google Sheet.

The DB is the source of truth; this module regenerates the Stations tab so
the Apps Script map always reflects the latest DB state.

The sheet is rewritten in full on each call (header row + all station rows).
"""
from __future__ import annotations

import re
import sys
import unicodedata
from pathlib import Path
from typing import Any, Dict, List

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from main.googleUtils import get_sheets_service, SPREADSHEET_ID
from main.database.stations import StationDatabase


def _col_letter(idx: int) -> str:
    """Convert 0-based column index to A1 letter notation."""
    result = ""
    n = idx + 1
    while n > 0:
        n, rem = divmod(n - 1, 26)
        result = chr(ord("A") + rem) + result
    return result


def _destination_column_name(destination: str) -> str:
    text = " ".join((destination or "").strip().split())
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.upper()
    text = re.sub(r"[^A-Z0-9]+", "_", text).strip("_")
    return f"TO_{text or 'DESTINATION'}"


def sync_stations_to_sheet(
    sheet_name: str = "Stations",
    destination: str = "Sandvika",
) -> bool:
    """
    Rewrite the Stations sheet from the DB.

    Returns True on success, False on failure.
    """
    db = StationDatabase()
    export_rows = db.get_all_for_export(destination=destination)

    if not export_rows:
        print("No station data in DB; Stations sheet not updated.")
        return True  # Not an error — DB may simply be empty

    # Deterministic row-per-line schema with one destination travel column.
    travel_col = _destination_column_name(destination)
    headers = ["Name", "LAT", "LNG", "Line", travel_col]

    # Build 2-D list of values (header row + data rows)
    data: List[List[Any]] = [headers]
    for row in export_rows:
        data.append([str(row.get(h, "")) for h in headers])

    service = get_sheets_service()

    # Determine the sheet's numeric sheetId (needed for clear request)
    meta = service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
    sheet_id = None
    for s in meta.get("sheets", []):
        if s["properties"]["title"] == sheet_name:
            sheet_id = s["properties"]["sheetId"]
            break

    if sheet_id is None:
        # Sheet tab does not yet exist — create it
        service.spreadsheets().batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body={"requests": [{"addSheet": {"properties": {"title": sheet_name}}}]},
        ).execute()
        print(f"Created sheet tab '{sheet_name}'.")

    # Clear existing content then write fresh data
    clear_range = sheet_name
    service.spreadsheets().values().clear(
        spreadsheetId=SPREADSHEET_ID, range=clear_range, body={}
    ).execute()

    service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{sheet_name}!A1",
        valueInputOption="RAW",
        body={"values": data},
    ).execute()

    n_stations = len(export_rows)
    n_cols = len(headers)
    print(
        f"Stations sheet '{sheet_name}' updated: "
        f"{n_stations} station rows, {n_cols} columns "
        f"(destination='{destination}')."
    )
    return True
