#!/usr/bin/env python3
"""Sync FINN polygon points from code into Google Sheet tab 'Finn Polygon Coords'."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

# Add parent and project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

try:
    from main.googleUtils import get_sheets_service, SPREADSHEET_ID
    from main.tools.finn_polygon_editor import load_defaults_from_source
except ImportError:
    from googleUtils import get_sheets_service, SPREADSHEET_ID
    from tools.finn_polygon_editor import load_defaults_from_source


DEFAULT_SHEET_NAME = "Finn Polygon Coords"
DEFAULT_SOURCE_FILE = Path("main/runners/run_eiendom_db.py")


def ensure_sheet_exists(service, sheet_name: str) -> None:
    """Create a sheet tab if it does not already exist."""
    spreadsheet = service.spreadsheets().get(
        spreadsheetId=SPREADSHEET_ID,
        fields="sheets(properties(title))",
    ).execute()

    existing_titles = {
        s.get("properties", {}).get("title", "")
        for s in spreadsheet.get("sheets", [])
    }
    if sheet_name in existing_titles:
        return

    service.spreadsheets().batchUpdate(
        spreadsheetId=SPREADSHEET_ID,
        body={
            "requests": [
                {
                    "addSheet": {
                        "properties": {
                            "title": sheet_name,
                        }
                    }
                }
            ]
        },
    ).execute()



def sync_polygon_to_sheet(sheet_name: str, source_file: Path) -> bool:
    """Write polygon points from source file to sheet."""
    source_path = Path(source_file)
    _, points = load_defaults_from_source(source_path)

    if not points:
        print("No polygon points found in source file.")
        return False

    service = get_sheets_service()
    ensure_sheet_exists(service, sheet_name)

    values = [["Order", "LAT", "LNG"]]
    for idx, (lng, lat) in enumerate(points, start=1):
        values.append([idx, lat, lng])

    service.spreadsheets().values().clear(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{sheet_name}!A1:Z1000",
    ).execute()

    service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{sheet_name}!A1",
        valueInputOption="RAW",
        body={"values": values},
    ).execute()

    print(f"Synced {len(points)} polygon points to '{sheet_name}'")
    return True



def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync FINN polygon coords to Google Sheets")
    parser.add_argument("--sheet", default=DEFAULT_SHEET_NAME, help=f"Sheet tab name (default: {DEFAULT_SHEET_NAME})")
    parser.add_argument(
        "--source-file",
        type=Path,
        default=DEFAULT_SOURCE_FILE,
        help=f"Source file containing finn_polygon_points (default: {DEFAULT_SOURCE_FILE})",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    ok = sync_polygon_to_sheet(args.sheet, args.source_file)
    raise SystemExit(0 if ok else 1)
