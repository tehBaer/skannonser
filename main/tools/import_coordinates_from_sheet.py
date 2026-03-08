#!/usr/bin/env python3
"""Import LAT/LNG values from a Google Sheet into the local database.

Default behavior only fills listings that are missing coordinates in DB.
Use --overwrite to replace existing DB coordinates as well.
"""
import argparse
import os
import re
import sys
from typing import Dict, Optional, Tuple

# Add parent and project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

try:
    from main.database.db import PropertyDatabase
    from main.googleUtils import SPREADSHEET_ID, get_sheets_service
except ImportError:
    from database.db import PropertyDatabase
    from googleUtils import SPREADSHEET_ID, get_sheets_service


HEADER_ALIASES = {
    "finnkode": "Finnkode",
    "lat": "LAT",
    "latitude": "LAT",
    "lng": "LNG",
    "lon": "LNG",
    "longitude": "LNG",
}


def canonical_header(name: str) -> str:
    raw = str(name or "").strip()
    if not raw:
        return raw
    return HEADER_ALIASES.get(raw.lower(), raw)


def normalize_finnkode(value: object) -> str:
    if value is None:
        return ""

    text = str(value).strip()
    if not text:
        return ""

    # HYPERLINK("url", "12345678") -> 12345678
    if "HYPERLINK" in text.upper():
        parts = text.split('"')
        if len(parts) >= 4:
            text = parts[3].strip()

    try:
        as_float = float(text)
        if as_float.is_integer():
            return str(int(as_float))
    except (ValueError, TypeError):
        pass

    # Keep only pure digits where possible.
    digits = re.sub(r"\D", "", text)
    return digits if digits else text


def parse_coord(value: object) -> Optional[float]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None

    # Accept both decimal comma and decimal dot.
    text = text.replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return None


def get_column_indexes(header_row: list[str]) -> Dict[str, int]:
    canonical = [canonical_header(h) for h in header_row]

    required = ["Finnkode", "LAT", "LNG"]
    indexes: Dict[str, int] = {}

    for col in required:
        if col in canonical:
            indexes[col] = canonical.index(col)

    return indexes


def read_sheet_coordinates(sheet_name: str, max_rows: int) -> Dict[str, Tuple[float, float]]:
    service = get_sheets_service()
    range_name = f"{sheet_name}!A1:AZ{max_rows}"

    result = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=range_name,
    ).execute()

    values = result.get("values", [])
    if not values:
        return {}

    header_row = values[0]
    indexes = get_column_indexes(header_row)

    missing_cols = [c for c in ["Finnkode", "LAT", "LNG"] if c not in indexes]
    if missing_cols:
        raise ValueError(f"Missing required column(s) in sheet '{sheet_name}': {', '.join(missing_cols)}")

    out: Dict[str, Tuple[float, float]] = {}
    for row in values[1:]:
        finnkode_raw = row[indexes["Finnkode"]] if len(row) > indexes["Finnkode"] else ""
        lat_raw = row[indexes["LAT"]] if len(row) > indexes["LAT"] else ""
        lng_raw = row[indexes["LNG"]] if len(row) > indexes["LNG"] else ""

        finnkode = normalize_finnkode(finnkode_raw)
        lat = parse_coord(lat_raw)
        lng = parse_coord(lng_raw)

        if not finnkode or lat is None or lng is None:
            continue

        out[finnkode] = (lat, lng)

    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="Import LAT/LNG from Google Sheet into DB")
    parser.add_argument("--sheet", default="Eie", help="Google Sheet tab name (default: Eie)")
    parser.add_argument("--db", help="Optional path to properties.db")
    parser.add_argument("--max-rows", type=int, default=10000, help="Rows to read from sheet (default: 10000)")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing DB coordinates")
    args = parser.parse_args()

    print("=" * 72)
    print("Import Coordinates From Sheet")
    print("=" * 72)
    print(f"Sheet: {args.sheet}")
    print(f"Mode: {'overwrite all matching rows' if args.overwrite else 'fill missing DB coordinates only'}")

    try:
        sheet_coords = read_sheet_coordinates(args.sheet, args.max_rows)
    except Exception as exc:
        print(f"Failed to read sheet coordinates: {exc}")
        return 1

    if not sheet_coords:
        print("No valid LAT/LNG rows found in sheet")
        return 0

    db = PropertyDatabase(args.db)

    existing_missing_df = db.get_eiendom_missing_coordinates()
    missing_set = set(existing_missing_df["Finnkode"].astype(str).str.strip()) if not existing_missing_df.empty else set()

    matched = 0
    updated = 0
    skipped_not_missing = 0

    for finnkode, (lat, lng) in sheet_coords.items():
        if not args.overwrite and finnkode not in missing_set:
            skipped_not_missing += 1
            continue

        matched += 1
        if db.set_eiendom_coordinates(finnkode, lat, lng):
            updated += 1

    print(f"Rows with valid sheet coordinates: {len(sheet_coords)}")
    print(f"Matched rows considered for DB update: {matched}")
    if not args.overwrite:
        print(f"Skipped (already had DB coordinates): {skipped_not_missing}")
    print(f"Updated in DB: {updated}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
