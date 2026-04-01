#!/usr/bin/env python3
"""Backfill station transfer travel and optionally clean legacy destinations.

Default behavior:
1) Compute Sandvika transfer minutes as:
   station->Oslo S + Oslo S->Sandvika (same line)
2) Write transfer minutes to destination "Sandvika Transfer".
3) Sync Stations sheet so TO_SANDVIKA and TO_SANDVIKA_TRANSFER are exported.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from main.database.stations import StationDatabase
from main.sync.sync_stations_to_sheet import sync_stations_to_sheet


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill station transfer times and remove legacy destinations"
    )
    parser.add_argument(
        "--drop-destination",
        action="append",
        default=[],
        help="Destination name to delete from station_travel (repeatable)",
    )
    parser.add_argument(
        "--from-destination",
        default="Oslo S",
        help="Base destination used from station to via-station",
    )
    parser.add_argument(
        "--via-station",
        default="Oslo S",
        help="Via station name used for transfer leg",
    )
    parser.add_argument(
        "--to-destination",
        default="Sandvika",
        help="Final direct destination leg from via station",
    )
    parser.add_argument(
        "--transfer-destination",
        default="Sandvika Transfer",
        help="Destination label used for computed transfer rows",
    )
    parser.add_argument(
        "--stations-sheet",
        default="Stations",
        help="Stations sheet tab name",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing transfer destination rows",
    )
    parser.add_argument(
        "--no-sync",
        action="store_true",
        help="Skip Stations sheet sync after DB updates",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    db = StationDatabase()

    result = db.backfill_transfer_destination(
        from_destination=args.from_destination,
        via_station_name=args.via_station,
        to_destination=args.to_destination,
        transfer_destination=args.transfer_destination,
        overwrite=args.overwrite,
    )
    print(
        "Transfer backfill:",
        f"updated={result.get('updated', 0)}",
        f"skipped_missing_via_leg={result.get('skipped_missing_via_leg', 0)}",
        f"skipped_existing={result.get('skipped_existing', 0)}",
    )

    total_deleted = 0
    for destination in args.drop_destination:
        if not destination or not str(destination).strip():
            continue
        before = db.count_station_travel_for_destination(destination)
        deleted = db.delete_station_travel_for_destination(destination)
        total_deleted += deleted
        print(f"Deleted destination '{destination}': {deleted} rows (before={before})")

    if not args.no_sync:
        ok = sync_stations_to_sheet(sheet_name=args.stations_sheet, destination=args.to_destination)
        if not ok:
            print("Stations sheet sync failed")
            return 2

    print(
        "Done:",
        f"deleted_legacy_rows={total_deleted}",
        f"db_station_travel_total={db.count_station_travel()}",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
