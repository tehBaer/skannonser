#!/usr/bin/env python3
"""
Manual sheet updater.
Run this locally whenever you want to push current DB data to Google Sheets.
"""
import sys
import os

# Add parent and project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

try:
    from main.sync.helper_sync_to_sheets import sync_eiendom_to_sheets, sync_stale_eiendom_to_sheets
    from main.sync.update_rows_in_sheet import update_existing_rows
    from main.tools.sync_finn_polygon_sheet import sync_polygon_to_sheet
    from main.sync.sync_stations_to_sheet import sync_stations_to_sheet
except ImportError:
    from sync.helper_sync_to_sheets import sync_eiendom_to_sheets, sync_stale_eiendom_to_sheets
    from sync.update_rows_in_sheet import update_existing_rows
    from tools.sync_finn_polygon_sheet import sync_polygon_to_sheet
    from sync.sync_stations_to_sheet import sync_stations_to_sheet


if __name__ == "__main__":
    ok_new = sync_eiendom_to_sheets()
    ok_existing = update_existing_rows()
    ok_stale = sync_stale_eiendom_to_sheets()
    ok_polygon = sync_polygon_to_sheet(sheet_name="Finn Polygon Coords", source_file=os.path.join("main", "runners", "run_eiendom_db.py"))
    ok_stations = sync_stations_to_sheet(destination="Sandvika")

    if ok_new and ok_existing and ok_stale and ok_polygon and ok_stations:
        print("\n✓ Manual sheet update completed successfully")
        sys.exit(0)

    print("\n✗ Manual sheet update failed")
    sys.exit(1)
