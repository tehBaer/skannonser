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
    from main.sync.helper_sync_to_sheets import sync_eiendom_to_sheets
    from main.sync.update_rows_in_sheet import update_existing_rows
except ImportError:
    from sync.helper_sync_to_sheets import sync_eiendom_to_sheets
    from sync.update_rows_in_sheet import update_existing_rows


if __name__ == "__main__":
    ok_new = sync_eiendom_to_sheets()
    ok_existing = update_existing_rows()

    if ok_new and ok_existing:
        print("\n✓ Manual sheet update completed successfully")
        sys.exit(0)

    print("\n✗ Manual sheet update failed")
    sys.exit(1)
