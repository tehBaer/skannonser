"""
Add new listings to Google Sheets.
Only adds property listings that aren't already in the sheet.
"""
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from main.sync.sync_to_sheets import sync_eiendom_to_sheets, sync_unlisted_eiendom_to_sheets
except ImportError:
    from sync.sync_to_sheets import sync_eiendom_to_sheets, sync_unlisted_eiendom_to_sheets

if __name__ == "__main__":
    sync_eiendom_to_sheets()
    sync_unlisted_eiendom_to_sheets()
