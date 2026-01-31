"""
Update existing rows in Google Sheets with new data from database.
Checks for differences and updates cells that have changed.
"""
import sys
import os
import pandas as pd
from typing import Dict

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from main.database.db import PropertyDatabase
    from main.googleUtils import get_credentials, SPREADSHEET_ID
    from main.sync.sync_to_sheets import sanitize_for_sheets, ensure_sheet_headers
except ImportError:
    from database.db import PropertyDatabase
    from googleUtils import get_credentials, SPREADSHEET_ID
    from sync.sync_to_sheets import sanitize_for_sheets, ensure_sheet_headers

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


def get_sheet_data_with_row_numbers(service, sheet_name: str) -> Dict:
    """Get all sheet data with row numbers (1-indexed)."""
    try:
        range_name = f"{sheet_name}!A1:Z10000"
        result = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID, 
            range=range_name
        ).execute()
        
        values = result.get("values", [])
        # Returns dict with row numbers as keys (starting from 1 for header)
        return {i + 1: row for i, row in enumerate(values)}
        
    except HttpError as e:
        print(f"Error reading from sheet: {e}")
        return {}


def update_existing_rows(db_path: str = None, sheet_name: str = "Eie"):
    """
    Update existing property listings in Google Sheets with new data from database.
    Checks for differences and updates cells that have changed.
    """
    print(f"\n{'='*60}")
    print(f"Updating existing rows in Google Sheets")
    print(f"Sheet: {sheet_name}")
    print(f"{'='*60}\n")
    
    # Initialize database
    db = PropertyDatabase(db_path)
    
    # Get credentials and service
    try:
        creds = get_credentials()
        service = build("sheets", "v4", credentials=creds)
    except Exception as e:
        print(f"Error connecting to Google Sheets: {e}")
        return False
    
    # Get all active listings from database
    df = db.get_eiendom_for_sheets()
    
    if df.empty:
        print("No active listings in database")
        return True
    
    # Sanitize data
    df = sanitize_for_sheets(df)
    df['Finnkode'] = df['Finnkode'].astype(str).str.strip()
    
    # Ensure headers contain any new columns
    ensure_sheet_headers(service, sheet_name, list(df.columns))

    # Get sheet data with row numbers
    sheet_data = get_sheet_data_with_row_numbers(service, sheet_name)
    
    if not sheet_data:
        print("No data in sheet")
        return False
    
    # Extract header
    header_row = sheet_data.get(1, [])
    if not header_row:
        print("Could not find header row")
        return False

    # Normalize header (strip whitespace from column names)
    header_row_normalized = [col.strip() for col in header_row]
    print(f"Sheet columns: {header_row_normalized}")
    
    # Build finnkode to row mapping
    finnkode_to_row = {}
    for row_num, row_data in sheet_data.items():
        if row_num == 1:  # Skip header
            continue
        if row_data and len(row_data) > 0:
            finnkode = str(row_data[0]).strip()
            finnkode_to_row[finnkode] = row_num
    
    print(f"Found {len(finnkode_to_row)} existing listings in sheet\n")
    
    # Find updates needed
    updates_list = []
    updated_count = 0
    
    for _, db_row in df.iterrows():
        finnkode = str(db_row['Finnkode']).strip()
        
        if finnkode not in finnkode_to_row:
            continue  # Not in sheet, skip
        
        sheet_row_num = finnkode_to_row[finnkode]
        sheet_row_data = sheet_data.get(sheet_row_num, [])
        
        # Build new row data
        new_row_data = []
        for col in header_row_normalized:
            val = db_row.get(col, '')
            new_row_data.append('' if pd.isna(val) else val)
        
        # Check if any data is different
        if sheet_row_data != new_row_data:
            updates_list.append({
                "range": f"{sheet_name}!A{sheet_row_num}",
                "values": [new_row_data]
            })
            updated_count += 1
            
            # Output what's changing
            print(f"✓ {finnkode} needs update")
            for i, (header, old_val, new_val) in enumerate(zip(header_row_normalized, sheet_row_data, new_row_data)):
                if old_val != new_val:
                    print(f"    {header}: '{old_val}' → '{new_val}'")
    
    if not updates_list:
        print("\nNo updates needed - all data is current")
        return True
    
    # Apply updates using batch update
    try:
        body = {"data": updates_list, "valueInputOption": "USER_ENTERED"}
        result = service.spreadsheets().values().batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body=body
        ).execute()
        
        total_updated = result.get('totalUpdatedCells', 0)
        print(f"\n✓ Successfully updated {updated_count} rows ({total_updated} cells)")
        
        return True
        
    except HttpError as e:
        print(f"Error updating sheet: {e}")
        return False


if __name__ == "__main__":
    update_existing_rows()
