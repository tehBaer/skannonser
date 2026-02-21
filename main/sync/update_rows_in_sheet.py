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
    from main.googleUtils import get_sheets_service, SPREADSHEET_ID
    from main.sync.helper_sync_to_sheets import sanitize_for_sheets, ensure_sheet_headers
except ImportError:
    from database.db import PropertyDatabase
    from googleUtils import get_sheets_service, SPREADSHEET_ID
    from sync.helper_sync_to_sheets import sanitize_for_sheets, ensure_sheet_headers
from googleapiclient.errors import HttpError


def normalize_value(val):
    """Normalize values for comparison - strip formatting, convert to comparable form."""
    if pd.isna(val) or val == '':
        return ''
    
    val_str = str(val).strip()
    
    # Remove all whitespace characters (including non-breaking spaces)
    import re
    val_str = re.sub(r'\s+', '', val_str)
    
    # Remove currency formatting
    # Example: "7144740kr" → "7144740"
    val_str = val_str.lower().replace('kr', '').strip()
    
    # For numeric values, try to normalize
    try:
        # Try to parse as number
        num = float(val_str.replace(',', ''))
        # If it's a whole number, return as int string
        if num == int(num):
            return str(int(num))
        return str(num)
    except (ValueError, TypeError):
        # Not a number, just return the cleaned string
        return val_str


def get_sheet_data_with_row_numbers(service, sheet_name: str) -> Dict:
    """Get all sheet data with row numbers (1-indexed)."""
    try:
        range_name = f"{sheet_name}!A1:AZ10000"
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


def requires_confirmation(old_val, new_val) -> bool:
    """Check if a change from old_val to new_val requires user confirmation."""
    # Consider value non-null if it's not empty/None after normalization
    old_is_non_null = normalize_value(old_val) != ''
    new_is_non_null = normalize_value(new_val) != ''
    
    # Require confirmation if changing from one non-null value to another different non-null value
    return old_is_non_null and new_is_non_null and normalize_value(old_val) != normalize_value(new_val)


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
        service = get_sheets_service()
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
    requires_confirmation_list = []
    
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
        
        # Normalize both for comparison
        sheet_row_normalized = [normalize_value(v) for v in sheet_row_data]
        new_row_normalized = [normalize_value(v) for v in new_row_data]
        
        # Collect differences and check if any require confirmation
        differences = []
        needs_confirmation = False
        
        for i, header in enumerate(header_row_normalized):
            old_val = sheet_row_data[i] if i < len(sheet_row_data) else ''
            new_val = new_row_data[i] if i < len(new_row_data) else ''
            old_norm = sheet_row_normalized[i] if i < len(sheet_row_normalized) else ''
            new_norm = new_row_normalized[i] if i < len(new_row_normalized) else ''
            
            if old_norm != new_norm:
                differences.append(f"{header}: '{old_val}' → '{new_val}'")
                if requires_confirmation(old_val, new_val):
                    needs_confirmation = True
        
        # Only proceed with update if there are actual differences
        if differences:
            update_info = {
                "range": f"{sheet_name}!A{sheet_row_num}",
                "values": [new_row_data],
                "finnkode": finnkode,
                "adresse": db_row.get('ADRESSE', 'N/A'),
                "differences": differences
            }
            
            if needs_confirmation:
                requires_confirmation_list.append(update_info)
            else:
                updates_list.append(update_info)
                updated_count += 1
                
                # Output on single line with property address
                diff_str = " //// ".join(differences)
                print(f"✓ {update_info['adresse']} ({finnkode}): {diff_str}")
    
    # Handle confirmation for non-null to non-null changes
    if requires_confirmation_list:
        print(f"\n{'='*60}")
        print(f"⚠️  WARNING: The following changes would replace non-null values:")
        print(f"{'='*60}\n")
        
        for update_info in requires_confirmation_list:
            diff_str = " //// ".join(update_info['differences'])
            print(f"  {update_info['adresse']} ({update_info['finnkode']})")
            print(f"    {diff_str}\n")
        
        print(f"Total changes requiring confirmation: {len(requires_confirmation_list)}")
        response = input("\nDo you want to proceed with these changes? (yes/no): ").strip().lower()
        
        if response in ['yes', 'y']:
            # Add confirmed updates to the main updates list
            for update_info in requires_confirmation_list:
                updates_list.append({
                    "range": update_info["range"],
                    "values": update_info["values"]
                })
                updated_count += 1
                diff_str = " //// ".join(update_info['differences'])
                print(f"✓ {update_info['adresse']} ({update_info['finnkode']}): {diff_str}")
        else:
            print(f"\n⚠️  Skipped {len(requires_confirmation_list)} changes requiring confirmation")
    
    if not updates_list:
        print("\nNo updates needed - all data is current")
        return True
    
    # Apply updates using batch update
    try:
        # Extract only range and values for API call
        batch_data = [{"range": u["range"], "values": u["values"]} for u in updates_list]
        body = {"data": batch_data, "valueInputOption": "USER_ENTERED"}
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
