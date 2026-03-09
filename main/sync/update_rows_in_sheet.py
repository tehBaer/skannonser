"""
Update existing rows in Google Sheets with new data from database.
Checks for differences and updates cells that have changed.
"""
import sys
import os
import pandas as pd
from typing import Dict, Set
import re

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from main.database.db import PropertyDatabase
    from main.googleUtils import get_sheets_service, SPREADSHEET_ID
    from main.sync.helper_sync_to_sheets import (
        sanitize_for_sheets,
        ensure_sheet_headers,
        filter_hidden_sheet_columns,
        filter_rows_for_sheet_visibility,
        canonicalize_header_name,
    )
except ImportError:
    from database.db import PropertyDatabase
    from googleUtils import get_sheets_service, SPREADSHEET_ID
    from sync.helper_sync_to_sheets import (
        sanitize_for_sheets,
        ensure_sheet_headers,
        filter_hidden_sheet_columns,
        filter_rows_for_sheet_visibility,
        canonicalize_header_name,
    )
from googleapiclient.errors import HttpError


def normalize_value(val):
    """Normalize values for comparison - strip formatting, convert to comparable form."""
    if pd.isna(val) or val == '':
        return ''
    
    val_str = str(val).strip()
    
    # Remove all whitespace characters (including non-breaking spaces)
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


def format_grouped_int(value: int) -> str:
    """Format integer with spaces as thousands separators."""
    return f"{value:,}".replace(",", " ")


def format_clean_value_for_display(header: str, value) -> str:
    """Return a cleaned, human-friendly display value for selected numeric columns."""
    header_normalized = str(header or "").strip().upper()
    norm = normalize_value(value)
    if norm == "":
        return ""

    try:
        num = int(float(norm))
    except (ValueError, TypeError):
        return str(value)

    if header_normalized == "PRIS KVM":
        return f"{format_grouped_int(num)} kr"
    if header_normalized == "PRIS":
        return f"{format_grouped_int(num)} kr"
    return str(value)


def format_diff_string(header: str, old_val, new_val) -> str:
    """Format diff string for display in confirmation prompts."""
    base = f"{header}: '{old_val}' → '{new_val}'"

    header_normalized = str(header or "").strip().upper()
    if header_normalized not in {"PRIS", "PRIS KVM"}:
        return base

    old_clean = format_clean_value_for_display(header, old_val)
    new_clean = format_clean_value_for_display(header, new_val)
    if not old_clean and not new_clean:
        return base
    return f"{header}: '{old_clean}' → '{new_clean}'"


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


def normalize_finnkode_for_compare(value) -> str:
    """Normalize Finnkode values from Sheets/DB for stable comparison."""
    if value is None:
        return ""

    finnkode = str(value).strip()
    if not finnkode:
        return ""

    # If it's a HYPERLINK formula, extract display text.
    if 'HYPERLINK' in finnkode.upper():
        parts = finnkode.split('"')
        if len(parts) >= 4:
            finnkode = parts[3].strip()

    try:
        as_float = float(finnkode)
        if as_float.is_integer():
            return str(int(as_float))
    except (ValueError, TypeError):
        pass

    return finnkode


def get_sheet_id(service, sheet_name: str):
    """Get numeric Google Sheets sheetId for a tab name."""
    spreadsheet = service.spreadsheets().get(
        spreadsheetId=SPREADSHEET_ID,
        fields="sheets(properties(sheetId,title))"
    ).execute()

    for sheet in spreadsheet.get("sheets", []):
        props = sheet.get("properties", {})
        if props.get("title") == sheet_name:
            return props.get("sheetId")
    return None


def prune_non_visible_rows(service, sheet_name: str, visible_finnkodes: Set[str]) -> bool:
    """Delete rows from sheet where Finnkode is not in current visible set."""
    sheet_data = get_sheet_data_with_row_numbers(service, sheet_name)
    if not sheet_data:
        return True

    header_row = sheet_data.get(1, [])
    if not header_row:
        return True

    header_row_normalized = [canonicalize_header_name(col) for col in header_row]
    if 'Finnkode' in header_row_normalized:
        finnkode_col_idx = header_row_normalized.index('Finnkode')
    else:
        finnkode_col_idx = 0

    rows_to_delete = []
    for row_num, row_data in sheet_data.items():
        if row_num == 1:
            continue
        if not row_data or len(row_data) <= finnkode_col_idx:
            continue

        finnkode = normalize_finnkode_for_compare(row_data[finnkode_col_idx])
        if not finnkode:
            continue

        if finnkode not in visible_finnkodes:
            rows_to_delete.append(row_num)

    if not rows_to_delete:
        print("No non-visible rows to remove from Eie")
        return True

    sheet_id = get_sheet_id(service, sheet_name)
    if sheet_id is None:
        print(f"Could not resolve sheetId for '{sheet_name}', skipping prune")
        return False

    # Delete bottom-up to avoid row index shifts.
    requests = []
    for row_num in sorted(rows_to_delete, reverse=True):
        requests.append({
            "deleteDimension": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "ROWS",
                    "startIndex": row_num - 1,
                    "endIndex": row_num,
                }
            }
        })

    try:
        service.spreadsheets().batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body={"requests": requests}
        ).execute()
        print(f"✓ Removed {len(rows_to_delete)} non-visible rows from {sheet_name}")
        return True
    except HttpError as e:
        print(f"Error pruning non-visible rows from {sheet_name}: {e}")
        return False


def requires_confirmation(old_val, new_val) -> bool:
    """Check if a change from old_val to new_val requires user confirmation."""
    # Consider value non-null if it's not empty/None after normalization
    old_is_non_null = normalize_value(old_val) != ''
    new_is_non_null = normalize_value(new_val) != ''
    
    # Require confirmation if changing from one non-null value to another different non-null value
    return old_is_non_null and new_is_non_null and normalize_value(old_val) != normalize_value(new_val)


def is_api_column(header_name: str) -> bool:
    """Return True for columns that are API-derived travel/commute values."""
    header_upper = str(header_name or "").upper()
    api_tokens = ["PENDL", "BIL", "BRJ", "MVV"]
    return any(token in header_upper for token in api_tokens)


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
    
    # Get all listings and apply temporary sheet visibility rules.
    df = db.get_eiendom_for_sheets()
    df = filter_rows_for_sheet_visibility(df, db)
    
    if df.empty:
        print("No active listings in database")
        return True
    
    # Keep internal-only columns in DB, but hide them from Sheets.
    df = filter_hidden_sheet_columns(df)

    # Sanitize data
    df = sanitize_for_sheets(df)
    df['Finnkode'] = df['Finnkode'].apply(normalize_finnkode_for_compare)
    visible_finnkodes = set(df['Finnkode'].tolist())
    
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
    header_row_normalized = [canonicalize_header_name(col) for col in header_row]

    if 'Finnkode' in header_row_normalized:
        finnkode_col_idx = header_row_normalized.index('Finnkode')
    else:
        finnkode_col_idx = 0
    
    # Build finnkode to row mapping
    finnkode_to_row = {}
    for row_num, row_data in sheet_data.items():
        if row_num == 1:  # Skip header
            continue
        if row_data and len(row_data) > finnkode_col_idx:
            finnkode = str(row_data[finnkode_col_idx]).strip()
            finnkode_to_row[finnkode] = row_num
    
    print(f"Found {len(finnkode_to_row)} existing listings in sheet\n")
    
    # Find updates needed
    updates_list = []
    updated_count = 0
    api_confirmation_list = []
    non_api_confirmation_list = []
    
    for _, db_row in df.iterrows():
        finnkode = str(db_row['Finnkode']).strip()
        
        if finnkode not in finnkode_to_row:
            continue  # Not in sheet, skip
        
        sheet_row_num = finnkode_to_row[finnkode]
        sheet_row_data = sheet_data.get(sheet_row_num, [])
        
        # Build new row data.
        # Preserve existing values for sheet-only/custom columns that are not present
        # in the DB export (for example LAT/LNG used by the interactive map).
        new_row_data = []
        for col in header_row_normalized:
            if col in db_row.index:
                val = db_row.get(col, '')
            else:
                col_idx = header_row_normalized.index(col)
                val = sheet_row_data[col_idx] if col_idx < len(sheet_row_data) else ''
            new_row_data.append('' if pd.isna(val) else val)
        
        # Normalize both for comparison
        sheet_row_normalized = [normalize_value(v) for v in sheet_row_data]
        new_row_normalized = [normalize_value(v) for v in new_row_data]
        
        # Collect differences and split by confirmation/API category
        differences = []
        auto_changes = []
        api_confirmation_changes = []
        non_api_confirmation_changes = []
        
        for i, header in enumerate(header_row_normalized):
            old_val = sheet_row_data[i] if i < len(sheet_row_data) else ''
            new_val = new_row_data[i] if i < len(new_row_data) else ''
            old_norm = sheet_row_normalized[i] if i < len(sheet_row_normalized) else ''
            new_norm = new_row_normalized[i] if i < len(new_row_normalized) else ''
            
            if old_norm != new_norm:
                diff_str = format_diff_string(header, old_val, new_val)
                differences.append(diff_str)
                change = {
                    "col_index": i,
                    "header": header,
                    "old_val": old_val,
                    "new_val": new_val,
                    "diff_str": diff_str,
                }
                if requires_confirmation(old_val, new_val):
                    if is_api_column(header):
                        api_confirmation_changes.append(change)
                    else:
                        non_api_confirmation_changes.append(change)
                else:
                    auto_changes.append(change)
        
        # Only proceed with update if there are actual differences
        if differences:
            update_info = {
                "range": f"{sheet_name}!A{sheet_row_num}",
                "values": [new_row_data],
                "finnkode": finnkode,
                "adresse": db_row.get('ADRESSE', 'N/A'),
                "differences": differences,
                "old_row": sheet_row_data,
                "new_row": new_row_data,
                "auto_changes": auto_changes,
                "api_confirmation_changes": api_confirmation_changes,
                "non_api_confirmation_changes": non_api_confirmation_changes,
            }
            if api_confirmation_changes:
                api_confirmation_list.append(update_info)
            if non_api_confirmation_changes:
                non_api_confirmation_list.append(update_info)

            # If no confirmation is required at all, update immediately.
            if not api_confirmation_changes and not non_api_confirmation_changes:
                updates_list.append(update_info)
                updated_count += 1
                diff_str = " //// ".join(differences)
                print(f"✓ {update_info['adresse']} ({finnkode}): {diff_str}")
    
    # Build per-row accepted changes (auto changes + confirmed categories)
    accepted_changes = {}
    row_context = {}

    def _add_accepted_change(update_info, change):
        key = update_info["range"]
        row_context[key] = update_info
        if key not in accepted_changes:
            accepted_changes[key] = {}
        accepted_changes[key][change["col_index"]] = change

    # Always include non-confirmation changes.
    for group in [api_confirmation_list, non_api_confirmation_list]:
        for update_info in group:
            for change in update_info.get("auto_changes", []):
                _add_accepted_change(update_info, change)

    # Include fully-auto rows that were already added to updates_list.
    for update_info in updates_list:
        key = update_info["range"]
        row_context[key] = update_info
        if key not in accepted_changes:
            accepted_changes[key] = {}
        for idx, (old_val, new_val) in enumerate(zip(update_info.get("old_row", []), update_info.get("new_row", []))):
            if normalize_value(old_val) != normalize_value(new_val):
                accepted_changes[key][idx] = {
                    "col_index": idx,
                    "header": header_row_normalized[idx] if idx < len(header_row_normalized) else str(idx),
                    "old_val": old_val,
                    "new_val": new_val,
                    "diff_str": format_diff_string(
                        header_row_normalized[idx] if idx < len(header_row_normalized) else idx,
                        old_val,
                        new_val,
                    ),
                }

    # Handle API confirmation-required changes first.
    if api_confirmation_list:
        print(f"\n{'='*60}")
        print("⚠️  WARNING: API-derived changes would replace non-null values:")
        print(f"{'='*60}\n")

        total_api_changes = 0
        for update_info in api_confirmation_list:
            api_diffs = [c["diff_str"] for c in update_info.get("api_confirmation_changes", [])]
            if not api_diffs:
                continue
            total_api_changes += len(api_diffs)
            print(f"  {update_info['adresse']} ({update_info['finnkode']})")
            print(f"    {' //// '.join(api_diffs)}\n")

        print(f"Total API field changes requiring confirmation: {total_api_changes}")
        response = input("\nProceed with API-derived changes? (yes/no): ").strip().lower()
        if response in ['yes', 'y']:
            for update_info in api_confirmation_list:
                for change in update_info.get("api_confirmation_changes", []):
                    _add_accepted_change(update_info, change)
        else:
            print(f"\n⚠️  Skipped API confirmation-required changes")

    # Handle non-API confirmation-required changes separately.
    if non_api_confirmation_list:
        print(f"\n{'='*60}")
        print("⚠️  WARNING: Non-API changes would replace non-null values:")
        print(f"{'='*60}\n")

        total_non_api_changes = 0
        for update_info in non_api_confirmation_list:
            non_api_diffs = [c["diff_str"] for c in update_info.get("non_api_confirmation_changes", [])]
            if not non_api_diffs:
                continue
            total_non_api_changes += len(non_api_diffs)
            print(f"  {update_info['adresse']} ({update_info['finnkode']})")
            print(f"    {' //// '.join(non_api_diffs)}\n")

        print(f"Total non-API field changes requiring confirmation: {total_non_api_changes}")
        response = input("\nProceed with non-API changes? (yes/no): ").strip().lower()
        if response in ['yes', 'y']:
            for update_info in non_api_confirmation_list:
                for change in update_info.get("non_api_confirmation_changes", []):
                    _add_accepted_change(update_info, change)
        else:
            print(f"\n⚠️  Skipped non-API confirmation-required changes")

    # Rebuild updates_list from accepted changes, allowing partial row updates per decision.
    updates_list = []
    updated_count = 0
    for range_name, changes_by_col in accepted_changes.items():
        if not changes_by_col:
            continue
        context = row_context[range_name]
        old_row = list(context.get("old_row", []))
        new_row = list(context.get("new_row", []))

        # Ensure row has full width for assignment.
        max_cols = max(len(header_row_normalized), len(old_row), len(new_row))
        while len(old_row) < max_cols:
            old_row.append('')
        while len(new_row) < max_cols:
            new_row.append('')

        final_row = list(old_row)
        for col_idx, change in changes_by_col.items():
            if col_idx < len(new_row):
                final_row[col_idx] = new_row[col_idx]

        # Skip if no effective change remains.
        if [normalize_value(v) for v in final_row] == [normalize_value(v) for v in old_row]:
            continue

        applied_diffs = [changes_by_col[idx]["diff_str"] for idx in sorted(changes_by_col.keys())]
        updates_list.append({"range": range_name, "values": [final_row]})
        updated_count += 1
        print(f"✓ {context['adresse']} ({context['finnkode']}): {' //// '.join(applied_diffs)}")
    
    if not updates_list:
        print("\nNo updates needed - all data is current")
        return prune_non_visible_rows(service, sheet_name, visible_finnkodes)
    
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

        return prune_non_visible_rows(service, sheet_name, visible_finnkodes)
        
    except HttpError as e:
        print(f"Error updating sheet: {e}")
        return False


if __name__ == "__main__":
    update_existing_rows()
