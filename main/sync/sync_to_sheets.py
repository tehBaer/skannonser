"""
Sync database data to Google Sheets for visualization.
This maintains the Google Sheets interface while using a database backend.
"""
import sys
import os
import pandas as pd
from typing import List

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from main.database.db import PropertyDatabase
    from main.googleUtils import get_credentials, SPREADSHEET_ID, download_sheet_as_csv
except ImportError:
    from database.db import PropertyDatabase
    from googleUtils import get_credentials, SPREADSHEET_ID, download_sheet_as_csv

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


def sanitize_for_sheets(df: pd.DataFrame) -> pd.DataFrame:
    """Clean data for Google Sheets export."""
    # Convert commute columns to integers without decimals before fillna
    commute_cols = ['PENDL MORN BRJ', 'BIL MORN BRJ', 'PENDL DAG BRJ', 'BIL DAG BRJ',
                    'PENDL MORN MVV', 'BIL MORN MVV', 'PENDL DAG MVV', 'BIL DAG MVV']
    for col in commute_cols:
        if col in df.columns:
            # Convert to numeric, round, and convert to int (NaN becomes empty string in next step)
            df[col] = pd.to_numeric(df[col], errors='coerce').round()
    
    # Replace NaN with empty string
    df = df.fillna('')
    
    # Convert numeric columns that should be integers (after fillna converted NaN to '')
    for col in commute_cols:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: int(x) if x != '' and pd.notna(x) else x)
    
    # Clean strings: replace newlines and strip whitespace
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].apply(lambda x: str(x).replace('\n', ' ').replace('\r', ' ').strip() if x else '')
    
    return df


def ensure_sheet_headers(service, sheet_name: str, desired_columns: List[str]) -> List[str]:
    """Ensure sheet header includes all desired columns, appending missing ones."""
    range_name = f"{sheet_name}!A1:Z1"
    result = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=range_name
    ).execute()

    header_row = result.get("values", [[]])
    header = header_row[0] if header_row else []
    header_normalized = [col.strip() for col in header]

    missing = [col for col in desired_columns if col not in header_normalized]

    if not header:
        updated_header = desired_columns
    elif missing:
        updated_header = header + missing
    else:
        updated_header = header

    if updated_header != header:
        service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{sheet_name}!A1",
            valueInputOption="RAW",
            body={"values": [updated_header]}
        ).execute()
        print(f"✓ Updated sheet headers with {len(updated_header)} columns")

    return [col.strip() for col in updated_header]


def get_existing_finnkodes_from_sheet(service, sheet_name: str) -> List[str]:
    """Get all existing Finnkodes from the Google Sheet."""
    try:
        range_name = f"{sheet_name}!A2:A10000"  # Start from row 2 (skip header)
        result = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID, 
            range=range_name
        ).execute()
        
        values = result.get("values", [])
        
        # Extract Finnkodes (they might be wrapped in HYPERLINK formulas)
        finnkodes = []
        for row in values:
            if row and len(row) > 0:
                finnkode = row[0]
                # If it's a HYPERLINK formula, extract the display text
                if isinstance(finnkode, str) and 'HYPERLINK' in finnkode:
                    # Extract from: =HYPERLINK("url", "finnkode")
                    parts = finnkode.split('"')
                    if len(parts) >= 4:
                        finnkode = parts[3]
                finnkodes.append(str(finnkode).strip())
        
        return finnkodes
        
    except HttpError as e:
        print(f"Error reading from sheet: {e}")
        return []


def sync_eiendom_to_sheets(db_path: str = None, sheet_name: str = "Eie"):
    """
    Sync property listings from database to Google Sheets.
    Only adds new listings that aren't already in the sheet.
    
    Args:
        db_path: Optional path to database file
        sheet_name: Name of the sheet to sync to (default: "Eie")
    """
    print(f"\n{'='*60}")
    print(f"Syncing eiendom data to Google Sheets")
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
        print("No active listings to sync")
        return True
    
    print(f"Found {len(df)} active listings in database")
    
    # Get existing Finnkodes from sheet
    existing_finnkodes = get_existing_finnkodes_from_sheet(service, sheet_name)
    print(f"Found {len(existing_finnkodes)} existing listings in Google Sheets")
    
    # Filter to only new listings
    df['Finnkode'] = df['Finnkode'].astype(str).str.strip()
    new_listings = df[~df['Finnkode'].isin(existing_finnkodes)]
    
    if new_listings.empty:
        print("No new listings to add to Google Sheets")
        return True
    
    print(f"Found {len(new_listings)} new listings to add")
    
    # Sanitize data
    new_listings = sanitize_for_sheets(new_listings)

    # Ensure headers contain new columns and align row order
    desired_columns = list(new_listings.columns)
    header_row = ensure_sheet_headers(service, sheet_name, desired_columns)
    new_rows = []
    for _, row in new_listings.iterrows():
        new_rows.append([row.get(col, '') for col in header_row])
    
    # Find the next available row in the sheet
    try:
        range_name = f"{sheet_name}!A1:H10000"
        result = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID, 
            range=range_name
        ).execute()
        existing_data = result.get("values", [])
        next_row = len(existing_data) + 1
    except HttpError as e:
        print(f"Error finding next row: {e}")
        next_row = 2  # Default to row 2 if error
    
    # Append new rows to the sheet
    try:
        body = {"values": new_rows}
        result = service.spreadsheets().values().append(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{sheet_name}!A{next_row}",
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body=body
        ).execute()
        
        updates = result.get('updates', {})
        updated_rows = updates.get('updatedRows', 0)
        
        print(f"\n✓ Successfully added {updated_rows} new listings to Google Sheets")
        
        # Mark as exported in database
        exported_finnkodes = new_listings['Finnkode'].tolist()
        clean_finnkodes = [str(fk) for fk in exported_finnkodes]
        
        marked = db.mark_as_exported('eiendom', clean_finnkodes)
        print(f"✓ Marked {marked} listings as exported in database")
        
        return True
        
    except HttpError as e:
        print(f"Error appending to sheet: {e}")
        return False


def sync_unlisted_eiendom_to_sheets(db_path: str = None, sheet_name: str = "Eie(unlisted)"):
    """
    Sync unlisted property listings from database to Google Sheets.
    Only adds new unlisted listings that aren't already in the sheet.
    
    Args:
        db_path: Optional path to database file
        sheet_name: Name of the sheet to sync to (default: "Eie(unlisted)")
    """
    # Check if unlisted should be included
    try:
        from main.config.filters import INCLUDE_UNLISTED
    except ImportError:
        try:
            from config.filters import INCLUDE_UNLISTED
        except ImportError:
            INCLUDE_UNLISTED = True
    
    if not INCLUDE_UNLISTED:
        print("INCLUDE_UNLISTED is disabled - skipping unlisted sync")
        return True
    
    print(f"\n{'='*60}")
    print(f"Syncing unlisted eiendom data to Google Sheets")
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
    
    # Get all unlisted listings from database
    df = db.get_unlisted_eiendom_for_sheets()
    
    if df.empty:
        print("No unlisted listings to sync")
        return True
    
    print(f"Found {len(df)} unlisted listings in database")
    
    # Get existing Finnkodes from sheet
    existing_finnkodes = get_existing_finnkodes_from_sheet(service, sheet_name)
    print(f"Found {len(existing_finnkodes)} existing listings in Google Sheets")
    
    # Filter to only new listings
    df['Finnkode'] = df['Finnkode'].astype(str).str.strip()
    new_listings = df[~df['Finnkode'].isin(existing_finnkodes)]
    
    if new_listings.empty:
        print("No new unlisted listings to add to Google Sheets")
        return True
    
    print(f"Found {len(new_listings)} new unlisted listings to add")
    
    # Sanitize data
    new_listings = sanitize_for_sheets(new_listings)

    # Ensure headers contain new columns and align row order
    desired_columns = list(new_listings.columns)
    header_row = ensure_sheet_headers(service, sheet_name, desired_columns)
    new_rows = []
    for _, row in new_listings.iterrows():
        new_rows.append([row.get(col, '') for col in header_row])
    
    # Find the next available row in the sheet
    try:
        range_name = f"{sheet_name}!A1:H10000"
        result = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID, 
            range=range_name
        ).execute()
        existing_data = result.get("values", [])
        next_row = len(existing_data) + 1
    except HttpError as e:
        print(f"Error finding next row: {e}")
        next_row = 2  # Default to row 2 if error
    
    # Append new rows to the sheet
    try:
        body = {"values": new_rows}
        result = service.spreadsheets().values().append(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{sheet_name}!A{next_row}",
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body=body
        ).execute()
        
        updates = result.get('updates', {})
        updated_rows = updates.get('updatedRows', 0)
        
        print(f"\n✓ Successfully added {updated_rows} new unlisted listings to Google Sheets")
        
        # Mark as exported in database
        exported_finnkodes = new_listings['Finnkode'].tolist()
        clean_finnkodes = [str(fk) for fk in exported_finnkodes]
        
        marked = db.mark_as_exported('eiendom', clean_finnkodes)
        print(f"✓ Marked {marked} unlisted listings as exported in database")
        
        return True
        
    except HttpError as e:
        print(f"Error appending to sheet: {e}")
        return False


def full_sync_eiendom_to_sheets(db_path: str = None, sheet_name: str = "Eie"):
    """
    Perform a full sync - replace all data in Google Sheets with database data.
    WARNING: This will overwrite the entire sheet!
    
    Args:
        db_path: Optional path to database file
        sheet_name: Name of the sheet to sync to (default: "Eie")
    """
    print(f"\n{'='*60}")
    print(f"FULL SYNC: Replacing all data in Google Sheets")
    print(f"Sheet: {sheet_name}")
    print(f"{'='*60}\n")
    
    response = input("Proceed with Google Sheets API calls? (yes/no): ").strip().lower()
    if response not in ['yes', 'y']:
        print("✗ Full sync cancelled. No changes made.")
        return False

    response = input("This will OVERWRITE all data in the sheet. Continue? (yes/no): ")
    if response.lower() != 'yes':
        print("Sync cancelled")
        return False
    
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
        print("No active listings to sync")
        return True
    
    print(f"Syncing {len(df)} listings to Google Sheets")
    
    # Sanitize data
    df = sanitize_for_sheets(df)
    
    # Convert to list format (header + data)
    all_rows = [df.columns.tolist()] + df.values.tolist()
    
    # Clear existing data and write new data
    try:
        # Clear the sheet
        service.spreadsheets().values().clear(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{sheet_name}!A1:Z10000"
        ).execute()
        
        # Write new data
        body = {"values": all_rows}
        result = service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{sheet_name}!A1",
            valueInputOption="USER_ENTERED",
            body=body
        ).execute()
        
        updated_cells = result.get('updatedCells', 0)
        print(f"\n✓ Successfully synced {len(df)} listings to Google Sheets ({updated_cells} cells updated)")
        
        # Mark all as exported
        all_finnkodes = df['Finnkode'].tolist()
        clean_finnkodes = [str(fk) for fk in all_finnkodes]
        
        marked = db.mark_as_exported('eiendom', clean_finnkodes)
        print(f"✓ Marked {marked} listings as exported in database")
        
        return True
        
    except HttpError as e:
        print(f"Error syncing to sheet: {e}")
        return False


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Sync database to Google Sheets')
    parser.add_argument('--full', action='store_true', 
                       help='Perform full sync (overwrite all data)')
    parser.add_argument('--sheet', default='Eie',
                       help='Sheet name to sync to (default: Eie)')
    
    args = parser.parse_args()
    
    if args.full:
        full_sync_eiendom_to_sheets(sheet_name=args.sheet)
    else:
        sync_eiendom_to_sheets(sheet_name=args.sheet)
