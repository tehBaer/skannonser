#!/usr/bin/env python3
"""
Check what columns are actually in the Google Sheet.
"""
import sys
import os

# Add project root to path for imports
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

try:
    from main.googleUtils import get_credentials, SPREADSHEET_ID
except ImportError:
    from googleUtils import get_credentials, SPREADSHEET_ID

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


def check_sheet_headers():
    """Check the headers and first few rows in Google Sheets."""
    try:
        creds = get_credentials()
        service = build("sheets", "v4", credentials=creds)
    except Exception as e:
        print(f"Error connecting to Google Sheets: {e}")
        return
    
    sheet_name = "Eie"
    
    print("=" * 120)
    print(f"Checking Google Sheet: {sheet_name}")
    print("=" * 120)
    print()
    
    try:
        # Get all data
        range_name = f"{sheet_name}!A1:Z100"
        result = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID, 
            range=range_name
        ).execute()
        
        values = result.get("values", [])
        
        if not values:
            print("No data in sheet")
            return
        
        # Get header row
        header = values[0] if values else []
        
        print(f"Header row (columns): {header}")
        print(f"Total columns: {len(header)}")
        print()
        
        # Show where Pris is
        if 'Pris' in header:
            pris_index = header.index('Pris')
            print(f"✓ 'Pris' column found at position {pris_index} (column {chr(65 + pris_index)})")
        else:
            print("✗ 'Pris' column NOT found in header")
            print(f"  Available columns: {header}")
        
        # Show first 3 data rows
        print("\nFirst 3 data rows:")
        print("-" * 120)
        
        for i, row in enumerate(values[1:4], start=1):
            # Pad row to match header length
            padded_row = row + [''] * (len(header) - len(row))
            
            print(f"\nRow {i+1}:")
            for col_idx, (col_name, value) in enumerate(zip(header, padded_row)):
                display_val = str(value)[:50] if value else "(empty)"
                print(f"  {col_name:15} ({chr(65 + col_idx)}): {display_val}")
        
    except HttpError as e:
        print(f"Error reading from sheet: {e}")


if __name__ == '__main__':
    check_sheet_headers()
