#!/usr/bin/env python3
"""
Debug script to check what data is being exported to sheets.
"""
import sys
import os

# Add project root to path for imports
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

from main.database.db import PropertyDatabase


def check_sheets_export():
    """Check what data is prepared for sheets export."""
    db = PropertyDatabase()
    
    print("=" * 120)
    print("Checking Data Exported to Sheets")
    print("=" * 120)
    print()
    
    # Get the data that would be exported to sheets
    df = db.get_eiendom_for_sheets()
    
    print(f"Total records: {len(df)}")
    print(f"Columns: {df.columns.tolist()}")
    print()
    
    # Check for Pris column
    if 'Pris' in df.columns:
        print("✓ 'Pris' column exists")
        
        # Check how many have values
        with_pris = df['Pris'].notna().sum()
        without_pris = df['Pris'].isna().sum()
        
        print(f"  Records with Pris: {with_pris}")
        print(f"  Records without Pris: {without_pris}")
        
        if with_pris > 0:
            print(f"  Min: {df['Pris'].min():,}")
            print(f"  Max: {df['Pris'].max():,}")
            print(f"  Mean: {df['Pris'].mean():,.0f}")
    else:
        print("✗ 'Pris' column NOT in export data")
    
    print("\n" + "-" * 120)
    print("First 5 rows of export data:")
    print("-" * 120)
    
    # Show first few rows
    pd_display = df.head(5).to_string()
    print(pd_display)
    
    print("\n" + "-" * 120)
    print("Column data types:")
    print("-" * 120)
    print(df.dtypes)
    
    print("\n" + "=" * 120)


if __name__ == '__main__':
    import pandas as pd
    check_sheets_export()
