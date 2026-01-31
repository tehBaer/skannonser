#!/usr/bin/env python3
"""
Debug script to test sanitize_for_sheets function.
"""
import sys
import os
import pandas as pd

# Add project root to path for imports
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

from main.database.db import PropertyDatabase
from main.sync.sync_to_sheets import sanitize_for_sheets


def test_sanitize():
    """Test the sanitize function."""
    db = PropertyDatabase()
    
    print("=" * 120)
    print("Testing sanitize_for_sheets Function")
    print("=" * 120)
    print()
    
    # Get the data that would be exported to sheets
    df = db.get_eiendom_for_sheets()
    
    print("BEFORE sanitize_for_sheets:")
    print("-" * 120)
    print(f"Columns: {df.columns.tolist()}")
    print(f"Data types:\n{df.dtypes}")
    print(f"\nFirst row Pris values:")
    print(f"  Type: {type(df.iloc[0]['Pris'])}")
    print(f"  Value: {df.iloc[0]['Pris']}")
    print(f"\nFirst 3 rows:")
    print(df[['Finnkode', 'ADRESSE', 'Pris', 'AREAL']].head(3))
    
    # Now sanitize
    df_sanitized = sanitize_for_sheets(df.copy())
    
    print("\n\nAFTER sanitize_for_sheets:")
    print("-" * 120)
    print(f"Data types:\n{df_sanitized.dtypes}")
    print(f"\nFirst row Pris values:")
    print(f"  Type: {type(df_sanitized.iloc[0]['Pris'])}")
    print(f"  Value: {df_sanitized.iloc[0]['Pris']}")
    print(f"\nFirst 3 rows:")
    print(df_sanitized[['Finnkode', 'ADRESSE', 'Pris', 'AREAL']].head(3))
    
    # Check if Pris values are being lost
    print("\n\nComparison:")
    print("-" * 120)
    original_pris_count = (df['Pris'].notna()).sum()
    sanitized_pris_count = (df_sanitized['Pris'] != '').sum() if isinstance(df_sanitized['Pris'].iloc[0], str) else (df_sanitized['Pris'].notna()).sum()
    
    print(f"Original non-null Pris: {original_pris_count}")
    print(f"Sanitized non-empty Pris: {sanitized_pris_count}")
    
    # Check for empty strings
    if isinstance(df_sanitized['Pris'].iloc[0], str):
        empty_pris = (df_sanitized['Pris'] == '').sum()
        print(f"Sanitized empty Pris strings: {empty_pris}")


if __name__ == '__main__':
    test_sanitize()
