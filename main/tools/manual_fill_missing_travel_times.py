#!/usr/bin/env python3
"""
Manually fill missing travel-time fields for eiendom listings.

This script:
- Reads existing listings from the local database
- Filters to listings with missing commute/travel fields
- Runs post_process_eiendom location calculations only for those rows
- Writes the updated commute values back to the database

No scraping and no Google Sheets sync are performed.
"""
import os
import sys

# Add parent and project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import pandas as pd

try:
    from main.database.db import PropertyDatabase
    from main.post_process import post_process_eiendom
except ImportError:
    from database.db import PropertyDatabase
    from post_process import post_process_eiendom


COMMUTE_COLS = [
    'PENDL MORN BRJ',
    'BIL MORN BRJ',
    'PENDL DAG BRJ',
    'BIL DAG BRJ',
    'PENDL MORN MVV',
    'BIL MORN MVV',
    'PENDL DAG MVV',
    'BIL DAG MVV',
]


def main() -> int:
    db = PropertyDatabase()

    df = db.get_eiendom_for_sheets()
    if df.empty:
        print("No eiendom listings found in database.")
        return 0

    for col in COMMUTE_COLS:
        if col not in df.columns:
            df[col] = pd.NA

    missing_mask = df[COMMUTE_COLS].isna().any(axis=1)
    df_missing = df.loc[missing_mask].copy()

    if df_missing.empty:
        print("All listings already have commute/travel fields populated.")
        return 0

    # post_process_eiendom expects 'Adresse' (not 'ADRESSE')
    if 'ADRESSE' in df_missing.columns and 'Adresse' not in df_missing.columns:
        df_missing.rename(columns={'ADRESSE': 'Adresse'}, inplace=True)

    print(f"Found {len(df_missing)} listings with at least one missing travel field.")
    print("You will be prompted for confirmation and optional request rate.")

    processed = post_process_eiendom(
        df_missing,
        projectName='data/eiendom',
        db=db,
        calculate_location_features=True,
    )

    inserted, updated = db.insert_or_update_eiendom(processed)
    print(f"Done. Database rows touched: {inserted} inserted, {updated} updated")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
