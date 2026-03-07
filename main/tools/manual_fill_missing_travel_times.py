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
import argparse
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
    'PENDL DAG BRJ',
    'PENDL MORN MVV',
    'PENDL DAG MVV',
]

TRANSIT_COLS = [
    'PENDL MORN BRJ',
    'PENDL DAG BRJ',
    'PENDL MORN MVV',
    'PENDL DAG MVV',
]

# Google Maps Routes API pricing assumptions (USD), last checked 2026-02
FREE_CAP_ESSENTIALS = 10_000
PRICE_ESSENTIALS_PER_1K = 5.0

TARGET_COLUMNS = {
    'all': COMMUTE_COLS,
    'brj': ['PENDL MORN BRJ', 'PENDL DAG BRJ'],
    'mvv': ['PENDL MORN MVV', 'PENDL DAG MVV'],
}


def _get_max_price():
    try:
        from main.config.filters import MAX_PRICE
        return MAX_PRICE
    except ImportError:
        try:
            from config.filters import MAX_PRICE
            return MAX_PRICE
        except ImportError:
            return None


def print_preflight_estimate(df_missing: pd.DataFrame, target_columns: list[str]) -> None:
    max_price = _get_max_price()

    eligible_mask = pd.Series([True] * len(df_missing), index=df_missing.index)
    if max_price is not None and 'Pris' in df_missing.columns:
        eligible_mask = df_missing['Pris'].fillna(0) <= max_price

    df_eligible = df_missing.loc[eligible_mask]
    skipped_due_to_price = len(df_missing) - len(df_eligible)

    missing_by_col = {
        col: int(df_eligible[col].isna().sum())
        for col in target_columns
        if col in df_eligible.columns
    }

    transit_requests = sum(missing_by_col.get(col, 0) for col in target_columns)
    total_requests = transit_requests

    billable_essentials = max(0, transit_requests - FREE_CAP_ESSENTIALS)

    estimated_cost_essentials = (billable_essentials / 1000) * PRICE_ESSENTIALS_PER_1K
    estimated_total_cost = estimated_cost_essentials

    print("\n=== Google Routes Preflight Estimate ===")
    print(f"Listings with missing travel fields: {len(df_missing)}")
    print(f"Listings eligible for API calls: {len(df_eligible)}")
    if max_price is not None:
        print(f"MAX_PRICE filter: {max_price} (skipped by price: {skipped_due_to_price})")

    print("\nEstimated requests (eligible listings only):")
    print(f"  Essentials-like (TRANSIT): {transit_requests}")
    print(f"  Total: {total_requests}")

    print("\nFree cap check (monthly, per SKU):")
    print(
        f"  Essentials cap {FREE_CAP_ESSENTIALS}: "
        f"{max(0, FREE_CAP_ESSENTIALS - transit_requests)} remaining"
    )

    print("\nEstimated overage (if this is your only usage in the month):")
    print(f"  Essentials billable requests: {billable_essentials}")
    print(f"  Estimated cost: ${estimated_total_cost:.2f} USD")

    print("\nMissing fields by column (eligible listings):")
    for col in target_columns:
        print(f"  {col}: {missing_by_col.get(col, 0)}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fill missing travel-time fields, with optional preflight estimate only"
    )
    parser.add_argument(
        "--estimate-only",
        action="store_true",
        help="Only print Google Routes request/cost estimate; do not call APIs or update DB",
    )
    parser.add_argument(
        "--target",
        choices=["all", "brj", "mvv"],
        default="all",
        help="Select which transit destination group to fill",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    db = PropertyDatabase()

    df = db.get_eiendom_for_sheets()
    if df.empty:
        print("No eiendom listings found in database.")
        return 0

    target_columns = TARGET_COLUMNS[args.target]

    for col in target_columns:
        if col not in df.columns:
            df[col] = pd.NA

    missing_mask = df[target_columns].isna().any(axis=1)
    df_missing = df.loc[missing_mask].copy()

    if df_missing.empty:
        print("All listings already have commute/travel fields populated.")
        return 0

    # post_process_eiendom expects 'Adresse' (not 'ADRESSE')
    if 'ADRESSE' in df_missing.columns and 'Adresse' not in df_missing.columns:
        df_missing.rename(columns={'ADRESSE': 'Adresse'}, inplace=True)

    print(f"Found {len(df_missing)} listings with at least one missing travel field for target '{args.target}'.")
    print_preflight_estimate(df_missing, target_columns)

    if args.estimate_only:
        print("\nEstimate-only mode: no API calls made and no database updates applied.")
        return 0

    print("You will be prompted for confirmation and optional request rate.")

    processed = post_process_eiendom(
        df_missing,
        projectName='data/eiendom',
        db=db,
        calculate_location_features=True,
        calculate_google_directions=True,
        travel_targets=args.target,
    )

    inserted, updated = db.insert_or_update_eiendom(processed)
    print(f"Done. Database rows touched: {inserted} inserted, {updated} updated")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
