#!/usr/bin/env python3
"""Estimate travel API candidates for run_eiendom_db.py --step process.

This mirrors the pre-API missing-count logic in post_process_eiendom for the
FINN process path:
- Source rows from data/eiendom/A_live.csv
- Merge existing travel values from DB by Finnkode
- Apply MAX_PRICE eligibility filter
- Count missing target travel fields (NaN only)
"""

import argparse
import os
import sys
from typing import Dict

import pandas as pd

# Add parent and project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

try:
    from main.database.db import PropertyDatabase
except ImportError:
    from database.db import PropertyDatabase


TARGET_COLUMNS = {
    "all": ["PENDL RUSH BRJ", "PENDL RUSH MVV"],
    "brj": ["PENDL RUSH BRJ"],
    "mvv": ["PENDL RUSH MVV"],
}


def _get_max_price():
    try:
        from main.config.filters import SHEETS_MAX_PRICE
        return SHEETS_MAX_PRICE
    except Exception:
        try:
            from config.filters import SHEETS_MAX_PRICE
            return SHEETS_MAX_PRICE
        except Exception:
            return None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Estimate process-step travel API candidates")
    parser.add_argument(
        "--target",
        choices=["all", "brj", "mvv"],
        default="all",
        help="Which travel destination group to estimate",
    )
    parser.add_argument(
        "--csv",
        default="data/eiendom/A_live.csv",
        help="Input FINN CSV used by process step (default: data/eiendom/A_live.csv)",
    )
    parser.add_argument(
        "--format",
        choices=["text", "count"],
        default="text",
        help="Output style: human text or just total count",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if not os.path.exists(args.csv):
        if args.format == "count":
            print("0")
        else:
            print(f"Process travel estimate: input CSV not found: {args.csv}")
            print("Candidates: 0")
        return 0

    df = pd.read_csv(args.csv)
    if df.empty:
        if args.format == "count":
            print("0")
        else:
            print("Process travel estimate: no rows in input CSV")
            print("Candidates: 0")
        return 0

    target_cols = TARGET_COLUMNS[args.target]

    db = PropertyDatabase()
    if hasattr(db, "get_eiendom_commute_data"):
        existing_data = db.get_eiendom_commute_data()
    else:
        existing_data = db.get_eiendom_for_sheets()

    commute_columns = [
        "Finnkode",
        "PENDL RUSH BRJ",
        "PENDL RUSH MVV",
        "PENDL MORN CNTR",
        "BIL MORN CNTR",
        "PENDL DAG CNTR",
        "BIL DAG CNTR",
        "TRAVEL_COPY_FROM_FINNKODE",
    ]

    existing_commute_cols = ["Finnkode"] + [col for col in commute_columns[1:] if col in existing_data.columns]
    existing_commute = existing_data[existing_commute_cols].copy() if len(existing_commute_cols) > 1 else None

    if existing_commute is not None and not existing_commute.empty:
        existing_commute["Finnkode"] = existing_commute["Finnkode"].astype(str)
        df["Finnkode"] = df["Finnkode"].astype(str)

        df = df.merge(existing_commute, on="Finnkode", how="left", suffixes=("", "_old"))
        for col in commute_columns[1:]:
            old_col = f"{col}_old"
            if col in df.columns and old_col in df.columns:
                df[col] = df[col].combine_first(df[old_col])
                df = df.drop(columns=[old_col])

    for col in target_cols:
        if col not in df.columns:
            df[col] = pd.NA

    eligible_mask = pd.Series([True] * len(df), index=df.index)
    max_price = _get_max_price()
    if max_price is not None and "Pris" in df.columns:
        eligible_mask = pd.to_numeric(df["Pris"], errors="coerce").fillna(0) <= max_price

    missing_by_col: Dict[str, int] = {
        col: int(df.loc[eligible_mask, col].isna().sum())
        for col in target_cols
    }
    total_candidates = sum(missing_by_col.values())

    if args.format == "count":
        print(str(total_candidates))
        return 0

    print("=== Process-step Travel Estimate ===")
    print(f"Input CSV rows: {len(df)}")
    print(f"Eligible rows: {int(eligible_mask.sum())}")
    if max_price is not None:
        print(f"MAX_PRICE filter: {max_price}")
    for col in target_cols:
        print(f"Missing {col}: {missing_by_col[col]}")
    print(f"Candidates: {total_candidates}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
