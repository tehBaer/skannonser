#!/usr/bin/env python3
"""Report listings missing LAT/LNG in the database."""
import argparse
import os
import sys

# Add parent and project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

try:
    from main.database.db import PropertyDatabase
except ImportError:
    from database.db import PropertyDatabase


def main() -> int:
    parser = argparse.ArgumentParser(description="Report missing LAT/LNG coordinates for eiendom listings")
    parser.add_argument("--db", help="Optional path to properties.db")
    parser.add_argument("--limit", type=int, default=50, help="How many rows to print (default: 50)")
    parser.add_argument("--csv", help="Optional output CSV path")
    args = parser.parse_args()

    db = PropertyDatabase(args.db)
    df = db.get_eiendom_missing_coordinates()

    total = len(df)
    active_count = int(df["IsActive"].fillna(0).astype(int).sum()) if not df.empty else 0

    print("=" * 72)
    print("Missing Coordinates Report")
    print("=" * 72)
    print(f"Total missing LAT/LNG: {total}")
    print(f"Active listings missing LAT/LNG: {active_count}")

    if total == 0:
        print("All listings have coordinates.")
        return 0

    show = df.head(max(args.limit, 0)).copy()
    if "URL" in show.columns:
        show["URL"] = show["URL"].fillna("").apply(lambda x: str(x)[:100])

    print("\nSample rows:")
    print(show.to_string(index=False))

    if args.csv:
        df.to_csv(args.csv, index=False)
        print(f"\nSaved full report to: {args.csv}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
