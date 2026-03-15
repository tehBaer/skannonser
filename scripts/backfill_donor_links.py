#!/usr/bin/env python3
"""
Backfill travel_copy_from_finnkode for listings that already have their own
travel API values but have a nearby complete donor within TRAVEL_REUSE_WITHIN_METERS.

Only assigns the donor link if the absolute difference between own and donor
values is within MAX_DIFF_MINUTES for both BRJ and MVV (default 10 min).

This is a retroactive fix for listings that were processed before donor reuse was
enforced across all code paths (e.g. `make travel`).

Once travel_copy_from_finnkode is set, the DB export SQL automatically uses the
donor's values in place of the listing's own stored values.
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd

from main.database.db import PropertyDatabase
from main.config.filters import TRAVEL_REUSE_WITHIN_METERS, MAX_TRAVEL_MINUTES
from main.post_process import _build_travel_donor_cache, _find_nearby_donor_finnkode

MAX_DIFF_MINUTES = 10.0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Backfill donor links for listings with own travel values near a complete donor"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be updated without writing to the DB",
    )
    parser.add_argument(
        "--max-diff",
        type=float,
        default=MAX_DIFF_MINUTES,
        metavar="MINUTES",
        help=f"Max allowed absolute difference in minutes (default {MAX_DIFF_MINUTES:.0f})",
    )
    args = parser.parse_args()

    db = PropertyDatabase()
    conn = db.get_connection()

    df = pd.read_sql_query(
        """
        SELECT finnkode, lat, lng,
               pendl_rush_brj, pendl_rush_mvv,
               travel_copy_from_finnkode
        FROM eiendom_processed
        WHERE lat IS NOT NULL
          AND lng IS NOT NULL
        """,
        conn,
    )

    print(f"Loaded {len(df)} eiendom_processed rows with coordinates.")

    # Rename for _build_travel_donor_cache
    df_work = df.rename(
        columns={
            "finnkode": "Finnkode",
            "pendl_rush_brj": "PENDL RUSH BRJ",
            "pendl_rush_mvv": "PENDL RUSH MVV",
            "travel_copy_from_finnkode": "TRAVEL_COPY_FROM_FINNKODE",
        }
    )

    all_cols = ["PENDL RUSH BRJ", "PENDL RUSH MVV"]
    donor_cache = _build_travel_donor_cache(df_work, all_cols, "lat", "lng", MAX_TRAVEL_MINUTES)
    print(f"Donor cache size: {len(donor_cache)}")

    # Value lookup by finnkode
    val_lookup = (
        df_work.dropna(subset=["PENDL RUSH BRJ", "PENDL RUSH MVV"])
        .set_index("Finnkode")[["PENDL RUSH BRJ", "PENDL RUSH MVV"]]
        .to_dict("index")
    )

    # Target: listings with own values and no donor link
    has_own = df_work[
        df_work["TRAVEL_COPY_FROM_FINNKODE"].fillna("").str.strip().eq("")
        & df_work["PENDL RUSH BRJ"].notna()
        & df_work["PENDL RUSH MVV"].notna()
    ].copy()

    print(f"Listings with own values and no donor link: {len(has_own)}")

    updates = []  # [(listing_fk, donor_fk)]
    n_no_donor = 0
    n_diff_too_big = 0

    for _, row in has_own.iterrows():
        fk = str(row["Finnkode"])
        lat, lng = float(row["lat"]), float(row["lng"])

        cache_excl_self = [(la, ln, f) for la, ln, f in donor_cache if f != fk]
        donor_fk = _find_nearby_donor_finnkode(lat, lng, cache_excl_self, TRAVEL_REUSE_WITHIN_METERS)

        if donor_fk is None:
            n_no_donor += 1
            continue

        donor_vals = val_lookup.get(donor_fk)
        if donor_vals is None:
            n_no_donor += 1
            continue

        own_brj = float(row["PENDL RUSH BRJ"])
        own_mvv = float(row["PENDL RUSH MVV"])
        donor_brj = float(donor_vals["PENDL RUSH BRJ"])
        donor_mvv = float(donor_vals["PENDL RUSH MVV"])

        brj_diff = abs(own_brj - donor_brj)
        mvv_diff = abs(own_mvv - donor_mvv)
        brj_fail = brj_diff > args.max_diff
        mvv_fail = mvv_diff > args.max_diff

        if brj_fail or mvv_fail:
            n_diff_too_big += 1
            continue

        updates.append((fk, donor_fk))

    print(f"\n--- Summary ---")
    print(f"  No nearby donor within {TRAVEL_REUSE_WITHIN_METERS}m: {n_no_donor}")
    print(f"  Donor found but diff >{args.max_diff:.0f} min: {n_diff_too_big}")
    print(f"  Will assign donor link: {len(updates)}")

    if args.dry_run:
        if updates:
            sample = pd.DataFrame(updates[:20], columns=["listing_finnkode", "donor_finnkode"])
            print(f"\nDry run — sample of first {len(sample)} updates:")
            print(sample.to_string(index=False))
        print("\nDry run — no changes written.")
        conn.close()
        return 0

    if not updates:
        print("Nothing to update.")
        conn.close()
        return 0

    ans = input(f"\nAssign {len(updates)} donor links? [y/N] ").strip().lower()
    if ans not in ("y", "yes"):
        print("Aborted.")
        conn.close()
        return 0

    cur = conn.cursor()
    for listing_fk, donor_fk in updates:
        cur.execute(
            "UPDATE eiendom_processed SET travel_copy_from_finnkode = ? WHERE finnkode = ?",
            (donor_fk, listing_fk),
        )
    conn.commit()
    conn.close()

    print(f"Done. {len(updates)} rows updated.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
