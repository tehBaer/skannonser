#!/usr/bin/env python3
"""Populate travel values from donors for listings where travel_copy_from_finnkode is set.

For each row in eiendom_processed where travel_copy_from_finnkode is not empty,
copies the donor's actual travel column values directly into the recipient row.

This is useful after backfill_donor_links.py has assigned donor pointers but the
recipient rows still have NULL travel values — e.g. if the listing was inserted
before its donor was processed.
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd

from main.database.db import PropertyDatabase

TRAVEL_COLS = [
    "pendl_rush_brj",
    "pendl_rush_mvv",
    "pendl_rush_mvv_uni_rush",
    "pendl_morn_cntr",
    "bil_morn_cntr",
    "pendl_dag_cntr",
    "bil_dag_cntr",
]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Copy travel values from donors into recipient rows in eiendom_processed."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be updated without writing to the DB.",
    )
    args = parser.parse_args()

    db = PropertyDatabase()
    conn = db.get_connection()

    cols_sql = ", ".join(["finnkode", "travel_copy_from_finnkode"] + TRAVEL_COLS)
    df = pd.read_sql_query(f"SELECT {cols_sql} FROM eiendom_processed", conn)

    print(f"Loaded {len(df)} rows from eiendom_processed.")

    # Build pointer map and value lookup indexed by finnkode
    pointer_map = (
        df[df["travel_copy_from_finnkode"].notna() & df["travel_copy_from_finnkode"].str.strip().ne("")]
        .set_index("finnkode")["travel_copy_from_finnkode"]
        .str.strip()
        .to_dict()
    )
    value_lookup = df.set_index("finnkode")[TRAVEL_COLS].to_dict("index")

    def resolve_donor(start_fk: str):
        """Follow travel_copy_from_finnkode chain to the ultimate source.

        Returns (resolved_finnkode, chain) where resolved_finnkode is the first
        node with no pointer (true root), OR — if a cycle is detected — the first
        node in the cycle that has stored travel values.
        Returns (None, chain) only if a cycle exists and no node in it has values.
        """
        chain = [start_fk]
        visited = {start_fk}
        current = start_fk
        while current in pointer_map:
            nxt = pointer_map[current]
            if nxt in visited:
                # Cycle detected — find the first node in the cycle with actual values
                cycle_start_idx = chain.index(nxt)
                cycle_nodes = chain[cycle_start_idx:]
                for candidate in cycle_nodes:
                    vals = value_lookup.get(candidate, {})
                    if any(v is not None and pd.notna(v) for v in vals.values()):
                        return candidate, chain + [f"(cycle→{candidate})"]
                return None, chain + [nxt]  # cycle but no node has values
            visited.add(nxt)
            chain.append(nxt)
            current = nxt
        return current, chain

    # Recipients: rows that have a donor pointer set
    recipients = df[
        df["travel_copy_from_finnkode"].notna()
        & df["travel_copy_from_finnkode"].str.strip().ne("")
    ].copy()

    print(f"Found {len(recipients)} recipient rows with travel_copy_from_finnkode set.")

    n_updated = 0
    n_flattened = 0
    n_donor_missing = 0
    n_donor_no_values = 0
    n_cycle = 0

    cursor = conn.cursor()

    for _, row in recipients.iterrows():
        fk = row["finnkode"]
        direct_donor = str(row["travel_copy_from_finnkode"]).strip()

        if direct_donor not in value_lookup:
            print(f"  SKIP {fk}: donor {direct_donor!r} not found in eiendom_processed")
            n_donor_missing += 1
            continue

        resolved_fk, chain = resolve_donor(direct_donor)

        if resolved_fk is None:
            print(f"  SKIP {fk}: cycle detected, no node has values — chain {' -> '.join(chain)}")
            n_cycle += 1
            continue
        else:
            if resolved_fk not in value_lookup:
                print(f"  SKIP {fk}: resolved donor {resolved_fk!r} not found in eiendom_processed")
                n_donor_missing += 1
                continue
            donor_vals = value_lookup[resolved_fk]
            chain_str = " -> ".join(chain) if len(chain) > 1 else resolved_fk
            # Chain has more than one hop — pointer needs to be flattened to root
            needs_flatten = resolved_fk != direct_donor

        has_any = any(v is not None and pd.notna(v) for v in donor_vals.values())
        if not has_any:
            print(f"  SKIP {fk}: resolved donor {resolved_fk!r} has no travel values yet")
            n_donor_no_values += 1
            continue

        set_clause = ", ".join(f"{c} = ?" for c in TRAVEL_COLS)
        values = [donor_vals[c] for c in TRAVEL_COLS] + [fk]

        if args.dry_run:
            brj = donor_vals.get("pendl_rush_brj")
            mvv = donor_vals.get("pendl_rush_mvv")
            uni = donor_vals.get("pendl_rush_mvv_uni_rush")
            flatten_note = f" [flatten pointer -> {resolved_fk}]" if needs_flatten else ""
            print(f"  DRY-RUN {fk} <- {chain_str}: BRJ={brj} MVV={mvv} UNI={uni}{flatten_note}")
        else:
            if needs_flatten:
                cursor.execute(
                    f"UPDATE eiendom_processed "
                    f"SET {set_clause}, travel_copy_from_finnkode = ?, updated_at = CURRENT_TIMESTAMP "
                    f"WHERE finnkode = ?",
                    values + [resolved_fk, fk],
                )
            else:
                cursor.execute(
                    f"UPDATE eiendom_processed "
                    f"SET {set_clause}, updated_at = CURRENT_TIMESTAMP "
                    f"WHERE finnkode = ?",
                    values,
                )

        n_updated += 1
        if needs_flatten:
            n_flattened += 1

    if not args.dry_run:
        conn.commit()

    conn.close()

    action = "Would update" if args.dry_run else "Updated"
    print(f"\n{action} {n_updated} recipient rows.")
    if n_flattened:
        print(f"  {n_flattened} pointer(s) flattened to root donor")
    if n_donor_missing:
        print(f"  {n_donor_missing} skipped: donor finnkode not in DB")
    if n_donor_no_values:
        print(f"  {n_donor_no_values} skipped: donor has no travel values yet")
    if n_cycle:
        print(f"  {n_cycle} skipped: cycle with no stored values in any node")

    # --- Pass 2: fix rows that are both a donor and an acceptor ---
    # Reload pointer state (pass 1 may have mutated value_lookup in dry-run only;
    # for real runs we re-read so the fix uses up-to-date stored values).
    print("\n--- Pass 2: clearing pointers from rows that are also donors ---")

    conn2 = db.get_connection()
    df2 = pd.read_sql_query(f"SELECT {cols_sql} FROM eiendom_processed", conn2)

    pointer_map2 = (
        df2[df2["travel_copy_from_finnkode"].notna() & df2["travel_copy_from_finnkode"].str.strip().ne("")]
        .set_index("finnkode")["travel_copy_from_finnkode"]
        .str.strip()
        .to_dict()
    )
    value_lookup2 = df2.set_index("finnkode")[TRAVEL_COLS].to_dict("index")

    # Rows pointed to by at least one other row
    donor_fks = set(df2["travel_copy_from_finnkode"].dropna().str.strip())
    donor_fks.discard("")

    # Among those, rows that also have their own pointer (the problematic ones)
    dual = df2[
        df2["finnkode"].isin(donor_fks)
        & df2["travel_copy_from_finnkode"].notna()
        & df2["travel_copy_from_finnkode"].str.strip().ne("")
    ].copy()

    print(f"Found {len(dual)} rows that are both a donor and an acceptor.")

    n_fixed = 0
    n_fix_skip_no_values = 0
    n_fix_skip_missing = 0

    cursor2 = conn2.cursor()

    for _, row in dual.iterrows():
        fk = row["finnkode"]
        direct_donor = str(row["travel_copy_from_finnkode"]).strip()

        if direct_donor not in value_lookup2:
            print(f"  SKIP {fk}: donor {direct_donor!r} not found")
            n_fix_skip_missing += 1
            continue

        # Re-use the same resolve logic with the refreshed maps
        chain = [direct_donor]
        visited = {direct_donor}
        current = direct_donor
        while current in pointer_map2:
            nxt = pointer_map2[current]
            if nxt in visited:
                cycle_start_idx = chain.index(nxt)
                cycle_nodes = chain[cycle_start_idx:]
                resolved = next(
                    (c for c in cycle_nodes if any(
                        v is not None and pd.notna(v)
                        for v in value_lookup2.get(c, {}).values()
                    )),
                    None,
                )
                if resolved is None:
                    current = None
                else:
                    current = resolved
                break
            visited.add(nxt)
            chain.append(nxt)
            current = nxt

        if current is None or current not in value_lookup2:
            print(f"  SKIP {fk}: could not resolve a donor with values")
            n_fix_skip_no_values += 1
            continue

        donor_vals = value_lookup2[current]
        has_any = any(v is not None and pd.notna(v) for v in donor_vals.values())
        if not has_any:
            print(f"  SKIP {fk}: resolved donor {current!r} has no travel values")
            n_fix_skip_no_values += 1
            continue

        set_clause = ", ".join(f"{c} = ?" for c in TRAVEL_COLS)
        values = [donor_vals[c] for c in TRAVEL_COLS] + [fk]

        if args.dry_run:
            brj = donor_vals.get("pendl_rush_brj")
            mvv = donor_vals.get("pendl_rush_mvv")
            print(f"  DRY-RUN fix {fk}: copy from {current}, clear pointer  BRJ={brj} MVV={mvv}")
        else:
            cursor2.execute(
                f"UPDATE eiendom_processed "
                f"SET {set_clause}, travel_copy_from_finnkode = NULL, updated_at = CURRENT_TIMESTAMP "
                f"WHERE finnkode = ?",
                values,
            )

        n_fixed += 1

    if not args.dry_run:
        conn2.commit()

    conn2.close()

    action2 = "Would fix" if args.dry_run else "Fixed"
    print(f"\n{action2} {n_fixed} dual donor-acceptor rows (stored values copied, pointer cleared).")
    if n_fix_skip_missing:
        print(f"  {n_fix_skip_missing} skipped: donor not in DB")
    if n_fix_skip_no_values:
        print(f"  {n_fix_skip_no_values} skipped: no resolvable values")

    return 0


if __name__ == "__main__":
    sys.exit(main())
