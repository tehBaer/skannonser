#!/usr/bin/env python3
"""Delete eiendom listings that fall outside configured MAX_PRICE/MIN_BRA_I filters.

Default mode is dry-run (no deletion). Use --apply to execute deletions.
"""

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


def _load_filters() -> tuple[int | None, int | None]:
    try:
        from main.config.filters import MAX_PRICE, MIN_BRA_I
        return MAX_PRICE, MIN_BRA_I
    except ImportError:
        try:
            from config.filters import MAX_PRICE, MIN_BRA_I
            return MAX_PRICE, MIN_BRA_I
        except ImportError:
            return None, None


def _build_outside_filter_clause(max_price: int | None, min_bra_i: int | None) -> tuple[str, list]:
    # Rows are considered "outside" when they fail at least one active filter.
    clauses = []
    params = []

    if max_price is not None:
        clauses.append("(pris IS NULL OR pris > ?)")
        params.append(max_price)

    if min_bra_i is not None:
        clauses.append("(info_usable_i_area IS NULL OR CAST(info_usable_i_area AS REAL) < ?)")
        params.append(min_bra_i)

    if not clauses:
        return "", []

    return " OR ".join(clauses), params


def _preview_rows(conn, where_clause: str, params: list, limit: int):
    query = f'''
        SELECT
            e.finnkode,
            e.adresse,
            e.pris,
            e.info_usable_i_area,
            e.active,
            e.tilgjengelighet
        FROM eiendom e
        WHERE {where_clause}
        ORDER BY e.scraped_at DESC
        LIMIT ?
    '''
    return conn.execute(query, [*params, limit]).fetchall()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Delete eiendom rows outside MAX_PRICE/MIN_BRA_I filters"
    )
    parser.add_argument("--db", help="Optional path to properties.db")
    parser.add_argument("--apply", action="store_true", help="Actually delete rows (default is dry-run)")
    parser.add_argument("--yes", action="store_true", help="Skip interactive confirmation when using --apply")
    parser.add_argument("--preview", type=int, default=20, help="How many candidate rows to print (default: 20)")
    args = parser.parse_args()

    max_price, min_bra_i = _load_filters()

    print("=" * 72)
    print("Prune Eiendom Outside Filters")
    print("=" * 72)
    print(f"MAX_PRICE: {max_price}")
    print(f"MIN_BRA_I: {min_bra_i}")

    outside_clause, outside_params = _build_outside_filter_clause(max_price, min_bra_i)
    if not outside_clause:
        print("No active MAX_PRICE/MIN_BRA_I filters found. Nothing to prune.")
        return 0

    db = PropertyDatabase(args.db)
    conn = db.get_connection()

    try:
        count_query = f"SELECT COUNT(*) FROM eiendom WHERE {outside_clause}"
        total_outside = int(conn.execute(count_query, outside_params).fetchone()[0])

        print(f"\nRows outside filters: {total_outside}")
        if total_outside == 0:
            print("Database already matches current filters.")
            return 0

        preview_rows = _preview_rows(conn, outside_clause, outside_params, max(args.preview, 0))
        if preview_rows:
            print("\nPreview candidates:")
            for finnkode, adresse, pris, bra_i, active, tilgjengelighet in preview_rows:
                print(
                    f"- {finnkode} | pris={pris} | bra_i={bra_i} | active={active} | "
                    f"status={tilgjengelighet} | {adresse}"
                )

        if not args.apply:
            print("\nDry-run only. Re-run with --apply to delete these rows.")
            return 0

        if not args.yes:
            confirm = input("\nType DELETE to confirm permanent removal: ").strip()
            if confirm != "DELETE":
                print("Aborted. No changes made.")
                return 1

        # Delete dependent rows first, then base table.
        delete_processed_query = f'''
            DELETE FROM eiendom_processed
            WHERE finnkode IN (
                SELECT finnkode
                FROM eiendom
                WHERE {outside_clause}
            )
        '''
        delete_overrides_query = f'''
            DELETE FROM manual_overrides
            WHERE finnkode IN (
                SELECT finnkode
                FROM eiendom
                WHERE {outside_clause}
            )
        '''
        delete_eiendom_query = f"DELETE FROM eiendom WHERE {outside_clause}"

        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute(delete_processed_query, outside_params)
        deleted_processed = cur.rowcount
        cur.execute(delete_overrides_query, outside_params)
        deleted_overrides = cur.rowcount
        cur.execute(delete_eiendom_query, outside_params)
        deleted_eiendom = cur.rowcount
        conn.commit()

        print("\nDeletion complete:")
        print(f"- Deleted from eiendom: {deleted_eiendom}")
        print(f"- Deleted from eiendom_processed: {deleted_processed}")
        print(f"- Deleted from manual_overrides: {deleted_overrides}")
        return 0
    except Exception as e:
        conn.rollback()
        print(f"Failed to prune listings: {e}")
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
