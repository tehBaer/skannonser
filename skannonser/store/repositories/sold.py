"""``sold_prices`` repository: upsert/read of FINN sold-price records.

Fill-only on ``sold_price``/``cadastral_sold_date`` (``COALESCE(?, existing)``,
matching :class:`~skannonser.store.repositories.processed.ProcessedRepo`'s
coordinate/travel columns): a later re-fetch that lacks the price -- e.g. a
sweep that runs before a sale is tinglyst -- must never clobber a value already
stored. ``sold_date``/``price_suggestion``/``address`` are set as given.
"""

import sqlite3

_FILL_ONLY = ("sold_price", "cadastral_sold_date")
_SET = ("sold_date", "price_suggestion", "address")
_ALL = ("finnkode",) + _FILL_ONLY + _SET


class SoldPricesRepo:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def upsert(self, records: list[dict]) -> dict:
        """Insert new sold-price rows, or update existing ones (fill-only for
        the price/registration-date; set for the rest). Returns
        ``{"inserted", "updated"}``."""
        conn = self.conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            inserted = updated = 0
            for rec in records:
                finnkode = str(rec.get("finnkode", "") or "").strip()
                if not finnkode:
                    continue

                existing = conn.execute(
                    "SELECT 1 FROM sold_prices WHERE finnkode = ?", (finnkode,)
                ).fetchone()

                if existing is None:
                    cols = list(_ALL)
                    placeholders = ", ".join("?" * len(cols))
                    params = [finnkode] + [rec.get(c) for c in _FILL_ONLY + _SET]
                    conn.execute(
                        f"INSERT INTO sold_prices ({', '.join(cols)}) "
                        f"VALUES ({placeholders})",
                        params,
                    )
                    inserted += 1
                else:
                    fill = ", ".join(f"{c} = COALESCE(?, {c})" for c in _FILL_ONLY)
                    setc = ", ".join(f"{c} = ?" for c in _SET)
                    params = (
                        [rec.get(c) for c in _FILL_ONLY]
                        + [rec.get(c) for c in _SET]
                        + [finnkode]
                    )
                    conn.execute(
                        f"UPDATE sold_prices SET {fill}, {setc}, "
                        f"updated_at = datetime('now') WHERE finnkode = ?",
                        params,
                    )
                    updated += 1
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        return {"inserted": inserted, "updated": updated}
