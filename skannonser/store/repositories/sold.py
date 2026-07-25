"""``sold_prices`` repository: upsert/read of FINN sold-price records.

Two distinct "don't clobber" semantics live here, each with its own tuple:

- ``_FILL_ONLY`` (``sold_price``, ``cadastral_sold_date``) generates
  ``col = COALESCE(?, col)``: a NULL in a later re-fetch (e.g. a sweep that
  runs before a sale is tinglyst) never wipes a stored value, but FINN issuing
  a corrected non-null value still lands.
- ``_FIRST_WINS`` (``discovered_near_finnkode``) generates
  ``col = COALESCE(col, ?)``: once set, the value is permanent regardless of
  what a later sweep offers. This is the discovery anchor -- the first
  tracked listing whose ~120 m query box surfaced a neighbour's sold card --
  and it must not flip to a different target just because a later sweep
  rediscovers the same card nearer someone else.

``sold_date``/``price_suggestion``/``address`` and the card facts
(``size``/``property_type``/``bedrooms``/``collective_debt``/``ownership_type``)
are set as given (``_SET``).
"""

import sqlite3

_FILL_ONLY = ("sold_price", "cadastral_sold_date")
_FIRST_WINS = ("discovered_near_finnkode",)
_SET = (
    "sold_date",
    "price_suggestion",
    "address",
    "size",
    "property_type",
    "bedrooms",
    "collective_debt",
    "ownership_type",
)
_ALL = ("finnkode",) + _FILL_ONLY + _FIRST_WINS + _SET


class SoldPricesRepo:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def upsert(self, records: list[dict]) -> dict:
        """Insert new sold-price rows, or update existing ones (fill-only for
        the price/registration-date, first-wins for the discovery anchor, set
        for the rest). Returns ``{"inserted", "updated"}``."""
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
                    params = [finnkode] + [
                        rec.get(c) for c in _FILL_ONLY + _FIRST_WINS + _SET
                    ]
                    conn.execute(
                        f"INSERT INTO sold_prices ({', '.join(cols)}) "
                        f"VALUES ({placeholders})",
                        params,
                    )
                    inserted += 1
                else:
                    fill = ", ".join(f"{c} = COALESCE(?, {c})" for c in _FILL_ONLY)
                    anchor = ", ".join(f"{c} = COALESCE({c}, ?)" for c in _FIRST_WINS)
                    setc = ", ".join(f"{c} = ?" for c in _SET)
                    params = (
                        [rec.get(c) for c in _FILL_ONLY]
                        + [rec.get(c) for c in _FIRST_WINS]
                        + [rec.get(c) for c in _SET]
                        + [finnkode]
                    )
                    conn.execute(
                        f"UPDATE sold_prices SET {fill}, {anchor}, {setc}, "
                        f"updated_at = datetime('now') WHERE finnkode = ?",
                        params,
                    )
                    updated += 1
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        return {"inserted": inserted, "updated": updated}
