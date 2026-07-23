"""``listing_details``/``listing_facilities`` repository (migration 010).

Full-row REPLACE semantics throughout -- these tables are a DERIVED cache of
`parse_details` output over cached ad HTML, never hand-curated data. The
rebuild path for any parser change is `tools backfill-details --wipe`, so
unlike ProcessedRepo/SoldPricesRepo there is deliberately NO fill-only or
partial-update logic here.
"""
import sqlite3

from skannonser.ingest.finn.parse_details import ListingDetails

_SCALAR_COLS = (
    "bedrooms", "rooms", "floor", "eieform", "nabolag",
    "totalpris", "omkostninger", "fellesgjeld", "felleskost_mnd",
    "fellesformue", "formuesverdi", "kommunale_avg_aar",
    "energimerke", "energifarge",
    "kommunenr", "gardsnr", "bruksnr", "seksjonsnr",
    "borettslag_navn", "borettslag_orgnr", "borettslag_andelsnr",
)


class DetailsRepo:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def upsert_details(self, items: list[ListingDetails]) -> dict:
        """REPLACE each item's scalar row (parsed_at stamped now) and its
        facilities set, all in one transaction. Returns {"upserted": n}."""
        if not items:
            return {"upserted": 0}
        cols = ("finnkode",) + _SCALAR_COLS + ("parsed_at",)
        placeholders = ", ".join("?" * (len(cols) - 1))
        sql = (
            f"INSERT OR REPLACE INTO listing_details ({', '.join(cols)}) "
            f"VALUES ({placeholders}, datetime('now'))"
        )
        conn = self.conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            for item in items:
                data = item.model_dump()
                conn.execute(
                    sql, [item.finnkode] + [data[c] for c in _SCALAR_COLS]
                )
                self.replace_facilities(item.finnkode, item.facilities)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        return {"upserted": len(items)}

    def replace_facilities(self, finnkode: str, facilities: list[str]) -> None:
        """Delete + insert this finnkode's facility rows. No transaction of
        its own -- `upsert_details` (the only production caller) wraps it."""
        self.conn.execute(
            "DELETE FROM listing_facilities WHERE finnkode = ?", (finnkode,)
        )
        self.conn.executemany(
            "INSERT OR IGNORE INTO listing_facilities (finnkode, facility) VALUES (?, ?)",
            [(finnkode, f) for f in facilities],
        )

    def wipe(self) -> None:
        conn = self.conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.execute("DELETE FROM listing_facilities")
            conn.execute("DELETE FROM listing_details")
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    def coverage(self) -> dict:
        one = lambda sql: self.conn.execute(sql).fetchone()[0]  # noqa: E731
        return {
            "eiendom_rows": one("SELECT COUNT(*) FROM eiendom"),
            "details_rows": one("SELECT COUNT(*) FROM listing_details"),
            "with_totalpris": one(
                "SELECT COUNT(*) FROM listing_details WHERE totalpris IS NOT NULL"
            ),
            "with_felleskost": one(
                "SELECT COUNT(*) FROM listing_details WHERE felleskost_mnd IS NOT NULL"
            ),
            "facilities_rows": one("SELECT COUNT(*) FROM listing_facilities"),
        }
