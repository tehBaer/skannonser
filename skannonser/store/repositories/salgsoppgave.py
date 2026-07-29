"""``listing_salgsoppgave`` repository (migration 015).

Full-row REPLACE semantics, matching DetailsRepo: these tables are a DERIVED
cache of parser output over cached ad HTML, never hand-curated data. The
rebuild path is `tools backfill-salgsoppgave --wipe`, so there is deliberately
no fill-only or partial-update logic.

`wipe()` deliberately spares `salgsoppgave_llm_cache` -- that cache is keyed by
content hash and is precisely what lets a rebuild replay Phase 2's classifier
results for free. Clearing it would turn every rebuild back into a paid run.
"""
import sqlite3

from skannonser.ingest.finn.parse_salgsoppgave import Salgsoppgave

_SCALAR_COLS = (
    "boligselgerforsikring", "eiendomsskatt_kr",
    "ferdigattest", "radon_omtalt", "utleie", "husdyr", "heftelser",
)


class SalgsoppgaveRepo:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def upsert(self, items: list[Salgsoppgave]) -> dict:
        """REPLACE each item's row (parsed_at stamped now) in one transaction.
        Returns {"upserted": n}."""
        if not items:
            return {"upserted": 0}
        cols = ("finnkode",) + _SCALAR_COLS + ("parsed_at",)
        placeholders = ", ".join("?" * (len(cols) - 1))
        sql = (
            f"INSERT OR REPLACE INTO listing_salgsoppgave ({', '.join(cols)}) "
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
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        return {"upserted": len(items)}

    def wipe(self) -> None:
        """Clear the derived tables. Leaves salgsoppgave_llm_cache intact."""
        conn = self.conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.execute("DELETE FROM listing_tg_findings")
            conn.execute("DELETE FROM listing_egenerklaering")
            conn.execute("DELETE FROM listing_salgsoppgave")
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    def coverage(self) -> dict:
        one = lambda sql: self.conn.execute(sql).fetchone()[0]  # noqa: E731
        return {
            "eiendom_rows": one("SELECT COUNT(*) FROM eiendom"),
            "salgsoppgave_rows": one("SELECT COUNT(*) FROM listing_salgsoppgave"),
            "with_eiendomsskatt": one(
                "SELECT COUNT(*) FROM listing_salgsoppgave WHERE eiendomsskatt_kr IS NOT NULL"
            ),
            "with_ferdigattest": one(
                "SELECT COUNT(*) FROM listing_salgsoppgave WHERE ferdigattest IS NOT NULL"
            ),
            "tg_findings_rows": one("SELECT COUNT(*) FROM listing_tg_findings"),
        }
