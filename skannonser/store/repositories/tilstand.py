"""Classifier-owned tables (migration 016): listing_tilstand,
listing_tg_findings, listing_egenerklaering.

Full per-ad REPLACE semantics like the other derived caches: these are LLM
classifier output over cached ad HTML, rebuildable via
`tools classify-tilstand --wipe`. `wipe()` spares salgsoppgave_llm_cache --
that cache is what makes a rebuild free instead of a paid re-run.
Phase-1's SalgsoppgaveRepo must never touch these tables (and since 016, it
structurally cannot: they are not in its SQL).
"""
import sqlite3

_FINDING_COLS = (
    "tg", "bygningsdel", "tiltak", "alvorlighet",
    "kostnad_lav", "kostnad_hoy", "kostnad_kilde",
)
_ROLLUP_COLS = (
    "tg2_count", "tg3_count", "reparasjon_lav", "reparasjon_hoy",
    "reparasjon_est", "alvorlighet", "verste_bygningsdel", "reparasjon_kilde",
    "tilstandsrapport_dato", "tilstandsrapport_utsteder", "egenerklaering_antall",
)


class TilstandRepo:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def upsert_ad(
        self,
        finnkode: str,
        findings: list[dict],
        egenerklaering: list[str],
        rollup: dict,
    ) -> None:
        """Replace one ad's classifier output atomically."""
        conn = self.conn
        # Commit any pending transaction to avoid "cannot start a transaction within a transaction"
        if conn.in_transaction:
            conn.commit()
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.execute("DELETE FROM listing_tg_findings WHERE finnkode = ?", (finnkode,))
            conn.execute("DELETE FROM listing_egenerklaering WHERE finnkode = ?", (finnkode,))
            for f in findings:
                conn.execute(
                    "INSERT INTO listing_tg_findings "
                    f"(finnkode, {', '.join(_FINDING_COLS)}) "
                    f"VALUES (?, {', '.join('?' * len(_FINDING_COLS))})",
                    [finnkode] + [f[c] for c in _FINDING_COLS],
                )
            for forhold in egenerklaering:
                conn.execute(
                    "INSERT OR IGNORE INTO listing_egenerklaering (finnkode, forhold) "
                    "VALUES (?, ?)",
                    (finnkode, forhold),
                )
            conn.execute(
                "INSERT OR REPLACE INTO listing_tilstand "
                f"(finnkode, {', '.join(_ROLLUP_COLS)}, classified_at) "
                f"VALUES (?, {', '.join('?' * len(_ROLLUP_COLS))}, datetime('now'))",
                [finnkode] + [rollup[c] for c in _ROLLUP_COLS],
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    def wipe(self) -> None:
        """Clear the classifier tables. Leaves salgsoppgave_llm_cache intact."""
        conn = self.conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.execute("DELETE FROM listing_tg_findings")
            conn.execute("DELETE FROM listing_egenerklaering")
            conn.execute("DELETE FROM listing_tilstand")
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    def coverage(self) -> dict:
        one = lambda sql: self.conn.execute(sql).fetchone()[0]  # noqa: E731
        return {
            "eiendom_rows": one("SELECT COUNT(*) FROM eiendom"),
            "tilstand_rows": one("SELECT COUNT(*) FROM listing_tilstand"),
            "tg_findings_rows": one("SELECT COUNT(*) FROM listing_tg_findings"),
            "egenerklaering_rows": one("SELECT COUNT(*) FROM listing_egenerklaering"),
            "llm_cache_rows": one("SELECT COUNT(*) FROM salgsoppgave_llm_cache"),
            "with_tg3": one("SELECT COUNT(*) FROM listing_tilstand WHERE tg3_count > 0"),
        }
