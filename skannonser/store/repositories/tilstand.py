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

# --- Classification priority order (2026-08-06 spec) -----------------------
# `--limit` is the spend control and it cuts the walk wherever it lands, so
# the order decides what a bounded run pays for. Status is the outer key;
# commute+size fit only breaks ties inside a tier.

# Active exactly as the WHERE clause of _EIE_SQL in publish/rows.py defines
# it (e.active = 1 AND tilgjengelighet NOT IN ('solgt', 'inaktiv')) -- 77
# production rows have active=1 AND tilgjengelighet='Inaktiv', and that rule
# resolves them to inactive. One definition of "active", not two.
_STATUS_TIER = """
    CASE
        WHEN LOWER(TRIM(COALESCE(e.tilgjengelighet, ''))) = 'solgt' THEN 2
        WHEN e.active = 1
             AND LOWER(TRIM(COALESCE(e.tilgjengelighet, ''))) NOT IN ('solgt', 'inaktiv')
        THEN 0
        ELSE 1
    END
"""

# Donor-resolved travel, mirroring the _DONOR_TRAVEL_SQL constant in
# publish/rows.py: a
# listing that borrows a donor's times is ranked on the borrowed values, the
# same ones the web UI shows for it.
def _donor_travel(dest: str) -> str:
    return f"""
    CASE
        WHEN ep.travel_copy_from_finnkode IS NOT NULL
             AND TRIM(ep.travel_copy_from_finnkode) != ''
             AND ep_src.pendl_rush_{dest} IS NOT NULL
        THEN ep_src.pendl_rush_{dest}
        ELSE ep.pendl_rush_{dest}
    END
    """


# `areal` is NULL on 5776 of 5863 rows; info_usable_area (BRA) is the real
# source at 5827. The others are fallbacks, not alternatives.
_AREA = "COALESCE(e.info_usable_area, e.info_primary_area, e.areal)"

# BETWEEN 0 AND 70 rather than <= 70: travel sentinels are negative (-1 no
# routes, -2 unrealistic, -3 API error; see enrich/sentinels.py) and every one
# of them would read as an excellent commute under a bare <= test.
_CANDIDATE_SQL = f"""
    SELECT e.finnkode
    FROM eiendom e
    LEFT JOIN eiendom_processed ep ON ep.finnkode = e.finnkode
    LEFT JOIN eiendom_processed ep_src ON ep_src.finnkode = ep.travel_copy_from_finnkode
    ORDER BY
        {_STATUS_TIER},
        CASE
            WHEN ({_AREA} IS NOT NULL AND {_AREA} < 80)
                 OR ({_donor_travel('brj')}) > 70
                 OR ({_donor_travel('mvv')}) > 70
            THEN 2
            WHEN {_AREA} >= 80
                 AND ({_donor_travel('brj')}) BETWEEN 0 AND 70
                 AND ({_donor_travel('mvv')}) BETWEEN 0 AND 70
            THEN 0
            ELSE 1
        END,
        e.finnkode
"""


class TilstandRepo:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def upsert_ad(
        self,
        finnkode: str,
        findings: list[dict],
        egenerklaering: list[str],
        rollup: dict,
        content_sha256: str | None = None,
    ) -> None:
        """Replace one ad's classifier output atomically.

        `content_sha256` is the cache key of the response this row was derived
        from -- the join back to salgsoppgave_llm_cache for model and effort.
        Optional so a caller that genuinely has no cache row (a direct
        hand-built rollup in a test) is not forced to invent one."""
        conn = self.conn
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
                f"(finnkode, {', '.join(_ROLLUP_COLS)}, content_sha256, classified_at) "
                f"VALUES (?, {', '.join('?' * len(_ROLLUP_COLS))}, ?, datetime('now'))",
                [finnkode] + [rollup[c] for c in _ROLLUP_COLS] + [content_sha256],
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

    def candidate_finnkodes(self) -> list[str]:
        """Every ad in `eiendom`, ordered for classification: active before
        inactive before sold, and inside each tier the ads matching >= 80 m2
        BRA with both rush commutes <= 70 min first, then the ads we cannot
        rate, then the ones we know miss.

        Nothing is filtered -- a bounded run just spends on the good end.
        """
        return [str(r[0]) for r in self.conn.execute(_CANDIDATE_SQL)]

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
