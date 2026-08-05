from skannonser.store import connection, migrations
from skannonser.store.repositories.tilstand import TilstandRepo

ROLLUP = {
    "tg2_count": 1, "tg3_count": 1,
    "reparasjon_lav": 210_000, "reparasjon_hoy": 550_000, "reparasjon_est": 380_000,
    "alvorlighet": "alvorlig", "verste_bygningsdel": "vatrom",
    "reparasjon_kilde": "blandet",
    "tilstandsrapport_dato": "2026-05-01", "tilstandsrapport_utsteder": "anticimex",
    "egenerklaering_antall": 1,
}
FINDINGS = [
    {"tg": 3, "bygningsdel": "vatrom", "tiltak": "utskiftning", "alvorlighet": "alvorlig",
     "kostnad_lav": 200_000, "kostnad_hoy": 500_000, "kostnad_kilde": "takst"},
    {"tg": 2, "bygningsdel": "tak", "tiltak": None, "alvorlighet": "mindre",
     "kostnad_lav": 10_000, "kostnad_hoy": 50_000, "kostnad_kilde": "estimat"},
]


def _db(tmp_path):
    conn = connection.connect(tmp_path / "t.db")
    migrations.migrate(conn)
    conn.execute("INSERT INTO eiendom (finnkode) VALUES ('42')")
    conn.commit()
    return conn


def test_upsert_ad_writes_all_three_tables(tmp_path):
    conn = _db(tmp_path)
    TilstandRepo(conn).upsert_ad("42", FINDINGS, ["vannskade"], ROLLUP)
    assert conn.execute("SELECT COUNT(*) FROM listing_tg_findings").fetchone()[0] == 2
    assert conn.execute("SELECT forhold FROM listing_egenerklaering").fetchone()[0] == "vannskade"
    row = conn.execute("SELECT * FROM listing_tilstand WHERE finnkode='42'").fetchone()
    assert row["reparasjon_est"] == 380_000
    assert row["classified_at"] is not None


def test_upsert_ad_is_a_full_replace(tmp_path):
    conn = _db(tmp_path)
    repo = TilstandRepo(conn)
    repo.upsert_ad("42", FINDINGS, ["vannskade"], ROLLUP)
    repo.upsert_ad("42", FINDINGS[:1], [], {**ROLLUP, "tg2_count": 0, "egenerklaering_antall": None})
    assert conn.execute("SELECT COUNT(*) FROM listing_tg_findings").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM listing_egenerklaering").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM listing_tilstand").fetchone()[0] == 1


def test_wipe_spares_llm_cache(tmp_path):
    conn = _db(tmp_path)
    conn.execute(
        "INSERT INTO salgsoppgave_llm_cache (content_sha256, response_json, model, created_at) "
        "VALUES ('abc', '{}', 'm', datetime('now'))"
    )
    repo = TilstandRepo(conn)
    repo.upsert_ad("42", FINDINGS, ["vannskade"], ROLLUP)
    repo.wipe()
    assert conn.execute("SELECT COUNT(*) FROM listing_tilstand").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM listing_tg_findings").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM listing_egenerklaering").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM salgsoppgave_llm_cache").fetchone()[0] == 1
