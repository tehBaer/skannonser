import pytest

from skannonser.enrich.sentinels import TRAVEL_API_ERROR, TRAVEL_NO_ROUTES, TRAVEL_UNREALISTIC
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
    conn.commit()
    repo = TilstandRepo(conn)
    repo.upsert_ad("42", FINDINGS, ["vannskade"], ROLLUP)
    repo.wipe()
    assert conn.execute("SELECT COUNT(*) FROM listing_tilstand").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM listing_tg_findings").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM listing_egenerklaering").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM salgsoppgave_llm_cache").fetchone()[0] == 1


def _rank_db(tmp_path):
    """A DB with one eiendom row per (tier, band) case we care about."""
    conn = connection.connect(tmp_path / "rank.db")
    migrations.migrate(conn)
    return conn


def _ad(conn, finnkode, *, active=1, tilg=None, area=None, brj=None, mvv=None, donor=None):
    conn.execute(
        "INSERT INTO eiendom (finnkode, active, tilgjengelighet, info_usable_area) "
        "VALUES (?, ?, ?, ?)",
        (finnkode, active, tilg, area),
    )
    conn.execute(
        "INSERT INTO eiendom_processed "
        "(finnkode, pendl_rush_brj, pendl_rush_mvv, travel_copy_from_finnkode) "
        "VALUES (?, ?, ?, ?)",
        (finnkode, brj, mvv, donor),
    )
    conn.commit()


def test_candidate_order_puts_active_before_inactive_before_sold(tmp_path):
    conn = _rank_db(tmp_path)
    _ad(conn, "sold", active=0, tilg="Solgt", area=100, brj=30, mvv=30)
    _ad(conn, "inactive", active=0, tilg="Inaktiv", area=100, brj=30, mvv=30)
    _ad(conn, "active", active=1, tilg=None, area=100, brj=30, mvv=30)
    assert TilstandRepo(conn).candidate_finnkodes() == ["active", "inactive", "sold"]


def test_active_flag_loses_to_inaktiv_tilgjengelighet(tmp_path):
    # 77 production rows carry active=1 AND tilgjengelighet='Inaktiv'.
    # publish/rows.py:205 resolves those to NOT active; so must we.
    conn = _rank_db(tmp_path)
    _ad(conn, "conflicted", active=1, tilg="Inaktiv", area=100, brj=30, mvv=30)
    _ad(conn, "clean", active=1, tilg=None, area=100, brj=30, mvv=30)
    assert TilstandRepo(conn).candidate_finnkodes() == ["clean", "conflicted"]


def test_band_orders_match_then_unknown_then_miss(tmp_path):
    conn = _rank_db(tmp_path)
    _ad(conn, "miss_far", area=100, brj=30, mvv=99)
    _ad(conn, "miss_small", area=60, brj=30, mvv=30)
    _ad(conn, "unknown", area=100, brj=None, mvv=30)
    _ad(conn, "match", area=100, brj=30, mvv=30)
    order = TilstandRepo(conn).candidate_finnkodes()
    assert order[0] == "match"
    assert order[1] == "unknown"
    assert set(order[2:]) == {"miss_far", "miss_small"}


def test_both_commutes_must_qualify(tmp_path):
    conn = _rank_db(tmp_path)
    _ad(conn, "one_only", area=100, brj=30, mvv=71)
    _ad(conn, "both", area=100, brj=70, mvv=70)
    assert TilstandRepo(conn).candidate_finnkodes() == ["both", "one_only"]


@pytest.mark.parametrize(
    "sentinel", [TRAVEL_NO_ROUTES, TRAVEL_UNREALISTIC, TRAVEL_API_ERROR]
)
def test_travel_sentinel_is_unknown_not_a_great_commute(tmp_path, sentinel):
    # All three sentinels are negative (enrich/sentinels.py) and numerically
    # under 70, but none of them means "an excellent commute" -- the
    # BETWEEN 0 AND 70 guard must reject all three the same way.
    conn = _rank_db(tmp_path)
    _ad(conn, "sentinel", area=100, brj=sentinel, mvv=30)
    _ad(conn, "real", area=100, brj=30, mvv=30)
    _ad(conn, "miss", area=100, brj=90, mvv=30)
    assert TilstandRepo(conn).candidate_finnkodes() == ["real", "sentinel", "miss"]


def test_dangling_donor_falls_back_to_own_travel(tmp_path):
    # travel_copy_from_finnkode points at a finnkode with no
    # eiendom_processed row at all -- the LEFT JOIN to ep_src yields NULL,
    # and the CASE's ELSE branch must fall back to the row's own travel
    # values rather than the row silently dropping to band 1. Give the
    # dangling-donor row good travel of its own and prove it lands in band 0,
    # ahead of a genuine unknown-travel row.
    conn = _rank_db(tmp_path)
    _ad(conn, "dangling", area=100, brj=30, mvv=30, donor="ghost")
    _ad(conn, "unknown", area=100, brj=None, mvv=30)
    assert TilstandRepo(conn).candidate_finnkodes() == ["dangling", "unknown"]


def test_donor_travel_times_decide_the_band(tmp_path):
    # "aaa_control" has no donor and genuinely unknown travel, so it lands in
    # band 1. "zzz_borrower" has the same unknown own travel but borrows the
    # donor's good times, so with donor resolution working it lands in band 0
    # and must sort ahead of the control. Without donor resolution, both are
    # band 1 and tie-break on finnkode -- "aaa_control" (alphabetically first)
    # would then sort ahead of "zzz_borrower", failing this assertion.
    conn = _rank_db(tmp_path)
    _ad(conn, "donor", area=100, brj=30, mvv=30)
    _ad(conn, "zzz_borrower", area=100, brj=None, mvv=None, donor="donor")
    _ad(conn, "aaa_control", area=100, brj=None, mvv=None, donor=None)
    order = TilstandRepo(conn).candidate_finnkodes()
    assert order.index("zzz_borrower") < order.index("aaa_control")


def test_every_ad_is_returned_even_without_a_processed_row(tmp_path):
    conn = _rank_db(tmp_path)
    conn.execute("INSERT INTO eiendom (finnkode, active) VALUES ('orphan', 1)")
    conn.commit()
    _ad(conn, "normal", area=100, brj=30, mvv=30)
    assert set(TilstandRepo(conn).candidate_finnkodes()) == {"orphan", "normal"}


def test_order_is_total_and_stable_on_finnkode(tmp_path):
    conn = _rank_db(tmp_path)
    for finnkode in ("300", "100", "200"):
        _ad(conn, finnkode, area=100, brj=30, mvv=30)
    repo = TilstandRepo(conn)
    assert repo.candidate_finnkodes() == ["100", "200", "300"]
    assert repo.candidate_finnkodes() == repo.candidate_finnkodes()


def test_cache_put_records_effort(tmp_path):
    from skannonser.enrich.tilstand import cache_put
    conn = connection.connect(tmp_path / "t.db")
    migrations.migrate(conn)
    cache_put(conn, "sha_a", "{}", model="claude-opus-5", effort="high")
    cache_put(conn, "sha_b", "{}")
    got = {r[0]: (r[1], r[2]) for r in conn.execute(
        "SELECT content_sha256, model, effort FROM salgsoppgave_llm_cache")}
    assert got["sha_a"] == ("claude-opus-5", "high")
    assert got["sha_b"][1] is None, "effort defaults to NULL = not recorded"


def test_upsert_ad_stores_the_producing_sha(tmp_path):
    conn = _db(tmp_path)
    TilstandRepo(conn).upsert_ad("42", FINDINGS, ["vannskade"], ROLLUP,
                                 content_sha256="abc123")
    row = conn.execute(
        "SELECT content_sha256 FROM listing_tilstand WHERE finnkode='42'").fetchone()
    assert row[0] == "abc123"
