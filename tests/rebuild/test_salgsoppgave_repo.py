import pytest

from skannonser.ingest.finn.parse_salgsoppgave import Salgsoppgave
from skannonser.store import connection, migrations
from skannonser.store.repositories.salgsoppgave import SalgsoppgaveRepo


@pytest.fixture()
def conn(tmp_path):
    c = connection.connect(tmp_path / "t.db")
    migrations.migrate(c)
    c.execute("INSERT INTO eiendom (finnkode, url) VALUES ('1', 'u')")
    c.commit()
    return c


def test_upsert_writes_a_row(conn):
    repo = SalgsoppgaveRepo(conn)
    item = Salgsoppgave(finnkode="1", ferdigattest="midlertidig")
    assert repo.upsert([item]) == {"upserted": 1}
    row = conn.execute(
        "SELECT ferdigattest, parsed_at FROM listing_salgsoppgave WHERE finnkode='1'"
    ).fetchone()
    assert row["ferdigattest"] == "midlertidig"
    assert row["parsed_at"] is not None


def test_upsert_is_idempotent_and_replaces(conn):
    repo = SalgsoppgaveRepo(conn)
    repo.upsert([Salgsoppgave(finnkode="1", ferdigattest="ingen", radon_omtalt=True)])
    repo.upsert([Salgsoppgave(finnkode="1", ferdigattest="ferdigattest")])
    assert conn.execute("SELECT COUNT(*) FROM listing_salgsoppgave").fetchone()[0] == 1
    row = conn.execute(
        "SELECT ferdigattest, radon_omtalt FROM listing_salgsoppgave WHERE finnkode='1'"
    ).fetchone()
    assert row["ferdigattest"] == "ferdigattest"
    assert row["radon_omtalt"] is None, "full-row REPLACE, not fill-only"


def test_upsert_empty_list_is_a_noop(conn):
    assert SalgsoppgaveRepo(conn).upsert([]) == {"upserted": 0}


def test_wipe_clears_only_salgsoppgave_spares_classifier_tables(conn):
    """Phase-2 tables are now owned by TilstandRepo and must be spared during wipe."""
    repo = SalgsoppgaveRepo(conn)
    repo.upsert([Salgsoppgave(finnkode="1", ferdigattest="ingen")])
    conn.execute(
        "INSERT INTO listing_tg_findings (finnkode, tg, bygningsdel, alvorlighet) VALUES ('1',2,'vatrom','vesentlig')"
    )
    conn.execute("INSERT INTO listing_egenerklaering (finnkode, forhold) VALUES ('1','tvist')")
    conn.commit()
    repo.wipe()
    # Phase-1 table is cleared
    assert conn.execute("SELECT COUNT(*) FROM listing_salgsoppgave").fetchone()[0] == 0
    # Phase-2 tables are spared
    assert conn.execute("SELECT COUNT(*) FROM listing_tg_findings").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM listing_egenerklaering").fetchone()[0] == 1


def test_wipe_preserves_the_llm_cache(conn):
    """The cache is what makes a --wipe rebuild free; wiping it defeats the point."""
    conn.execute(
        "INSERT INTO salgsoppgave_llm_cache (content_sha256, response_json, model, created_at)"
        " VALUES ('abc', '{}', 'm', datetime('now'))"
    )
    conn.commit()
    SalgsoppgaveRepo(conn).wipe()
    assert conn.execute("SELECT COUNT(*) FROM salgsoppgave_llm_cache").fetchone()[0] == 1


def test_phase1_wipe_spares_classifier_tables(conn):
    repo = SalgsoppgaveRepo(conn)
    conn.execute(
        "INSERT INTO listing_tg_findings (finnkode, tg, bygningsdel, alvorlighet) "
        "VALUES ('1', 3, 'vatrom', 'alvorlig')"
    )
    conn.execute("INSERT INTO listing_egenerklaering (finnkode, forhold) VALUES ('1', 'vannskade')")
    conn.commit()
    repo.wipe()
    assert conn.execute("SELECT COUNT(*) FROM listing_tg_findings").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM listing_egenerklaering").fetchone()[0] == 1


def test_coverage_counts(conn):
    repo = SalgsoppgaveRepo(conn)
    repo.upsert([Salgsoppgave(finnkode="1", ferdigattest="ingen")])
    stats = repo.coverage()
    assert stats["salgsoppgave_rows"] == 1
    assert stats["with_ferdigattest"] == 1
