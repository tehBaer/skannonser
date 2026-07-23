"""tools backfill-details: local re-parse of cached ad HTML into
listing_details. Purely offline -- the whole point is zero FINN traffic."""
import shutil
from pathlib import Path

import pytest

from skannonser.ingest.finn.backfill import backfill_details
from skannonser.store import connection, migrations

FIXTURES = Path(__file__).parent / "fixtures" / "finn"


@pytest.fixture()
def conn(tmp_path):
    c = connection.connect(tmp_path / "t.db")
    migrations.migrate(c)
    return c


def _seed(conn, finnkode):
    conn.execute(
        "INSERT INTO eiendom (finnkode, url) VALUES (?, ?)", (finnkode, "u")
    )
    conn.commit()


def test_backfill_parses_cached_html(conn, tmp_path):
    project = tmp_path / "eiendom"
    (project / "html_extracted").mkdir(parents=True)
    shutil.copy(FIXTURES / "448347467.html", project / "html_extracted" / "448347467.html")
    _seed(conn, "448347467")
    _seed(conn, "999999999")  # no cached HTML for this one

    stats = backfill_details(conn, project)
    assert stats == {
        "eiendom_rows": 2,
        "parsed": 1,
        "missing_html": 1,
        "upserted": 1,
    }
    row = conn.execute(
        "SELECT totalpris FROM listing_details WHERE finnkode = '448347467'"
    ).fetchone()
    assert row["totalpris"] == 4944646


def test_backfill_is_idempotent(conn, tmp_path):
    project = tmp_path / "eiendom"
    (project / "html_extracted").mkdir(parents=True)
    shutil.copy(FIXTURES / "448347467.html", project / "html_extracted" / "448347467.html")
    _seed(conn, "448347467")
    backfill_details(conn, project)
    backfill_details(conn, project)
    assert conn.execute("SELECT COUNT(*) FROM listing_details").fetchone()[0] == 1
    # facilities not duplicated either
    n = conn.execute(
        "SELECT COUNT(*) FROM listing_facilities WHERE finnkode='448347467' AND facility='Heis'"
    ).fetchone()[0]
    assert n == 1


def test_backfill_wipe_rebuilds(conn, tmp_path):
    project = tmp_path / "eiendom"
    (project / "html_extracted").mkdir(parents=True)
    shutil.copy(FIXTURES / "448347467.html", project / "html_extracted" / "448347467.html")
    _seed(conn, "448347467")
    backfill_details(conn, project)
    # Stale row for a finnkode whose HTML no longer exists must vanish on --wipe.
    # Seed an eiendom row for '42' first due to FK constraint
    _seed(conn, "42")
    conn.execute("INSERT INTO listing_details (finnkode) VALUES ('42')")
    conn.commit()
    backfill_details(conn, project, wipe=True)
    finnkodes = {
        r[0] for r in conn.execute("SELECT finnkode FROM listing_details")
    }
    assert finnkodes == {"448347467"}
