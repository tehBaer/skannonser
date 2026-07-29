"""tools backfill-salgsoppgave: local re-parse of cached ad HTML. Purely
offline -- the whole point is zero FINN traffic."""
import shutil
from pathlib import Path

import pytest

from skannonser.ingest.finn.backfill_salgsoppgave import backfill_salgsoppgave
from skannonser.store import connection, migrations

FIXTURES = Path(__file__).parent / "fixtures" / "finn"


@pytest.fixture()
def conn(tmp_path):
    c = connection.connect(tmp_path / "t.db")
    migrations.migrate(c)
    return c


def _seed(conn, finnkode):
    conn.execute("INSERT INTO eiendom (finnkode, url) VALUES (?, ?)", (finnkode, "u"))
    conn.commit()


def _project(tmp_path, *finnkodes):
    project = tmp_path / "eiendom"
    (project / "html_extracted").mkdir(parents=True)
    for code in finnkodes:
        shutil.copy(FIXTURES / f"{code}.html", project / "html_extracted" / f"{code}.html")
    return project


def test_backfill_parses_cached_html(conn, tmp_path):
    project = _project(tmp_path, "448347467")
    _seed(conn, "448347467")
    _seed(conn, "999999999")  # no cached HTML

    stats = backfill_salgsoppgave(conn, project)
    assert stats == {
        "eiendom_rows": 2,
        "parsed": 1,
        "missing_html": 1,
        "upserted": 1,
        "skipped": 0,
    }
    row = conn.execute(
        "SELECT finnkode, parsed_at FROM listing_salgsoppgave WHERE finnkode='448347467'"
    ).fetchone()
    assert row["parsed_at"] is not None


def test_backfill_is_idempotent(conn, tmp_path):
    project = _project(tmp_path, "448347467")
    _seed(conn, "448347467")
    backfill_salgsoppgave(conn, project)
    backfill_salgsoppgave(conn, project)
    assert conn.execute("SELECT COUNT(*) FROM listing_salgsoppgave").fetchone()[0] == 1


def test_backfill_handles_both_payload_formats(conn, tmp_path):
    project = _project(tmp_path, "448347467", "432672475")
    _seed(conn, "448347467")
    _seed(conn, "432672475")
    stats = backfill_salgsoppgave(conn, project)
    assert stats["parsed"] == 2
    assert conn.execute("SELECT COUNT(*) FROM listing_salgsoppgave").fetchone()[0] == 2


def test_a_failing_listing_is_skipped_not_fatal(conn, tmp_path, monkeypatch):
    """Finding 1: a single bad listing must not abort a batch run, nor
    discard the other rows already buffered in `batch` when it hits. Two
    good listings bracket the failing one so the assertion actually
    exercises resuming after the exception, not just tolerating a trailing
    failure."""
    import skannonser.ingest.finn.backfill_salgsoppgave as mod

    project = _project(tmp_path, "448347467", "432672475", "451631591")
    _seed(conn, "448347467")
    _seed(conn, "432672475")
    _seed(conn, "451631591")

    real_parse = mod.parse_salgsoppgave

    def flaky_parse(html, finnkode):
        if finnkode == "432672475":
            raise RecursionError("simulated pathological turbo-stream payload")
        return real_parse(html, finnkode)

    monkeypatch.setattr(mod, "parse_salgsoppgave", flaky_parse)

    stats = backfill_salgsoppgave(conn, project)
    assert stats == {
        "eiendom_rows": 3,
        "parsed": 2,
        "missing_html": 0,
        "upserted": 2,
        "skipped": 1,
    }
    codes = {r[0] for r in conn.execute("SELECT finnkode FROM listing_salgsoppgave")}
    assert codes == {"448347467", "451631591"}


def test_wipe_clears_before_rebuilding(conn, tmp_path):
    project = _project(tmp_path, "448347467")
    _seed(conn, "448347467")
    backfill_salgsoppgave(conn, project)
    conn.execute("INSERT INTO eiendom (finnkode, url) VALUES ('stale', 'u')")
    conn.execute("INSERT INTO listing_salgsoppgave (finnkode) VALUES ('stale')")
    conn.commit()
    backfill_salgsoppgave(conn, project, wipe=True)
    codes = {r[0] for r in conn.execute("SELECT finnkode FROM listing_salgsoppgave")}
    assert codes == {"448347467"}
