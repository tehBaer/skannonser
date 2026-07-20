"""Offline end-to-end tests for the ingest pipeline (skannonser/pipeline.py)
and the `skannonser run ingest` CLI command
(skannonser/commands/run_cmd.py), plus the two mandatory guards:

1. mark_inactive / deactivate_missing are skipped when a crawl yields zero
   urls (never treat "crawl found nothing" as "everything got delisted").
2. The CLI exits non-zero when a source's parse-failure rate exceeds 20%.
"""

import shutil
from pathlib import Path

import pytest
from typer.testing import CliRunner

from skannonser.cli import app
from skannonser.commands import run_cmd
from skannonser.config.domain import load_domain
from skannonser.ingest.base import NormalizedListing
from skannonser.pipeline import run_dnb_ingest, run_finn_ingest
from skannonser.store import connection, migrations
from skannonser.store.repositories.dnb import DnbRepo
from skannonser.store.repositories.listings import ListingsRepo

FINN_FIXTURES = Path(__file__).parent / "fixtures" / "finn"
DNB_FIXTURES = Path(__file__).parent / "fixtures" / "dnb"


def _fail_if_called(*a, **k):
    raise AssertionError("network hit when the pipeline should have used the cache/skip path")


@pytest.fixture()
def conn(tmp_path):
    c = connection.connect(tmp_path / "p.db")
    migrations.migrate(c)
    return c


@pytest.fixture()
def domain():
    return load_domain()


# ---------------------------------------------------------------------------
# Step 1 (brief-mandated): full offline FINN pipeline against fixtures.
# ---------------------------------------------------------------------------


def test_finn_pipeline_offline_end_to_end(tmp_path):
    conn = connection.connect(tmp_path / "p.db")
    migrations.migrate(conn)
    proj = tmp_path / "proj"
    # Seed the cache with two fixture ads so fetch is never called:
    fixture_dir = Path("tests/rebuild/fixtures/finn")
    cases = sorted(fixture_dir.glob("*.html"))[:2]
    (proj / "html_extracted").mkdir(parents=True)
    for c in cases:
        shutil.copy(c, proj / "html_extracted" / c.name)
    urls = [
        (c.stem, f"https://www.finn.no/realestate/homes/ad.html?finnkode={c.stem}")
        for c in cases
    ]

    stats = run_finn_ingest(
        load_domain(), conn, proj, fetch=_fail_if_called, skip_crawl_urls=urls
    )

    assert stats["parsed"] == 2 and stats["failed"] == 0
    assert conn.execute("SELECT COUNT(*) FROM eiendom").fetchone()[0] == 2
    # Legacy quirk preserved: first-seen listings activate on 2nd appearance (Task 6 ruling).
    assert conn.execute("SELECT COUNT(*) FROM eiendom WHERE active=1").fetchone()[0] == 0

    # Running the same ingest again (same finnkodes) hits the UPDATE branch,
    # which activates them -- and nothing gets deactivated in the process.
    stats2 = run_finn_ingest(
        load_domain(), conn, proj, fetch=_fail_if_called, skip_crawl_urls=urls
    )

    assert stats2["parsed"] == 2 and stats2["failed"] == 0
    assert conn.execute("SELECT COUNT(*) FROM eiendom WHERE active=1").fetchone()[0] == 2
    assert stats2["deactivated"] == 0


def test_finn_pipeline_reports_crawled_upserted_deactivated(tmp_path):
    proj = tmp_path / "proj"
    fixture_dir = FINN_FIXTURES
    cases = sorted(fixture_dir.glob("*.html"))[:2]
    (proj / "html_extracted").mkdir(parents=True)
    for c in cases:
        shutil.copy(c, proj / "html_extracted" / c.name)
    urls = [
        (c.stem, f"https://www.finn.no/realestate/homes/ad.html?finnkode={c.stem}")
        for c in cases
    ]
    conn = connection.connect(tmp_path / "p.db")
    migrations.migrate(conn)

    stats = run_finn_ingest(
        load_domain(), conn, proj, fetch=_fail_if_called, skip_crawl_urls=urls
    )

    assert stats["crawled"] == 2
    assert stats["upserted"] == 2
    assert stats["deactivated"] == 0


# ---------------------------------------------------------------------------
# Guard 1: skip mark_inactive / deactivate_missing on a zero-url crawl.
# ---------------------------------------------------------------------------


def test_finn_mark_inactive_skipped_when_crawl_yields_zero_urls(conn, domain, tmp_path):
    # Seed one already-active listing (two upserts to clear the legacy
    # activate-on-second-appearance quirk).
    repo = ListingsRepo(conn)
    listing = NormalizedListing(
        Finnkode="999",
        URL="https://www.finn.no/realestate/homes/ad.html?finnkode=999",
    )
    repo.upsert([listing])
    repo.upsert([listing])
    assert repo.active_finnkodes() == {"999"}

    stats = run_finn_ingest(
        domain, conn, tmp_path / "proj", fetch=_fail_if_called, skip_crawl_urls=[]
    )

    assert stats == {
        "crawled": 0,
        "parsed": 0,
        "failed": 0,
        "upserted": 0,
        "deactivated": 0,
    }
    # The previously-active listing must NOT have been deactivated by an
    # empty crawl.
    assert repo.active_finnkodes() == {"999"}


def test_dnb_deactivate_missing_skipped_when_crawl_yields_zero_urls(conn, domain):
    row = {
        "URL": "https://dnbeiendom.no/bolig/existing",
        "StreetAddress": "Nowhere 1",
        "PostalCode": "0000",
        "Latitude": 59.9139,
        "Longitude": 10.7522,
        "PropertyType": "Leilighet",
        "Price": 1000000,
    }
    repo = DnbRepo(conn)
    repo.upsert([row])
    repo.upsert([row])
    active = {
        r["url"] for r in conn.execute("SELECT url FROM dnbeiendom WHERE active = 1")
    }
    assert active == {"https://dnbeiendom.no/bolig/existing"}

    stats = run_dnb_ingest(domain, conn, fetch=_fail_if_called, skip_crawl_urls=[])

    assert stats == {
        "crawled": 0,
        "parsed": 0,
        "failed": 0,
        "upserted": 0,
        "deactivated": 0,
    }
    active = {
        r["url"] for r in conn.execute("SELECT url FROM dnbeiendom WHERE active = 1")
    }
    assert active == {"https://dnbeiendom.no/bolig/existing"}


# ---------------------------------------------------------------------------
# Guard 2: skip mark_inactive / deactivate_missing when the parse-failure
# rate is too high, even though the crawl itself found urls (crawled > 0).
# Without this, a FINN/DNB layout change that breaks parsing but not
# crawling would pass a near-empty active set to mark_inactive/
# deactivate_missing and wipe out every other active listing -- the CLI's
# non-zero exit code alone would be too late to prevent that.
# ---------------------------------------------------------------------------


def test_finn_mark_inactive_skipped_when_failure_rate_too_high(conn, domain, tmp_path):
    repo = ListingsRepo(conn)
    listing = NormalizedListing(
        Finnkode="999",
        URL="https://www.finn.no/realestate/homes/ad.html?finnkode=999",
    )
    repo.upsert([listing])
    repo.upsert([listing])
    assert repo.active_finnkodes() == {"999"}

    def flaky_fetch(url):
        raise RuntimeError("boom")

    # 2/2 = 100% failure rate, crawled > 0 so guard 1 alone wouldn't fire.
    stats = run_finn_ingest(
        domain,
        conn,
        tmp_path / "proj",
        fetch=flaky_fetch,
        skip_crawl_urls=[
            ("111", "https://www.finn.no/realestate/homes/ad.html?finnkode=111"),
            ("222", "https://www.finn.no/realestate/homes/ad.html?finnkode=222"),
        ],
    )

    assert stats["crawled"] == 2 and stats["failed"] == 2
    assert stats["deactivated"] == 0
    # The pre-existing active listing must survive a near-total parse
    # failure, not just get flagged by the caller after the fact.
    assert repo.active_finnkodes() == {"999"}


def test_dnb_deactivate_missing_skipped_when_failure_rate_too_high(conn, domain):
    row = {
        "URL": "https://dnbeiendom.no/bolig/existing",
        "StreetAddress": "Nowhere 1",
        "PostalCode": "0000",
        "Latitude": 59.9139,
        "Longitude": 10.7522,
        "PropertyType": "Leilighet",
        "Price": 1000000,
    }
    repo = DnbRepo(conn)
    repo.upsert([row])
    repo.upsert([row])
    active = {
        r["url"] for r in conn.execute("SELECT url FROM dnbeiendom WHERE active = 1")
    }
    assert active == {"https://dnbeiendom.no/bolig/existing"}

    def flaky_fetch(url):
        raise RuntimeError("boom")

    stats = run_dnb_ingest(
        domain,
        conn,
        fetch=flaky_fetch,
        skip_crawl_urls=[
            "https://dnbeiendom.no/bolig/a",
            "https://dnbeiendom.no/bolig/b",
        ],
    )

    assert stats["crawled"] == 2 and stats["failed"] == 2
    assert stats["deactivated"] == 0
    active = {
        r["url"] for r in conn.execute("SELECT url FROM dnbeiendom WHERE active = 1")
    }
    assert active == {"https://dnbeiendom.no/bolig/existing"}


# ---------------------------------------------------------------------------
# DNB offline end-to-end (mirrors the FINN pipeline test).
# ---------------------------------------------------------------------------


def test_dnb_pipeline_offline_end_to_end(conn, domain):
    html = (DNB_FIXTURES / "listing1.html").read_text(errors="replace")
    url = "https://dnbeiendom.no/bolig/listing1"

    class FakeResponse:
        text = html

        def raise_for_status(self):
            pass

    def fake_fetch(u):
        assert u == url
        return FakeResponse()

    stats = run_dnb_ingest(domain, conn, fetch=fake_fetch, skip_crawl_urls=[url])

    assert stats["crawled"] == 1
    assert stats["parsed"] == 1
    assert stats["failed"] == 0


def test_finn_parse_failure_is_counted_and_not_upserted(tmp_path):
    """A listing whose fetch raises must count toward `failed`, not `parsed`,
    and must not appear in the upsert batch."""
    conn = connection.connect(tmp_path / "p.db")
    migrations.migrate(conn)
    proj = tmp_path / "proj"

    def flaky_fetch(url):
        raise RuntimeError("boom")

    stats = run_finn_ingest(
        load_domain(),
        conn,
        proj,
        fetch=flaky_fetch,
        skip_crawl_urls=[("111", "https://www.finn.no/realestate/homes/ad.html?finnkode=111")],
    )

    assert stats == {
        "crawled": 1,
        "parsed": 0,
        "failed": 1,
        "upserted": 0,
        "deactivated": 0,
    }
    assert conn.execute("SELECT COUNT(*) FROM eiendom").fetchone()[0] == 0


# ---------------------------------------------------------------------------
# CLI: `skannonser run ingest`
# ---------------------------------------------------------------------------


def _seeded_db(tmp_path) -> Path:
    db = tmp_path / "cli.db"
    c = connection.connect(db)
    migrations.migrate(c)
    c.close()
    return db


def test_cli_ingest_missing_db_exits_nonzero(tmp_path, monkeypatch):
    monkeypatch.setenv("SKANNONSER_DB_PATH", str(tmp_path / "nope.db"))
    result = CliRunner().invoke(app, ["run", "ingest", "--source", "finn"])
    assert result.exit_code == 1
    assert "not found" in result.output


def test_cli_ingest_rejects_bad_source(tmp_path, monkeypatch):
    db = _seeded_db(tmp_path)
    result = CliRunner().invoke(
        app, ["run", "ingest", "--source", "bogus", "--db", str(db)]
    )
    assert result.exit_code == 2


def test_cli_ingest_source_routing_and_db_override(tmp_path, monkeypatch):
    db = _seeded_db(tmp_path)
    calls = []

    def fake_finn(domain, conn, project_dir, **kwargs):
        calls.append("finn")
        return {"crawled": 3, "parsed": 3, "failed": 0, "upserted": 3, "deactivated": 0}

    def fake_dnb(domain, conn):
        calls.append("dnb")
        return {"crawled": 2, "parsed": 2, "failed": 0, "upserted": 2, "deactivated": 0}

    monkeypatch.setattr(run_cmd, "run_finn_ingest", fake_finn)
    monkeypatch.setattr(run_cmd, "run_dnb_ingest", fake_dnb)

    result = CliRunner().invoke(
        app, ["run", "ingest", "--source", "finn", "--db", str(db)]
    )
    assert result.exit_code == 0, result.output
    assert calls == ["finn"]

    calls.clear()
    result = CliRunner().invoke(
        app, ["run", "ingest", "--source", "all", "--db", str(db)]
    )
    assert result.exit_code == 0, result.output
    assert calls == ["finn", "dnb"]


def test_cli_ingest_exits_nonzero_on_high_failure_rate(tmp_path, monkeypatch):
    db = _seeded_db(tmp_path)

    def fake_finn(domain, conn, project_dir, **kwargs):
        # 3/10 = 30% > 20% threshold.
        return {"crawled": 10, "parsed": 7, "failed": 3, "upserted": 7, "deactivated": 0}

    monkeypatch.setattr(run_cmd, "run_finn_ingest", fake_finn)

    result = CliRunner().invoke(
        app, ["run", "ingest", "--source", "finn", "--db", str(db)]
    )
    assert result.exit_code == 1
    assert "exceeds" in result.output


def test_cli_ingest_ok_at_exactly_the_threshold(tmp_path, monkeypatch):
    db = _seeded_db(tmp_path)

    def fake_finn(domain, conn, project_dir, **kwargs):
        # 2/10 = 20%, not > 20%, so this must pass.
        return {"crawled": 10, "parsed": 8, "failed": 2, "upserted": 8, "deactivated": 0}

    monkeypatch.setattr(run_cmd, "run_finn_ingest", fake_finn)

    result = CliRunner().invoke(
        app, ["run", "ingest", "--source", "finn", "--db", str(db)]
    )
    assert result.exit_code == 0, result.output
