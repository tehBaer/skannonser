"""Tests for the status-refresh port: append-only history on `ListingsRepo`,
the `refresh_listings` orchestration, and its three row-selection modes.

Port sources:
- `main/sync/refresh_listings.py:24-213` (`refresh_listing`, `_normalize_status`,
  `refresh_all_listings`)
- `main/database/db.py:616-660` (`update_eiendom_status`,
  `record_status_change_if_changed`)
- `main/database/db.py:926-1006` (the three row-selection queries)

Legacy tests `tests/test_status_history.py` / `tests/test_refresh_records_history.py`
describe the exact history semantics mirrored here.
"""

from pathlib import Path

import pytest
from typer.testing import CliRunner

from skannonser.cli import app
from skannonser.commands import run_cmd
from skannonser.config.domain import load_domain
from skannonser.ingest.base import NormalizedListing
from skannonser.ingest.finn.refresh import refresh_listings
from skannonser.store import connection, migrations
from skannonser.store.repositories.listings import ListingsRepo

FINN_FIXTURES = Path(__file__).parent / "fixtures" / "finn"


def _listing(finnkode: str, **kw) -> NormalizedListing:
    kw.setdefault("URL", f"https://www.finn.no/realestate/ad.html?finnkode={finnkode}")
    return NormalizedListing(Finnkode=finnkode, **kw)


@pytest.fixture()
def conn(tmp_path):
    c = connection.connect(tmp_path / "t.db")
    migrations.migrate(c)
    return c


@pytest.fixture()
def repo(conn):
    return ListingsRepo(conn)


@pytest.fixture()
def domain():
    return load_domain()


def _fake_fetch_minimal(url):
    class FakeResponse:
        content = b"<html><body>ok</body></html>"

        def raise_for_status(self):
            pass

    return FakeResponse()


# ---------------------------------------------------------------------------
# ListingsRepo.update_status / record_status_change_if_changed
# (db.py:616-660 -- append to eiendom_status_history ONLY on actual change).
# Cases mirror tests/test_status_history.py.
# ---------------------------------------------------------------------------


def test_status_history_appends_only_on_change(repo):
    repo.upsert([_listing("111")])

    recorded = repo.record_status_change_if_changed("111", "", "Solgt")
    assert recorded is True
    repo.update_status("111", "Solgt")

    # Refreshing again with the SAME value must not append a second row.
    recorded_again = repo.record_status_change_if_changed("111", "Solgt", "Solgt")
    assert recorded_again is False
    repo.update_status("111", "Solgt")

    row = repo.conn.execute(
        "SELECT tilgjengelighet FROM eiendom WHERE finnkode = '111'"
    ).fetchone()
    assert row["tilgjengelighet"] == "Solgt"

    history = repo.conn.execute(
        "SELECT old_status, new_status FROM eiendom_status_history "
        "WHERE finnkode = '111' ORDER BY id"
    ).fetchall()
    assert len(history) == 1
    assert history[0]["old_status"] == ""
    assert history[0]["new_status"] == "Solgt"


def test_status_history_whitespace_only_difference_is_not_a_change(repo):
    repo.upsert([_listing("111")])
    recorded = repo.record_status_change_if_changed("111", "Solgt", "  Solgt  ")
    assert recorded is False
    assert repo.conn.execute(
        "SELECT COUNT(*) FROM eiendom_status_history WHERE finnkode = '111'"
    ).fetchone()[0] == 0


def test_status_history_multiple_changes_accumulate_in_order(repo):
    repo.upsert([_listing("111")])
    repo.record_status_change_if_changed("111", "", "Reservert")
    repo.record_status_change_if_changed("111", "Reservert", "Solgt")

    history = repo.conn.execute(
        "SELECT old_status, new_status FROM eiendom_status_history "
        "WHERE finnkode = '111' ORDER BY id"
    ).fetchall()
    assert [(h["old_status"], h["new_status"]) for h in history] == [
        ("", "Reservert"),
        ("Reservert", "Solgt"),
    ]


def test_status_history_scoped_per_finnkode(repo):
    repo.upsert([_listing("111"), _listing("222")])
    repo.record_status_change_if_changed("111", "", "Solgt")
    repo.record_status_change_if_changed("222", "", "Reservert")

    history_111 = repo.conn.execute(
        "SELECT new_status FROM eiendom_status_history WHERE finnkode = '111'"
    ).fetchall()
    assert len(history_111) == 1
    assert history_111[0]["new_status"] == "Solgt"


# ---------------------------------------------------------------------------
# refresh_listings: force re-download, update tilgjengelighet, and append
# history only where the fetched status actually differs from the stored one.
# ---------------------------------------------------------------------------


def test_refresh_updates_status_from_html(conn, domain, tmp_path):
    repo = ListingsRepo(conn)
    repo.upsert([_listing("999", Tilgjengelighet="Til salgs")])
    repo.upsert([_listing("999", Tilgjengelighet="Til salgs")])  # activate (legacy quirk)
    assert repo.active_finnkodes() == {"999"}

    html = (FINN_FIXTURES / "451631591.html").read_text(encoding="utf-8", errors="replace")

    def fake_fetch(url):
        class FakeResponse:
            content = html.encode("utf-8")

            def raise_for_status(self):
                pass

        return FakeResponse()

    stats = refresh_listings(
        conn, domain, tmp_path / "proj", mode="all", fetch=fake_fetch, fetch_delay=lambda: None
    )

    row = conn.execute(
        "SELECT tilgjengelighet FROM eiendom WHERE finnkode = '999'"
    ).fetchone()
    assert row["tilgjengelighet"] == "Solgt"

    history = conn.execute(
        "SELECT old_status, new_status FROM eiendom_status_history WHERE finnkode = '999'"
    ).fetchall()
    assert len(history) == 1
    assert history[0]["old_status"] == "Til salgs"
    assert history[0]["new_status"] == "Solgt"

    assert stats["candidates"] == 1
    assert stats["refreshed"] == 1
    assert stats["status_changed"] == 1
    assert stats["errors"] == 0

    # force=True means the re-download actually happened; the fetched HTML
    # was cached as the new canonical for "999".
    canonical = tmp_path / "proj" / "html_extracted" / "999.html"
    assert canonical.exists()


def test_refresh_no_change_records_no_history(conn, domain, tmp_path):
    repo = ListingsRepo(conn)
    repo.upsert([_listing("999", Tilgjengelighet="Solgt")])
    repo.upsert([_listing("999", Tilgjengelighet="Solgt")])

    html = (FINN_FIXTURES / "451631591.html").read_text(encoding="utf-8", errors="replace")

    def fake_fetch(url):
        class FakeResponse:
            content = html.encode("utf-8")

            def raise_for_status(self):
                pass

        return FakeResponse()

    stats = refresh_listings(
        conn, domain, tmp_path / "proj", mode="all", fetch=fake_fetch, fetch_delay=lambda: None
    )

    assert stats["status_changed"] == 0
    history = conn.execute(
        "SELECT COUNT(*) FROM eiendom_status_history WHERE finnkode = '999'"
    ).fetchone()[0]
    assert history == 0


def test_refresh_fetch_error_counted_and_no_update(conn, domain, tmp_path):
    repo = ListingsRepo(conn)
    repo.upsert([_listing("999", Tilgjengelighet="Til salgs")])
    repo.upsert([_listing("999", Tilgjengelighet="Til salgs")])

    def flaky_fetch(url):
        raise RuntimeError("boom")

    stats = refresh_listings(
        conn, domain, tmp_path / "proj", mode="all", fetch=flaky_fetch, fetch_delay=lambda: None
    )

    assert stats == {"candidates": 1, "refreshed": 0, "status_changed": 0, "errors": 1}
    row = conn.execute(
        "SELECT tilgjengelighet FROM eiendom WHERE finnkode = '999'"
    ).fetchone()
    assert row["tilgjengelighet"] == "Til salgs"


# ---------------------------------------------------------------------------
# Row-selection semantics (db.py:926-1006). "stale-open" excludes listings
# already known-closed (Tilgjengelighet in {'Solgt', 'Inaktiv'}) and applies
# the domain's sheets_max_price / min_bra_i filters, same as the legacy
# get_stale_eiendom_for_status_refresh scope.
# ---------------------------------------------------------------------------


def _url_for(finnkode: str) -> str:
    return f"https://www.finn.no/realestate/ad.html?finnkode={finnkode}"


def test_stale_open_mode_selects_correct_rows(conn, domain, tmp_path):
    repo = ListingsRepo(conn)
    max_price = domain.filters.sheets_max_price
    min_bra_i = domain.filters.min_bra_i

    # A: active -> excluded from any inactive-scoped mode.
    repo.upsert([_listing("100", Tilgjengelighet="Til salgs", Pris=1000000)])
    repo.upsert([_listing("100", Tilgjengelighet="Til salgs", Pris=1000000)])

    # B: inactive but already closed (Solgt) -> excluded from stale-open.
    repo.upsert([_listing(
        "200", Tilgjengelighet="Solgt", Pris=1000000,
        **{"Internt bruksareal (BRA-i)": min_bra_i + 10},
    )])

    # C: inactive but already closed (Inaktiv, mixed case) -> excluded.
    repo.upsert([_listing(
        "300", Tilgjengelighet="inaktiv", Pris=1000000,
        **{"Internt bruksareal (BRA-i)": min_bra_i + 10},
    )])

    # D: inactive, open status, but price above the sheet filter -> excluded.
    repo.upsert([_listing(
        "400", Tilgjengelighet="Til salgs", Pris=max_price + 1,
        **{"Internt bruksareal (BRA-i)": min_bra_i + 10},
    )])

    # E: inactive, open status, but area below the sheet filter -> excluded.
    repo.upsert([_listing(
        "500", Tilgjengelighet="Til salgs", Pris=1000000,
        **{"Internt bruksareal (BRA-i)": min_bra_i - 10},
    )])

    # F: inactive, open status, within both filters -> INCLUDED.
    repo.upsert([_listing(
        "600", Tilgjengelighet="Til salgs", Pris=1000000,
        **{"Internt bruksareal (BRA-i)": min_bra_i + 10},
    )])

    # G: inactive, never-checked (no status yet), within filters -> INCLUDED.
    repo.upsert([_listing(
        "700", Pris=1000000, **{"Internt bruksareal (BRA-i)": min_bra_i + 10}
    )])

    calls = []

    def recording_fetch(url):
        calls.append(url)
        return _fake_fetch_minimal(url)

    stats = refresh_listings(
        conn, domain, tmp_path / "proj", mode="stale-open",
        fetch=recording_fetch, fetch_delay=lambda: None, listing_delay=lambda: None,
    )

    assert set(calls) == {_url_for("600"), _url_for("700")}
    assert stats["candidates"] == 2


def test_inactive_mode_includes_closed_statuses(conn, domain, tmp_path):
    """Unlike "stale-open", plain "inactive" does not exclude Solgt/Inaktiv --
    it is the full get_stale_eiendom_for_status_refresh scope (active=0 +
    price/area filters only)."""
    repo = ListingsRepo(conn)
    min_bra_i = domain.filters.min_bra_i

    repo.upsert([_listing(
        "200", Tilgjengelighet="Solgt", Pris=1000000,
        **{"Internt bruksareal (BRA-i)": min_bra_i + 10},
    )])
    repo.upsert([_listing(
        "600", Tilgjengelighet="Til salgs", Pris=1000000,
        **{"Internt bruksareal (BRA-i)": min_bra_i + 10},
    )])

    calls = []

    def recording_fetch(url):
        calls.append(url)
        return _fake_fetch_minimal(url)

    stats = refresh_listings(
        conn, domain, tmp_path / "proj", mode="inactive",
        fetch=recording_fetch, fetch_delay=lambda: None, listing_delay=lambda: None,
    )

    assert set(calls) == {_url_for("200"), _url_for("600")}
    assert stats["candidates"] == 2


def test_all_mode_includes_active_rows(conn, domain, tmp_path):
    repo = ListingsRepo(conn)
    repo.upsert([_listing("100", Tilgjengelighet="Til salgs")])
    repo.upsert([_listing("100", Tilgjengelighet="Til salgs")])  # activate
    assert repo.active_finnkodes() == {"100"}

    calls = []

    def recording_fetch(url):
        calls.append(url)
        return _fake_fetch_minimal(url)

    stats = refresh_listings(
        conn, domain, tmp_path / "proj", mode="all",
        fetch=recording_fetch, fetch_delay=lambda: None,
    )

    assert calls == [_url_for("100")]
    assert stats["candidates"] == 1


def test_unknown_mode_rejected(conn, domain, tmp_path):
    with pytest.raises(ValueError):
        refresh_listings(conn, domain, tmp_path / "proj", mode="bogus")


# ---------------------------------------------------------------------------
# listing_delay: inter-listing pacing, additional to fetch_delay's own
# per-fetch pacing inside html_cache.load_or_fetch (legacy runs both --
# main/sync/refresh_listings.py:65,167-168).
# ---------------------------------------------------------------------------


def test_listing_delay_fires_between_listings_not_after_last(conn, domain, tmp_path):
    repo = ListingsRepo(conn)
    repo.upsert([_listing("100", Tilgjengelighet="Til salgs")])
    repo.upsert([_listing("100", Tilgjengelighet="Til salgs")])  # activate
    repo.upsert([_listing("200", Tilgjengelighet="Til salgs")])
    repo.upsert([_listing("200", Tilgjengelighet="Til salgs")])  # activate
    repo.upsert([_listing("300", Tilgjengelighet="Til salgs")])
    repo.upsert([_listing("300", Tilgjengelighet="Til salgs")])  # activate
    assert repo.active_finnkodes() == {"100", "200", "300"}

    delay_calls = []

    stats = refresh_listings(
        conn, domain, tmp_path / "proj", mode="all",
        fetch=_fake_fetch_minimal, fetch_delay=lambda: None,
        listing_delay=lambda: delay_calls.append(1),
    )

    assert stats["candidates"] == 3
    # Fires between listings only: 3 candidates -> 2 delay calls, none after
    # the last listing (matches legacy's `if current_num < total`).
    assert len(delay_calls) == 2


# ---------------------------------------------------------------------------
# Fix 7 (deferred-#9): regression-lock the default-delay branches. Each
# default-delay function gets one test that monkeypatches `time.sleep` in
# its own module, drives the None-default path once, and asserts the
# expected sleep argument -- locking the defaults, this phase's proven
# regression class.
# ---------------------------------------------------------------------------


def test_listing_delay_default_sleeps_0_2s(conn, domain, tmp_path, monkeypatch):
    from skannonser.ingest.finn import refresh as refresh_module

    repo = ListingsRepo(conn)
    repo.upsert([_listing("100", Tilgjengelighet="Til salgs")])
    repo.upsert([_listing("100", Tilgjengelighet="Til salgs")])
    repo.upsert([_listing("200", Tilgjengelighet="Til salgs")])
    repo.upsert([_listing("200", Tilgjengelighet="Til salgs")])

    sleep_calls = []
    monkeypatch.setattr(refresh_module.time, "sleep", lambda s: sleep_calls.append(s))

    refresh_listings(
        conn, domain, tmp_path / "proj", mode="all",
        fetch=_fake_fetch_minimal, fetch_delay=lambda: None,
    )

    # html_cache's own default fetch_delay (0.1s) is bypassed above via the
    # explicit no-op, isolating this to listing_delay's default sleep only.
    assert sleep_calls == [0.2]


# ---------------------------------------------------------------------------
# CLI: `skannonser run refresh`
# ---------------------------------------------------------------------------


def _seeded_db(tmp_path) -> Path:
    db = tmp_path / "cli.db"
    c = connection.connect(db)
    migrations.migrate(c)
    c.close()
    return db


def test_cli_refresh_missing_db_exits_nonzero(tmp_path, monkeypatch):
    monkeypatch.setenv("SKANNONSER_DB_PATH", str(tmp_path / "nope.db"))
    result = CliRunner().invoke(app, ["run", "refresh"])
    assert result.exit_code == 1
    assert "not found" in result.output


def test_cli_refresh_rejects_bad_mode(tmp_path):
    db = _seeded_db(tmp_path)
    result = CliRunner().invoke(
        app, ["run", "refresh", "--mode", "bogus", "--db", str(db)]
    )
    assert result.exit_code == 2


def test_cli_refresh_exits_nonzero_when_migrations_pending(tmp_path):
    db = tmp_path / "unmigrated.db"
    connection.connect(db).close()  # touches the file but applies no migrations

    result = CliRunner().invoke(app, ["run", "refresh", "--db", str(db)])
    assert result.exit_code == 1
    assert "pending migrations" in result.output
    assert "skannonser db migrate" in result.output


def test_cli_refresh_routes_to_refresh_listings(tmp_path, monkeypatch):
    db = _seeded_db(tmp_path)
    calls = []

    def fake_refresh_listings(conn, domain, project_dir, mode):
        calls.append(mode)
        return {"candidates": 0, "refreshed": 0, "status_changed": 0, "errors": 0}

    monkeypatch.setattr(run_cmd, "refresh_listings", fake_refresh_listings)

    result = CliRunner().invoke(
        app, ["run", "refresh", "--mode", "stale-open", "--db", str(db)]
    )
    assert result.exit_code == 0, result.output
    assert calls == ["stale-open"]
