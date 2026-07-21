"""Tests for the DNB travel enrichment port
(`skannonser/enrich/dnb_travel.py`) and the `skannonser run enrich-dnb` CLI
command.

No network: every test drives `run_dnb_travel` with a fake `post` returning
canned Google Routes API JSON, through a real `Gateway` backed by a migrated
tmp-file sqlite DB so the `api_usage` ledger and budget enforcement are
exercised for real. `DnbRepo` seeding follows the two-upsert
activate-on-second-appearance pattern from `tests/rebuild/test_dnb_load.py`.
"""

from pathlib import Path

import pytest
from typer.testing import CliRunner

from skannonser.cli import app
from skannonser.commands import run_cmd
from skannonser.config.domain import Budget, load_domain
from skannonser.enrich.dnb_travel import run_dnb_travel
from skannonser.gateway import BudgetExceeded, Gateway
from skannonser.ingest.base import NormalizedListing
from skannonser.store import connection, migrations
from skannonser.store.repositories.dnb import DnbRepo
from skannonser.store.repositories.listings import ListingsRepo

API_KEY = "test-key"


# --- fixtures / helpers ------------------------------------------------


class FakeResponse:
    def __init__(self, status_code=200, payload=None):
        self.status_code = status_code
        self._payload = payload if payload is not None else {}

    def json(self):
        return self._payload


def make_budget(**overrides) -> Budget:
    defaults = dict(
        routes_monthly_cap=9000,
        geocode_monthly_cap=9000,
        warn_pcts=[50, 80],
        routes_rpm=6000,
        geocode_rpm=6000,
    )
    defaults.update(overrides)
    return Budget(**defaults)


@pytest.fixture
def conn(tmp_path):
    c = connection.connect(tmp_path / "dnbtravel.db")
    migrations.migrate(c)
    return c


@pytest.fixture
def domain():
    return load_domain()


@pytest.fixture
def gateway(conn):
    return Gateway(conn, make_budget(), notify=lambda m: None, sleeper=lambda s: None)


def _routes_row_count(conn) -> int:
    return conn.execute(
        "SELECT COUNT(*) AS c FROM api_usage WHERE api='routes'"
    ).fetchone()["c"]


def _seed_ok_rows(conn, api, count, month):
    called_at = f"{month}-01 00:00:00"
    conn.executemany(
        "INSERT INTO api_usage (api, outcome, called_at) VALUES (?, ?, ?)",
        [(api, "ok", called_at) for _ in range(count)],
    )
    conn.commit()


def _dnb_row(url: str, **kw) -> dict:
    row = {
        "URL": url,
        "Title": "Pen leilighet",
        "StreetAddress": "Storgata 1",
        "PostalCode": "0155",
        "PropertyType": "Leilighet",
        "Latitude": 59.9139,
        "Longitude": 10.7522,
        "Price": 5000000,
    }
    row.update(kw)
    return row


def _seed_dnb_row(conn, url: str, *, matched=False, **kw) -> None:
    """Seed an active dnbeiendom row (two upserts, activate-on-second-
    appearance quirk -- see DnbRepo/test_dnb_load.py)."""
    row = _dnb_row(url, **kw)
    if matched:
        # duplicate_of_finnkode is FK-constrained to eiendom.finnkode.
        listing = NormalizedListing(
            Finnkode="123456",
            URL="https://www.finn.no/realestate/ad.html?finnkode=123456",
            Adresse="Storgata 1",
            Postnummer="0155",
        )
        ListingsRepo(conn).upsert([listing])
        row["duplicate_of_finnkode"] = "123456"
    repo = DnbRepo(conn)
    repo.upsert([row])
    repo.upsert([row])


def _get_row(conn, url: str) -> dict:
    r = conn.execute(
        "SELECT pendl_rush_brj, pendl_rush_mvv FROM dnbeiendom WHERE url = ?", (url,)
    ).fetchone()
    return dict(r)


def _routes_response(seconds: int) -> FakeResponse:
    return FakeResponse(200, {"routes": [{"duration": f"{seconds}s"}]})


_NO_ROUTES_RESPONSE = FakeResponse(200, {"routes": []})


def _counting_post(response_fn):
    calls = []

    def post(url, headers=None, json=None, timeout=None):
        calls.append((url, headers, json))
        return response_fn(len(calls))

    return post, calls


# --- run_dnb_travel: candidacy -----------------------------------------


def test_matched_row_not_called(conn, domain, gateway):
    _seed_dnb_row(conn, "https://dnbeiendom.no/bolig/matched", matched=True)
    post, calls = _counting_post(lambda n: _routes_response(600))

    stats = run_dnb_travel(conn, domain, gateway, API_KEY, post=post)

    assert calls == []
    assert stats["candidates"] == 0
    row = _get_row(conn, "https://dnbeiendom.no/bolig/matched")
    assert row == {"pendl_rush_brj": None, "pendl_rush_mvv": None}


def test_unmatched_missing_row_called_for_both_destinations_and_stored(conn, domain, gateway):
    url = "https://dnbeiendom.no/bolig/unmatched"
    _seed_dnb_row(conn, url)
    post, calls = _counting_post(lambda n: _routes_response(600))

    stats = run_dnb_travel(conn, domain, gateway, API_KEY, post=post)

    assert len(calls) == 2
    assert stats == {
        "candidates": 1,
        "api_calls": 2,
        "brj_written": 1,
        "mvv_written": 1,
        "sentinels_written": 0,
    }
    row = _get_row(conn, url)
    assert row == {"pendl_rush_brj": 10, "pendl_rush_mvv": 10}


def test_existing_value_not_overwritten_and_not_recalled(conn, domain, gateway):
    url = "https://dnbeiendom.no/bolig/half-done"
    _seed_dnb_row(conn, url)
    conn.execute("UPDATE dnbeiendom SET pendl_rush_brj = 30 WHERE url = ?", (url,))
    conn.commit()

    post, calls = _counting_post(lambda n: _routes_response(1200))

    stats = run_dnb_travel(conn, domain, gateway, API_KEY, post=post)

    # Only MVV was missing -- exactly one HTTP attempt, for MVV alone.
    assert len(calls) == 1
    assert stats["candidates"] == 1
    assert stats["brj_written"] == 0
    assert stats["mvv_written"] == 1
    row = _get_row(conn, url)
    assert row["pendl_rush_brj"] == 30  # untouched
    assert row["pendl_rush_mvv"] == 20


def test_sentinel_stored_and_skipped_on_rerun(conn, domain, gateway):
    url = "https://dnbeiendom.no/bolig/no-routes"
    _seed_dnb_row(conn, url)
    post, calls = _counting_post(lambda n: _NO_ROUTES_RESPONSE)

    stats = run_dnb_travel(conn, domain, gateway, API_KEY, post=post)
    assert len(calls) == 2
    assert stats["sentinels_written"] == 2
    row = _get_row(conn, url)
    assert row == {"pendl_rush_brj": -1, "pendl_rush_mvv": -1}  # TRAVEL_NO_ROUTES

    # Rerun: both columns are non-NULL sentinels now -- not candidates again.
    post2, calls2 = _counting_post(lambda n: _routes_response(600))
    stats2 = run_dnb_travel(conn, domain, gateway, API_KEY, post=post2)
    assert calls2 == []
    assert stats2["candidates"] == 0
    row = _get_row(conn, url)
    assert row == {"pendl_rush_brj": -1, "pendl_rush_mvv": -1}  # still untouched


def test_limit_stops_at_n_calls(conn, domain, gateway):
    _seed_dnb_row(conn, "https://dnbeiendom.no/bolig/a")
    _seed_dnb_row(conn, "https://dnbeiendom.no/bolig/b", StreetAddress="Annen gate 2")
    post, calls = _counting_post(lambda n: _routes_response(600))

    stats = run_dnb_travel(conn, domain, gateway, API_KEY, post=post, limit=1)

    assert len(calls) == 1
    assert stats["api_calls"] == 1
    assert _routes_row_count(conn) == 1


def test_budget_exceeded_propagates_row_untouched(conn, domain):
    url = "https://dnbeiendom.no/bolig/budget"
    _seed_dnb_row(conn, url)
    budget = make_budget(routes_monthly_cap=1)
    gw = Gateway(conn, budget, notify=lambda m: None, sleeper=lambda s: None)
    _seed_ok_rows(conn, "routes", 1, month=gw.clock())

    post, calls = _counting_post(lambda n: _routes_response(600))

    with pytest.raises(BudgetExceeded):
        run_dnb_travel(conn, domain, gw, API_KEY, post=post)

    assert calls == []  # gateway blocks before the HTTP call ever fires
    row = _get_row(conn, url)
    assert row == {"pendl_rush_brj": None, "pendl_rush_mvv": None}


def test_ledger_rows_equal_http_attempts(conn, domain, gateway):
    _seed_dnb_row(conn, "https://dnbeiendom.no/bolig/ledger")
    post, calls = _counting_post(lambda n: _routes_response(600))

    run_dnb_travel(conn, domain, gateway, API_KEY, post=post)

    assert _routes_row_count(conn) == len(calls) == 2


# ---------------------------------------------------------------------------
# CLI: `skannonser run enrich-dnb`
# ---------------------------------------------------------------------------


def _seeded_db(tmp_path) -> Path:
    db = tmp_path / "cli.db"
    c = connection.connect(db)
    migrations.migrate(c)
    c.close()
    return db


def test_cli_enrich_dnb_missing_db_exits_nonzero(tmp_path, monkeypatch):
    monkeypatch.setenv("SKANNONSER_DB_PATH", str(tmp_path / "nope.db"))
    monkeypatch.setenv("GOOGLE_MAPS_API_KEY", "K")
    result = CliRunner().invoke(app, ["run", "enrich-dnb"])
    assert result.exit_code == 1
    assert "not found" in result.output


def test_cli_enrich_dnb_exits_nonzero_when_migrations_pending(tmp_path, monkeypatch):
    monkeypatch.setenv("GOOGLE_MAPS_API_KEY", "K")
    db = tmp_path / "unmigrated.db"
    connection.connect(db).close()

    result = CliRunner().invoke(app, ["run", "enrich-dnb", "--db", str(db)])
    assert result.exit_code == 1
    assert "pending migrations" in result.output


def test_cli_enrich_dnb_missing_api_key_exits_1(tmp_path, monkeypatch):
    monkeypatch.setenv("GOOGLE_MAPS_API_KEY", "")
    db = _seeded_db(tmp_path)
    result = CliRunner().invoke(app, ["run", "enrich-dnb", "--db", str(db)])
    assert result.exit_code == 1
    assert "GOOGLE_MAPS_API_KEY not set" in result.output


def test_cli_enrich_dnb_routes_to_run_dnb_travel(tmp_path, monkeypatch):
    monkeypatch.setenv("GOOGLE_MAPS_API_KEY", "K")
    db = _seeded_db(tmp_path)
    calls = []

    def fake_run_dnb_travel(conn, domain, gateway, api_key, post=None, limit=0):
        calls.append((limit, api_key))
        return {"candidates": 0, "api_calls": 0}

    monkeypatch.setattr(run_cmd, "run_dnb_travel", fake_run_dnb_travel)

    result = CliRunner().invoke(
        app, ["run", "enrich-dnb", "--limit", "5", "--db", str(db)]
    )
    assert result.exit_code == 0, result.output
    assert calls == [(5, "K")]


def test_cli_enrich_dnb_budget_exceeded_exits_3(tmp_path, monkeypatch):
    monkeypatch.setenv("GOOGLE_MAPS_API_KEY", "K")
    db = _seeded_db(tmp_path)

    def fake_run_dnb_travel(*a, **k):
        raise BudgetExceeded("routes", 9000, 9000)

    monkeypatch.setattr(run_cmd, "run_dnb_travel", fake_run_dnb_travel)

    result = CliRunner().invoke(app, ["run", "enrich-dnb", "--db", str(db)])
    assert result.exit_code == 3
    assert "dnb travel budget exhausted - resumes next window" in result.output
