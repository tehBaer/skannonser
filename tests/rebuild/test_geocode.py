"""Tests for the geocoder port (`skannonser/enrich/geocode.py`) and the
`skannonser run geocode` CLI command.

No network: every test drives `geocode_address`/`run_geocode` with a fake
`get` returning canned Google Geocoding API JSON, through a real `Gateway`
backed by an in-memory-ish sqlite file so the `api_usage` ledger and budget
enforcement are exercised for real.
"""

from pathlib import Path

import pytest
from typer.testing import CliRunner

from skannonser.cli import app
from skannonser.commands import run_cmd
from skannonser.config.domain import Budget
from skannonser.enrich.geocode import (
    geocode_address,
    normalize_postal_code,
    run_geocode,
)
from skannonser.gateway import BudgetExceeded, Gateway
from skannonser.ingest.base import NormalizedListing
from skannonser.store import connection, migrations
from skannonser.store.repositories.listings import ListingsRepo
from skannonser.store.repositories.processed import ProcessedRepo

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
    c = connection.connect(tmp_path / "geocode.db")
    migrations.migrate(c)
    return c


@pytest.fixture
def gateway(conn):
    return Gateway(conn, make_budget(), notify=lambda m: None, sleeper=lambda s: None)


def _geocode_row_count(gw: Gateway) -> int:
    return gw.conn.execute(
        "SELECT COUNT(*) AS c FROM api_usage WHERE api='geocode'"
    ).fetchone()["c"]


def _seed_ok_rows(conn, api, count, month):
    called_at = f"{month}-01 00:00:00"
    conn.executemany(
        "INSERT INTO api_usage (api, outcome, called_at) VALUES (?, ?, ?)",
        [(api, "ok", called_at) for _ in range(count)],
    )
    conn.commit()


def _addr_component(value: str, types: list[str]) -> dict:
    return {"long_name": value, "short_name": value, "types": types}


def _result(
    *,
    country: str | None = "NO",
    postal: str | None = None,
    result_types: list[str] | None = None,
    extra_component_types: list[str] | None = None,
    location_type: str = "ROOFTOP",
    lat: float = 59.91,
    lng: float = 10.75,
) -> dict:
    components = []
    if country is not None:
        components.append(_addr_component(country, ["country", "political"]))
    if postal is not None:
        components.append(_addr_component(postal, ["postal_code"]))
    for t in extra_component_types or []:
        components.append(_addr_component("X", [t]))
    return {
        "types": list(result_types or []),
        "address_components": components,
        "geometry": {"location": {"lat": lat, "lng": lng}, "location_type": location_type},
    }


def _payload(*results: dict) -> dict:
    return {"status": "OK", "results": list(results)}


_EMPTY_PAYLOAD = {"status": "OK", "results": []}


def _seed_eiendom(listings_repo: ListingsRepo, finnkode: str, *, adresse="Storgata 1", postnummer="0575"):
    """Seed an active `eiendom` row -- FK target for `eiendom_processed`.
    Active from the single upsert (listings activate on first appearance,
    user mandate 2026-07-20)."""
    listing = NormalizedListing(
        Finnkode=finnkode,
        URL=f"https://www.finn.no/realestate/ad.html?finnkode={finnkode}",
        Adresse=adresse,
        Postnummer=postnummer,
    )
    listings_repo.upsert([listing])


# --- normalize_postal_code ----------------------------------------------


def test_normalize_postal_code_zfills_short_codes():
    assert normalize_postal_code("575") == "0575"


def test_normalize_postal_code_strips_non_digits():
    assert normalize_postal_code(" NO-0575 ") == "0575"


def test_normalize_postal_code_empty():
    assert normalize_postal_code(None) == ""


# --- geocode_address: three-pass strategy --------------------------------


def test_strict_pass_hit_returns_first_result(gateway):
    """A strict-pass hit stops after one HTTP attempt and returns the raw
    lat/lng from the API (any swap-normalization is ProcessedRepo's job, not
    geocode_address's -- exercised end-to-end via run_geocode below)."""
    get = lambda *a, **k: FakeResponse(200, _payload(_result(postal="0575")))
    result = geocode_address("Storgata 1", "0575", API_KEY, gateway, get=get)
    assert result == (59.91, 10.75)
    assert _geocode_row_count(gateway) == 1


def test_strict_postal_mismatch_falls_through_to_relaxed_which_accepts_street_level(gateway):
    calls = []

    def get(url, params=None, timeout=None):
        calls.append(params["components"])
        if "postal_code" in params["components"]:
            # Strict pass: a result comes back but its postal doesn't match.
            return FakeResponse(200, _payload(_result(postal="0577")))
        # Relaxed pass: no postal filter in components, but has street signal.
        return FakeResponse(
            200,
            _payload(_result(postal="0575", result_types=["route"])),
        )

    result = geocode_address("Storgata 1", "0575", API_KEY, gateway, get=get)
    assert result == (59.91, 10.75)
    assert len(calls) == 2
    assert "postal_code" in calls[0]
    assert calls[1] == "country:NO"
    assert _geocode_row_count(gateway) == 2


def test_relaxed_pass_rejects_approximate_location_type(gateway):
    def get(url, params=None, timeout=None):
        if "postal_code" in params["components"]:
            return FakeResponse(200, _EMPTY_PAYLOAD)
        # Relaxed/fallback: street-level signal present, but geometry is only
        # APPROXIMATE and the postal doesn't match -> must be rejected.
        return FakeResponse(
            200,
            _payload(
                _result(
                    postal="0999",
                    result_types=["route"],
                    location_type="APPROXIMATE",
                )
            ),
        )

    result = geocode_address("Storgata 1", "0575", API_KEY, gateway, get=get)
    assert result is None
    # strict + relaxed + fallback = 3 attempts, all rejected.
    assert _geocode_row_count(gateway) == 3


def test_relaxed_pass_rejects_missing_street_level_signal(gateway):
    def get(url, params=None, timeout=None):
        if "postal_code" in params["components"]:
            return FakeResponse(200, _EMPTY_PAYLOAD)
        # No street_address/premise/subpremise/route type, no matching
        # component types either -- must be rejected even though ROOFTOP.
        return FakeResponse(
            200, _payload(_result(postal="0575", result_types=["locality"]))
        )

    result = geocode_address("Storgata 1", "0575", API_KEY, gateway, get=get)
    assert result is None
    assert _geocode_row_count(gateway) == 3


def test_wrong_country_rejected_in_every_pass(gateway):
    get = lambda *a, **k: FakeResponse(
        200, _payload(_result(country="SE", postal="0575", result_types=["street_address"]))
    )
    result = geocode_address("Storgata 1", "0575", API_KEY, gateway, get=get)
    assert result is None
    assert _geocode_row_count(gateway) == 3


def test_fallback_pass_accepts_address_and_country_only(gateway):
    def get(url, params=None, timeout=None):
        if params["components"] == "country:NO" and "0575" not in params["address"]:
            # Fallback pass only: no postal code in query at all.
            return FakeResponse(
                200, _payload(_result(postal=None, result_types=["street_address"]))
            )
        return FakeResponse(200, _EMPTY_PAYLOAD)

    result = geocode_address("Storgata 1", "0575", API_KEY, gateway, get=get)
    assert result == (59.91, 10.75)
    assert _geocode_row_count(gateway) == 3


def test_no_postal_code_accepts_on_first_pass_without_street_level_check(gateway):
    """When postal_code is blank, the first ("strict") pass degenerates to
    plain `country:NO` components (no postal to filter on), but its
    acceptance check is still the strict-pass branch: since
    `request_postal` is falsy, the postal-mismatch check never fires and
    the first country-NO result is accepted outright -- no street-level
    signal required, unlike a genuine relaxed/fallback pass. This mirrors
    legacy exactly: `strict_postal=True` is passed unconditionally on the
    first call regardless of whether a postal code was available. Only one
    HTTP attempt is made; the postal-gated second pass is skipped entirely
    since `normalized_postal` is empty."""
    calls = []

    def get(url, params=None, timeout=None):
        calls.append(params["components"])
        return FakeResponse(200, _payload(_result(postal=None, result_types=["locality"])))

    result = geocode_address("Storgata 1", "", API_KEY, gateway, get=get)
    assert result == (59.91, 10.75)
    assert calls == ["country:NO"]


def test_blank_address_returns_none_without_any_http_call(gateway):
    calls = []
    result = geocode_address("   ", "0575", API_KEY, gateway, get=lambda *a, **k: calls.append(1))
    assert result is None
    assert calls == []
    assert _geocode_row_count(gateway) == 0


def test_non_200_status_falls_through_passes(gateway):
    def get(url, params=None, timeout=None):
        if "postal_code" in params["components"]:
            return FakeResponse(500, {})
        return FakeResponse(200, _payload(_result(postal="0575", result_types=["route"])))

    result = geocode_address("Storgata 1", "0575", API_KEY, gateway, get=get)
    assert result == (59.91, 10.75)


def test_api_status_not_ok_falls_through_passes(gateway):
    def get(url, params=None, timeout=None):
        if "postal_code" in params["components"]:
            return FakeResponse(200, {"status": "ZERO_RESULTS"})
        return FakeResponse(200, _payload(_result(postal="0575", result_types=["route"])))

    result = geocode_address("Storgata 1", "0575", API_KEY, gateway, get=get)
    assert result == (59.91, 10.75)


def test_budget_exceeded_propagates_out_of_geocode_address(conn):
    budget = make_budget(geocode_monthly_cap=1)
    gw = Gateway(conn, budget, notify=lambda m: None, sleeper=lambda s: None)
    _seed_ok_rows(conn, "geocode", 1, month=gw.clock())

    get = lambda *a, **k: FakeResponse(200, _payload(_result(postal="0575")))
    with pytest.raises(BudgetExceeded):
        geocode_address("Storgata 1", "0575", API_KEY, gw, get=get)


# --- run_geocode -----------------------------------------------------------


@pytest.fixture
def domain():
    from skannonser.config.domain import load_domain

    return load_domain()


@pytest.fixture
def listings(conn):
    return ListingsRepo(conn)


def test_run_geocode_success_stores_swap_normalized_coords(conn, domain, gateway, listings):
    _seed_eiendom(listings, "111")

    # API returns lat/lng already swapped relative to real-world Norway
    # coords (lat outside [57,72], lng inside it) -- run_geocode must not
    # do its own swap; ProcessedRepo.set_coordinates is responsible, and
    # this proves the value flows through untouched into it.
    get = lambda *a, **k: FakeResponse(
        200, _payload(_result(postal="0575", lat=10.75, lng=59.91))
    )

    stats = run_geocode(conn, domain, gateway, API_KEY, get=get)
    assert stats == {"candidates": 1, "geocoded": 1, "failed": 0}

    row = conn.execute(
        "SELECT lat, lng, geocode_failed FROM eiendom_processed WHERE finnkode='111'"
    ).fetchone()
    assert (row["lat"], row["lng"], row["geocode_failed"]) == (59.91, 10.75, 0)


def test_run_geocode_definitive_miss_marks_failed_and_excludes_next_call(
    conn, domain, gateway, listings
):
    _seed_eiendom(listings, "111")
    get = lambda *a, **k: FakeResponse(200, _EMPTY_PAYLOAD)

    stats = run_geocode(conn, domain, gateway, API_KEY, get=get)
    assert stats == {"candidates": 1, "geocoded": 0, "failed": 1}

    repo = ProcessedRepo(conn)
    assert repo.missing_coordinates() == []
    row = conn.execute(
        "SELECT geocode_failed FROM eiendom_processed WHERE finnkode='111'"
    ).fetchone()
    assert row["geocode_failed"] == 1


def test_run_geocode_respects_limit(conn, domain, gateway, listings):
    _seed_eiendom(listings, "111")
    _seed_eiendom(listings, "222")
    get = lambda *a, **k: FakeResponse(200, _payload(_result(postal="0575")))

    stats = run_geocode(conn, domain, gateway, API_KEY, limit=1, get=get)
    assert stats["candidates"] == 1
    assert stats["geocoded"] == 1


def test_run_geocode_include_inactive(conn, domain, gateway, listings):
    listing = NormalizedListing(
        Finnkode="111",
        URL="https://www.finn.no/realestate/ad.html?finnkode=111",
        Adresse="Storgata 1",
        Postnummer="0575",
    )
    listings.upsert([listing])
    # Listings activate on first appearance now (user mandate 2026-07-20);
    # force this row inactive via direct SQL so the test still proves
    # include_inactive's effect on candidacy, not just "a row exists".
    conn.execute("UPDATE eiendom SET active = 0 WHERE finnkode = '111'")

    get = lambda *a, **k: FakeResponse(200, _payload(_result(postal="0575")))

    assert run_geocode(conn, domain, gateway, API_KEY, get=get)["candidates"] == 0
    stats = run_geocode(conn, domain, gateway, API_KEY, include_inactive=True, get=get)
    assert stats == {"candidates": 1, "geocoded": 1, "failed": 0}


def test_run_geocode_gateway_ledger_rows_equal_http_attempts(conn, domain, gateway, listings):
    _seed_eiendom(listings, "111")
    _seed_eiendom(listings, "222")

    def get(url, params=None, timeout=None):
        # Strict pass (postal in components) always misses; relaxed pass
        # (no postal filter) always hits -- 2 HTTP attempts per candidate.
        if "postal_code" in params["components"]:
            return FakeResponse(200, _EMPTY_PAYLOAD)
        return FakeResponse(200, _payload(_result(result_types=["route"])))

    run_geocode(conn, domain, gateway, API_KEY, get=get)
    # Each candidate takes strict + relaxed = 2 attempts before succeeding.
    assert _geocode_row_count(gateway) == 4


def test_run_geocode_budget_exceeded_propagates_untouched(conn, domain, listings):
    _seed_eiendom(listings, "111")
    budget = make_budget(geocode_monthly_cap=1)
    gw = Gateway(conn, budget, notify=lambda m: None, sleeper=lambda s: None)
    _seed_ok_rows(conn, "geocode", 1, month=gw.clock())

    get = lambda *a, **k: FakeResponse(200, _payload(_result(postal="0575")))

    with pytest.raises(BudgetExceeded):
        run_geocode(conn, domain, gw, API_KEY, get=get)

    # The candidate must NOT have been marked failed -- BudgetExceeded is an
    # administrative stop, not a per-row geocoding miss.
    repo = ProcessedRepo(conn)
    row = conn.execute(
        "SELECT * FROM eiendom_processed WHERE finnkode='111'"
    ).fetchone()
    assert row is None
    assert len(repo.missing_coordinates()) == 1


# ---------------------------------------------------------------------------
# CLI: `skannonser run geocode`
# ---------------------------------------------------------------------------


def _seeded_db(tmp_path) -> Path:
    db = tmp_path / "cli.db"
    c = connection.connect(db)
    migrations.migrate(c)
    c.close()
    return db


def test_cli_geocode_missing_db_exits_nonzero(tmp_path, monkeypatch):
    monkeypatch.setenv("SKANNONSER_DB_PATH", str(tmp_path / "nope.db"))
    monkeypatch.setenv("GOOGLE_MAPS_API_KEY", "K")
    result = CliRunner().invoke(app, ["run", "geocode"])
    assert result.exit_code == 1
    assert "not found" in result.output


def test_cli_geocode_exits_nonzero_when_migrations_pending(tmp_path, monkeypatch):
    monkeypatch.setenv("GOOGLE_MAPS_API_KEY", "K")
    db = tmp_path / "unmigrated.db"
    connection.connect(db).close()

    result = CliRunner().invoke(app, ["run", "geocode", "--db", str(db)])
    assert result.exit_code == 1
    assert "pending migrations" in result.output


def test_cli_geocode_missing_api_key_exits_1(tmp_path, monkeypatch):
    monkeypatch.setenv("GOOGLE_MAPS_API_KEY", "")
    db = _seeded_db(tmp_path)
    result = CliRunner().invoke(app, ["run", "geocode", "--db", str(db)])
    assert result.exit_code == 1
    assert "GOOGLE_MAPS_API_KEY not set" in result.output


def test_cli_geocode_routes_to_run_geocode(tmp_path, monkeypatch):
    monkeypatch.setenv("GOOGLE_MAPS_API_KEY", "K")
    db = _seeded_db(tmp_path)
    calls = []

    def fake_run_geocode(conn, domain, gateway, api_key, limit=0, include_inactive=False, get=None):
        calls.append((limit, include_inactive, api_key))
        return {"candidates": 0, "geocoded": 0, "failed": 0}

    monkeypatch.setattr(run_cmd, "run_geocode", fake_run_geocode)

    result = CliRunner().invoke(
        app, ["run", "geocode", "--limit", "5", "--include-inactive", "--db", str(db)]
    )
    assert result.exit_code == 0, result.output
    assert calls == [(5, True, "K")]


def test_cli_geocode_budget_exceeded_exits_3(tmp_path, monkeypatch):
    monkeypatch.setenv("GOOGLE_MAPS_API_KEY", "K")
    db = _seeded_db(tmp_path)

    def fake_run_geocode(*a, **k):
        raise BudgetExceeded("geocode", 9000, 9000)

    monkeypatch.setattr(run_cmd, "run_geocode", fake_run_geocode)

    result = CliRunner().invoke(app, ["run", "geocode", "--db", str(db)])
    assert result.exit_code == 3
    assert "geocode budget exhausted - resumes next window" in result.output
