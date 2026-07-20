"""Tests for the enrich orchestrator (`skannonser/enrich/travel.py`), the
`ListingsRepo.update_derived` derivation write, and the `skannonser run enrich`
/ `skannonser estimate` CLI commands.

No network: `run_enrich` is driven with a fake `post` returning canned Routes
API JSON, through a real `Gateway` over a migrated tmp DB, so the `api_usage`
ledger and budget enforcement are exercised for real. Active listings are
seeded the legacy way (two `ListingsRepo.upsert` calls -> `active = 1`);
coordinates and existing travel values are seeded via `ProcessedRepo.upsert`.
"""

import pandas as pd
import pytest
from typer.testing import CliRunner

from skannonser.cli import app
from skannonser.commands import run_cmd
from skannonser.config.domain import Budget, load_domain
from skannonser.enrich.travel import (
    compute_pris_kvm,
    estimate,
    run_enrich,
    title_address,
)
from skannonser.gateway import BudgetExceeded, Gateway
from skannonser.ingest.base import NormalizedListing
from skannonser.store import connection, migrations
from skannonser.store.repositories.listings import ListingsRepo
from skannonser.store.repositories.processed import ProcessedRepo

API_KEY = "test-key"
OSLO_LAT = 59.9139
OSLO_LNG = 10.7522


def _north(meters: float) -> float:
    return OSLO_LAT + meters / 111_320.0


# --------------------------------------------------------------------------
# fakes / fixtures
# --------------------------------------------------------------------------


class FakeResponse:
    def __init__(self, status_code=200, payload=None):
        self.status_code = status_code
        self._payload = payload if payload is not None else {}

    def json(self):
        return self._payload


def _routes_payload(minutes: int) -> dict:
    return {"routes": [{"duration": f"{minutes * 60}s", "distanceMeters": 1000}]}


class FakePost:
    """Callable matching `requests.post(url, headers=, json=, timeout=)`."""

    def __init__(self, minutes=25, empty=False, status=200):
        self.calls = []
        self.minutes = minutes
        self.empty = empty
        self.status = status

    def __call__(self, url, headers=None, json=None, timeout=None):
        self.calls.append(json)
        if self.status != 200:
            return FakeResponse(self.status, {})
        if self.empty:
            return FakeResponse(200, {"routes": []})  # -> TRAVEL_NO_ROUTES sentinel
        return FakeResponse(200, _routes_payload(self.minutes))


def _make_budget(**overrides) -> Budget:
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
    c = connection.connect(tmp_path / "enrich.db")
    migrations.migrate(c)
    return c


@pytest.fixture
def domain():
    return load_domain()


@pytest.fixture
def gateway(conn):
    return Gateway(conn, _make_budget(), notify=lambda m: None, sleeper=lambda s: None)


def _seed_listing(
    conn, finnkode, *, adresse=None, postnummer="0575", pris=None, primary_area=None
):
    """Seed an ACTIVE eiendom row (two upserts -> active=1)."""
    repo = ListingsRepo(conn)
    listing = NormalizedListing(
        **{
            "Finnkode": finnkode,
            "URL": f"https://www.finn.no/realestate/ad.html?finnkode={finnkode}",
            "Adresse": adresse,
            "Postnummer": postnummer,
            "Pris": pris,
            "Primærrom": primary_area,
        }
    )
    repo.upsert([listing])
    repo.upsert([listing])


def _seed_processed(conn, finnkode, *, lat=None, lng=None, travel=None, link=None, adresse="Gata 1"):
    ProcessedRepo(conn).upsert(
        finnkode,
        adresse,
        "0575",
        lat=lat,
        lng=lng,
        travel=travel or {},
        travel_copy_from_finnkode=link,
    )


def _processed_row(conn, finnkode):
    return conn.execute(
        "SELECT * FROM eiendom_processed WHERE finnkode = ?", (finnkode,)
    ).fetchone()


def _api_usage_count(conn):
    return conn.execute("SELECT COUNT(*) AS c FROM api_usage").fetchone()["c"]


# ==========================================================================
# 1. compute_pris_kvm matrix
# ==========================================================================


@pytest.mark.parametrize(
    "pris,primary,usable_i,usable,expected",
    [
        (5_000_000, 70, None, None, 71429),          # primary wins
        (5_000_000, None, 50, 40, 100000),           # fallback to usable_i
        (4_000_000, None, None, 80, 50000),          # fallback to usable
        (5_000_000, 0, None, None, None),            # zero area -> None
        (5_000_000, -10, None, None, None),          # negative area -> None
        (None, 70, None, None, None),                # no price -> None
        ("5000000", "70", None, None, 71429),        # string coercion
        ("abc", 70, None, None, None),               # unparseable price -> None
        (5_000_000, "abc", 50, None, 100000),        # bad primary falls through
        (5_000_000, None, None, None, None),         # no area at all -> None
    ],
)
def test_compute_pris_kvm_matrix(pris, primary, usable_i, usable, expected):
    assert compute_pris_kvm(pris, primary, usable_i, usable) == expected


# ==========================================================================
# 2. title_address parity vs pandas
# ==========================================================================


def test_title_address_parity_vs_pandas():
    samples = ["bjørnsons gate 2a", "storgata 1 b", "ULLEVÅLSVEIEN 3", None]
    pandas_out = list(pd.Series(samples, dtype="object").str.title())
    ours = [title_address(s) for s in samples]
    assert ours == pandas_out
    # Explicit expected values (locks the digit-boundary rule).
    assert ours == ["Bjørnsons Gate 2A", "Storgata 1 B", "Ullevålsveien 3", None]


# ==========================================================================
# 9. derivations write titled adresse + pris_kvm (run every target)
# ==========================================================================


def test_run_enrich_derivations_write_titled_adresse_and_pris_kvm(conn, domain, gateway):
    _seed_listing(conn, "100", adresse="storgata 1 b", pris=5_000_000, primary_area=70)
    post = FakePost()

    stats = run_enrich(conn, domain, gateway, API_KEY, targets="brj", post=post)

    row = conn.execute(
        "SELECT adresse, pris_kvm FROM eiendom WHERE finnkode='100'"
    ).fetchone()
    assert row["adresse"] == "Storgata 1 B"
    assert row["pris_kvm"] == 71429
    assert stats["derived"] == 1
    # No coords -> no API candidate.
    assert post.calls == []


# ==========================================================================
# 3. linked brj row is skipped (no API, no write, donor_skipped++)
# ==========================================================================


def test_run_enrich_linked_brj_row_skipped_no_api(conn, domain, gateway):
    # A: a complete BRJ donor (has value + coords); B: explicitly linked to A,
    # missing BRJ -> must be skipped, no API call, no BRJ value written.
    _seed_listing(conn, "A", adresse="A gate 1")
    _seed_listing(conn, "B", adresse="B gate 2")
    _seed_processed(conn, "A", lat=OSLO_LAT, lng=OSLO_LNG, travel={"pendl_rush_brj": 30})
    _seed_processed(conn, "B", lat=_north(50), lng=OSLO_LNG, link="A")

    post = FakePost()
    stats = run_enrich(conn, domain, gateway, API_KEY, targets="brj", post=post)

    assert post.calls == []
    assert stats["api_calls"] == 0
    assert stats["donor_skipped"] == 1
    b = _processed_row(conn, "B")
    assert b["pendl_rush_brj"] is None
    assert b["travel_copy_from_finnkode"] == "A"


def test_run_enrich_prepass_assigned_link_is_persisted(conn, domain, gateway):
    # A is a complete donor for ALL three columns (so it lands in the "all"
    # cache the pre-pass searches); B is nearby and missing BRJ with NO stored
    # link. The pre-pass assigns B -> A in memory; the per-row upsert must
    # persist that link to the DB (otherwise read-time donor resolution and
    # the next run both break).
    _seed_listing(conn, "A", adresse="A gate 1")
    _seed_listing(conn, "B", adresse="B gate 2")
    _seed_processed(
        conn, "A", lat=OSLO_LAT, lng=OSLO_LNG,
        travel={"pendl_rush_brj": 30, "pendl_rush_mvv": 31, "pendl_rush_mvv_uni_rush": 32},
    )
    _seed_processed(conn, "B", lat=_north(50), lng=OSLO_LNG)  # no stored link

    post = FakePost()
    stats = run_enrich(conn, domain, gateway, API_KEY, targets="brj", post=post)

    assert post.calls == []  # B skipped via the pre-pass donor
    assert stats["donor_skipped"] == 1
    assert _processed_row(conn, "B")["travel_copy_from_finnkode"] == "A"


# ==========================================================================
# 4. mvv_uni linked row gets chain value WRITTEN
# ==========================================================================


def test_run_enrich_mvv_uni_linked_row_gets_chain_value_written(conn, domain, gateway):
    _seed_listing(conn, "A", adresse="A gate 1")
    _seed_listing(conn, "B", adresse="B gate 2")
    _seed_processed(conn, "A", lat=OSLO_LAT, lng=OSLO_LNG, travel={"pendl_rush_mvv_uni_rush": 42})
    _seed_processed(conn, "B", lat=_north(50), lng=OSLO_LNG, link="A")

    post = FakePost()
    stats = run_enrich(conn, domain, gateway, API_KEY, targets="mvv_uni", post=post)

    assert post.calls == []
    assert stats["api_calls"] == 0
    assert stats["mvv_uni_donor_written"] == 1
    b = _processed_row(conn, "B")
    assert b["pendl_rush_mvv_uni_rush"] == 42
    assert b["travel_copy_from_finnkode"] == "A"


# ==========================================================================
# 5. unlinked row gets API value; COALESCE preserves a pre-seeded column
# ==========================================================================


def test_run_enrich_api_value_written_coalesce_preserves_other_column(conn, domain, gateway):
    # C is missing BRJ but already has an MVV value + coords. Running brj must
    # write BRJ without clobbering the pre-seeded MVV (fill-only COALESCE).
    _seed_listing(conn, "C", adresse="C gate 3")
    _seed_processed(
        conn, "C", lat=OSLO_LAT, lng=OSLO_LNG, travel={"pendl_rush_mvv": 17}, adresse="C gate 3"
    )

    post = FakePost(minutes=25)
    stats = run_enrich(conn, domain, gateway, API_KEY, targets="brj", post=post)

    assert len(post.calls) == 1
    assert stats["api_calls"] == 1
    c = _processed_row(conn, "C")
    assert c["pendl_rush_brj"] == 25
    assert c["pendl_rush_mvv"] == 17  # survived
    # adresse_cleaned recomputed from the (now title-cased) eiendom.adresse
    # the enrich upsert passes through -- proving it is NOT clobbered to NULL.
    assert c["adresse_cleaned"] == "C Gate 3"


# ==========================================================================
# 6. sentinel from API is stored and NOT retried on a second run
# ==========================================================================


def test_run_enrich_sentinel_stored_and_not_retried(conn, domain, gateway):
    _seed_listing(conn, "D", adresse="D gate 4")
    _seed_processed(conn, "D", lat=OSLO_LAT, lng=OSLO_LNG)

    post = FakePost(empty=True)  # empty routes -> TRAVEL_NO_ROUTES (-1)
    stats1 = run_enrich(conn, domain, gateway, API_KEY, targets="brj", post=post)

    assert stats1["api_calls"] == 1
    assert stats1["sentinels_written"] == 1
    d = _processed_row(conn, "D")
    assert d["pendl_rush_brj"] == -1

    # Second run: -1 is not None -> not a candidate -> no new API call.
    stats2 = run_enrich(conn, domain, gateway, API_KEY, targets="brj", post=post)
    assert len(post.calls) == 1  # unchanged
    assert stats2["api_calls"] == 0


# ==========================================================================
# 7. force_api calls the API despite an existing donor link
# ==========================================================================


def test_run_enrich_force_api_calls_api_despite_link(conn, domain, gateway):
    _seed_listing(conn, "A", adresse="A gate 1")
    _seed_listing(conn, "B", adresse="B gate 2")
    _seed_processed(conn, "A", lat=OSLO_LAT, lng=OSLO_LNG, travel={"pendl_rush_brj": 30})
    _seed_processed(conn, "B", lat=_north(50), lng=OSLO_LNG, link="A")

    post = FakePost(minutes=12)
    stats = run_enrich(
        conn, domain, gateway, API_KEY, targets="brj", post=post, force_api=True
    )

    assert len(post.calls) == 1  # B got an API call despite its link to A
    assert stats["api_calls"] == 1
    assert stats["donor_skipped"] == 0
    b = _processed_row(conn, "B")
    assert b["pendl_rush_brj"] == 12


# ==========================================================================
# 8. BudgetExceeded mid-loop: earlier row written, later untouched, flag set
# ==========================================================================


def test_run_enrich_budget_exceeded_mid_loop(conn, domain):
    _seed_listing(conn, "100", adresse="Gate 100")
    _seed_listing(conn, "200", adresse="Gate 200")
    # Far apart so neither can donate to the other.
    _seed_processed(conn, "100", lat=OSLO_LAT, lng=OSLO_LNG)
    _seed_processed(conn, "200", lat=_north(3000), lng=OSLO_LNG)

    # Cap of 1 route call: row 100 succeeds, row 200 trips BudgetExceeded.
    gw = Gateway(conn, _make_budget(routes_monthly_cap=1), notify=lambda m: None, sleeper=lambda s: None)
    post = FakePost(minutes=20)

    stats = run_enrich(conn, domain, gw, API_KEY, targets="brj", post=post)

    assert stats["budget_exhausted"] is True
    assert stats["api_calls"] == 1
    assert _processed_row(conn, "100")["pendl_rush_brj"] == 20
    assert _processed_row(conn, "200")["pendl_rush_brj"] is None


# ==========================================================================
# 10 + 11. estimate: hand-computed fixtures, and zero gateway calls
# ==========================================================================


def test_estimate_brj_max_vs_simulated(conn, domain):
    # Three unlinked rows missing BRJ, clustered within 300 m, no seed donors.
    # max: no donors -> 3 attempts. simulated: first attempt seeds a donor the
    # other two reuse -> 1 attempt.
    for fk, m in (("H", 0), ("I", 60), ("J", 120)):
        _seed_listing(conn, fk, adresse=f"{fk} gate")
        _seed_processed(conn, fk, lat=_north(m), lng=OSLO_LNG)

    result = estimate(conn, domain, targets="brj")
    assert result["per_destination"]["brj"] == {"max_attempts": 3, "simulated_attempts": 1}
    assert result["totals"] == {"max_attempts": 3, "simulated_attempts": 1}
    assert _api_usage_count(conn) == 0  # estimate never calls the gateway


def test_estimate_mvv_uni_chain_resolution(conn, domain):
    # K: donor WITH a uni value. L: linked to K -> resolves -> reuse (not counted).
    # N: donor WITHOUT a uni value, far from K. P: linked to N -> no resolvable
    # value -> counts as an attempt (the mvv_uni asymmetry vs brj).
    _seed_listing(conn, "K", adresse="K gate")
    _seed_listing(conn, "L", adresse="L gate")
    _seed_listing(conn, "N", adresse="N gate")
    _seed_listing(conn, "P", adresse="P gate")
    _seed_processed(conn, "K", lat=OSLO_LAT, lng=OSLO_LNG, travel={"pendl_rush_mvv_uni_rush": 20})
    _seed_processed(conn, "L", lat=_north(50), lng=OSLO_LNG, link="K")
    _seed_processed(conn, "N", lat=_north(5000), lng=OSLO_LNG)
    _seed_processed(conn, "P", lat=_north(5050), lng=OSLO_LNG, link="N")

    result = estimate(conn, domain, targets="mvv_uni")
    # candidates missing uni: L (reuse), N (attempt), P (attempt) -> 2.
    assert result["per_destination"]["mvv_uni"] == {"max_attempts": 2, "simulated_attempts": 2}
    assert _api_usage_count(conn) == 0


def test_estimate_invalid_targets_raises(conn, domain):
    with pytest.raises(ValueError):
        estimate(conn, domain, targets="bogus")


# ==========================================================================
# CLI
# ==========================================================================


def _seeded_db(tmp_path):
    from pathlib import Path

    db = tmp_path / "cli.db"
    c = connection.connect(db)
    migrations.migrate(c)
    c.close()
    return db


def test_cli_enrich_invalid_targets_exit_2(tmp_path, monkeypatch):
    monkeypatch.setenv("GOOGLE_MAPS_API_KEY", "K")
    db = _seeded_db(tmp_path)
    result = CliRunner().invoke(app, ["run", "enrich", "--targets", "bogus", "--db", str(db)])
    assert result.exit_code == 2


def test_cli_enrich_missing_api_key_exit_1(tmp_path, monkeypatch):
    monkeypatch.setenv("GOOGLE_MAPS_API_KEY", "")
    db = _seeded_db(tmp_path)
    result = CliRunner().invoke(app, ["run", "enrich", "--db", str(db)])
    assert result.exit_code == 1
    assert "GOOGLE_MAPS_API_KEY not set" in result.output


def test_cli_enrich_pending_migrations_exit_1(tmp_path, monkeypatch):
    from pathlib import Path

    monkeypatch.setenv("GOOGLE_MAPS_API_KEY", "K")
    db = tmp_path / "unmigrated.db"
    connection.connect(db).close()
    result = CliRunner().invoke(app, ["run", "enrich", "--db", str(db)])
    assert result.exit_code == 1
    assert "pending migrations" in result.output


def test_cli_enrich_routes_to_run_enrich(tmp_path, monkeypatch):
    monkeypatch.setenv("GOOGLE_MAPS_API_KEY", "K")
    db = _seeded_db(tmp_path)
    captured = {}

    def fake_run_enrich(conn, domain, gateway, api_key, targets="all", force_api=False):
        captured["args"] = (targets, force_api, api_key)
        return {
            "derived": 0, "api_calls": 0, "donor_skipped": 0,
            "mvv_uni_donor_written": 0, "sentinels_written": 0, "budget_exhausted": False,
        }

    monkeypatch.setattr(run_cmd, "run_enrich", fake_run_enrich)
    result = CliRunner().invoke(
        app, ["run", "enrich", "--targets", "mvv", "--force-api", "--db", str(db)]
    )
    assert result.exit_code == 0, result.output
    assert captured["args"] == ("mvv", True, "K")


def test_cli_enrich_budget_exhausted_flag_exit_3(tmp_path, monkeypatch):
    monkeypatch.setenv("GOOGLE_MAPS_API_KEY", "K")
    db = _seeded_db(tmp_path)

    def fake_run_enrich(*a, **k):
        return {
            "derived": 0, "api_calls": 0, "donor_skipped": 0,
            "mvv_uni_donor_written": 0, "sentinels_written": 0, "budget_exhausted": True,
        }

    monkeypatch.setattr(run_cmd, "run_enrich", fake_run_enrich)
    result = CliRunner().invoke(app, ["run", "enrich", "--db", str(db)])
    assert result.exit_code == 3
    assert "enrich budget exhausted - resumes next window" in result.output


def test_cli_enrich_budget_exceeded_raised_exit_3(tmp_path, monkeypatch):
    monkeypatch.setenv("GOOGLE_MAPS_API_KEY", "K")
    db = _seeded_db(tmp_path)

    def fake_run_enrich(*a, **k):
        raise BudgetExceeded("routes", 9000, 9000)

    monkeypatch.setattr(run_cmd, "run_enrich", fake_run_enrich)
    result = CliRunner().invoke(app, ["run", "enrich", "--db", str(db)])
    assert result.exit_code == 3
    assert "enrich budget exhausted - resumes next window" in result.output


def test_cli_estimate_prints_totals(tmp_path, monkeypatch):
    db = _seeded_db(tmp_path)
    # Seed one active row missing BRJ, with coords.
    c = connection.connect(db)
    _seed_listing(c, "H", adresse="H gate")
    _seed_processed(c, "H", lat=OSLO_LAT, lng=OSLO_LNG)
    c.close()

    result = CliRunner().invoke(app, ["estimate", "--targets", "brj", "--db", str(db)])
    assert result.exit_code == 0, result.output
    assert "brj:" in result.output
    assert "total:" in result.output
