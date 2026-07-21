"""Tests for the enrich golden-master harness (`skannonser/verify/enrich.py`).

No network, no API key anywhere: `verify_enrich` drives legacy through
`main.post_process.post_process_eiendom` with `calculate_google_directions`
paths that structurally never reach the Google Directions client (see the
module's docstrings for the exact line evidence), and the new side never
touches `Gateway`/`TransitCommute` at all (`estimate`/`_prepare` are pure
reads). A zero `api_usage` row count after every call is asserted directly.
"""
from pathlib import Path

import pytest

from skannonser.config.domain import load_domain
from skannonser.ingest.base import NormalizedListing
from skannonser.store import connection, migrations
from skannonser.store.repositories.listings import ListingsRepo
from skannonser.store.repositories.processed import ProcessedRepo
from skannonser.verify import enrich as enrich_mod
from skannonser.verify.enrich import verify_enrich

OSLO_LAT = 59.9139
OSLO_LNG = 10.7522


def _north(meters: float) -> float:
    return OSLO_LAT + meters / 111_320.0


@pytest.fixture
def db_path(tmp_path) -> Path:
    path = tmp_path / "verify_enrich.db"
    conn = connection.connect(path)
    migrations.migrate(conn)
    conn.close()
    return path


def _seed_listing(conn, finnkode, *, adresse, postnummer="0575", pris=3_000_000):
    """Seed an ACTIVE eiendom row (active from the first upsert -- listings
    activate on first appearance, user mandate 2026-07-20)."""
    repo = ListingsRepo(conn)
    listing = NormalizedListing(
        **{
            "Finnkode": finnkode,
            "URL": f"https://www.finn.no/realestate/ad.html?finnkode={finnkode}",
            "Adresse": adresse,
            "Postnummer": postnummer,
            "Pris": pris,
        }
    )
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


def _api_usage_count(conn) -> int:
    return conn.execute("SELECT COUNT(*) AS c FROM api_usage").fetchone()["c"]


def _seed_scenario(db_path: Path) -> None:
    """One donor pair within 300m (donor -> nearby row picks it up in the
    pre-pass), one row holding a sentinel (must not be retried/donor-cache-
    eligible), one row with NULL travel data and no coords (still a
    candidate -- legacy's API is address-based), one row with a pre-existing
    donor link (pre-pass must leave it alone)."""
    conn = connection.connect(db_path)
    try:
        _seed_listing(conn, "1001", adresse="Donorveien 1")
        _seed_listing(conn, "1002", adresse="Nabogata 2")
        _seed_listing(conn, "1003", adresse="Sentinelveien 3")
        _seed_listing(conn, "1004", adresse="Lenkegata 4")

        # 1001: complete donor (all three columns valid) at OSLO_LAT/LNG.
        _seed_processed(
            conn,
            "1001",
            lat=OSLO_LAT,
            lng=OSLO_LNG,
            travel={
                "pendl_rush_brj": 20,
                "pendl_rush_mvv": 25,
                "pendl_rush_mvv_uni_rush": 30,
            },
            adresse="Donorveien 1",
        )
        # 1002: 100m north of 1001 (within the 300m reuse radius), no travel
        # data, no existing link -> pre-pass should link it to 1001.
        _seed_processed(conn, "1002", lat=_north(100), lng=OSLO_LNG, adresse="Nabogata 2")
        # 1003: far away, holds a BRJ sentinel (TRAVEL_NO_ROUTES = -1).
        _seed_processed(
            conn,
            "1003",
            lat=_north(50_000),
            lng=OSLO_LNG,
            travel={"pendl_rush_brj": -1},
            adresse="Sentinelveien 3",
        )
        # 1004: pre-existing donor link to 1001, no coords at all.
        _seed_processed(conn, "1004", link="1001", adresse="Lenkegata 4")
    finally:
        conn.close()


def test_verify_enrich_matches_on_constructed_scenario(db_path):
    _seed_scenario(db_path)

    result = verify_enrich(db_path)

    assert result.estimate_diffs == []
    assert result.donor_diffs == []
    assert result.sheet_value_diffs == []


def test_verify_enrich_makes_zero_api_calls(db_path):
    _seed_scenario(db_path)

    verify_enrich(db_path)

    conn = connection.connect(db_path)
    try:
        assert _api_usage_count(conn) == 0
    finally:
        conn.close()


def test_verify_enrich_detects_desync(db_path, monkeypatch):
    _seed_scenario(db_path)

    def _wrong_estimate(conn, domain, targets="all"):
        selected = {"all": ("brj", "mvv"), "mvv_uni": ("mvv_uni",)}[targets]
        per_destination = {
            key: {"max_attempts": 999, "simulated_attempts": 999} for key in selected
        }
        totals = {"max_attempts": 999, "simulated_attempts": 999}
        return {"per_destination": per_destination, "totals": totals}

    monkeypatch.setattr(enrich_mod, "estimate", _wrong_estimate)

    result = verify_enrich(db_path)

    assert result.estimate_diffs != []
    assert all(d.new_value == 999 for d in result.estimate_diffs)


def test_domain_loads_for_verify_enrich():
    # Sanity check that the fixture DB / domain config combination used above
    # is actually valid before relying on it in the other tests.
    domain = load_domain()
    assert {d.key for d in domain.destinations} == {"brj", "mvv", "mvv_uni"}
