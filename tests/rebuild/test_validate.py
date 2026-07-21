"""Tests for the travel-value validation port (`skannonser/enrich/validate.py`)
and the `skannonser run validate-travel` CLI command.

No network, no writes: `validate_travel` is a pure read (a `SELECT` over
`eiendom`/`eiendom_processed`) -- these tests only ever seed a tmp DB and
call it, matching the legacy tool's own read-only contract.

Synthetic clusters (all placed along the same longitude, offset in latitude
by meters -- same convention as `test_enrich.py`'s `_north`):

  * `test_local_outlier_flagged`: a 6-row cluster within `radius_m` of each
    other, sharing one postnummer (peer count 5 < `min_postcode_group` 6, so
    only the LOCAL/neighbor check can fire, not the postcode one). Five rows
    hold ~30 minutes; one holds 90. The outlier's local-neighbor diff (60)
    clears both `min_abs_diff` (20) and the MAD-robust floor, and its
    relative diff (2.0) clears `min_rel_diff` (0.35) -- it must be flagged
    with `score >= score_threshold`.
  * `test_consistent_cluster_unflagged`: a same-shaped 6-row cluster with
    all values within 1-2 minutes of each other -- no diff clears
    `min_abs_diff`, so nothing is flagged.
  * `test_small_group_skipped`: a 3-row cluster (below `min_neighbors` AND
    `min_postcode_group`) with wildly different values (30/90/150) -- every
    check is skipped for group-size reasons alone, so nothing is flagged
    despite the values looking like obvious outliers.
"""
from pathlib import Path

import pytest
from typer.testing import CliRunner

from skannonser.cli import app
from skannonser.config.domain import load_domain
from skannonser.enrich.validate import validate_travel
from skannonser.ingest.base import NormalizedListing
from skannonser.store import connection, migrations
from skannonser.store.repositories.listings import ListingsRepo
from skannonser.store.repositories.processed import ProcessedRepo

OSLO_LAT = 59.9139
OSLO_LNG = 10.7522


def _north(meters: float) -> float:
    return OSLO_LAT + meters / 111_320.0


@pytest.fixture
def db_path(tmp_path) -> Path:
    path = tmp_path / "validate.db"
    conn = connection.connect(path)
    migrations.migrate(conn)
    conn.close()
    return path


@pytest.fixture
def domain():
    return load_domain()


def _seed_listing(conn, finnkode, *, adresse, postnummer, pris=3_000_000):
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


def _seed_processed(conn, finnkode, *, lat, lng, brj, link=None, adresse="Gata 1", postnummer="0575"):
    ProcessedRepo(conn).upsert(
        finnkode,
        adresse,
        postnummer,
        lat=lat,
        lng=lng,
        travel={"pendl_rush_brj": brj},
        travel_copy_from_finnkode=link,
    )


def _seed_row(conn, finnkode, *, offset_m, brj, postnummer):
    lat = _north(offset_m)
    _seed_listing(conn, finnkode, adresse=f"Gate {finnkode}", postnummer=postnummer)
    _seed_processed(conn, finnkode, lat=lat, lng=OSLO_LNG, brj=brj, postnummer=postnummer)


# ---------------------------------------------------------------------------
# Cluster 1: outlier among >= min_neighbors peers -> flagged
# ---------------------------------------------------------------------------


def test_local_outlier_flagged(db_path, domain):
    conn = connection.connect(db_path)
    base = 0  # cluster anchored at OSLO_LAT
    baseline_values = {"O1": 29, "O2": 30, "O3": 31, "O4": 30, "O5": 32}
    for i, (fk, val) in enumerate(baseline_values.items()):
        _seed_row(conn, fk, offset_m=base + i * 40, brj=val, postnummer="1001")
    _seed_row(conn, "OUT", offset_m=base + 200, brj=90, postnummer="1001")
    conn.close()

    conn = connection.connect(db_path)
    findings = validate_travel(conn, domain)

    matches = [f for f in findings if f["finnkode"] == "OUT" and f["column"] == "pendl_rush_brj"]
    assert len(matches) == 1, findings
    finding = matches[0]
    assert finding["score"] >= 3  # score_threshold default
    assert finding["value"] == 90
    assert finding["neighbor_count"] >= 5
    assert any(r.startswith("Local:") for r in finding["reasons"])

    # Baseline rows must NOT be flagged.
    baseline_flagged = [f for f in findings if f["finnkode"] in baseline_values]
    assert baseline_flagged == []


# ---------------------------------------------------------------------------
# Cluster 2: consistent cluster -> unflagged
# ---------------------------------------------------------------------------


def test_consistent_cluster_unflagged(db_path, domain):
    conn = connection.connect(db_path)
    values = {"C1": 30, "C2": 31, "C3": 29, "C4": 32, "C5": 30, "C6": 31}
    for i, (fk, val) in enumerate(values.items()):
        _seed_row(conn, fk, offset_m=5000 + i * 40, brj=val, postnummer="1002")
    conn.close()

    conn = connection.connect(db_path)
    findings = validate_travel(conn, domain)

    flagged = [f for f in findings if f["finnkode"] in values]
    assert flagged == [], findings


# ---------------------------------------------------------------------------
# Cluster 3: small group (below min_neighbors/min_postcode_group) -> skipped
# ---------------------------------------------------------------------------


def test_small_group_skipped(db_path, domain):
    conn = connection.connect(db_path)
    values = {"S1": 30, "S2": 90, "S3": 150}
    for i, (fk, val) in enumerate(values.items()):
        _seed_row(conn, fk, offset_m=10_000 + i * 40, brj=val, postnummer="1003")
    conn.close()

    conn = connection.connect(db_path)
    findings = validate_travel(conn, domain)

    flagged = [f for f in findings if f["finnkode"] in values]
    assert flagged == [], findings


# ---------------------------------------------------------------------------
# Threshold plumbing: a lower score_threshold surfaces findings a stricter
# one would hide (sanity check the parameter is actually wired through).
# ---------------------------------------------------------------------------


def test_score_threshold_is_respected(db_path, domain):
    conn = connection.connect(db_path)
    baseline_values = {"T1": 29, "T2": 30, "T3": 31, "T4": 30, "T5": 32}
    for i, (fk, val) in enumerate(baseline_values.items()):
        _seed_row(conn, fk, offset_m=i * 40, brj=val, postnummer="1004")
    _seed_row(conn, "TOUT", offset_m=200, brj=90, postnummer="1004")
    conn.close()

    conn = connection.connect(db_path)
    findings = validate_travel(conn, domain, score_threshold=100)
    assert [f for f in findings if f["finnkode"] == "TOUT"] == []


# ---------------------------------------------------------------------------
# Postcode-group check: fires when the LOCAL/neighbor check structurally
# cannot (rows spaced beyond radius_m apart) but >= min_postcode_group peers
# share a postnummer and one value is a gross outlier.
# ---------------------------------------------------------------------------


def test_postcode_outlier_flagged_when_local_check_cannot_fire(db_path, domain):
    conn = connection.connect(db_path)
    # Rows spaced 1000m apart (well beyond the default radius_m=300) so no
    # row ever has a spatial neighbor within radius -- the LOCAL check is
    # structurally unable to fire (neighbor_count stays 0, below
    # min_neighbors=5) even though all 7 rows share one postnummer
    # (peer count 6 == min_postcode_group).
    baseline_values = {"PC1": 29, "PC2": 30, "PC3": 31, "PC4": 30, "PC5": 32, "PC6": 29}
    for i, (fk, val) in enumerate(baseline_values.items()):
        _seed_row(conn, fk, offset_m=i * 1000, brj=val, postnummer="2001")
    _seed_row(conn, "PCOUT", offset_m=6 * 1000, brj=90, postnummer="2001")
    conn.close()

    conn = connection.connect(db_path)
    # Lower score_threshold to 2 to isolate the postcode check's own +2
    # contribution -- alone it sits below the module's default threshold of
    # 3 (which is tuned for local+postcode/local+donor combos, not a lone
    # postcode hit).
    findings = validate_travel(conn, domain, score_threshold=2)

    matches = [f for f in findings if f["finnkode"] == "PCOUT" and f["column"] == "pendl_rush_brj"]
    assert len(matches) == 1, findings
    finding = matches[0]
    assert finding["neighbor_count"] < 5  # LOCAL check could not fire
    assert finding["postcode_group_size"] >= 6
    assert finding["score"] == 2
    assert any(r.startswith("Postnr:") for r in finding["reasons"])
    assert not any(r.startswith("Local:") for r in finding["reasons"])

    baseline_flagged = [f for f in findings if f["finnkode"] in baseline_values]
    assert baseline_flagged == []


# ---------------------------------------------------------------------------
# Donor-distance check: a row's donor-resolved value comes from a donor
# (travel_copy_from_finnkode) whose own coordinates sit far from the row's
# own -- flagged even though the row has no local/postcode peers of its own.
# ---------------------------------------------------------------------------


def test_donor_distance_outlier_flagged(db_path, domain):
    conn = connection.connect(db_path)

    # Donor: forced inactive via direct SQL after the upsert (listings now
    # activate on first appearance -- user mandate 2026-07-20 -- so a plain
    # upsert alone would no longer leave it inactive here). It must stay
    # inactive: `_SHEET_QUERY` scopes its candidate/dedup scan to `active =
    # 1`, so an active DONOR (valid brj=50) would become its own dedup-group
    # representative and collapse DONEE out of the candidate set entirely,
    # defeating this test's point. `donor_seed()` (used for the donor-coords
    # / donor-link graph) reads `eiendom_processed` alone with no active
    # filter, so an inactive donor still resolves there -- its own brj (50)
    # is what DONEE's donor-resolved value below picks up.
    donor_listing = NormalizedListing(
        **{
            "Finnkode": "DONOR",
            "URL": "https://www.finn.no/realestate/ad.html?finnkode=DONOR",
            "Adresse": "Donor Gate 1",
            "Postnummer": "3009",
            "Pris": 3_000_000,
        }
    )
    ListingsRepo(conn).upsert([donor_listing])
    conn.execute("UPDATE eiendom SET active = 0 WHERE finnkode = 'DONOR'")
    _seed_processed(conn, "DONOR", lat=_north(20_000), lng=OSLO_LNG, brj=50, postnummer="3009")

    # Donee: active, geographically isolated (unique postnummer, no nearby
    # candidates) so only the donor-distance check can fire. Its own brj is
    # left unset -- the CASE/COALESCE donor-resolution query picks DONOR's
    # value (50) once travel_copy_from_finnkode links to it.
    _seed_listing(conn, "DONEE", adresse="Donee Gate 1", postnummer="3010")
    _seed_processed(
        conn, "DONEE", lat=OSLO_LAT, lng=OSLO_LNG, brj=None, link="DONOR", postnummer="3010"
    )
    conn.close()

    conn = connection.connect(db_path)
    findings = validate_travel(conn, domain)

    matches = [f for f in findings if f["finnkode"] == "DONEE" and f["column"] == "pendl_rush_brj"]
    assert len(matches) == 1, findings
    finding = matches[0]
    assert finding["value"] == 50  # donor-resolved value, not DONEE's own (null)
    assert finding["score"] == 3
    assert any(r.startswith("Donor:") for r in finding["reasons"])
    assert not any(r.startswith("Local:") for r in finding["reasons"])
    assert not any(r.startswith("Postnr:") for r in finding["reasons"])


# ---------------------------------------------------------------------------
# CLI smoke test
# ---------------------------------------------------------------------------


def test_cli_validate_travel_exit_0_no_findings(db_path):
    result = CliRunner().invoke(app, ["run", "validate-travel", "--db", str(db_path)])
    assert result.exit_code == 0, result.output
    assert "Travel Value Validation Report" in result.output
    assert "Flagged findings: 0" in result.output


def test_cli_validate_travel_reports_findings(db_path):
    conn = connection.connect(db_path)
    baseline_values = {"K1": 29, "K2": 30, "K3": 31, "K4": 30, "K5": 32}
    for i, (fk, val) in enumerate(baseline_values.items()):
        _seed_row(conn, fk, offset_m=i * 40, brj=val, postnummer="1005")
    _seed_row(conn, "KOUT", offset_m=200, brj=90, postnummer="1005")
    conn.close()

    result = CliRunner().invoke(app, ["run", "validate-travel", "--db", str(db_path)])
    assert result.exit_code == 0, result.output
    assert "KOUT" in result.output


def test_cli_validate_travel_missing_db_exit_1(tmp_path):
    missing = tmp_path / "nope.db"
    result = CliRunner().invoke(app, ["run", "validate-travel", "--db", str(missing)])
    assert result.exit_code == 1
