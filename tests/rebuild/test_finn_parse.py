"""Byte-identical port check for the FINN ad parser against a legacy-frozen
fixture corpus (12 cached ads; see fixtures/finn/README.md)."""
import json
from pathlib import Path

import pytest

from skannonser.ingest.finn.parse import parse_ad

FIXTURES = Path(__file__).parent / "fixtures" / "finn"
CASES = sorted(
    p for p in FIXTURES.glob("*.expected.json")
    if not p.name.endswith(".details.expected.json")
)


@pytest.mark.parametrize("expected_path", CASES, ids=lambda p: p.stem.split(".")[0])
def test_parse_matches_legacy_fixture(expected_path):
    finnkode = expected_path.stem.split(".")[0]
    html = (FIXTURES / f"{finnkode}.html").read_text(encoding="utf-8", errors="replace")
    expected = json.loads(expected_path.read_text())
    listing = parse_ad(html, finnkode, f"https://www.finn.no/realestate/homes/ad.html?finnkode={finnkode}")
    row = listing.to_row()
    for field, want in expected.items():
        assert row.get(field) == want, f"{finnkode}.{field}: {row.get(field)!r} != {want!r}"


# --- Primærrom (P-ROM) ------------------------------------------------------
# FINN stopped emitting the `info-primary-area` key-info block when Norway
# moved to NS 3940:2023 (BRA-i/e/b): it is absent from all 12 golden fixtures,
# so the testid lookup alone yields "" on every modern ad. The value is still
# published, as `primary_size` in the GAM ad-targeting JSON.


def _parse(finnkode: str):
    html = (FIXTURES / f"{finnkode}.html").read_text(encoding="utf-8", errors="replace")
    return parse_ad(
        html, finnkode, f"https://www.finn.no/realestate/homes/ad.html?finnkode={finnkode}"
    ).to_row()


def test_primary_area_falls_back_to_gam_primary_size():
    """424071751 carries primary_size=100 while BRA-i is 105 -- P-ROM is a
    distinct measurement, not a duplicate of BRA-i, so losing it changes
    pris/kvm."""
    row = _parse("424071751")
    assert row["Primærrom"] == "100"
    assert row["Internt bruksareal (BRA-i)"] == "105"


def test_primary_area_empty_when_gam_has_no_primary_size():
    """448347467 has neither the testid nor a GAM primary_size -- the field
    stays "" rather than borrowing BRA-i."""
    assert _parse("448347467")["Primærrom"] == ""
