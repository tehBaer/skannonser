"""Byte-identical port check for the FINN ad parser against a legacy-frozen
fixture corpus (12 cached ads; see fixtures/finn/generate_expected.py)."""
import json
from pathlib import Path

import pytest

from skannonser.ingest.finn.parse import parse_ad

FIXTURES = Path(__file__).parent / "fixtures" / "finn"
CASES = sorted(FIXTURES.glob("*.expected.json"))


@pytest.mark.parametrize("expected_path", CASES, ids=lambda p: p.stem.split(".")[0])
def test_parse_matches_legacy_fixture(expected_path):
    finnkode = expected_path.stem.split(".")[0]
    html = (FIXTURES / f"{finnkode}.html").read_text(encoding="utf-8", errors="replace")
    expected = json.loads(expected_path.read_text())
    listing = parse_ad(html, finnkode, f"https://www.finn.no/realestate/homes/ad.html?finnkode={finnkode}")
    row = listing.to_row()
    for field, want in expected.items():
        assert row.get(field) == want, f"{finnkode}.{field}: {row.get(field)!r} != {want!r}"
