"""Regenerate expected-output fixtures by running the LEGACY parser.

Usage: .venv/bin/python tests/rebuild/fixtures/finn/generate_expected.py

Picks a deterministic spread of cached ads and freezes the legacy parser's
field dict for each as <finnkode>.expected.json, copying the HTML alongside.

The `expected` dict keys are the *real* `NormalizedListing` field names
(== the exact dict keys `main/extractors/extraction_eiendom.py:
extract_eiendom_data` assembles, lines 30-49), not the brief's placeholder
names. `Finnkode` and `URL` are not derived from the HTML by the legacy
parser (extract_eiendom_data pulls `Finnkode` out of the caller's `url`
argument) -- here we freeze them to the same values the fixture test drives
`parse_ad` with, so the row-comparison exercises straight pass-through of
those two fields alongside the HTML-derived ones.
"""
import json
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

CACHE = Path("data/eiendom/html_extracted")
DEST = Path(__file__).parent
# Deterministic sample: sort by name, take every len//12-th -- covers old and new ads.
ads = sorted(CACHE.glob("*.html"))
sample = ads[:: max(1, len(ads) // 12)][:12]

from bs4 import BeautifulSoup
from main.extractors import parsing_helpers_common as legacy


def build_expected(soup, finnkode, url):
    address, area = legacy.getAddress(soup)
    sizes = legacy.getAllSizes(soup)
    return {
        "Finnkode": finnkode,
        "URL": url,
        "Tilgjengelighet": legacy.getStatus(soup),
        "Adresse": address,
        "Postnummer": area,
        "Pris": legacy.getBuyPrice(soup),
        "IMAGE_URL": legacy.getImageUrl(soup),
        "Primærrom": sizes.get("info-primary-area"),
        "Internt bruksareal (BRA-i)": sizes.get("info-usable-i-area"),
        "Bruksareal": sizes.get("info-usable-area"),
        "Eksternt bruksareal (BRA-e)": sizes.get("info-usable-e-area"),
        "Innglasset balkong (BRA-b)": sizes.get("info-usable-b-area"),
        "Balkong/Terrasse (TBA)": sizes.get("info-open-area"),
        "Tomteareal": sizes.get("info-plot-area"),
        "Eierskap, tomt": legacy.getPlotOwnership(soup),
        "Boligtype": legacy.getPropertyType(soup),
        "Bruttoareal": sizes.get("info-gross-area"),
        "Byggeår": legacy.getConstructionYear(soup),
    }


if __name__ == "__main__":
    for path in sample:
        finnkode = path.stem
        html = path.read_text(encoding="utf-8", errors="replace")
        soup = BeautifulSoup(html, "html.parser")
        url = f"https://www.finn.no/realestate/homes/ad.html?finnkode={finnkode}"

        try:
            expected = build_expected(soup, finnkode, url)
        except Exception as e:
            print(f"CRASH: {finnkode}: {type(e).__name__}: {e}")
            continue

        (DEST / f"{finnkode}.expected.json").write_text(
            json.dumps(expected, ensure_ascii=False, indent=1, default=str))
        shutil.copy(path, DEST / f"{finnkode}.html")
        print("fixture:", finnkode)
