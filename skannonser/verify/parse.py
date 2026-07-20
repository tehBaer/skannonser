"""Golden-master harness: compare the rebuilt FINN parser (Task 9,
`skannonser.ingest.finn.parse.parse_ad`) against the LEGACY field
extractors, ad-by-ad, over the cached HTML corpus.

The legacy call pattern mirrors `tests/rebuild/fixtures/finn/generate_expected.py`
exactly (same functions, same argument shapes) -- that script is the
sanctioned reference for "how to call legacy".

Legacy is imported lazily inside `verify_parse` (not at module import time)
so that merely importing this module never pulls in the legacy package or
its dependencies -- only running a verification does. The legacy package
(`main.extractors...`) lives at the repo root, which is not on `sys.path`
when this code runs as the installed `skannonser` console script, so the
repo root is inserted before the import.
"""
import sys
from dataclasses import dataclass, field
from pathlib import Path

from bs4 import BeautifulSoup

from skannonser.ingest.finn.parse import parse_ad

_REPO_ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class FieldDiff:
    finnkode: str
    field: str
    legacy_value: object
    new_value: object


@dataclass
class VerifyResult:
    total: int = 0
    identical: int = 0
    allowlisted: int = 0
    diffs: list[FieldDiff] = field(default_factory=list)


def _import_legacy():
    """Import the legacy extractor module, adding the repo root to
    `sys.path` first if it isn't already importable (needed when running
    as the installed console script rather than under pytest)."""
    try:
        from main.extractors import parsing_helpers_common as legacy
    except ModuleNotFoundError:
        root = str(_REPO_ROOT)
        if root not in sys.path:
            sys.path.insert(0, root)
        from main.extractors import parsing_helpers_common as legacy
    return legacy


def _legacy_row(legacy, soup, finnkode: str, url: str) -> dict:
    """Run the same LEGACY extractor calls, in the same shape, as
    `main/extractors/extraction_eiendom.py:extract_eiendom_data` (and as
    `generate_expected.py` freezes into fixtures)."""
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


def _ad_url(finnkode: str) -> str:
    return f"https://www.finn.no/realestate/homes/ad.html?finnkode={finnkode}"


def verify_parse(
    cache_dir: Path,
    limit: int | None,
    allowlist: dict,
    *,
    progress_every: int = 500,
) -> VerifyResult:
    """Compare legacy vs. rebuilt parse output over every cached ad under
    `cache_dir/html_extracted/*.html` (sorted, first `limit` if given).

    A field diff is "allowlisted" (explained) if its field name matches an
    `[[allow]]` entry in `allowlist` (the parsed `config/verify-allowlist.toml`
    dict, i.e. `{"allow": [{"field": ..., "reason": ...}, ...]}`); otherwise
    it's an unexplained diff.

    A row (ad) with zero field diffs counts toward `.identical`. A row with
    at least one field diff, all of them allowlisted, counts toward
    `.allowlisted`. Any field diff that is not allowlisted is appended to
    `.diffs`, regardless of how the rest of that row's fields compared.
    """
    legacy = _import_legacy()

    allowed_fields = {entry["field"] for entry in allowlist.get("allow", [])}

    html_dir = cache_dir / "html_extracted"
    paths = sorted(html_dir.glob("*.html"))
    if limit is not None:
        paths = paths[:limit]

    result = VerifyResult(total=len(paths))

    for i, path in enumerate(paths, start=1):
        finnkode = path.stem
        url = _ad_url(finnkode)
        html = path.read_text(encoding="utf-8", errors="replace")
        soup = BeautifulSoup(html, "html.parser")

        legacy_row = _legacy_row(legacy, soup, finnkode, url)
        new_row = parse_ad(html, finnkode, url).to_row()

        row_diffs = []
        row_has_unexplained = False
        for field_name, legacy_value in legacy_row.items():
            new_value = new_row.get(field_name)
            if legacy_value == new_value:
                continue
            row_diffs.append((field_name, legacy_value, new_value))
            if field_name in allowed_fields:
                continue
            row_has_unexplained = True
            result.diffs.append(FieldDiff(finnkode, field_name, legacy_value, new_value))

        if not row_diffs:
            result.identical += 1
        elif not row_has_unexplained:
            result.allowlisted += 1

        if progress_every and i % progress_every == 0:
            print(f"...{i}/{len(paths)} ads compared", file=sys.stderr)

    return result
