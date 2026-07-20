"""DNB Eiendom search-result crawler.

Ports `_build_search_url` and `_extract_listing_urls_from_html` from
`main/extractors/extract_dnbeiendom.py:23-128`.

The region GUIDs and `MAX_PAGES` are lifted out of the legacy hardcoded
constants into `config/domain.toml`'s `[dnb]` section (`region_guids`,
`max_pages`); everything else -- the `estateStatus`/`estateTypes` filter
literals, the price/area filter suffix (driven by `domain.filters`, matching
legacy's `get_dnb_search_filter_params`), and the JSON-LD-first/anchor-
fallback URL extraction -- matches legacy exactly.

No inter-page pacing is added here: legacy's `fetch_urls_from_search` (the
page-fetch loop in `extract_dnbeiendom.py:130-191`) contains no `time.sleep`
between page fetches, unlike the FINN crawler (Task 7), so there is no
legacy behavior to mirror. `max_pages` is carried in config for a future
page-fetch loop but is not exercised by this module's functions.
"""

import json
from urllib.parse import urlencode, urljoin, urlparse

from bs4 import BeautifulSoup

from skannonser.config.domain import DomainConfig

LISTING_PATH_PREFIX = "/bolig/"


def build_search_url(domain: DomainConfig) -> str:
    """Build the DNB Eiendom search URL for the configured domain.

    Direct port of `main.extractors.extract_dnbeiendom._build_search_url`,
    with the region GUIDs pulled from `domain.dnb.region_guids` (position
    order preserved) instead of hardcoded literals, and the price/area
    filter suffix derived from `domain.filters` (matching legacy's
    `get_dnb_search_filter_params`, which reads the same underlying
    constants as `config/domain.toml`'s `[filters]` section).
    """
    base_pairs: list[tuple[str, str]] = [("estateStatus", "project_false")]
    base_pairs.extend(("locations", guid) for guid in domain.dnb.region_guids)
    base_pairs.extend(
        ("estateTypes", t)
        for t in (
            "Leilighet",
            "Enebolig",
            "Tomannsbolig",
            "Rekkehus",
            "Landbruk",
            "Småbruk",
        )
    )

    if domain.filters.url_max_price is not None:
        base_pairs.append(("priceSuggestion", f"max_{int(domain.filters.url_max_price)}"))
    if domain.filters.min_bra_i is not None:
        base_pairs.append(("primaryRoomArea", f"min_{int(domain.filters.min_bra_i)}"))

    return f"https://dnbeiendom.no/bolig?{urlencode(base_pairs, doseq=True)}"


def extract_listing_urls(html: str) -> list[str]:
    """Extract canonical DNB listing URLs from a search-result page.

    Direct port of `_extract_listing_urls_from_html`
    (`main/extractors/extract_dnbeiendom.py:64-128`): JSON-LD `ItemList`
    first, falling back to anchor tags only when no JSON-LD `ItemList`
    entries are found. Returns a sorted list rather than legacy's set, for a
    stable, testable interface.
    """
    soup = BeautifulSoup(html, "html.parser")
    found: set[str] = set()

    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        script_content = script.string or script.get_text() or ""
        script_content = script_content.strip()
        if not script_content:
            continue

        try:
            payload = json.loads(script_content)
        except Exception:
            continue

        entries = payload if isinstance(payload, list) else [payload]
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            if entry.get("@type") != "ItemList":
                continue

            item_elements = entry.get("itemListElement") or []
            for li in item_elements:
                if not isinstance(li, dict):
                    continue
                item = li.get("item") if isinstance(li.get("item"), dict) else li
                url = item.get("url") or item.get("@id")
                if not isinstance(url, str):
                    continue

                absolute = urljoin("https://dnbeiendom.no", url)
                parsed = urlparse(absolute)
                if parsed.netloc != "dnbeiendom.no":
                    continue
                if not parsed.path.startswith(LISTING_PATH_PREFIX):
                    continue

                path = parsed.path.rstrip("/")
                path = path.replace("%", "").lower() if "%" in path else path.lower().rstrip("/")
                canonical = f"https://dnbeiendom.no{path}"
                found.add(canonical)

    if found:
        return sorted(found)

    for tag in soup.find_all("a", href=True):
        href = tag["href"].strip()
        absolute = urljoin("https://dnbeiendom.no", href)
        parsed = urlparse(absolute)
        if parsed.netloc != "dnbeiendom.no":
            continue
        if not parsed.path.startswith(LISTING_PATH_PREFIX):
            continue
        path = parsed.path.rstrip("/")
        path = path.replace("%", "").lower() if "%" in path else path.lower().rstrip("/")
        canonical = f"https://dnbeiendom.no{path}"
        found.add(canonical)

    return sorted(found)
