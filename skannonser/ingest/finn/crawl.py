"""FINN.no result-page crawler.

Ports `build_finn_polylocation` / `get_finn_scrape_config` from
`main/runners/run_eiendom_db.py:106-155` and the ad-link extraction from
`main/crawl.py:15-48`, with two sanctioned fixes over the legacy behavior:

1. The finnkode is parsed with `urllib.parse.urlparse`/`parse_qs` instead of
   `url.split('finnkode=')[1]`, which breaks (captures trailing params) once
   the ad link carries anything after `finnkode=NNN` (e.g. `&utm_source=x`).
2. Ad links are matched with an explicit FINN homes-ad pattern instead of the
   legacy `len(href) <= 100` heuristic, which was an incidental proxy for
   "this is a short ad link, not some other on-page link" rather than an
   actual structural check.

Everything else -- the search URL construction, the polygon/filter suffix,
and the page-param pagination mechanism -- matches legacy exactly.
"""

import re
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlparse

import requests

from skannonser.config.domain import DomainConfig

# Matches FINN homes ad links, e.g.
# /realestate/homes/ad.html?finnkode=123456789&some=other&params=too
# The legacy regex (`/realestate/.*?/ad\.html\?finnkode=\d+`) only ever
# matched the "homes" segment in practice (get_finn_scrape_config only
# crawls /realestate/homes/search.html), so this pins that segment
# explicitly rather than relying on `.*?` to happen to land on "homes".
_AD_LINK_PATTERN = re.compile(r'/realestate/homes/ad\.html\?[^"\']*finnkode=\d+')

_FINN_URL_BASE = (
    "https://www.finn.no/realestate/homes/search.html?filters="
    "&property_type=4&property_type=1&property_type=2&property_type=11"
    "&lifecycle=1&is_new_property=false"
    "&property_type=3"
)


def build_finn_polylocation(points: list[tuple[float, float]]) -> str:
    """Build a FINN polylocation string from (lng, lat) point tuples.

    FINN expects pairs in `lng+lat` format separated by `%2C`, with the
    polygon closed (first point repeated as the last, if not already).

    Direct port of `main.runners.run_eiendom_db.build_finn_polylocation`.
    """
    if len(points) < 3:
        raise ValueError("Polygon must contain at least 3 points")

    polygon_points = list(points)
    if polygon_points[0] != polygon_points[-1]:
        polygon_points.append(polygon_points[0])

    return "%2C".join(f"{lng}+{lat}" for lng, lat in polygon_points)


def build_search_url(domain: DomainConfig) -> str:
    """Build the FINN homes search URL for the configured domain.

    Direct port of `main.runners.run_eiendom_db.get_finn_scrape_config`'s
    url_base construction (filter suffix + polylocation), driven by
    `DomainConfig` instead of the legacy module-level constants.
    """
    params: dict[str, str] = {}
    if domain.filters.url_max_price is not None:
        params["price_to"] = str(int(domain.filters.url_max_price))
    if domain.filters.min_bra_i is not None:
        params["area_from"] = str(int(domain.filters.min_bra_i))
    filter_suffix = f"&{urlencode(params)}" if params else ""

    polylocation = build_finn_polylocation(domain.polygon_points)
    return f"{_FINN_URL_BASE}{filter_suffix}&polylocation={polylocation}"


def _extract_finnkode(url: str) -> str | None:
    """Extract the finnkode query param from a FINN ad URL.

    Sanctioned fix #1: uses urlparse/parse_qs instead of
    `url.split('finnkode=')[1]`, which is robust to trailing params after
    `finnkode=NNN` in the URL.
    """
    query = parse_qs(urlparse(url).query)
    values = query.get("finnkode")
    if not values:
        return None
    return values[0]


def extract_ad_urls(html: str) -> list[tuple[str, str]]:
    """Extract (finnkode, url) pairs for every FINN homes ad link on a
    result page, deduplicated by finnkode.

    Ports the extraction loop in `main/crawl.py:parse_resultpage` (hrefs of
    `<a>` tags, filtered by an ad-link pattern), with sanctioned fix #2
    (explicit ad-link pattern instead of `len(href) <= 100`) and sanctioned
    fix #1 for finnkode parsing.
    """
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")
    hrefs = [a.get("href") for a in soup.find_all("a", href=True)]

    # search() rather than legacy's match(): FINN result pages link ads with
    # relative hrefs (`/realestate/homes/ad.html?...`), which both match()
    # and search() find identically at position 0. search() additionally
    # tolerates an absolute href (`https://www.finn.no/realestate/...`)
    # without needing a second code path -- the href is preserved verbatim
    # either way via the startswith('http') check below, matching legacy's
    # own (otherwise dead, since match() never lets a match start with
    # 'http') intent.
    matches = {href for href in hrefs if _AD_LINK_PATTERN.search(href)}

    full_urls = sorted(
        href if href.startswith("http") else f"https://www.finn.no{href}"
        for href in matches
    )

    pairs: list[tuple[str, str]] = []
    seen: set[str] = set()
    for url in full_urls:
        finnkode = _extract_finnkode(url)
        if finnkode is None or finnkode in seen:
            continue
        seen.add(finnkode)
        pairs.append((finnkode, url))
    return pairs


def crawl(
    domain: DomainConfig,
    fetch=requests.get,
    archive_dir: Path | None = None,
    max_pages: int = 50,
) -> list[tuple[str, str]]:
    """Crawl FINN result pages for `domain`, returning deduplicated
    (finnkode, url) pairs across all pages.

    Pagination follows the legacy page-param mechanism (`parse_resultpage`
    in `main/crawl.py`): page 1 uses the bare search URL, subsequent pages
    append `&page=N`. Crawling stops once a page yields no ads not already
    seen on an earlier page, or after `max_pages` pages, whichever comes
    first. When `archive_dir` is given, each page's raw HTML is written
    there as `page{N}.html`.
    """
    url_base = build_search_url(domain)

    if archive_dir is not None:
        archive_dir.mkdir(parents=True, exist_ok=True)

    all_pairs: list[tuple[str, str]] = []
    seen_finnkodes: set[str] = set()

    for page in range(1, max_pages + 1):
        url = url_base if page == 1 else f"{url_base}&page={page}"

        response = fetch(url)
        response.raise_for_status()
        html = response.text

        if archive_dir is not None:
            (archive_dir / f"page{page}.html").write_text(html, encoding="utf-8")

        page_pairs = extract_ad_urls(html)
        new_pairs = [(fk, u) for fk, u in page_pairs if fk not in seen_finnkodes]

        if not new_pairs:
            break

        for fk, u in new_pairs:
            seen_finnkodes.add(fk)
            all_pairs.append((fk, u))

    return all_pairs
