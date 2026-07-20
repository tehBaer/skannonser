import sys
from pathlib import Path

from skannonser.config.domain import load_domain
from skannonser.ingest.dnb import crawl, parse

FIXTURES = Path(__file__).parent / "fixtures" / "dnb"
SEARCH_PAGE = Path("data/dnbeiendom/html_crawled/page1.html")


def test_search_url_contains_all_region_guids():
    d = load_domain()
    url = crawl.build_search_url(d)
    for guid in d.dnb.region_guids:
        assert guid in url


def test_search_url_matches_legacy():
    """Legacy `_build_search_url()` reads no env vars for this path -- it
    pulls region GUIDs/estate types from hardcoded literals and the
    price/area filter suffix from `main.config.filters.get_dnb_search_filter_params()`,
    which itself reads module-level constants (URL_MAX_PRICE=7500000,
    MIN_BRA_I=70), not env/CLI input. Those constants match
    config/domain.toml's [filters] section verbatim (see the file's header
    comment), so both sides agree without extra pinning.
    """
    sys.path.insert(0, ".")
    from main.extractors.extract_dnbeiendom import _build_search_url as legacy_build_search_url

    new_url = crawl.build_search_url(load_domain())
    assert new_url == legacy_build_search_url()


def test_extract_urls_from_real_search_page():
    html = SEARCH_PAGE.read_text(errors="replace")
    urls = crawl.extract_listing_urls(html)
    assert len(urls) >= 5
    assert all(u.startswith("https://") for u in urls)


def test_extract_urls_matches_legacy_on_real_page():
    sys.path.insert(0, ".")
    from main.extractors.extract_dnbeiendom import (
        _extract_listing_urls_from_html as legacy_extract,
    )

    html = SEARCH_PAGE.read_text(errors="replace")
    legacy_urls = sorted(legacy_extract(html))
    new_urls = crawl.extract_listing_urls(html)

    assert new_urls == legacy_urls


def test_parse_listing_jsonld():
    html = (FIXTURES / "listing1.html").read_text(errors="replace")
    row = parse.parse_listing(html, "https://dnbeiendom.no/x")
    assert row is not None
    assert row.get("Latitude") and row.get("Longitude")
    assert row.get("StreetAddress")
    assert row.get("Price")
    assert row.get("PropertyType") == "Enebolig"


def test_parse_listing_matches_legacy_extract_fields(monkeypatch):
    """Pin skannonser.ingest.dnb.parse.parse_listing's output dict against
    legacy extract_fields_from_entry on the same JSON-LD entry (same keys,
    same values -- including legacy's IMAGE_URL quirk of storing the raw
    ImageObject dict rather than a URL string when `image` is a list)."""
    sys.path.insert(0, ".")
    from bs4 import BeautifulSoup

    from main.extractors.extract_dnbeiendom_ads import (
        extract_fields_from_entry as legacy_extract_fields,
        parse_listing_jsonld as legacy_parse_jsonld,
    )

    html = (FIXTURES / "listing1.html").read_text(errors="replace")
    soup = BeautifulSoup(html, "html.parser")
    legacy_entry = legacy_parse_jsonld(soup)
    assert legacy_entry is not None
    legacy_row = legacy_extract_fields(legacy_entry)

    new_row = parse.parse_listing(html, "https://dnbeiendom.no/x")

    assert new_row == legacy_row


def test_parse_listing_returns_none_without_jsonld():
    assert parse.parse_listing("<html><body>no jsonld here</body></html>", "https://dnbeiendom.no/x") is None
