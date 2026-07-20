import sys
from pathlib import Path

from skannonser.config.domain import load_domain
from skannonser.ingest.finn.crawl import build_search_url, crawl, extract_ad_urls

FIXTURE = Path("data/eiendom/html_crawled/page1.html")


def test_build_search_url_contains_polygon_and_price():
    url = build_search_url(load_domain())
    assert "polylocation=" in url
    assert "price_to=7500000" in url or "7500000" in url


def test_extract_ad_urls_from_real_result_page():
    pairs = extract_ad_urls(FIXTURE.read_text(encoding="utf-8", errors="replace"))
    assert len(pairs) >= 10
    finnkodes = [fk for fk, _ in pairs]
    assert all(fk.isdigit() for fk in finnkodes)
    assert len(set(finnkodes)) == len(finnkodes)          # deduped


def test_finnkode_robust_to_trailing_params():
    html = '<a href="https://www.finn.no/realestate/homes/ad.html?finnkode=123456789&utm_source=x">a</a>'
    assert extract_ad_urls(html) == [("123456789",
        "https://www.finn.no/realestate/homes/ad.html?finnkode=123456789&utm_source=x")]


def test_search_url_matches_legacy():
    """The new build_search_url must produce a byte-for-byte identical URL
    to the legacy get_finn_scrape_config's url_base. Legacy reads no env
    vars for this path (verified: main/config/filters.py and the relevant
    slice of main/runners/run_eiendom_db.py use hardcoded constants, not
    os.getenv, for the URL-building logic), so no env pinning is needed for
    both sides to agree.
    """
    sys.path.insert(0, ".")
    from main.runners.run_eiendom_db import get_finn_scrape_config

    _project_name, legacy_url_base, _regex = get_finn_scrape_config()

    new_url = build_search_url(load_domain())

    assert new_url == legacy_url_base


def test_extract_ad_urls_superset_of_legacy_on_real_page():
    """On the real archived result page, the new extractor must find at
    least every ad link the legacy len(href)<=100 heuristic found (that
    length cap is not binding on this fixture -- every legacy match is 44
    chars, so this isolates the effect of the pattern-based sanctioned fix).

    It finds exactly one more: finnkode 468598543 is rendered on this page
    only via an absolute href (`https://www.finn.no/realestate/homes/ad.html
    ?finnkode=468598543`), never a relative one. Legacy's `pattern.match(href)`
    anchors at position 0 of the href string, so a pattern starting with
    `/realestate/...` can never match an absolute href -- this ad is
    silently dropped by legacy on every crawl, not deduplicated against a
    relative-href copy (grep confirms no relative-form href for this
    finnkode exists anywhere on the page). The explicit ad-link pattern
    (sanctioned fix #2) doesn't anchor to a bare leading slash, so it
    recovers this one legitimate ad. This is verified, deliberate, and
    documented -- not a stray port bug.
    """
    import re

    from bs4 import BeautifulSoup

    html = FIXTURE.read_text(encoding="utf-8", errors="replace")
    soup = BeautifulSoup(html, "html.parser")
    hrefs = [a.get("href") for a in soup.find_all("a", href=True)]
    legacy_pattern = re.compile(r"/realestate/.*?/ad\.html\?finnkode=\d+")
    legacy_matches = {href for href in hrefs if legacy_pattern.match(href) and len(href) <= 100}
    legacy_full_urls = {
        m if m.startswith("http") else f"https://www.finn.no{m}"
        for m in legacy_matches
    }

    pairs = extract_ad_urls(html)
    new_urls = {url for _fk, url in pairs}

    assert legacy_full_urls <= new_urls
    extra = new_urls - legacy_full_urls
    assert extra == {"https://www.finn.no/realestate/homes/ad.html?finnkode=468598543"}


def test_crawl_paginates_until_no_new_ads(tmp_path):
    domain = load_domain()

    page1_html = """
    <a href="https://www.finn.no/realestate/homes/ad.html?finnkode=100000001">a</a>
    <a href="https://www.finn.no/realestate/homes/ad.html?finnkode=100000002">a</a>
    """
    page2_html = """
    <a href="https://www.finn.no/realestate/homes/ad.html?finnkode=100000002">a</a>
    <a href="https://www.finn.no/realestate/homes/ad.html?finnkode=100000003">a</a>
    """
    page3_html = """
    <a href="https://www.finn.no/realestate/homes/ad.html?finnkode=100000003">a</a>
    """

    pages = [page1_html, page2_html, page3_html]
    fetched_urls = []

    class FakeResponse:
        def __init__(self, text):
            self.text = text

        def raise_for_status(self):
            pass

    def fake_fetch(url):
        fetched_urls.append(url)
        return FakeResponse(pages[len(fetched_urls) - 1])

    archive_dir = tmp_path / "archive"
    result = crawl(domain, fetch=fake_fetch, archive_dir=archive_dir, max_pages=50)

    finnkodes = [fk for fk, _ in result]
    assert finnkodes == ["100000001", "100000002", "100000003"]

    # Page-param mechanism: first page unadorned, subsequent pages use &page=N.
    assert "&page=" not in fetched_urls[0]
    assert fetched_urls[1].endswith("&page=2")
    assert fetched_urls[2].endswith("&page=3")

    # Stops after page 3 yields no new ads (page 4 never fetched).
    assert len(fetched_urls) == 3

    assert (archive_dir / "page1.html").exists()
    assert (archive_dir / "page2.html").exists()
    assert (archive_dir / "page3.html").exists()
    assert not (archive_dir / "page4.html").exists()


def test_crawl_respects_max_pages(tmp_path):
    domain = load_domain()

    class FakeResponse:
        def __init__(self, text):
            self.text = text

        def raise_for_status(self):
            pass

    call_count = 0

    def fake_fetch(url):
        nonlocal call_count
        call_count += 1
        finnkode = 200000000 + call_count
        return FakeResponse(
            f'<a href="https://www.finn.no/realestate/homes/ad.html?finnkode={finnkode}">a</a>'
        )

    result = crawl(domain, fetch=fake_fetch, archive_dir=None, max_pages=3)

    assert call_count == 3
    assert len(result) == 3
