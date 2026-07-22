from pathlib import Path

from skannonser.config.domain import load_domain
from skannonser.ingest.finn.crawl import build_search_url, crawl, extract_ad_urls

FIXTURE = Path("tests/rebuild/fixtures/finn/result_page1.html")


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


# The exact Finn search URL the legacy get_finn_scrape_config() produced,
# frozen from legacy at deletion, 2026-07-22. Legacy read no env vars for this
# path (hardcoded constants only), so this string is a stable golden the ported
# build_search_url must keep reproducing byte-for-byte.
LEGACY_FINN_SEARCH_URL = (
    "https://www.finn.no/realestate/homes/search.html?filters=&property_type=4"
    "&property_type=1&property_type=2&property_type=11&lifecycle=1"
    "&is_new_property=false&property_type=3&price_to=7500000&area_from=70"
    "&polylocation=10.65673828125+59.884802942124%2C10.536789920973+59.797487966246"
    "%2C10.545723856072+59.709734171804%2C10.332641601563+59.700380312509"
    "%2C9.971542814941+59.874465805403%2C11.260986328125+60.44096253531"
    "%2C11.585234663086+60.136034630691%2C10.947750935529+59.714239974969"
    "%2C10.721282958984+59.712097173323%2C10.715468622953+59.849132221282"
    "%2C10.65673828125+59.884802942124"
)


def test_search_url_matches_legacy():
    """The new build_search_url must produce the byte-for-byte URL the legacy
    get_finn_scrape_config() emitted (frozen literal above)."""
    assert build_search_url(load_domain()) == LEGACY_FINN_SEARCH_URL


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


def test_crawl_paginates_until_page_has_zero_ad_matches(tmp_path):
    """Legacy stop condition (main/crawl.py:79): stop only when a page has
    ZERO ad-link matches, not merely zero NEW ones -- a page of only
    already-seen ads must not stop the crawl."""
    domain = load_domain()

    page1_html = """
    <a href="https://www.finn.no/realestate/homes/ad.html?finnkode=100000001">a</a>
    <a href="https://www.finn.no/realestate/homes/ad.html?finnkode=100000002">a</a>
    """
    # Only-repeat page: every ad here was already seen on page 1. Must NOT
    # stop the crawl (it has non-zero ad-link matches).
    page2_html = """
    <a href="https://www.finn.no/realestate/homes/ad.html?finnkode=100000002">a</a>
    <a href="https://www.finn.no/realestate/homes/ad.html?finnkode=100000001">a</a>
    """
    page3_html = """
    <a href="https://www.finn.no/realestate/homes/ad.html?finnkode=100000003">a</a>
    """
    # Zero ad-link matches -> this is what actually stops the crawl.
    page4_html = "<p>no more results</p>"

    pages = [page1_html, page2_html, page3_html, page4_html]
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
    result = crawl(domain, fetch=fake_fetch, archive_dir=archive_dir, max_pages=50, page_delay=lambda: None)

    finnkodes = [fk for fk, _ in result]
    assert finnkodes == ["100000001", "100000002", "100000003"]

    # Page-param mechanism: first page unadorned, subsequent pages use &page=N.
    assert "&page=" not in fetched_urls[0]
    assert fetched_urls[1].endswith("&page=2")
    assert fetched_urls[2].endswith("&page=3")
    assert fetched_urls[3].endswith("&page=4")

    # Crawls through the only-repeat page 2 and the new-ad page 3, and only
    # stops after page 4 yields zero matches (page 5 never fetched).
    assert len(fetched_urls) == 4

    assert (archive_dir / "page1.html").exists()
    assert (archive_dir / "page2.html").exists()
    assert (archive_dir / "page3.html").exists()
    assert (archive_dir / "page4.html").exists()
    assert not (archive_dir / "page5.html").exists()


def test_crawl_page_of_only_repeat_ads_does_not_stop_crawl(tmp_path):
    """Narrow regression test for the specific defect this fix corrects:
    the OLD (wrong) stop condition would have broken after page 2 here
    (zero NEW finnkodes), but the correct legacy condition keeps going
    since page 2 has non-zero ad-link matches."""
    domain = load_domain()

    page1_html = '<a href="https://www.finn.no/realestate/homes/ad.html?finnkode=100000001">a</a>'
    page2_html = '<a href="https://www.finn.no/realestate/homes/ad.html?finnkode=100000001">a</a>'  # pure repeat
    page3_html = '<a href="https://www.finn.no/realestate/homes/ad.html?finnkode=100000002">a</a>'
    page4_html = ""

    pages = [page1_html, page2_html, page3_html, page4_html]
    fetched_urls = []

    class FakeResponse:
        def __init__(self, text):
            self.text = text

        def raise_for_status(self):
            pass

    def fake_fetch(url):
        fetched_urls.append(url)
        return FakeResponse(pages[len(fetched_urls) - 1])

    result = crawl(domain, fetch=fake_fetch, max_pages=50, page_delay=lambda: None)

    assert len(fetched_urls) == 4
    finnkodes = [fk for fk, _ in result]
    assert finnkodes == ["100000001", "100000002"]


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

    result = crawl(domain, fetch=fake_fetch, archive_dir=None, max_pages=3, page_delay=lambda: None)

    assert call_count == 3
    assert len(result) == 3


def test_crawl_paces_between_pages():
    domain = load_domain()

    delay_calls = []
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
    page4_html = ""  # zero matches -> stop

    pages = [page1_html, page2_html, page3_html, page4_html]
    fetched_urls = []

    class FakeResponse:
        def __init__(self, text):
            self.text = text

        def raise_for_status(self):
            pass

    def fake_fetch(url):
        fetched_urls.append(url)
        return FakeResponse(pages[len(fetched_urls) - 1])

    crawl(domain, fetch=fake_fetch, page_delay=lambda: delay_calls.append(1))
    assert len(delay_calls) >= 1


# ---------------------------------------------------------------------------
# Fix 7 (deferred-#9): regression-lock the page_delay default branch.
# ---------------------------------------------------------------------------


def test_page_delay_default_sleeps_random_200_to_500ms(monkeypatch):
    import skannonser.ingest.finn.crawl as crawl_module

    domain = load_domain()

    page1_html = '<a href="https://www.finn.no/realestate/homes/ad.html?finnkode=100000001">a</a>'
    page2_html = ""  # zero matches -> stop after exactly one page_delay() call

    pages = [page1_html, page2_html]
    fetched_urls = []

    class FakeResponse:
        def __init__(self, text):
            self.text = text

        def raise_for_status(self):
            pass

    def fake_fetch(url):
        fetched_urls.append(url)
        return FakeResponse(pages[len(fetched_urls) - 1])

    sleep_calls = []
    monkeypatch.setattr(crawl_module.time, "sleep", lambda s: sleep_calls.append(s))
    monkeypatch.setattr(crawl_module.random, "uniform", lambda a, b: 999)

    crawl_module.crawl(domain, fetch=fake_fetch, max_pages=50)

    assert sleep_calls == [999 / 1000]
