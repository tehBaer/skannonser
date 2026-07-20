"""Ingest pipeline: crawl -> fetch/parse -> upsert -> mark inactive, for
FINN and DNB. Orchestrates every component built in Tasks 4-12 into the
two end-to-end entry points the CLI (`skannonser run ingest`) drives.

Two guards are ledgered as MANDATORY for this task:

1. `mark_inactive` / `deactivate_missing` are skipped entirely when the
   crawl phase yields ZERO urls. This mirrors legacy's missing-CSV guard
   (`filter_and_load_dnbeiendom_no_buffer.main` only runs its
   stale-deactivation block when `0_URLs.csv` exists) -- an empty or failed
   crawl must never be interpreted as "nothing is listed anymore" and wipe
   out the active set.
2. `mark_inactive` / `deactivate_missing` are ALSO skipped when the
   parse-failure rate for the crawl exceeds `FAILURE_RATE_THRESHOLD`
   (20%), and the CLI wrapper (`skannonser/commands/run_cmd.py`) exits
   non-zero in that case. This protects against a FINN/DNB layout change
   that lets the crawl phase succeed (so guard 1's `crawled == 0` check
   doesn't fire) while nearly every ad fails to parse -- without this,
   the tiny handful of successfully-parsed finnkodes would be passed to
   `mark_inactive` as "the whole active set" and every other listing
   would be wrongly deactivated. The skip happens INSIDE the pipeline
   function, before any deactivation call -- the CLI's non-zero exit is
   an operational alert layered on top, not the protection itself (by
   the time the CLI could react to a bad exit code, the deactivation
   would already be committed). This mirrors the intent of legacy's
   `A_failed.csv` failure tracking (`extraction_eiendom.
   extractEiendomDataFromAds`, `extract_dnbeiendom_ads.extract_all`),
   which flagged failures for a human to notice rather than silently
   accepting them.

This pipeline calls `upsert()` exactly once per run for each source.
`ListingsRepo`/`DnbRepo` preserve legacy's "activate only on second
appearance" quirk (a fresh INSERT leaves `active` at its schema default,
0/NULL; only an UPDATE hard-sets `active = 1` -- see those modules'
docstrings, reaffirmed at Task 6/11 review, and pinned exactly at the
pipeline layer too -- Task 13 review). A listing this pipeline has never
seen before stays inactive after this run and activates on the run that
next observes it, matching legacy exactly.
"""

import random
import re
import sqlite3
import time
from pathlib import Path
from typing import Callable
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import requests

from skannonser.config.domain import DomainConfig
from skannonser.ingest.dnb import crawl as dnb_crawl
from skannonser.ingest.dnb import load as dnb_load
from skannonser.ingest.dnb import parse as dnb_parse
from skannonser.ingest.finn import crawl as finn_crawl
from skannonser.ingest.finn import html_cache
from skannonser.ingest.finn import parse as finn_parse
from skannonser.store.repositories.dnb import DnbRepo
from skannonser.store.repositories.listings import ListingsRepo

# Shared with skannonser/commands/run_cmd.py (imported from here, single
# source of truth). See module docstring guard 2.
FAILURE_RATE_THRESHOLD = 0.20


def _failure_rate_too_high(crawled: int, failed: int) -> bool:
    return crawled > 0 and (failed / crawled) > FAILURE_RATE_THRESHOLD


def run_finn_ingest(
    domain: DomainConfig,
    conn: sqlite3.Connection,
    project_dir: Path,
    fetch=requests.get,
    archive_dir: Path | None = None,
    max_pages: int = 50,
    page_delay: Callable[[], None] | None = None,
    fetch_delay: Callable[[], None] | None = None,
    skip_crawl_urls: list[tuple[str, str]] | None = None,
) -> dict:
    """Run the full FINN ingest pipeline once: crawl result pages for
    (finnkode, url) pairs, fetch+parse each ad, upsert into `eiendom`, then
    mark absent finnkodes inactive.

    `skip_crawl_urls`, when given, bypasses `finn_crawl.crawl` entirely --
    `fetch` is then never invoked for the crawl phase (only for any ad page
    not already cached under `project_dir/html_extracted`). Used by the
    offline end-to-end test to drive the pipeline purely off cached
    fixtures.

    Returns counts: `crawled`, `parsed`, `failed`, `upserted`, `deactivated`.
    """
    project_dir = Path(project_dir)

    if skip_crawl_urls is not None:
        pairs = skip_crawl_urls
    else:
        pairs = finn_crawl.crawl(
            domain,
            fetch=fetch,
            archive_dir=archive_dir,
            max_pages=max_pages,
            page_delay=page_delay,
        )

    crawled = len(pairs)
    parsed = 0
    failed = 0
    listings = []
    for finnkode, url in pairs:
        try:
            html = html_cache.load_or_fetch(
                url, project_dir, finnkode, fetch=fetch, fetch_delay=fetch_delay
            )
            listings.append(finn_parse.parse_ad(html, finnkode, url))
            parsed += 1
        except Exception:
            failed += 1

    repo = ListingsRepo(conn)
    upsert_stats = repo.upsert(listings)

    deactivated = 0
    if crawled > 0 and not _failure_rate_too_high(crawled, failed):
        active_finnkodes = [listing.Finnkode for listing in listings]
        deactivated = repo.mark_inactive(active_finnkodes)

    return {
        "crawled": crawled,
        "parsed": parsed,
        "failed": failed,
        "upserted": upsert_stats["inserted"] + upsert_stats["updated"],
        "deactivated": deactivated,
    }


# DNB listing-fetch discipline (Task 13 final-review fix wave). Legacy's
# per-listing loop (`main/extractors/extract_dnbeiendom_ads.py:118-149`)
# sends this exact UA + a 15s timeout, and only sleeps 200-800ms after a
# fetch that actually hit the network (never on a cache hit) -- that
# sleep lives on legacy's *fallback* path (raw `requests.get` after
# `load_or_fetch_ad_html` raises), since legacy's primary loader issues a
# bare, header-less `requests.get(url)` for the cache-miss case. This port
# hoists the UA/timeout/post-fetch-delay discipline onto every network
# fetch (not just the rare fallback), which is the sanctioned, simpler
# behavior this fix wave asks for -- see the module's Task 13 guard
# docstring for the project's convention of documenting such choices.
_DNB_LISTING_USER_AGENT = "Mozilla/5.0 (compatible; dnbscraper/1.0; +https://dnbeiendom.no)"
_DNB_LISTING_TIMEOUT = 15


def _default_dnb_listing_fetch(url: str):
    """Default per-listing fetch for DNB: legacy's exact UA string and a
    15s timeout (`extract_dnbeiendom_ads.py:118-131`). Only used when the
    caller leaves `fetch` at its default -- an explicit `fetch` override
    (e.g. a test fake) is used as-is for both the crawl and listing
    fetches, matching the pre-existing single-`fetch`-param convention."""
    return requests.get(
        url,
        headers={"User-Agent": _DNB_LISTING_USER_AGENT},
        timeout=_DNB_LISTING_TIMEOUT,
    )


def _dnb_listing_uid(url: str) -> str:
    """Derive the html_cache uid for a DNB listing URL exactly as legacy's
    `load_or_fetch_ad_html` does on its non-NAV branch (`main/extractors/
    ad_html_loader.py:90-97`): the last run of digits in the URL. Matching
    this exactly keeps the pre-existing `data/dnbeiendom/html_extracted/
    *.html` cache files (named by this same rule) hitting under the new
    pipeline."""
    match = re.search(r"(\d+)(?!.*\d)", url)
    if not match:
        raise ValueError(f"Could not extract UID from URL: {url}")
    return match.group(1)


def _dnb_page_url(search_url: str, page: int) -> str:
    """Set/replace the `page` query param on `search_url`. Direct port of
    `main/extractors/extract_dnbeiendom.py:_set_page`."""
    parsed = urlparse(search_url)
    pairs = [
        (k, v) for k, v in parse_qsl(parsed.query, keep_blank_values=True) if k != "page"
    ]
    pairs.append(("page", str(page)))
    new_query = urlencode(pairs, doseq=True)
    return urlunparse(
        (parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment)
    )


def _crawl_dnb_urls(
    domain: DomainConfig,
    fetch,
    max_pages: int,
    page_delay: Callable[[], None] | None,
) -> list[str]:
    """Paginate the DNB search results, collecting canonical listing URLs
    across pages. Ports the pagination loop of
    `extract_dnbeiendom.fetch_urls_from_search`: every page (including the
    first) carries an explicit `page=N` param, and crawling stops after two
    consecutive empty pages or `max_pages`, whichever comes first."""
    search_url = dnb_crawl.build_search_url(domain)
    all_urls: set[str] = set()
    consecutive_empty = 0

    for page in range(1, max_pages + 1):
        page_url = _dnb_page_url(search_url, page)
        response = fetch(page_url)
        response.raise_for_status()

        page_urls = set(dnb_crawl.extract_listing_urls(response.text))
        all_urls.update(page_urls)

        if page_urls:
            consecutive_empty = 0
        else:
            consecutive_empty += 1
        if consecutive_empty >= 2:
            break

        if page_delay is not None:
            page_delay()

    return sorted(all_urls)


def run_dnb_ingest(
    domain: DomainConfig,
    conn: sqlite3.Connection,
    project_dir: Path = Path("data/dnbeiendom"),
    fetch=None,
    max_pages: int | None = None,
    page_delay: Callable[[], None] | None = None,
    fetch_delay: Callable[[], None] | None = None,
    post_fetch_delay: Callable[[], None] | None = None,
    skip_crawl_urls: list[str] | None = None,
) -> dict:
    """Run the full DNB Eiendom ingest pipeline once: crawl search-result
    pages for listing urls, fetch+parse each listing's JSON-LD, polygon-
    filter + FINN-match, upsert into `dnbeiendom`, then deactivate urls no
    longer seen in this crawl.

    `skip_crawl_urls`, when given, bypasses the search-page crawl entirely
    (mirrors `run_finn_ingest`'s `skip_crawl_urls`).

    Listing pages are routed through `html_cache.load_or_fetch` under
    `project_dir` (default `data/dnbeiendom`, legacy's own output folder --
    see `extract_dnbeiendom_ads.py`'s `--output-folder` default), so a
    cache hit costs no fetch. `fetch`, when left at its default (`None`),
    resolves to a plain `requests.get` for the search-page crawl and to
    `_default_dnb_listing_fetch` (legacy's UA + 15s timeout) for listing
    fetches; an explicit `fetch` override is used as-is for both. Every
    network (non-cached) listing fetch is followed by `post_fetch_delay`
    (default: `random.uniform(200, 800) / 1000` seconds, legacy's pacing) --
    cache hits never pace.

    Returns counts: `crawled`, `parsed`, `failed`, `upserted`, `deactivated`.
    """
    project_dir = Path(project_dir)
    crawl_fetch = fetch if fetch is not None else requests.get
    listing_fetch = fetch if fetch is not None else _default_dnb_listing_fetch

    if skip_crawl_urls is not None:
        urls = skip_crawl_urls
    else:
        urls = _crawl_dnb_urls(
            domain, crawl_fetch, max_pages or domain.dnb.max_pages, page_delay
        )

    crawled = len(urls)
    parsed = 0
    failed = 0
    rows = []
    for url in urls:
        try:
            uid = _dnb_listing_uid(url)
            canonical_path = project_dir / "html_extracted" / f"{uid}.html"
            was_cached = canonical_path.exists()

            html = html_cache.load_or_fetch(
                url, project_dir, uid, fetch=listing_fetch, fetch_delay=fetch_delay
            )

            if not was_cached:
                if post_fetch_delay is not None:
                    post_fetch_delay()
                else:
                    time.sleep(random.uniform(200, 800) / 1000)

            row = dnb_parse.parse_listing(html, url)
            if row is None:
                failed += 1
                continue
            rows.append(row)
            parsed += 1
        except Exception:
            failed += 1

    matched = dnb_load.filter_and_match(rows, domain, conn)

    repo = DnbRepo(conn)
    upsert_stats = repo.upsert(matched)

    deactivated = 0
    if crawled > 0 and not _failure_rate_too_high(crawled, failed):
        # Deactivate against the FULL crawled url set (not just the
        # polygon-matched subset), matching legacy: listings outside the
        # polygon but still live on DNB must not be prematurely deactivated.
        deactivated = repo.deactivate_missing(urls)

    return {
        "crawled": crawled,
        "parsed": parsed,
        "failed": failed,
        "upserted": upsert_stats["inserted"] + upsert_stats["updated"],
        "deactivated": deactivated,
    }
