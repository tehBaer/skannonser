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

import sqlite3
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
    fetch=requests.get,
    max_pages: int | None = None,
    page_delay: Callable[[], None] | None = None,
    skip_crawl_urls: list[str] | None = None,
) -> dict:
    """Run the full DNB Eiendom ingest pipeline once: crawl search-result
    pages for listing urls, fetch+parse each listing's JSON-LD, polygon-
    filter + FINN-match, upsert into `dnbeiendom`, then deactivate urls no
    longer seen in this crawl.

    `skip_crawl_urls`, when given, bypasses the search-page crawl entirely
    (mirrors `run_finn_ingest`'s `skip_crawl_urls`).

    Returns counts: `crawled`, `parsed`, `failed`, `upserted`, `deactivated`.
    """
    if skip_crawl_urls is not None:
        urls = skip_crawl_urls
    else:
        urls = _crawl_dnb_urls(
            domain, fetch, max_pages or domain.dnb.max_pages, page_delay
        )

    crawled = len(urls)
    parsed = 0
    failed = 0
    rows = []
    for url in urls:
        try:
            response = fetch(url)
            response.raise_for_status()
            row = dnb_parse.parse_listing(response.text, url)
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
